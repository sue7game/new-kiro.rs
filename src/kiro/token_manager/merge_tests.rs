use super::*;

fn credential(key: &str, group: &str) -> KiroCredentials {
    KiroCredentials {
        auth_method: Some("api_key".to_string()),
        kiro_api_key: Some(key.to_string()),
        groups: vec![group.to_string()],
        ..Default::default()
    }
}

#[tokio::test]
async fn failover_reserves_account_rpm_without_changing_primary_selection() {
    let mut config = Config::default();
    config.account_rpm_limit_enabled = true;
    config.account_rpm_limit = 1;
    let manager = MultiTokenManager::new(
        config,
        vec![credential("first", "A"), credential("second", "A")],
        None,
        None,
        false,
    )
    .unwrap();
    let first = manager.acquire_context(None, Some("A")).await.unwrap();
    assert_eq!(first.id, 1);
    let excluded = HashSet::from([first.id]);
    let (retry, _) = manager
        .acquire_context_routed_excluding(None, Some("A"), Some("s"), &excluded)
        .await
        .unwrap();
    assert_eq!(retry.id, 2);
    assert_eq!(manager.snapshot().current_id, 1);
    let error = manager
        .acquire_context(None, Some("A"))
        .await
        .err()
        .unwrap();
    assert!(error.downcast_ref::<UpstreamRateLimitError>().is_some());
    assert_eq!(manager.available_count(), 2);
}

#[tokio::test]
async fn plain_failover_reserves_account_rpm() {
    let mut config = Config::default();
    config.account_rpm_limit_enabled = true;
    config.account_rpm_limit = 1;
    let manager = MultiTokenManager::new(
        config,
        vec![credential("first", "A"), credential("second", "A")],
        None,
        None,
        false,
    )
    .unwrap();
    let excluded = HashSet::from([1]);
    let retry = manager
        .acquire_context_excluding(None, Some("A"), &excluded)
        .await
        .unwrap();
    assert_eq!(retry.id, 2);
    assert_eq!(manager.available_count_for_request(None, Some("A")), 1);
    assert_eq!(manager.snapshot().current_id, 1);
}

#[test]
fn upstream_runtime_settings_preserve_custom_retry_config() {
    let path = std::env::temp_dir().join(format!("kiro-merge-{}.json", uuid::Uuid::new_v4()));
    std::fs::write(&path, "{}").unwrap();
    let config = Config::load(&path).unwrap();
    let manager = MultiTokenManager::new(config, Vec::new(), None, None, false).unwrap();
    manager
        .set_load_balancing_config(Some("round-robin".into()), Some(2), Some(3))
        .unwrap();
    manager.set_api_domain(ApiDomain::Kiro).unwrap();
    manager.set_model_api_429_retry_enabled(false).unwrap();
    manager.set_quota_reset_recovery_enabled(true).unwrap();
    let persisted = Config::load(&path).unwrap();
    assert_eq!(persisted.load_balancing_mode, "round-robin");
    assert_eq!(persisted.request_retry, 2);
    assert_eq!(persisted.max_retry_credentials, 3);
    assert_eq!(persisted.api_domain, ApiDomain::Kiro);
    assert!(!persisted.model_api_429_retry_enabled);
    assert!(persisted.quota_reset_recovery_enabled);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn excluded_credential_cannot_mask_remaining_account_rpm_exhaustion() {
    let mut config = Config::default();
    config.account_rpm_limit_enabled = true;
    config.account_rpm_limit = 1;
    let manager = MultiTokenManager::new(
        config,
        vec![credential("first", "A"), credential("second", "A")],
        None,
        None,
        false,
    )
    .unwrap();
    assert!(manager.record_request(2));
    let excluded = HashSet::from([1]);
    for routed in [false, true] {
        let result = if routed {
            manager
                .acquire_context_routed_excluding(None, Some("A"), Some("s"), &excluded)
                .await
                .map(|(context, _)| context)
        } else {
            manager
                .acquire_context_excluding(None, Some("A"), &excluded)
                .await
        };
        let error = result.err().expect("剩余账号的 RPM 已耗尽");
        let rate_limit = error
            .downcast_ref::<UpstreamRateLimitError>()
            .expect("已排除账号的可用额度不应掩盖当前请求的限流");
        let seconds: u64 = rate_limit.retry_after().unwrap().parse().unwrap();
        assert!((1..=RPM_WINDOW_SECS).contains(&seconds));
    }
    assert_eq!(manager.available_count(), 2);
}

#[test]
fn excluded_accounts_do_not_create_a_spurious_rpm_retry_delay() {
    let mut config = Config::default();
    config.account_rpm_limit_enabled = true;
    config.account_rpm_limit = 1;
    let manager =
        MultiTokenManager::new(config, vec![credential("first", "A")], None, None, false).unwrap();
    assert!(manager.record_request(1));
    assert_eq!(
        manager.rpm_retry_after_secs(
            &manager.entries.lock(),
            None,
            Some("A"),
            &HashSet::from([1]),
            Instant::now(),
        ),
        None,
    );
}

#[tokio::test]
async fn read_only_selection_preserves_rpm_and_scheduler_state_in_all_modes() {
    for mode in ["priority", "balanced", "round-robin"] {
        let mut config = Config::default();
        config.load_balancing_mode = mode.into();
        config.account_rpm_limit_enabled = true;
        config.account_rpm_limit = 1;
        let manager = MultiTokenManager::new(
            config,
            vec![credential("first", "A"), credential("second", "A")],
            None,
            None,
            false,
        )
        .unwrap();
        let current_id = manager.snapshot().current_id;
        let cursors = manager.round_robin_cursors.lock().clone();
        for _ in 0..8 {
            manager
                .acquire_context_impl(None, Some("A"), None, false, false)
                .await
                .unwrap();
        }
        assert_eq!(manager.snapshot().current_id, current_id, "{mode}");
        assert_eq!(*manager.round_robin_cursors.lock(), cursors, "{mode}");
        assert!(manager
            .entries
            .lock()
            .iter()
            .all(|e| e.rpm_window.is_empty()));
    }
}

#[tokio::test]
async fn round_robin_failover_preserves_model_rank_and_reserves_rpm() {
    let mut config = Config::default();
    config.load_balancing_mode = "round-robin".into();
    config.account_rpm_limit_enabled = true;
    config.account_rpm_limit = 1;
    let mut confirmed = credential("confirmed", "A");
    confirmed.priority = 10;
    let manager = MultiTokenManager::new(
        config,
        vec![credential("unknown", "A"), confirmed.clone(), confirmed],
        None,
        None,
        false,
    )
    .unwrap();
    for id in [2, 3] {
        manager.model_cache.lock().insert(
            id,
            ModelCacheEntry {
                response: ListAvailableModelsResponse {
                    models: vec![UpstreamModel {
                        model_id: "test-model".into(),
                        model_name: None,
                        description: None,
                        token_limits: None,
                    }],
                },
                refreshed_at: Instant::now(),
            },
        );
    }
    let first = manager
        .acquire_context(Some("test-model"), Some("A"))
        .await
        .unwrap();
    assert_eq!(
        first.id, 2,
        "已确认模型支持优先于未知缓存，即使其 priority 更低"
    );
    let retry = manager
        .acquire_context_excluding(Some("test-model"), Some("A"), &HashSet::from([2]))
        .await
        .unwrap();
    assert_eq!(retry.id, 3);
    assert_eq!(manager.snapshot().current_id, 2);
    let entries = manager.entries.lock();
    assert!(entries[0].rpm_window.is_empty());
    assert_eq!(entries[1].rpm_window.len(), 1);
    assert_eq!(entries[2].rpm_window.len(), 1);
}
