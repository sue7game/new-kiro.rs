//! 上游合并后的调度、限流、粘性和超时回归。

use super::merge_test_support::*;
use super::*;

const CAPACITY: &str = r#"{"reason":"INSUFFICIENT_MODEL_CAPACITY"}"#;
const ACCOUNT_REASONS: [&str; 4] = [
    r#"{"reason":"USER_REQUEST_RATE_EXCEEDED"}"#,
    r#"{"reason":"CREDIT_CONSUMPTION_RATE_EXCEEDED"}"#,
    r#"{"reason":"SERVICE_REQUEST_RATE_EXCEEDED"}"#,
    r#"{"message":"Suspicious activity detected. Temporary limits applied."}"#,
];

#[tokio::test]
async fn ordinary_429_switches_within_group_and_binds_only_success() {
    for stream in [false, true] {
        let fixture =
            Fixture::new(config(), &["A", "A", "B"], vec![Reply::new(429, CAPACITY)]).await;
        assert_eq!(fixture.call(stream, "A").await.unwrap().credential_id, 2);
        assert_eq!(fixture.call(stream, "B").await.unwrap().credential_id, 3);
        assert_eq!(fixture.call(stream, "A").await.unwrap().credential_id, 2);
        assert_eq!(fixture.attempts(), [1, 2, 3, 2]);
        let first = &fixture.provider.token_manager.snapshot().entries[0];
        assert!(!first.disabled);
        assert_eq!(first.failure_count, 0);
        assert_eq!(first.total_failure_count, 0);
        assert_eq!(first.throttled_remaining_secs, None);
    }
}

#[tokio::test]
async fn ordinary_429_switch_and_retry_after_can_stop_retries() {
    for enabled in [false, true] {
        let mut settings = config();
        settings.model_api_429_retry_enabled = enabled;
        let mut reply = Reply::new(429, CAPACITY);
        reply.retry_after = enabled.then_some("90");
        let fixture = Fixture::new(settings, &["A", "A"], vec![reply]).await;
        assert_rate_limit(fixture.call(false, "A").await, enabled.then_some("90"));
        assert_eq!(fixture.attempts(), [1]);
    }
}

#[tokio::test]
async fn request_retry_adds_rounds_and_max_credentials_caps_each_round() {
    let mut settings = config();
    settings.request_retry = 1;
    settings.max_retry_credentials = 2;
    let fixture = Fixture::new(
        settings,
        &["A", "A", "A", "B"],
        vec![Reply::new(429, CAPACITY); 6],
    )
    .await;
    assert_rate_limit(fixture.call(false, "A").await, None);
    assert_eq!(fixture.attempts(), [1, 2, 1, 2]);
}

#[tokio::test]
async fn zero_max_credentials_tries_whole_group_beyond_legacy_limit() {
    let fixture = Fixture::new(
        config(),
        &["A", "A", "A", "A", "A", "B"],
        vec![Reply::new(429, CAPACITY); 6],
    )
    .await;
    assert_rate_limit(fixture.call(false, "A").await, None);
    assert_eq!(fixture.attempts(), [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn account_429_uses_failover_switch_independently_of_model_switch() {
    for reason in ACCOUNT_REASONS {
        for failover in [false, true] {
            let mut settings = config();
            settings.account_throttle_failover = failover;
            settings.model_api_429_retry_enabled = !failover;
            let mut reply = Reply::new(429, reason);
            reply.retry_after = Some("90");
            let fixture = Fixture::new(settings, &["A", "A", "B"], vec![reply]).await;
            let result = fixture.call(false, "A").await;
            if failover {
                assert_eq!(result.unwrap().credential_id, 2);
                assert_eq!(fixture.attempts(), [1, 2]);
            } else {
                assert_rate_limit(result, Some("90"));
                assert_eq!(fixture.attempts(), [1]);
            }
            let first = &fixture.provider.token_manager.snapshot().entries[0];
            assert_eq!(first.throttled_remaining_secs.is_some(), failover);
            assert!(!first.disabled);
            assert_eq!(first.failure_count, 0);
        }
    }
}

#[tokio::test]
async fn account_429_respects_credential_budget_and_returns_wait_time() {
    for retry_after in [None, Some("90")] {
        let mut settings = config();
        settings.max_retry_credentials = 1;
        settings.account_throttle_cooldown_secs = 45;
        let mut reply = Reply::new(429, ACCOUNT_REASONS[0]);
        reply.retry_after = retry_after;
        let fixture = Fixture::new(settings, &["A", "A", "B"], vec![reply]).await;
        assert_rate_limit(
            fixture.call(false, "A").await,
            Some(retry_after.unwrap_or("45")),
        );
        assert_eq!(fixture.attempts(), [1]);
    }
}

#[tokio::test]
async fn exhausted_account_group_never_uses_other_groups() {
    let mut settings = config();
    settings.account_throttle_cooldown_secs = 45;
    let fixture = Fixture::new(
        settings,
        &["A", "B"],
        vec![Reply::new(429, ACCOUNT_REASONS[0])],
    )
    .await;
    assert_rate_limit(fixture.call(false, "A").await, Some("45"));
    assert_eq!(fixture.attempts(), [1]);
}

#[tokio::test]
async fn mcp_preserves_group_retry_budget_without_model_429_switch() {
    let mut settings = config();
    settings.model_api_429_retry_enabled = false;
    settings.max_retry_credentials = 2;
    let fixture = Fixture::new(
        settings,
        &["A", "A", "A", "B"],
        vec![Reply::new(429, CAPACITY); 3],
    )
    .await;
    assert_rate_limit(fixture.provider.call_mcp("{}", Some("A")).await, None);
    assert_eq!(fixture.attempts(), [1, 2]);
}

#[tokio::test]
async fn mcp_account_429_obeys_failover_switch() {
    for failover in [false, true] {
        let mut settings = config();
        settings.account_throttle_failover = failover;
        let fixture = Fixture::new(
            settings,
            &["A", "A", "B"],
            vec![Reply::new(429, ACCOUNT_REASONS[1])],
        )
        .await;
        let result = fixture.provider.call_mcp("{}", Some("A")).await;
        if failover {
            assert!(result.unwrap().status().is_success());
            assert_eq!(fixture.attempts(), [1, 2]);
        } else {
            assert_rate_limit(result, None);
            assert_eq!(fixture.attempts(), [1]);
        }
    }
}

#[tokio::test]
async fn round_robin_rotates_even_after_successful_session_binding() {
    let mut settings = config();
    settings.load_balancing_mode = "round-robin".to_string();
    let fixture = Fixture::new(settings, &["A", "A", "B"], Vec::new()).await;
    for expected in [1, 2, 1] {
        assert_eq!(
            fixture.call(false, "A").await.unwrap().credential_id,
            expected
        );
    }
}

#[tokio::test]
async fn retry_reserves_rpm_on_the_new_credential() {
    let mut settings = config();
    settings.account_rpm_limit_enabled = true;
    settings.account_rpm_limit = 1;
    let fixture = Fixture::new(settings, &["A", "A", "B"], vec![Reply::new(429, CAPACITY)]).await;
    assert_eq!(fixture.call(false, "A").await.unwrap().credential_id, 2);
    let error = fixture
        .call(false, "A")
        .await
        .err()
        .expect("组内两个账号均应耗尽 RPM");
    assert!(
        error
            .downcast_ref::<UpstreamRateLimitError>()
            .unwrap()
            .retry_after()
            .is_some()
    );
    assert_eq!(fixture.attempts(), [1, 2]);
}

#[tokio::test]
async fn model_unavailable_does_not_cool_the_model_or_disable_account() {
    let mut reply = Reply::new(503, r#"{"reason":"MODEL_TEMPORARILY_UNAVAILABLE"}"#);
    reply.retry_after = Some("12");
    let fixture = Fixture::new(config(), &["A", "A"], vec![reply]).await;
    let error = fixture
        .call(false, "A")
        .await
        .err()
        .expect("本次请求应失败");
    assert_eq!(
        error
            .downcast_ref::<UpstreamModelUnavailableError>()
            .unwrap()
            .retry_after,
        "12"
    );
    assert_eq!(fixture.call(false, "A").await.unwrap().credential_id, 1);
    assert_eq!(fixture.attempts(), [1, 1]);
}

#[tokio::test]
async fn header_and_error_body_timeouts_return_without_retrying() {
    for slow_body in [false, true] {
        let mut settings = config();
        settings.upstream_read_timeout_secs = 1;
        let mut reply = Reply::new(if slow_body { 503 } else { 200 }, "slow");
        reply.slow_body = slow_body;
        reply.slow_headers = !slow_body;
        let fixture = Fixture::new(settings, &["A", "A"], vec![reply]).await;
        let error = fixture
            .call(false, "A")
            .await
            .err()
            .expect("应返回超时错误");
        assert!(error.downcast_ref::<UpstreamTimeoutError>().is_some());
        assert_eq!(fixture.attempts(), [1]);
        assert_eq!(
            fixture.provider.token_manager.snapshot().entries[0].failure_count,
            0
        );
    }
}
