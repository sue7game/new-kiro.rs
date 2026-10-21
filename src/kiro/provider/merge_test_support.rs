//! 合并回归的本地服务和合成凭据 fixture；不访问真实配置或上游。

use super::*;
use crate::model::config::Config;
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    response::Response,
    routing::post,
};
use std::collections::VecDeque;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

const REQUEST: &str = r#"{"conversationState":{"conversationId":"shared-session","currentMessage":{"userInputMessage":{"modelId":"claude-sonnet-4"}}}}"#;
#[derive(Clone)]
pub(super) struct Reply {
    pub(super) status: u16,
    pub(super) body: &'static str,
    pub(super) retry_after: Option<&'static str>,
    pub(super) slow_headers: bool,
    pub(super) slow_body: bool,
}

impl Reply {
    /// 构造一条本地 HTTP 响应。
    pub(super) fn new(status: u16, body: &'static str) -> Self {
        Self {
            status,
            body,
            retry_after: None,
            slow_headers: false,
            slow_body: false,
        }
    }
}

#[derive(Default)]
struct Script {
    replies: VecDeque<Reply>,
    attempts: Vec<u64>,
}

async fn respond(State(script): State<Arc<Mutex<Script>>>, Path(id): Path<u64>) -> Response {
    let reply = {
        let mut script = script.lock();
        script.attempts.push(id);
        script
            .replies
            .pop_front()
            .unwrap_or_else(|| Reply::new(200, "ok"))
    };
    if reply.slow_headers {
        sleep(Duration::from_secs(3)).await;
    }
    let mut response = Response::builder().status(reply.status);
    if let Some(value) = reply.retry_after {
        response = response.header("retry-after", value);
    }
    let body = if reply.slow_body {
        Body::from_stream(futures::stream::once(async move {
            sleep(Duration::from_secs(3)).await;
            Ok::<_, std::io::Error>(reply.body)
        }))
    } else {
        Body::from(reply.body)
    };
    response.body(body).unwrap()
}

struct TestEndpoint(String);

impl KiroEndpoint for TestEndpoint {
    fn name(&self) -> &'static str {
        "merge-test"
    }

    fn api_url(&self, ctx: &RequestContext<'_>) -> String {
        format!("{}/{}", self.0, ctx.credentials.id.unwrap())
    }

    fn mcp_url(&self, ctx: &RequestContext<'_>) -> String {
        self.api_url(ctx)
    }

    fn decorate_api(
        &self,
        request: reqwest::RequestBuilder,
        _: &RequestContext<'_>,
    ) -> reqwest::RequestBuilder {
        request
    }

    fn decorate_mcp(
        &self,
        request: reqwest::RequestBuilder,
        _: &RequestContext<'_>,
    ) -> reqwest::RequestBuilder {
        request
    }

    fn transform_api_body(&self, body: &str, _: &RequestContext<'_>) -> String {
        body.to_owned()
    }
}

pub(super) struct Fixture {
    pub(super) provider: KiroProvider,
    script: Arc<Mutex<Script>>,
    server: JoinHandle<()>,
}

impl Fixture {
    /// 启动回环服务，并用内存中的合成凭据创建 Provider。
    pub(super) async fn new(config: Config, groups: &[&str], replies: Vec<Reply>) -> Self {
        let script = Arc::new(Mutex::new(Script {
            replies: replies.into(),
            attempts: Vec::new(),
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/{id}", post(respond))
            .with_state(script.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let credentials = groups
            .iter()
            .enumerate()
            .map(|(index, group)| KiroCredentials {
                id: Some(index as u64 + 1),
                auth_method: Some("api_key".to_string()),
                kiro_api_key: Some(format!("ksk_offline_merge_test_{}", index)),
                groups: vec![(*group).to_string()],
                proxy_url: Some("direct".to_string()),
                ..Default::default()
            })
            .collect();
        let manager =
            Arc::new(MultiTokenManager::new(config, credentials, None, None, false).unwrap());
        let endpoints = HashMap::from([(
            "merge-test".to_string(),
            Arc::new(TestEndpoint(url)) as Arc<dyn KiroEndpoint>,
        )]);
        let provider = KiroProvider::new(manager, endpoints, "merge-test".to_string());
        Self {
            provider,
            script,
            server,
        }
    }

    /// 使用相同会话和模型发起一次指定分组的请求。
    pub(super) async fn call(&self, stream: bool, group: &str) -> anyhow::Result<KiroCallResult> {
        timeout(
            Duration::from_secs(10),
            self.provider
                .call_api_with_retry(REQUEST, stream, None, Some(group)),
        )
        .await
        .expect("回环模型请求超时")
    }

    /// 返回回环服务实际收到的凭据 ID 序列。
    pub(super) fn attempts(&self) -> Vec<u64> {
        self.script.lock().attempts.clone()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// 创建关闭额外轮次和 RPM 限制的确定性 priority 调度配置。
pub(super) fn config() -> Config {
    let mut config = Config::default();
    config.request_retry = 0;
    config.max_retry_credentials = 0;
    config.load_balancing_mode = "priority".to_string();
    config.model_api_429_retry_enabled = true;
    config.account_throttle_failover = true;
    config.account_rpm_limit_enabled = false;
    config.session_affinity_enabled = true;
    config
}

/// 验证类型化 429 及其 Retry-After 值。
pub(super) fn assert_rate_limit<T>(result: anyhow::Result<T>, retry_after: Option<&str>) {
    let error = result.err().expect("应返回限流错误");
    let error = error
        .downcast_ref::<UpstreamRateLimitError>()
        .expect("应保留类型化 429");
    assert_eq!(error.retry_after(), retry_after);
}
