pub mod idc;
pub mod social;

/// OAuth 授权码或设备授权已失效、被拒绝，客户端需要重新发起登录。
#[derive(Debug)]
pub(crate) struct OAuthGrantInvalidError {
    pub message: &'static str,
}

impl std::fmt::Display for OAuthGrantInvalidError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for OAuthGrantInvalidError {}

/// 仅按 HTTP 400 和标准 OAuth 错误码识别 `invalid_grant`，不依赖错误描述。
pub(crate) fn is_invalid_grant_response(status: reqwest::StatusCode, body: &str) -> bool {
    if status != reqwest::StatusCode::BAD_REQUEST {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .is_some_and(|json| {
            json.get("error").and_then(|error| error.as_str()) == Some("invalid_grant")
        })
}
