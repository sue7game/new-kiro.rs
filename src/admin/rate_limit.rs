//! 客户端 Key 维度的单机 RPM 限流器。
//!
//! 本模块只维护进程内滑动窗口，不处理持久化和鉴权。当前部署为单实例时可精确限制
//! 每把客户端 Key 在最近 60 秒内允许通过的请求数。

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

const DEFAULT_WINDOW: Duration = Duration::from_secs(60);

/// 一次限流检查的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDecision {
    /// 请求未超过当前 Key 的 RPM 上限。
    Allowed,
    /// 请求已超过上限，调用方应返回 429 并附带 Retry-After。
    Limited { retry_after_secs: u64 },
}

/// 单机滑动窗口 RPM 限流器。
pub struct RateLimiter {
    window: Duration,
    inner: Mutex<HashMap<u64, VecDeque<Instant>>>,
}

impl RateLimiter {
    /// 创建默认 60 秒窗口的 RPM 限流器。
    pub fn new() -> Self {
        Self::with_window(DEFAULT_WINDOW)
    }

    /// 创建指定窗口长度的限流器，主要用于测试。
    pub fn with_window(window: Duration) -> Self {
        Self {
            window,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 检查指定 Key 是否仍可通过本次请求。
    pub fn check(&self, key_id: u64, rpm_limit: Option<u32>) -> RateLimitDecision {
        self.check_at(key_id, rpm_limit, Instant::now())
    }

    fn check_at(&self, key_id: u64, rpm_limit: Option<u32>, now: Instant) -> RateLimitDecision {
        let Some(limit) = rpm_limit.filter(|limit| *limit > 0) else {
            return RateLimitDecision::Allowed;
        };

        let mut inner = self.inner.lock();
        let window = inner.entry(key_id).or_default();
        while let Some(&oldest) = window.front() {
            if now.saturating_duration_since(oldest) < self.window {
                break;
            }
            window.pop_front();
        }

        if window.len() < limit as usize {
            window.push_back(now);
            return RateLimitDecision::Allowed;
        }

        let retry_after = window
            .front()
            .map(|oldest| {
                self.window
                    .saturating_sub(now.saturating_duration_since(*oldest))
            })
            .unwrap_or(self.window);

        RateLimitDecision::Limited {
            retry_after_secs: ceil_secs(retry_after),
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

fn ceil_secs(duration: Duration) -> u64 {
    let secs = duration.as_secs();
    if duration.subsec_nanos() == 0 {
        secs.max(1)
    } else {
        secs.saturating_add(1).max(1)
    }
}

/// 共享限流器句柄。
pub type SharedRateLimiter = Arc<RateLimiter>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_or_zero_limit_allows() {
        let limiter = RateLimiter::new();
        let now = Instant::now();

        assert_eq!(limiter.check_at(1, None, now), RateLimitDecision::Allowed);
        assert_eq!(
            limiter.check_at(1, Some(0), now),
            RateLimitDecision::Allowed
        );
    }

    #[test]
    fn allows_requests_under_limit() {
        let limiter = RateLimiter::with_window(Duration::from_secs(60));
        let now = Instant::now();

        assert_eq!(
            limiter.check_at(1, Some(2), now),
            RateLimitDecision::Allowed
        );
        assert_eq!(
            limiter.check_at(1, Some(2), now + Duration::from_secs(1)),
            RateLimitDecision::Allowed
        );
    }

    #[test]
    fn blocks_when_limit_exceeded() {
        let limiter = RateLimiter::with_window(Duration::from_secs(60));
        let now = Instant::now();

        assert_eq!(
            limiter.check_at(1, Some(2), now),
            RateLimitDecision::Allowed
        );
        assert_eq!(
            limiter.check_at(1, Some(2), now + Duration::from_secs(1)),
            RateLimitDecision::Allowed
        );
        assert_eq!(
            limiter.check_at(1, Some(2), now + Duration::from_secs(2)),
            RateLimitDecision::Limited {
                retry_after_secs: 58,
            }
        );
    }

    #[test]
    fn allows_after_window_expires() {
        let limiter = RateLimiter::with_window(Duration::from_secs(60));
        let now = Instant::now();

        assert_eq!(
            limiter.check_at(1, Some(1), now),
            RateLimitDecision::Allowed
        );
        assert!(matches!(
            limiter.check_at(1, Some(1), now + Duration::from_secs(59)),
            RateLimitDecision::Limited { .. }
        ));
        assert_eq!(
            limiter.check_at(1, Some(1), now + Duration::from_secs(60)),
            RateLimitDecision::Allowed
        );
    }
}
