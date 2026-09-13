//! Retry / backoff helpers and HTTP status classification.

use reqwest::StatusCode;
use std::time::Duration;

pub const DEFAULT_MAX_RETRIES: u32 = 20;
pub const DEFAULT_INITIAL_DELAY_MS: u64 = 1_000;
pub const DEFAULT_MAX_DELAY_MS: u64 = 30_000;
pub const DEFAULT_MAX_402_RETRIES: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassifiedStatus {
    Success200,
    Partial206,
    RangeNotSatisfiable416,
    Precondition412,
    Payment402,
    RateLimit429,
    Retryable(u16),
    Fatal(u16),
}

pub fn classify_status(status: StatusCode) -> ClassifiedStatus {
    match status.as_u16() {
        200 => ClassifiedStatus::Success200,
        206 => ClassifiedStatus::Partial206,
        402 => ClassifiedStatus::Payment402,
        412 => ClassifiedStatus::Precondition412,
        416 => ClassifiedStatus::RangeNotSatisfiable416,
        429 => ClassifiedStatus::RateLimit429,
        408 | 425 => ClassifiedStatus::Retryable(status.as_u16()),
        401 | 403 | 404 | 405 | 410 | 451 => ClassifiedStatus::Fatal(status.as_u16()),
        code if (500..600).contains(&code) => ClassifiedStatus::Retryable(code),
        code => {
            if status.is_success() {
                ClassifiedStatus::Fatal(code)
            } else if status.is_server_error() {
                ClassifiedStatus::Retryable(code)
            } else {
                ClassifiedStatus::Fatal(code)
            }
        }
    }
}

/// Exponential backoff: `initial * 2^attempt`, capped at `max_ms`, plus jitter.
///
/// `attempt` is 0-based (first retry uses `initial_ms`).
pub fn backoff_delay(attempt: u32, initial_ms: u64, max_ms: u64, jitter_seed: u32) -> Duration {
    let initial_ms = initial_ms.max(1);
    let max_ms = max_ms.max(initial_ms);
    let shift = attempt.min(16);
    let exp = initial_ms.saturating_mul(1u64 << shift);
    let capped = exp.min(max_ms);
    let jitter_span = (capped / 5).max(1);
    let jitter_ms = u64::from(jitter_seed) % (jitter_span + 1);
    Duration::from_millis(capped.saturating_add(jitter_ms))
}

pub fn jitter_seed() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0)
}

/// Parse `Retry-After` as an integer number of seconds. HTTP-dates are ignored.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    let secs: u64 = value.parse().ok()?;
    Some(Duration::from_secs(secs.min(300)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_then_caps() {
        let d0 = backoff_delay(0, 1_000, 30_000, 0);
        let d1 = backoff_delay(1, 1_000, 30_000, 0);
        let d2 = backoff_delay(2, 1_000, 30_000, 0);
        let d5 = backoff_delay(5, 1_000, 30_000, 0);
        let d10 = backoff_delay(10, 1_000, 30_000, 0);
        assert_eq!(d0, Duration::from_millis(1_000));
        assert_eq!(d1, Duration::from_millis(2_000));
        assert_eq!(d2, Duration::from_millis(4_000));
        assert_eq!(d5, Duration::from_millis(30_000));
        assert_eq!(d10, Duration::from_millis(30_000));
    }

    #[test]
    fn jitter_is_bounded() {
        for seed in [0u32, 1, 42, u32::MAX] {
            let d = backoff_delay(0, 1_000, 30_000, seed);
            assert!(d >= Duration::from_millis(1_000));
            assert!(d <= Duration::from_millis(1_000 + 1_000 / 5 + 1));
        }
    }

    #[test]
    fn retry_after_seconds() {
        assert_eq!(parse_retry_after("5"), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after(" 0 "), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after("9999"), Some(Duration::from_secs(300)));
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after(""), None);
    }

    #[test]
    fn status_classification() {
        assert_eq!(
            classify_status(StatusCode::OK),
            ClassifiedStatus::Success200
        );
        assert_eq!(
            classify_status(StatusCode::PARTIAL_CONTENT),
            ClassifiedStatus::Partial206
        );
        assert_eq!(
            classify_status(StatusCode::PAYMENT_REQUIRED),
            ClassifiedStatus::Payment402
        );
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS),
            ClassifiedStatus::RateLimit429
        );
        assert_eq!(
            classify_status(StatusCode::RANGE_NOT_SATISFIABLE),
            ClassifiedStatus::RangeNotSatisfiable416
        );
        assert_eq!(
            classify_status(StatusCode::PRECONDITION_FAILED),
            ClassifiedStatus::Precondition412
        );
        assert_eq!(
            classify_status(StatusCode::INTERNAL_SERVER_ERROR),
            ClassifiedStatus::Retryable(500)
        );
        assert_eq!(
            classify_status(StatusCode::NOT_FOUND),
            ClassifiedStatus::Fatal(404)
        );
        assert_eq!(
            classify_status(StatusCode::UNAUTHORIZED),
            ClassifiedStatus::Fatal(401)
        );
    }
}
