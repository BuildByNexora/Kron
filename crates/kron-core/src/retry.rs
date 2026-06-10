use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts (1 = no retry).
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::Exponential {
                base_seconds: 30,
                max_seconds: 3600,
            },
        }
    }
}

impl RetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            backoff: BackoffStrategy::Fixed { seconds: 0 },
        }
    }

    /// Compute the absolute UTC time for the next retry attempt.
    /// `attempt` is 1-based: attempt=1 is the first retry after first failure.
    /// Returns `None` if `attempt >= max_attempts` (run is dead).
    pub fn next_retry_at(&self, from: DateTime<Utc>, attempt: u32) -> Option<DateTime<Utc>> {
        if attempt >= self.max_attempts {
            return None;
        }
        let delay = self.backoff.delay_seconds(attempt);
        Some(from + Duration::seconds(delay as i64))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackoffStrategy {
    /// Always wait the same number of seconds.
    Fixed { seconds: u64 },
    /// Wait `base * 2^(attempt-1)`, capped at `max_seconds`.
    Exponential { base_seconds: u64, max_seconds: u64 },
    /// Wait `base + step * (attempt-1)`, capped at `max_seconds`.
    Linear {
        base_seconds: u64,
        step_seconds: u64,
        max_seconds: u64,
    },
}

impl BackoffStrategy {
    /// `attempt` is 1-based: 1 = first retry.
    pub fn delay_seconds(&self, attempt: u32) -> u64 {
        match self {
            BackoffStrategy::Fixed { seconds } => *seconds,

            BackoffStrategy::Exponential {
                base_seconds,
                max_seconds,
            } => {
                let exp = 2u64.saturating_pow(attempt.saturating_sub(1));
                let delay = base_seconds.saturating_mul(exp);
                delay.min(*max_seconds)
            }

            BackoffStrategy::Linear {
                base_seconds,
                step_seconds,
                max_seconds,
            } => {
                let delay =
                    base_seconds.saturating_add(step_seconds.saturating_mul((attempt - 1) as u64));
                delay.min(*max_seconds)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff() {
        let b = BackoffStrategy::Exponential {
            base_seconds: 30,
            max_seconds: 3600,
        };
        assert_eq!(b.delay_seconds(1), 30); // 30 * 2^0
        assert_eq!(b.delay_seconds(2), 60); // 30 * 2^1
        assert_eq!(b.delay_seconds(3), 120); // 30 * 2^2
        assert_eq!(b.delay_seconds(4), 240); // 30 * 2^3
    }

    #[test]
    fn exponential_caps_at_max() {
        let b = BackoffStrategy::Exponential {
            base_seconds: 30,
            max_seconds: 100,
        };
        assert_eq!(b.delay_seconds(4), 100); // 240 capped to 100
    }

    #[test]
    fn no_retry_returns_none() {
        let policy = RetryPolicy::no_retry();
        let now = Utc::now();
        assert!(policy.next_retry_at(now, 1).is_none());
    }

    #[test]
    fn retry_exhausted_returns_none() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };
        let now = Utc::now();
        assert!(policy.next_retry_at(now, 3).is_none()); // attempt == max
        assert!(policy.next_retry_at(now, 4).is_none()); // attempt > max
    }
}
