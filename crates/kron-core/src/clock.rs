use chrono::{DateTime, Utc};
use std::time::Duration;

/// Abstraction over "what time is it now" and "wait until".
/// Swap for a fake clock in tests without touching the engine logic.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// The real wall-clock implementation.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// How long to sleep until the next scheduled event.
/// Returns 100 milliseconds if the heap is empty (idle polling interval).
pub fn sleep_duration_until(next_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    match next_at {
        None => Duration::from_millis(100),
        Some(t) => {
            let delta = (t - now).num_milliseconds();
            if delta <= 0 {
                Duration::from_millis(0)
            } else {
                Duration::from_millis(delta as u64)
            }
        }
    }
}
