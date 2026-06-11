use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::timer::{RunId, TimerId, TimerSpec};

/// Every state change in Kron is an Event written to the AOF.
/// Current state is always derived by replaying the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Event {
    // -- Timer lifecycle --
    TimerCreated {
        spec: TimerSpec,
    },
    TimerUpdated {
        spec: TimerSpec,
    },
    TimerPaused {
        timer_id: TimerId,
        at: DateTime<Utc>,
    },
    TimerResumed {
        timer_id: TimerId,
        at: DateTime<Utc>,
    },
    TimerCancelled {
        timer_id: TimerId,
        at: DateTime<Utc>,
    },

    // -- Run lifecycle --
    RunDue {
        timer_id: TimerId,
        run_id: RunId,
        scheduled_at: DateTime<Utc>,
    },
    RunStarted {
        timer_id: TimerId,
        run_id: RunId,
        started_at: DateTime<Utc>,
        attempt: u32,
    },
    RunSkippedOverlap {
        timer_id: TimerId,
        run_id: RunId,
        scheduled_at: DateTime<Utc>,
        skipped_at: DateTime<Utc>,
    },
    RunSucceeded {
        timer_id: TimerId,
        run_id: RunId,
        finished_at: DateTime<Utc>,
        duration_ms: u64,
    },
    RunFailed {
        timer_id: TimerId,
        run_id: RunId,
        finished_at: DateTime<Utc>,
        error: String,
        attempt: u32,
    },
    RunRetrying {
        timer_id: TimerId,
        run_id: RunId,
        attempt: u32,
        next_retry_at: DateTime<Utc>,
    },
    RunDead {
        timer_id: TimerId,
        run_id: RunId,
        at: DateTime<Utc>,
    },
}

/// Envelope written to each AOF line.
/// Wrapping in a struct lets us add schema versioning later without
/// breaking the log format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub v: u8, // schema version, always 1 for now
    pub ts: DateTime<Utc>,
    pub event: Event,
}

impl LogEntry {
    pub fn new(event: Event) -> Self {
        Self {
            v: 1,
            ts: Utc::now(),
            event,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::RetryPolicy;
    use crate::schedule::Schedule;
    use chrono::Utc;

    #[test]
    fn event_json_roundtrip() {
        let event = Event::TimerCreated {
            spec: TimerSpec {
                id: TimerId::new("roundtrip"),
                schedule: Schedule::Every { seconds: 60 },
                retry: RetryPolicy::no_retry(),
                timezone: "UTC".to_string(),
                created_at: Utc::now(),
                overlap: Default::default(),
            },
        };
        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: Event = serde_json::from_str(&encoded).unwrap();
        assert!(matches!(decoded, Event::TimerCreated { .. }));
    }
}
