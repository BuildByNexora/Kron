use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::retry::RetryPolicy;
use crate::schedule::Schedule;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimerId(pub String);

impl TimerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TimerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(pub String);

impl RunId {
    pub fn new() -> Self {
        Self(format!("run_{}", Ulid::new().to_string().to_lowercase()))
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Timer specification — immutable intent
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerSpec {
    pub id: TimerId,
    pub schedule: Schedule,
    pub retry: RetryPolicy,
    pub timezone: String, // IANA tz name e.g. "Europe/Rome"
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Run — one execution attempt
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub timer_id: TimerId,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub attempt: u32,
}

impl Run {
    pub fn new(timer_id: TimerId, scheduled_at: DateTime<Utc>) -> Self {
        Self {
            id: RunId::new(),
            timer_id,
            scheduled_at,
            started_at: None,
            finished_at: None,
            attempt: 1,
        }
    }

    pub fn duration_ms(&self) -> Option<u64> {
        match (self.started_at, self.finished_at) {
            (Some(start), Some(end)) => {
                let delta = end - start;
                Some(delta.num_milliseconds().max(0) as u64)
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Timer runtime state — derived from the event log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerState {
    /// Timer is active and has a registered function.
    Scheduled,
    /// A run is currently executing.
    Running,
    /// A run failed and is waiting to retry.
    Retrying,
    /// A run exhausted all retry attempts.
    Dead,
    /// Timer exists in the log but no function was re-registered after restart.
    Orphaned,
    /// Timer was explicitly paused by the user.
    Paused,
    /// Timer was cancelled and will not fire again.
    Cancelled,
}

// ---------------------------------------------------------------------------
// Summary — what kron job status shows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerSummary {
    pub id: TimerId,
    pub state: TimerState,
    pub fn_name: Option<String>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_duration_ms: Option<u64>,
    pub last_status: Option<String>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub retries_last_7d: u32,
}
