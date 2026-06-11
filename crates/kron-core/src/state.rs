use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::event::{Event, LogEntry};
use crate::timer::{RunId, TimerId, TimerSpec, TimerState, TimerSummary};

/// All mutable state that the engine holds in memory.
/// This is always reconstructable by replaying `kron.aof` from the top.
#[derive(Debug, Default)]
pub struct EngineState {
    /// Current spec for each timer.
    pub specs: HashMap<TimerId, TimerSpec>,
    /// Derived lifecycle state.
    pub states: HashMap<TimerId, TimerState>,
    /// Name of the registered Python/host function (populated at runtime).
    pub fn_names: HashMap<TimerId, String>,
    /// Most recent run per timer (for status display).
    pub last_runs: HashMap<TimerId, LastRunInfo>,
    /// Next scheduled fire time per timer (recomputed after each run).
    pub next_runs: HashMap<TimerId, DateTime<Utc>>,
    /// Retry count in the last 7 days per timer.
    pub retries_7d: HashMap<TimerId, u32>,
    /// Active retry: timer → (run_id, next_retry_at, attempt)
    pub pending_retries: HashMap<TimerId, (RunId, DateTime<Utc>, u32)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LastRunInfo {
    pub run_id: RunId,
    pub finished_at: DateTime<Utc>,
    pub duration_ms: Option<u64>,
    pub status: String,
}

impl EngineState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replay a slice of log entries to reconstruct state.
    pub fn replay(&mut self, entries: &[LogEntry]) {
        for entry in entries {
            self.apply(&entry.event, entry.ts);
        }
    }

    /// Apply a single event.  This is the state-transition function.
    pub fn apply(&mut self, event: &Event, _ts: DateTime<Utc>) {
        match event {
            Event::TimerCreated { spec } => {
                self.states.insert(spec.id.clone(), TimerState::Orphaned);
                self.specs.insert(spec.id.clone(), spec.clone());
            }

            Event::TimerUpdated { spec } => {
                self.specs.insert(spec.id.clone(), spec.clone());
            }

            Event::TimerPaused { timer_id, .. } => {
                self.states.insert(timer_id.clone(), TimerState::Paused);
            }

            Event::TimerResumed { timer_id, .. } => {
                // Transition back to Scheduled only if function is registered.
                let has_fn = self.fn_names.contains_key(timer_id);
                let next_state = if has_fn {
                    TimerState::Scheduled
                } else {
                    TimerState::Orphaned
                };
                self.states.insert(timer_id.clone(), next_state);
            }

            Event::TimerCancelled { timer_id, .. } => {
                self.states.insert(timer_id.clone(), TimerState::Cancelled);
            }

            Event::RunDue { timer_id, .. } => {
                self.states.insert(timer_id.clone(), TimerState::Running);
            }

            Event::RunStarted { timer_id, .. } => {
                self.states.insert(timer_id.clone(), TimerState::Running);
            }

            Event::RunSkippedOverlap {
                timer_id,
                run_id,
                skipped_at,
                ..
            } => {
                self.last_runs.insert(
                    timer_id.clone(),
                    LastRunInfo {
                        run_id: run_id.clone(),
                        finished_at: *skipped_at,
                        duration_ms: None,
                        status: "SKIPPED_OVERLAP".to_string(),
                    },
                );
            }

            Event::RunSucceeded {
                timer_id,
                run_id,
                finished_at,
                duration_ms,
            } => {
                self.states.insert(timer_id.clone(), TimerState::Scheduled);
                self.last_runs.insert(
                    timer_id.clone(),
                    LastRunInfo {
                        run_id: run_id.clone(),
                        finished_at: *finished_at,
                        duration_ms: Some(*duration_ms),
                        status: "OK".to_string(),
                    },
                );
                self.pending_retries.remove(timer_id);
            }

            Event::RunFailed {
                timer_id,
                run_id,
                finished_at,
                error,
                attempt: _,
            } => {
                self.last_runs.insert(
                    timer_id.clone(),
                    LastRunInfo {
                        run_id: run_id.clone(),
                        finished_at: *finished_at,
                        duration_ms: None,
                        status: format!("FAILED: {}", error),
                    },
                );
                // State transitions to Retrying or Dead via RunRetrying / RunDead
            }

            Event::RunRetrying {
                timer_id,
                run_id,
                attempt,
                next_retry_at,
            } => {
                self.states.insert(timer_id.clone(), TimerState::Retrying);
                self.pending_retries
                    .insert(timer_id.clone(), (run_id.clone(), *next_retry_at, *attempt));
                *self.retries_7d.entry(timer_id.clone()).or_insert(0) += 1;
            }

            Event::RunDead {
                timer_id,
                run_id,
                at,
            } => {
                self.states.insert(timer_id.clone(), TimerState::Dead);
                self.last_runs.insert(
                    timer_id.clone(),
                    LastRunInfo {
                        run_id: run_id.clone(),
                        finished_at: *at,
                        duration_ms: None,
                        status: "DEAD".to_string(),
                    },
                );
                self.pending_retries.remove(timer_id);
            }
        }
    }

    /// Build a human-readable summary of one timer (for `kron job status`).
    pub fn summary(&self, id: &TimerId) -> Option<TimerSummary> {
        let state = self.states.get(id)?.clone();
        let last = self.last_runs.get(id);
        let next_run_at = self.next_runs.get(id).copied();

        Some(TimerSummary {
            id: id.clone(),
            state,
            fn_name: self.fn_names.get(id).cloned(),
            last_run_at: last.map(|r| r.finished_at),
            last_duration_ms: last.and_then(|r| r.duration_ms),
            last_status: last.map(|r| r.status.clone()),
            next_run_at,
            retries_last_7d: *self.retries_7d.get(id).unwrap_or(&0),
        })
    }

    pub fn register_function(&mut self, timer_id: TimerId, function_name: String) {
        self.fn_names.insert(timer_id.clone(), function_name);
        let current = self
            .states
            .get(&timer_id)
            .cloned()
            .unwrap_or(TimerState::Orphaned);
        if matches!(current, TimerState::Orphaned) {
            self.states.insert(timer_id, TimerState::Scheduled);
        }
    }

    pub fn mark_function_missing(&mut self, timer_id: &TimerId) {
        self.fn_names.remove(timer_id);
        let current = self
            .states
            .get(timer_id)
            .cloned()
            .unwrap_or(TimerState::Orphaned);
        if matches!(
            current,
            TimerState::Scheduled | TimerState::Retrying | TimerState::Running
        ) {
            self.states.insert(timer_id.clone(), TimerState::Orphaned);
        }
    }

    /// Expire retry counters older than 7 days.
    /// Call periodically — e.g. once per hour in the engine loop.
    pub fn prune_old_retries(&mut self, _now: DateTime<Utc>) {
        // v0: retries_7d is approximated by a simple counter reset daily.
        // A future version can store (run_id, timestamp) pairs and filter.
    }
}
