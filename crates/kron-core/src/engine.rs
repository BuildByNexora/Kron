use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use chrono::Utc;
use fs2::FileExt;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

use crate::clock::sleep_duration_until;
use crate::error::KronError;
use crate::event::Event;
use crate::heap::{ScheduledTimer, TimerHeap};
use crate::log::AppendOnlyLog;
use crate::retry::RetryPolicy;
use crate::schedule::Schedule;
use crate::snapshot;
use crate::state::EngineState;
use crate::timer::{RunId, TimerId, TimerSpec, TimerState, TimerSummary};

// ---------------------------------------------------------------------------
// Function registry — maps TimerId → host callable
// ---------------------------------------------------------------------------

/// The callable the engine invokes when a timer fires.
/// In kron-py this wraps a Python function; in tests it's a plain Rust fn.
pub trait TimerFn: Send + Sync + 'static {
    fn call(&self, timer_id: &TimerId, run_id: &RunId) -> Result<(), String>;
    fn name(&self) -> String;
}

/// Convenience wrapper for a plain Rust closure (tests, kron-cli, etc.)
pub struct FnTimer {
    name: String,
    f: Box<TimerClosure>,
}

type TimerClosure = dyn Fn(&TimerId, &RunId) -> Result<(), String> + Send + Sync + 'static;

impl FnTimer {
    pub fn new(
        name: impl Into<String>,
        f: impl Fn(&TimerId, &RunId) -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            f: Box::new(f),
        }
    }
}

impl TimerFn for FnTimer {
    fn call(&self, id: &TimerId, run: &RunId) -> Result<(), String> {
        (self.f)(id, run)
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Internal shared state accessed by the background loop and the public API.
struct Inner {
    state: EngineState,
    heap: TimerHeap,
    log: AppendOnlyLog,
    registry: HashMap<TimerId, Arc<dyn TimerFn>>,
    runtime: RuntimeState,
    notify: Arc<Notify>,
}

struct RuntimeState {
    started: bool,
    stopped: bool,
    shutting_down: bool,
    active_runs: usize,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            started: false,
            stopped: false,
            shutting_down: false,
            active_runs: 0,
        }
    }
}

/// The Kron embedded engine.
///
/// ```text
/// let engine = Engine::open(".kron")?;
/// engine.schedule("email_digest", Schedule::Cron { expr: "0 8 * * *".into() }, fn_timer)?;
/// engine.start();   // non-blocking background loop
/// ```
pub struct Engine {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
    idle_notify: Arc<Notify>,
    data_dir: PathBuf,
    lock_path: PathBuf,
    _lock_file: File,
}

impl Engine {
    /// Open (or create) a Kron data directory and replay the existing log.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, KronError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let lock_path = data_dir.join("kron.lock");
        #[cfg(unix)]
        let lock_file = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .mode(0o600)
                .open(&lock_path)?
        };
        #[cfg(not(unix))]
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| KronError::DataDirLocked {
                path: lock_path.display().to_string(),
            })?;

        let aof_path = data_dir.join("kron.aof");
        let log = AppendOnlyLog::open(&aof_path)?;

        // Load snapshot if present, otherwise replay persisted events.
        let mut state = snapshot::load_state(&data_dir)?;

        // Recompute next_run_at for all active timers.
        let heap = TimerHeap::new();
        let timer_ids: Vec<TimerId> = state.specs.keys().cloned().collect();
        state.next_runs.clear();

        for id in &timer_ids {
            state.states.insert(id.clone(), TimerState::Orphaned);
        }

        let notify = Arc::new(Notify::new());
        let idle_notify = Arc::new(Notify::new());

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                state,
                heap,
                log,
                registry: HashMap::new(),
                runtime: RuntimeState::new(),
                notify: Arc::clone(&notify),
            })),
            notify,
            idle_notify,
            data_dir,
            lock_path,
            _lock_file: lock_file,
        })
    }

    /// Register a timer. If a timer with the same id already exists in the
    /// persisted log, only the function is re-bound (spec is unchanged).
    pub fn schedule(
        &self,
        id: impl Into<String>,
        schedule: Schedule,
        fn_impl: Arc<dyn TimerFn>,
        retry: Option<RetryPolicy>,
        timezone: Option<String>,
    ) -> Result<(), KronError> {
        let id = TimerId::new(id);
        let fn_name = fn_impl.name();
        let tz = timezone.unwrap_or_else(|| "UTC".to_string());

        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.shutting_down || inner.runtime.stopped {
            return Err(KronError::AlreadyStopped);
        }

        let is_new = !inner.state.specs.contains_key(&id);

        let spec = TimerSpec {
            id: id.clone(),
            schedule: schedule.clone(),
            retry: retry.unwrap_or_default(),
            timezone: tz.clone(),
            created_at: inner
                .state
                .specs
                .get(&id)
                .map(|spec| spec.created_at)
                .unwrap_or_else(Utc::now),
        };

        if is_new {
            inner
                .log
                .append(Event::TimerCreated { spec: spec.clone() })?;
            inner.state.apply(&Event::TimerCreated { spec }, Utc::now());
        } else if inner.state.specs.get(&id) != Some(&spec) {
            inner
                .log
                .append(Event::TimerUpdated { spec: spec.clone() })?;
            inner.state.apply(&Event::TimerUpdated { spec }, Utc::now());
            inner.heap.remove_timer(&id);
            inner.state.next_runs.remove(&id);
        }

        // Register the callable.
        inner.registry.insert(id.clone(), fn_impl);
        inner.state.register_function(id.clone(), fn_name);

        // Schedule the next run if not already in the heap.
        if !inner.state.next_runs.contains_key(&id) {
            let spec = inner.state.specs[&id].clone();
            let now = Utc::now();
            if let Ok(Some(next)) = spec.schedule.next_run_after(now, &spec.timezone) {
                inner.state.next_runs.insert(id.clone(), next);
                inner.heap.push(ScheduledTimer {
                    timer_id: id,
                    next_run_at: next,
                    run_id: None,
                    attempt: 1,
                });
                self.notify.notify_one();
            }
        }

        Ok(())
    }

    /// Start the background event loop. Non-blocking: spawns a Tokio task.
    /// Call this after all `schedule()` calls.
    ///
    /// Requires a Tokio runtime to be active.  In kron-py, the runtime is
    /// created by `kron.start()` which runs this in a background OS thread
    /// with its own single-threaded Tokio runtime.
    pub fn start(&self) -> Result<(), KronError> {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.runtime.stopped {
                return Err(KronError::AlreadyStopped);
            }
            if inner.runtime.started {
                return Err(KronError::AlreadyStarted);
            }
            inner.runtime.started = true;
        }

        let inner = Arc::clone(&self.inner);
        let notify = Arc::clone(&self.notify);
        let idle_notify = Arc::clone(&self.idle_notify);
        tokio::spawn(async move {
            loop {
                let (due, next_at) = {
                    let mut guard = inner.lock().unwrap();
                    if guard.runtime.shutting_down {
                        if guard.runtime.active_runs == 0 {
                            idle_notify.notify_waiters();
                        }
                        break;
                    }
                    let now = Utc::now();
                    let due = guard.heap.pop_due(now);
                    let next_at = guard.heap.peek_next_at();
                    (due, next_at)
                };

                for scheduled in due {
                    let inner2 = Arc::clone(&inner);
                    let idle_notify2 = Arc::clone(&idle_notify);
                    {
                        let mut guard = inner.lock().unwrap();
                        if guard.runtime.shutting_down {
                            break;
                        }
                        guard.runtime.active_runs += 1;
                    }
                    tokio::spawn(async move {
                        run_timer(inner2, scheduled).await;
                        idle_notify2.notify_waiters();
                    });
                }

                let now = Utc::now();
                let duration = sleep_duration_until(next_at, now);
                tokio::select! {
                    _ = sleep(duration) => {}
                    _ = notify.notified() => {}
                }
            }
        });
        Ok(())
    }

    pub async fn shutdown(&self, timeout_duration: StdDuration) -> Result<(), KronError> {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.runtime.stopped {
                return Ok(());
            }
            if !inner.runtime.started {
                inner.runtime.stopped = true;
                return Ok(());
            }
            inner.runtime.shutting_down = true;
        }
        self.notify.notify_waiters();

        let wait = async {
            loop {
                let done = {
                    let inner = self.inner.lock().unwrap();
                    inner.runtime.active_runs == 0
                };
                if done {
                    break;
                }
                self.idle_notify.notified().await;
            }
        };

        timeout(timeout_duration, wait)
            .await
            .map_err(|_| KronError::ShutdownTimeout {
                timeout_ms: timeout_duration.as_millis() as u64,
            })?;

        let mut inner = self.inner.lock().unwrap();
        inner.runtime.stopped = true;
        Ok(())
    }

    pub fn compact(&self) -> Result<(), KronError> {
        let mut inner = self.inner.lock().unwrap();
        snapshot::compact(&self.data_dir, &inner.state)?;
        inner.log = AppendOnlyLog::open(self.data_dir.join("kron.aof"))?;
        Ok(())
    }

    /// Read current status for a timer (for CLI / observability).
    pub fn status(&self, id: &str) -> Option<TimerSummary> {
        let inner = self.inner.lock().unwrap();
        inner.state.summary(&TimerId::new(id))
    }

    /// List all timer summaries.
    pub fn list(&self) -> Vec<TimerSummary> {
        let inner = self.inner.lock().unwrap();
        inner
            .state
            .specs
            .keys()
            .filter_map(|id| inner.state.summary(id))
            .collect()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
}

// ---------------------------------------------------------------------------
// Timer execution (runs in its own Tokio task)
// ---------------------------------------------------------------------------

async fn run_timer(inner: Arc<Mutex<Inner>>, scheduled: ScheduledTimer) {
    let timer_id = scheduled.timer_id.clone();
    let (fn_impl, run_id, spec, started_at) = {
        let mut guard = inner.lock().unwrap();
        let run_id = scheduled.run_id.unwrap_or_default();
        let now = Utc::now();
        guard.state.next_runs.remove(&timer_id);

        let due_event = Event::RunDue {
            timer_id: timer_id.clone(),
            run_id: run_id.clone(),
            scheduled_at: scheduled.next_run_at,
        };
        if append_apply_or_stop(&mut guard, due_event, now).is_err() {
            finish_run(&mut guard);
            return;
        }

        let event = Event::RunStarted {
            timer_id: timer_id.clone(),
            run_id: run_id.clone(),
            started_at: now,
            attempt: scheduled.attempt,
        };
        if append_apply_or_stop(&mut guard, event, now).is_err() {
            finish_run(&mut guard);
            return;
        }

        let fn_impl = guard.registry.get(&timer_id).cloned();
        let spec = guard.state.specs.get(&timer_id).cloned();
        (fn_impl, run_id, spec, now)
    };

    let spec = match spec {
        Some(s) => s,
        None => {
            let mut guard = inner.lock().unwrap();
            finish_run(&mut guard);
            return;
        }
    };

    let missing_function = fn_impl.is_none();
    let result = match fn_impl {
        Some(f) => {
            let call_timer_id = timer_id.clone();
            let call_run_id = run_id.clone();
            // Execute outside the lock so other timers can run concurrently.
            tokio::task::spawn_blocking(move || f.call(&call_timer_id, &call_run_id))
                .await
                .unwrap_or_else(|e| Err(format!("task panicked: {}", e)))
        }
        None => Err("function not registered (orphaned)".to_string()),
    };

    let mut guard = inner.lock().unwrap();
    let now = Utc::now();
    let duration_ms = (now - started_at).num_milliseconds().max(0) as u64;

    match result {
        Ok(()) => {
            let event = Event::RunSucceeded {
                timer_id: spec.id.clone(),
                run_id: run_id.clone(),
                finished_at: now,
                duration_ms,
            };
            if append_apply_or_stop(&mut guard, event, now).is_err() {
                finish_run(&mut guard);
                return;
            }

            // Re-enqueue for the next fire.
            reschedule(&mut guard, &spec, now);
        }

        Err(err) => {
            if missing_function {
                guard.state.mark_function_missing(&spec.id);
            } else {
                let attempt = guard
                    .state
                    .pending_retries
                    .get(&spec.id)
                    .map(|(_, _, attempt)| *attempt)
                    .unwrap_or(1);
                let event = Event::RunFailed {
                    timer_id: spec.id.clone(),
                    run_id: run_id.clone(),
                    finished_at: now,
                    error: err.clone(),
                    attempt,
                };
                if append_apply_or_stop(&mut guard, event, now).is_err() {
                    finish_run(&mut guard);
                    return;
                }

                // Check retry policy.
                match spec.retry.next_retry_at(now, attempt) {
                    Some(retry_at) => {
                        let ev = Event::RunRetrying {
                            timer_id: spec.id.clone(),
                            run_id: run_id.clone(),
                            attempt: attempt + 1,
                            next_retry_at: retry_at,
                        };
                        if append_apply_or_stop(&mut guard, ev, now).is_err() {
                            finish_run(&mut guard);
                            return;
                        }

                        if !guard.runtime.shutting_down && !guard.runtime.stopped {
                            guard.heap.push(ScheduledTimer {
                                timer_id: spec.id.clone(),
                                next_run_at: retry_at,
                                run_id: Some(run_id.clone()),
                                attempt: attempt + 1,
                            });
                            guard.notify.notify_one();
                        }
                    }
                    None => {
                        let ev = Event::RunDead {
                            timer_id: spec.id.clone(),
                            run_id: run_id.clone(),
                            at: now,
                        };
                        if append_apply_or_stop(&mut guard, ev, now).is_err() {
                            finish_run(&mut guard);
                            return;
                        }
                        // For recurring timers, still reschedule the next occurrence
                        // even if this run died.
                        if !spec.schedule.is_one_shot() {
                            reschedule(&mut guard, &spec, now);
                        }
                    }
                }
            }
        }
    }
    finish_run(&mut guard);
}

fn finish_run(inner: &mut Inner) {
    inner.runtime.active_runs = inner.runtime.active_runs.saturating_sub(1);
}

fn append_apply_or_stop(
    inner: &mut Inner,
    event: Event,
    now: chrono::DateTime<Utc>,
) -> Result<(), KronError> {
    if let Err(err) = inner.log.append(event.clone()) {
        inner.runtime.shutting_down = true;
        inner.runtime.stopped = true;
        eprintln!("[kron] ERROR: stopping engine after durable log append failed: {err}");
        return Err(err);
    }
    inner.state.apply(&event, now);
    Ok(())
}

fn reschedule(inner: &mut Inner, spec: &TimerSpec, now: chrono::DateTime<Utc>) {
    if inner.runtime.shutting_down || inner.runtime.stopped {
        return;
    }
    if let Ok(Some(next)) = spec.schedule.next_run_after(now, &spec.timezone) {
        inner.state.next_runs.insert(spec.id.clone(), next);
        inner.heap.push(ScheduledTimer {
            timer_id: spec.id.clone(),
            next_run_at: next,
            run_id: None,
            attempt: 1,
        });
        inner.notify.notify_one();
    } else {
        inner.state.next_runs.remove(&spec.id);
    }
}
