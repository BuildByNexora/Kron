# Kron v0 Engine Design

Kron v0 proves one thing: a Python application can register persistent, observable timers and run them reliably from an embedded Rust engine.

## Milestone

- `kron.schedule()` registers a timer.
- `kron.start(data_dir=".kron")` starts a non-blocking background thread.
- The Rust runtime releases the Python GIL while the scheduler loop runs.
- Due timers call their registered Python function.
- Each run writes events to an append-only log.
- `kron job status` reads the log and shows derived state.
- Failed functions retry with backoff and write `RUN_RETRYING`.
- Timers survive process restart.
- Timers whose functions are not re-registered become `orphaned`.

## Python API

```python
import kron

kron.schedule("email_digest", cron="0 8 * * *", fn=send_digest)
kron.schedule("cleanup", every="30m", fn=cleanup_temp_files)

kron.start(data_dir=".kron")  # non-blocking, runs in a background thread
```

In v0, `start()` always uses a background thread. Native `asyncio` integration can be added later without changing timer semantics.

## Data Directory

The data directory is explicit and overridable.

```python
kron.start(data_dir=".kron")
```

Lookup order:

1. Explicit `data_dir` argument.
2. `KRON_HOME` environment variable.
3. `.kron/` beside the Python entry file.
4. `~/.kron/` fallback.

The engine stores:

```text
.kron/
  kron.aof
  kron.snapshot
```

## Function Registration

Python functions are not serialized.

The event log persists timer metadata. The application must re-register functions on startup by calling `kron.schedule()` again with the same timer name.

If a persisted timer exists but no function is registered after startup grace time, the timer state becomes `orphaned`.

```text
status: orphaned
fn:     send_digest (not registered)
```

Function registration is runtime-only state. It is not written to `kron.aof`.

## Rust Crate Layout

```text
crates/
  kron-core/
    src/
      lib.rs
      engine.rs
      timer.rs
      schedule.rs
      event.rs
      log.rs
      state.rs
      heap.rs
      retry.rs
      clock.rs
      error.rs
  kron-py/
    src/
      lib.rs
      registry.rs
      runtime.rs
      api.rs
  kron-cli/
    src/
      main.rs
      commands/
        job.rs
```

## Core Types

```rust
pub struct TimerId(String);

pub struct TimerSpec {
    pub id: TimerId,
    pub schedule: Schedule,
    pub retry: RetryPolicy,
    pub timezone: TimeZone,
}

pub enum Schedule {
    Cron(String),
    Every(Duration),
    At(DateTimeUtc),
    After(Duration),
}

pub struct RunId(String);

pub struct Run {
    pub id: RunId,
    pub timer_id: TimerId,
    pub scheduled_at: DateTimeUtc,
    pub started_at: Option<DateTimeUtc>,
    pub finished_at: Option<DateTimeUtc>,
    pub attempt: u32,
}

pub enum TimerState {
    Scheduled,
    Running,
    Retrying,
    Dead,
    Orphaned,
    Paused,
    Cancelled,
}
```

## Event Log

The append-only log is the source of truth. Current state is derived from events.

```rust
pub enum Event {
    TimerCreated { spec: TimerSpec },
    TimerUpdated { spec: TimerSpec },
    TimerPaused { timer_id: TimerId },
    TimerResumed { timer_id: TimerId },
    TimerCancelled { timer_id: TimerId },
    RunDue { timer_id: TimerId, run_id: RunId, scheduled_at: DateTimeUtc },
    RunStarted { timer_id: TimerId, run_id: RunId, attempt: u32 },
    RunSucceeded { timer_id: TimerId, run_id: RunId, duration_ms: u64 },
    RunFailed { timer_id: TimerId, run_id: RunId, error: String },
    RunRetrying { timer_id: TimerId, run_id: RunId, attempt: u32, next_run_at: DateTimeUtc },
    RunDead { timer_id: TimerId, run_id: RunId },
}
```

## Timer Heap

The scheduler keeps an in-memory min-heap ordered by `next_run_at`.

```rust
pub struct ScheduledTimer {
    pub timer_id: TimerId,
    pub next_run_at: DateTimeUtc,
}
```

On startup:

1. Replay `kron.aof`.
2. Derive timer state.
3. Rebuild the heap from active timers.
4. Mark timers without registered functions as `orphaned` after grace time.

## v0 Execution Semantics

Kron v0 guarantees:

- A due timer is selected once by the embedded process.
- Each run emits durable lifecycle events.
- Failed runs retry according to policy.
- Timer metadata survives restart.

Kron v0 does not guarantee:

- Exactly-once external side effects.
- Distributed ownership.
- Cross-process coordination.
- Serialization of Python callables.
