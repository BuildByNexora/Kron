use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};
use kron_core::engine::FnTimer;
use kron_core::error::KronError;
use kron_core::event::{Event, LogEntry};
use kron_core::ipc::{self, IpcRequest, IpcResponse};
use kron_core::log::AppendOnlyLog;
use kron_core::retry::{BackoffStrategy, RetryPolicy};
use kron_core::schedule::Schedule;
use kron_core::timer::{OverlapPolicy, TimerState};
use kron_core::Engine;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

async fn wait_until<F>(predicate: F)
where
    F: FnMut() -> bool,
{
    wait_until_with_timeout(predicate, StdDuration::from_secs(20)).await;
}

async fn wait_until_with_timeout<F>(mut predicate: F, timeout_duration: StdDuration)
where
    F: FnMut() -> bool,
{
    timeout(timeout_duration, async {
        loop {
            if predicate() {
                break;
            }
            sleep(StdDuration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition was not met before timeout");
}

fn replay_events(dir: &TempDir) -> Vec<LogEntry> {
    let log = AppendOnlyLog::open(dir.path().join("kron.aof")).unwrap();
    log.replay().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shot_timer_runs_and_writes_consistent_run_id() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule(
            "once",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(80),
            },
            Arc::new(FnTimer::new("once_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();

    engine.start().unwrap();
    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
    wait_until(|| {
        engine
            .status("once")
            .and_then(|summary| summary.last_status)
            .as_deref()
            == Some("OK")
    })
    .await;

    let summary = engine.status("once").unwrap();
    assert_eq!(summary.state, TimerState::Scheduled);
    assert_eq!(summary.last_status.as_deref(), Some("OK"));
    assert_eq!(summary.next_run_at, None);

    let events = replay_events(&dir);
    let run_ids: Vec<_> = events
        .iter()
        .filter_map(|entry| match &entry.event {
            Event::RunDue { run_id, .. }
            | Event::RunStarted { run_id, .. }
            | Event::RunSucceeded { run_id, .. } => Some(run_id.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(run_ids.len(), 3);
    assert!(run_ids.iter().all(|id| id == &run_ids[0]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_metadata_survives_reopen_as_orphaned_until_function_is_registered() {
    let dir = TempDir::new().unwrap();
    let engine = Engine::open(dir.path()).unwrap();

    engine
        .schedule(
            "persisted",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("persisted_fn", |_, _| Ok(()))),
            None,
            None,
        )
        .unwrap();
    drop(engine);

    let reopened = Engine::open(dir.path()).unwrap();
    let orphaned = reopened.status("persisted").unwrap();
    assert_eq!(orphaned.state, TimerState::Orphaned);
    assert_eq!(orphaned.fn_name, None);

    reopened
        .schedule(
            "persisted",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("persisted_fn", |_, _| Ok(()))),
            None,
            None,
        )
        .unwrap();
    let registered = reopened.status("persisted").unwrap();
    assert_eq!(registered.state, TimerState::Scheduled);
    assert_eq!(registered.fn_name.as_deref(), Some("persisted_fn"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_due_time_recovers_timer_metadata() {
    let dir = TempDir::new().unwrap();
    let engine = Engine::open(dir.path()).unwrap();

    engine
        .schedule(
            "recover_me",
            Schedule::At {
                at: Utc::now() + Duration::seconds(60),
            },
            Arc::new(FnTimer::new("recover_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    drop(engine);

    let reopened = Engine::open(dir.path()).unwrap();
    let summary = reopened.status("recover_me").unwrap();
    assert_eq!(summary.state, TimerState::Orphaned);
    assert!(summary.next_run_at.is_none());

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_fn = Arc::clone(&calls);
    reopened
        .schedule(
            "recover_me",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(80),
            },
            Arc::new(FnTimer::new("recover_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    reopened.start().unwrap();
    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn double_start_returns_error() {
    let dir = TempDir::new().unwrap();
    let engine = Engine::open(dir.path()).unwrap();

    engine.start().unwrap();
    let err = engine.start().unwrap_err();
    assert!(matches!(err, KronError::AlreadyStarted));
    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();
}

#[test]
fn data_dir_lock_is_exclusive_and_released_on_drop() {
    let dir = TempDir::new().unwrap();
    let first = Engine::open(dir.path()).unwrap();
    let err = match Engine::open(dir.path()) {
        Ok(_) => panic!("second engine unexpectedly acquired the data dir lock"),
        Err(err) => err,
    };
    assert!(matches!(err, KronError::DataDirLocked { .. }));

    drop(first);
    let reopened = Engine::open(dir.path()).unwrap();
    assert!(reopened.lock_path().ends_with("kron.lock"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_future_runs() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule(
            "future",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(250),
            },
            Arc::new(FnTimer::new("future_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();

    engine.start().unwrap();
    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();
    sleep(StdDuration::from_millis(350)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        engine.start().unwrap_err(),
        KronError::AlreadyStopped
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_overlap_records_skipped_run_while_previous_run_is_active() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule_with_overlap(
            "non_overlapping",
            Schedule::Every { seconds: 1 },
            Arc::new(FnTimer::new("non_overlapping_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                thread::sleep(StdDuration::from_millis(1800));
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
            Some(OverlapPolicy::Skip),
        )
        .unwrap();

    engine.start().unwrap();

    wait_until_with_timeout(
        || {
            replay_events(&dir)
                .iter()
                .any(|entry| matches!(entry.event, Event::RunSkippedOverlap { .. }))
        },
        StdDuration::from_secs(5),
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine.shutdown(StdDuration::from_secs(3)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn allow_overlap_can_run_same_timer_concurrently() {
    let dir = TempDir::new().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let active_for_fn = Arc::clone(&active);
    let max_for_fn = Arc::clone(&max_active);
    engine
        .schedule_with_overlap(
            "overlapping",
            Schedule::Every { seconds: 1 },
            Arc::new(FnTimer::new("overlapping_fn", move |_, _| {
                let now_active = active_for_fn.fetch_add(1, Ordering::SeqCst) + 1;
                max_for_fn.fetch_max(now_active, Ordering::SeqCst);
                thread::sleep(StdDuration::from_millis(1800));
                active_for_fn.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
            Some(OverlapPolicy::Allow),
        )
        .unwrap();

    engine.start().unwrap();
    wait_until_with_timeout(
        || max_active.load(Ordering::SeqCst) >= 2,
        StdDuration::from_secs(5),
    )
    .await;
    engine.shutdown(StdDuration::from_secs(5)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_running_job() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule(
            "slow",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(50),
            },
            Arc::new(FnTimer::new("slow_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                thread::sleep(StdDuration::from_millis(150));
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();

    engine.start().unwrap();
    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();

    let summary = engine.status("slow").unwrap();
    assert_eq!(summary.last_status.as_deref(), Some("OK"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_timeout_when_job_keeps_running() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule(
            "too_slow",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(50),
            },
            Arc::new(FnTimer::new("too_slow_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                thread::sleep(StdDuration::from_millis(250));
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();

    engine.start().unwrap();
    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
    let err = engine
        .shutdown(StdDuration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(err, KronError::ShutdownTimeout { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_after_start_wakes_scheduler() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();
    engine.start().unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule(
            "late_add",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(50),
            },
            Arc::new(FnTimer::new("late_add_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();

    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_writes_snapshot_and_preserves_status_after_reopen() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let calls_for_fn = Arc::clone(&calls);
    engine
        .schedule(
            "compact_me",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(50),
            },
            Arc::new(FnTimer::new("compact_fn", move |_, _| {
                calls_for_fn.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    engine.start().unwrap();
    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
    engine.compact().unwrap();
    assert!(dir.path().join("kron.snapshot").exists());
    assert!(dir.path().join("kron.aof.old").exists());
    drop(engine);

    let reopened = Engine::open(dir.path()).unwrap();
    let summary = reopened.status("compact_me").unwrap();
    assert_eq!(summary.state, TimerState::Orphaned);
    assert_eq!(summary.last_status.as_deref(), Some("OK"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_status_and_compact_work_with_active_runtime() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());
    engine
        .schedule(
            "ipc_timer",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("ipc_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    engine.start().unwrap();
    let ipc_join = ipc::start_server(Arc::clone(&engine)).unwrap();

    let response = ipc::request(
        dir.path(),
        &IpcRequest::Status {
            name: "ipc_timer".into(),
        },
    )
    .unwrap();
    assert!(matches!(response, IpcResponse::Ok { .. }));

    let response = ipc::request(dir.path(), &IpcRequest::Compact).unwrap();
    assert!(matches!(response, IpcResponse::Ok { .. }));
    assert!(dir.path().join("kron.snapshot").exists());

    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();
    let _ = std::fs::remove_file(ipc::socket_path(dir.path()));
    let _ = ipc_join.join();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_tcp_fallback_uses_token_and_rejects_bad_token() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());
    engine
        .schedule(
            "tcp_timer",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("tcp_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    engine.start().unwrap();
    let ipc_join = ipc::start_server(Arc::clone(&engine)).unwrap();

    std::fs::remove_file(ipc::socket_path(dir.path())).unwrap();
    let response = ipc::request(
        dir.path(),
        &IpcRequest::Status {
            name: "tcp_timer".into(),
        },
    )
    .unwrap();
    assert!(matches!(response, IpcResponse::Ok { .. }));

    let endpoint = std::fs::read_to_string(ipc::port_path(dir.path())).unwrap();
    let mut stream = TcpStream::connect(endpoint.trim()).unwrap();
    let bad = serde_json::json!({
        "cmd": "auth",
        "token": "bad",
        "inner": {"cmd": "list"}
    });
    writeln!(stream, "{}", bad).unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    assert!(line.contains("invalid IPC token"));

    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();
    let _ = std::fs::remove_file(ipc::port_path(dir.path()));
    let _ = ipc_join.join();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_falls_back_to_tcp_when_unix_socket_path_is_too_long() {
    let root = TempDir::new().unwrap();
    let long_dir = root.path().join("a".repeat(120));
    std::fs::create_dir_all(&long_dir).unwrap();
    let engine = Arc::new(Engine::open(&long_dir).unwrap());
    engine
        .schedule(
            "long_socket_timer",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("long_socket_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    engine.start().unwrap();
    let ipc_join = ipc::start_server(Arc::clone(&engine)).unwrap();

    let response = ipc::request(
        &long_dir,
        &IpcRequest::Status {
            name: "long_socket_timer".into(),
        },
    )
    .unwrap();
    assert!(matches!(response, IpcResponse::Ok { .. }));
    assert!(ipc::port_path(&long_dir).exists());

    engine.shutdown(StdDuration::from_secs(1)).await.unwrap();
    let _ = std::fs::remove_file(ipc::port_path(&long_dir));
    let _ = ipc_join.join();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_replays_new_aof_tail_after_compaction() {
    let dir = TempDir::new().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .schedule(
            "before_compact",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("before_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    engine.compact().unwrap();
    engine
        .schedule(
            "after_compact",
            Schedule::Every { seconds: 120 },
            Arc::new(FnTimer::new("after_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();
    drop(engine);

    let reopened = Engine::open(dir.path()).unwrap();
    assert!(reopened.status("before_compact").is_some());
    assert!(reopened.status("after_compact").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_with_old_aof_offset_recovers_after_interrupted_rotation() {
    let dir = TempDir::new().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .schedule(
            "before_interrupted_compact",
            Schedule::Every { seconds: 60 },
            Arc::new(FnTimer::new("before_fn", |_, _| Ok(()))),
            Some(RetryPolicy::no_retry()),
            None,
        )
        .unwrap();

    let offset = std::fs::metadata(dir.path().join("kron.aof"))
        .unwrap()
        .len();
    {
        let state = engine.status("before_interrupted_compact").unwrap();
        assert_eq!(state.state, TimerState::Scheduled);
    }
    let state = kron_core::snapshot::load_state(dir.path()).unwrap();
    kron_core::snapshot::write_snapshot_atomic(dir.path(), &state, offset).unwrap();
    std::fs::rename(dir.path().join("kron.aof"), dir.path().join("kron.aof.old")).unwrap();
    std::fs::File::create(dir.path().join("kron.aof")).unwrap();
    drop(engine);

    let reopened = Engine::open(dir.path()).unwrap();
    assert!(reopened.status("before_interrupted_compact").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_run_retries_with_same_run_id_and_eventually_succeeds() {
    let dir = TempDir::new().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();

    let attempts_for_fn = Arc::clone(&attempts);
    engine
        .schedule(
            "flaky",
            Schedule::At {
                at: Utc::now() + Duration::milliseconds(50),
            },
            Arc::new(FnTimer::new("flaky_fn", move |_, _| {
                let current = attempts_for_fn.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    Err("first failure".to_string())
                } else {
                    Ok(())
                }
            })),
            Some(RetryPolicy {
                max_attempts: 2,
                backoff: BackoffStrategy::Fixed { seconds: 0 },
            }),
            None,
        )
        .unwrap();

    engine.start().unwrap();
    wait_until(|| attempts.load(Ordering::SeqCst) == 2).await;
    wait_until(|| {
        engine
            .status("flaky")
            .and_then(|summary| summary.last_status)
            .as_deref()
            == Some("OK")
    })
    .await;

    let summary = engine.status("flaky").unwrap();
    assert_eq!(summary.last_status.as_deref(), Some("OK"));
    assert_eq!(summary.retries_last_7d, 1);

    let events = replay_events(&dir);
    let failed_run = events.iter().find_map(|entry| match &entry.event {
        Event::RunFailed { run_id, .. } => Some(run_id.clone()),
        _ => None,
    });
    let retry_run = events.iter().find_map(|entry| match &entry.event {
        Event::RunRetrying { run_id, .. } => Some(run_id.clone()),
        _ => None,
    });
    assert_eq!(failed_run, retry_run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stress_many_due_timers_all_execute_once() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();
    let count = 100;

    for idx in 0..count {
        let calls_for_fn = Arc::clone(&calls);
        engine
            .schedule(
                format!("stress_{idx}"),
                Schedule::At {
                    at: Utc::now() + Duration::milliseconds(100),
                },
                Arc::new(FnTimer::new(format!("stress_fn_{idx}"), move |_, _| {
                    calls_for_fn.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })),
                Some(RetryPolicy::no_retry()),
                None,
            )
            .unwrap();
    }

    engine.start().unwrap();
    wait_until_with_timeout(
        || calls.load(Ordering::SeqCst) == count,
        StdDuration::from_secs(30),
    )
    .await;
    wait_until_with_timeout(
        || {
            replay_events(&dir)
                .iter()
                .filter(|entry| matches!(entry.event, Event::RunSucceeded { .. }))
                .count()
                == count
        },
        StdDuration::from_secs(30),
    )
    .await;

    let events = replay_events(&dir);
    let succeeded = events
        .iter()
        .filter(|entry| matches!(entry.event, Event::RunSucceeded { .. }))
        .count();
    assert_eq!(succeeded, count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual stress test: run with `cargo test -p kron-core --test engine_integration -- --ignored`"]
async fn stress_one_thousand_due_timers_all_execute_once() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Engine::open(dir.path()).unwrap();
    let count = 1000;

    for idx in 0..count {
        let calls_for_fn = Arc::clone(&calls);
        engine
            .schedule(
                format!("heavy_{idx}"),
                Schedule::At {
                    at: Utc::now() + Duration::milliseconds(100),
                },
                Arc::new(FnTimer::new(format!("heavy_fn_{idx}"), move |_, _| {
                    calls_for_fn.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })),
                Some(RetryPolicy::no_retry()),
                None,
            )
            .unwrap();
    }

    engine.start().unwrap();
    wait_until(|| calls.load(Ordering::SeqCst) == count).await;
    wait_until(|| {
        replay_events(&dir)
            .iter()
            .filter(|entry| matches!(entry.event, Event::RunSucceeded { .. }))
            .count()
            == count
    })
    .await;

    let events = replay_events(&dir);
    let succeeded = events
        .iter()
        .filter(|entry| matches!(entry.event, Event::RunSucceeded { .. }))
        .count();
    assert_eq!(succeeded, count);
}
