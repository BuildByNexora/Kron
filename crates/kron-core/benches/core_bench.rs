use std::sync::Arc;

use chrono::{Duration, Utc};
use criterion::{criterion_group, criterion_main, Criterion};
use kron_core::heap::{ScheduledTimer, TimerHeap};
use kron_core::retry::RetryPolicy;
use kron_core::schedule::Schedule;
use kron_core::snapshot::Snapshot;
use kron_core::state::EngineState;
use kron_core::timer::{TimerId, TimerSpec};

fn bench_heap(c: &mut Criterion) {
    c.bench_function("heap push/pop 1000", |b| {
        b.iter(|| {
            let mut heap = TimerHeap::new();
            for i in 0..1000 {
                heap.push(ScheduledTimer {
                    timer_id: TimerId::new(format!("t{i}")),
                    next_run_at: Utc::now() + Duration::seconds(i),
                    run_id: None,
                    attempt: 1,
                });
            }
            let _ = heap.pop_due(Utc::now() + Duration::seconds(1000));
        })
    });
}

fn bench_snapshot(c: &mut Criterion) {
    let mut state = EngineState::new();
    for i in 0..1000 {
        let id = TimerId::new(format!("timer_{i}"));
        state.specs.insert(
            id.clone(),
            TimerSpec {
                id,
                schedule: Schedule::Every { seconds: 60 },
                retry: RetryPolicy::no_retry(),
                timezone: "UTC".to_string(),
                created_at: Utc::now(),
                overlap: Default::default(),
            },
        );
    }
    let state = Arc::new(state);
    c.bench_function("snapshot serialize 1000 timers", |b| {
        let state = Arc::clone(&state);
        b.iter(|| {
            let snapshot = Snapshot::from_state(&state, 0);
            serde_json::to_vec(&snapshot).unwrap()
        })
    });
}

criterion_group!(benches, bench_heap, bench_snapshot);
criterion_main!(benches);
