<div align="center">

# Kron

Persistent scheduling for Python, powered by Rust.

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)
[![CI](https://github.com/BuildByNexora/Kron/actions/workflows/ci.yml/badge.svg)](https://github.com/BuildByNexora/Kron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/kron-scheduler.svg)](https://pypi.org/project/kron-scheduler/)

</div>

---

## What It Is

Kron is an embedded scheduler.

It runs inside a Python process, stores timer state in a local `.kron/`
directory, and can execute Python callbacks on schedules such as `cron`,
`every`, `after`, and `at`.

Embedded mode does not require Redis, RabbitMQ, Celery, a separate scheduler
daemon, Kubernetes, or a cloud scheduler.

---

## Install

```bash
pip install kron-scheduler
```

```python
import kron
```

---

## Basic Usage

```python
import time
import kron

def send_digest():
    print("send digest")

def cleanup():
    print("cleanup")

kron.schedule("email_digest", cron="0 8 * * *", fn=send_digest)
kron.schedule("cleanup", every="30m", fn=cleanup)

kron.start(data_dir=".kron")

try:
    while True:
        time.sleep(60)
finally:
    kron.shutdown()
```

`kron.start()` is non-blocking. The scheduler runs in a background Rust runtime
thread.

---

## Schedule Types

```python
kron.schedule("daily_digest", cron="0 8 * * *", fn=send_digest)
kron.schedule("cleanup", every="30m", fn=cleanup)
kron.schedule("retry_later", after="10s", fn=retry)
kron.schedule("new_year", at="2027-01-01T00:00:00Z", fn=celebrate)
```

Exactly one schedule selector is used per timer.

---

## Callback Context

Callbacks can take no arguments:

```python
def cleanup():
    delete_temp_files()
```

Or one context dictionary:

```python
def cleanup(context):
    print(context["timer_id"])
    print(context["run_id"])
```

Python callback objects are not serialized. After process restart, the app must
call `kron.schedule(...)` again to reconnect the persisted timer metadata to the
Python function.

---

## Runtime API

```python
kron.start(data_dir=".kron")
kron.status("cleanup")
kron.list()
kron.shutdown(timeout=5.0)
```

Async wrappers:

```python
await kron.astart(data_dir=".kron")
timers = await kron.alist()
status = await kron.astatus("cleanup")
await kron.ashutdown()
```

The async wrappers call the same runtime without blocking the event loop
directly.

---

## Retry

```python
kron.schedule(
    "sync_customer_data",
    every="15m",
    fn=sync_customer_data,
    max_attempts=5,
)
```

Failed callbacks are recorded as events and retried until the configured attempt
limit is reached.

External side effects should be idempotent.

---

## Overlap Control

Kron can prevent multiple copies of the same timer from running at once.

```python
kron.schedule(
    "sync_reports",
    every="10m",
    fn=sync_reports,
    overlap="skip",
)
```

Overlap policies:

| Policy | Behavior |
|---|---|
| `delay` | Default. Wait for the current run to finish, then schedule the next run from that finish time. |
| `skip` | Keep the wall-clock schedule, but skip a due run if the previous run is still active. The skip is written to history. |
| `allow` | Allow concurrent runs of the same timer. |

Use `overlap="skip"` for jobs such as imports, report generation, billing sync,
or cache refreshes where a second copy should not start while the first one is
still running.

---

## CLI

```bash
kron job list
kron job status <timer>
kron job history <timer>
kron log compact
kron doctor
kron runtime status
kron runtime shutdown
```

Use a custom data directory:

```bash
kron --data-dir /var/lib/myapp/kron job list
```

The CLI uses local IPC when a runtime is active and read-only storage fallback
when it is not.

---

## Persistence

Embedded mode stores state under `data_dir`:

```text
.kron/
  kron.aof
  kron.snapshot
  kron.lock
  kron.token
  kron.sock
  kron.port
```

Storage behavior:

- timer changes and run transitions are written to an append-only log;
- snapshots compact derived state;
- startup restores state from snapshot plus AOF tail;
- `kron.lock` prevents two writers on the same data directory;
- truncated final tails are handled as crash tails;
- middle corruption fails loudly.

Persisted state includes:

- timer definitions;
- schedule metadata;
- next run time;
- retry state;
- last run status;
- run history;
- orphaned timer metadata after restart.

---

## Event Model

Kron records timer transitions as events:

```text
TIMER_CREATED
RUN_DUE
RUN_STARTED
RUN_SUCCEEDED
RUN_FAILED
RUN_RETRYING
RUN_DEAD
```

Server mode also uses worker/claim events:

```text
RUN_CLAIMED
RUN_LEASE_EXPIRED
WORKER_REGISTERED
WORKER_HEARTBEAT
WORKER_LOST
```

---

## Embedded And Edge

Embedded mode is useful when adding a scheduler service would be too much:

- local agents;
- edge devices;
- RISC-V boards;
- small services;
- offline-first tools;
- appliances;
- internal maintenance tasks.

Deployment shape:

```text
Python process
  -> Kron runtime
  -> .kron/ local state
```

---

## Server Mode

Kron also includes a standalone server mode for serializable worker tasks.

Start a node:

```bash
kron --data-dir .kron-n1 server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 127.0.0.1:7380 \
  --cluster-token dev-secret
```

Create a timer that targets a worker task:

```python
import kron

client = kron.Client("http://127.0.0.1:7379", token="dev-secret")

client.schedule(
    "email_digest",
    cron="0 8 * * *",
    task="send_digest",
    payload={"list": "daily"},
    max_attempts=3,
)
```

Worker:

```python
worker = kron.Worker("http://127.0.0.1:7379", token="dev-secret")

@worker.task("send_digest")
def send_digest(payload):
    send_email_digest(payload["list"])

worker.run()
```

Server mode uses OpenRaft-backed state, worker leases, fencing tokens, token
auth, role-scoped tokens, tenant-scoped workers, and an append-only audit log.

For public or enterprise exposure, put Kron behind a reverse proxy, service
mesh, or private network boundary for TLS/mTLS and network policy.

---

## Audit Log

Server mode writes security decisions to:

```text
.kron/kron.audit.jsonl
```

Audit records are hash-chained.

Verify:

```bash
kron audit verify
```

Inspect:

```bash
kron audit tail
kron audit query --actor tenant-a-worker
kron audit query --action worker.poll
```

---

## What It Replaces

For embedded scheduling, Kron can replace:

- system cron for app-owned jobs;
- `while True: sleep(...)` loops;
- Celery beat for small local schedules;
- RQ scheduler for simple jobs;
- custom database polling tables;
- cloud scheduler webhooks for local jobs.

For workflow DAGs, long-running business processes, or complex orchestration,
use a workflow engine such as Temporal, Airflow, or Prefect.

---

## Build And Test

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

```bash
python3 -m venv .venv
.venv/bin/pip install -U pip maturin pytest
.venv/bin/maturin develop
.venv/bin/python -m pytest -q tests/python
```

---

## Documentation

- [Python Usage](docs/usage/python.md)
- [CLI Usage](docs/usage/cli.md)
- [Security Guide](docs/usage/security.md)
- [Storage Format](docs/reference/storage-format.md)
- [Snapshot and Compaction](docs/design/snapshot-compaction.md)
- [Multiprocess IPC](docs/design/multiprocess-ipc.md)
- [Distributed Readiness](docs/design/distributed-production-readiness.md)
- [Release Checklist](docs/usage/release.md)

---

## License

BSD 3-Clause. See [LICENSE](LICENSE).
