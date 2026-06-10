<div align="center">

# Kron

### Reliable application time, embedded first.

Cron runs commands.  
Kron makes time observable, persistent, and coordinated.

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)
[![CI](https://github.com/BuildByNexora/Kron/actions/workflows/ci.yml/badge.svg)](https://github.com/BuildByNexora/Kron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/kron-scheduler.svg)](https://pypi.org/project/kron-scheduler/)
[![Rust](https://img.shields.io/badge/Rust-core-black.svg)](crates/kron-core)
[![Python](https://img.shields.io/badge/Python-bindings-blue.svg)](crates/kron-py)
[![OpenRaft](https://img.shields.io/badge/OpenRaft-distributed-blue.svg)](docs/design/distributed-production-readiness.md)

</div>

---

## Overview

Kron is a Rust-powered scheduling runtime with Python bindings.

It gives applications durable timers, callback execution, retries, status,
history, local crash recovery, CLI inspection, and a server mode for distributed
worker tasks.

The core abstraction is simple:

```text
schedule + target + persistent state + event history
```

Kron can run embedded inside a Python process, like SQLite, or as a standalone
server that assigns serializable tasks to workers.

---

## What Kron Replaces

Kron replaces the pile of infrastructure commonly added just to run scheduled
application code.

For many applications, scheduled work starts as one of these:

```text
system cron
while True: sleep(...)
Celery beat + Redis
RQ scheduler + Redis
Sidekiq cron
cloud scheduler + webhook endpoint
custom database table + polling loop
custom Redis locks
```

Kron gives the application one embedded runtime instead:

```text
Python process
  └── Kron runtime
        ├── durable timers
        ├── callback execution
        ├── retry
        ├── event history
        ├── status API
        └── local storage
```

Embedded mode does not require a scheduler server, Redis, RabbitMQ, Postgres,
Kubernetes, or a cloud scheduler. Timer metadata, run history, retry state, and
snapshots are stored locally in the Kron data directory.

---

## Problems Kron Covers

| Problem | Kron approach |
|---|---|
| System cron is invisible | Every timer has status, next run, last run, history, and errors |
| `while True: sleep(...)` loops are fragile | Kron persists timers and recovers metadata after restart |
| Celery/RQ/Sidekiq add broker complexity | Embedded mode runs without Redis, RabbitMQ, or a worker stack |
| Cloud schedulers create vendor coupling | Kron runs inside the application or as your own server |
| Failed jobs disappear in logs | Kron records structured run events and exposes CLI history |
| Retry is usually hand-written | Kron has retry state and max attempts in the timer runtime |
| Multiple processes can corrupt local state | Kron uses a data directory lock for single-writer safety |
| Distributed workers need ownership checks | Server mode uses worker leases and fencing tokens |
| Teams rebuild scheduling tables repeatedly | Kron provides a reusable timer state machine |

---

## Complexity Removed

For embedded scheduling, Kron removes these moving parts:

- separate scheduler daemon;
- external broker;
- external database for timer metadata;
- custom polling loop;
- custom retry table;
- custom job history table;
- custom distributed lock for local single-writer safety;
- cloud scheduler webhook glue.

The application keeps the scheduling intent in code:

```python
kron.schedule("cleanup", every="30m", fn=cleanup)
kron.schedule("email_digest", cron="0 8 * * *", fn=send_digest)
kron.start(data_dir=".kron")
```

Kron stores the runtime state:

```text
.kron/
  kron.aof
  kron.snapshot
  kron.lock
  kron.token
```

---

## Install

```bash
pip install kron-scheduler
```

The PyPI package is `kron-scheduler`.
The Python module is `kron`.

```python
import kron
```

On Ubuntu/Debian, use a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install kron-scheduler
```

---

## Quickstart

```python
import time
import kron

def send_digest():
    print("send daily digest")

def cleanup_temp_files():
    print("cleanup")

kron.schedule("email_digest", cron="0 8 * * *", fn=send_digest)
kron.schedule("cleanup", every="30m", fn=cleanup_temp_files)

kron.start(data_dir=".kron")  # non-blocking background runtime

try:
    while True:
        time.sleep(60)
finally:
    kron.shutdown()
```

Inspect timers from another terminal:

```bash
kron job list
kron job status email_digest
kron job history email_digest
```

---

## What Kron Does

Kron provides:

- embedded Python scheduling;
- `cron`, `every`, `after`, and `at` schedules;
- persistent timer metadata;
- append-only event log;
- snapshot and compaction;
- retry on callback failure;
- callback context with `timer_id` and `run_id`;
- CLI status, list, history, compact, doctor, runtime status, runtime shutdown;
- local IPC with token authentication;
- data directory locking to prevent two writers;
- async Python wrappers for async applications;
- standalone server mode;
- Python `Client` and `Worker` APIs for server tasks;
- OpenRaft-backed leader election, log replication, and membership;
- worker leasing and run reclaim;
- fencing tokens for claimed runs;
- role-scoped bearer tokens;
- online token reload through `kron.tokens.json`;
- tenant-scoped server timers and worker polling;
- append-only security audit log.

---

## Embedded Python API

### Schedule A Callback

```python
import kron

def task():
    print("ran")

kron.schedule("task", every="10m", fn=task)
kron.start(data_dir=".kron")
```

### Supported Schedules

```python
kron.schedule("daily_digest", cron="0 8 * * *", fn=send_digest)
kron.schedule("cleanup", every="30m", fn=cleanup)
kron.schedule("retry_later", after="10s", fn=retry)
kron.schedule("new_year", at="2027-01-01T00:00:00Z", fn=celebrate)
```

Exactly one schedule selector is used per timer.

### Callback Context

A callback can accept no arguments:

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

### Runtime Control

```python
kron.start(data_dir=".kron")
kron.status("cleanup")
kron.list()
kron.shutdown(timeout=5.0)
```

### Async Wrapper

The async API wraps the same runtime without blocking the event loop:

```python
import asyncio
import kron

def refresh_cache():
    print("refresh")

async def main():
    kron.schedule("refresh_cache", every="5m", fn=refresh_cache)
    await kron.astart(data_dir=".kron")
    timers = await kron.alist()
    print(timers)
    await kron.ashutdown()

asyncio.run(main())
```

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
kron --data-dir .kron-prod job list
```

The CLI uses local IPC when a runtime is active and read-only storage fallback
when the runtime is not running.

---

## Storage Layout

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

Server mode stores cluster state under `data_dir`:

```text
.kron/
  kron.cluster.json
  kron.token
  kron.tokens.json
  kron.audit.jsonl
  raft/
    manifest.json
    vote.json
    committed.json
    state.json
    log/
      0000000000000001-0000000000010000.seg
```

Core storage properties:

- append-first event model;
- atomic snapshot writes;
- fsync on critical writes;
- deterministic handling of truncated final log records;
- fatal error on corrupted middle storage records;
- exclusive data directory lock for writers.

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
RUN_CLAIMED
RUN_LEASE_EXPIRED
WORKER_REGISTERED
WORKER_HEARTBEAT
WORKER_LOST
```

Timer state is derived from persisted events and snapshots.

Example CLI history:

```text
2026-06-10 08:00:01  RUN_STARTED
2026-06-10 08:00:01  RUN_SUCCEEDED  340ms
2026-06-09 08:00:01  RUN_FAILED     timeout
2026-06-09 08:00:33  RUN_RETRYING   attempt 2
2026-06-09 08:00:34  RUN_SUCCEEDED  280ms
```

---

## Retry And Idempotency

Retries are part of the runtime.

```python
kron.schedule(
    "sync_customer_data",
    every="15m",
    fn=sync_customer_data,
    max_attempts=5,
)
```

Distributed runs include:

```text
timer
run_id
attempt
fencing_token
idempotency_key
```

Use `idempotency_key` in external systems such as databases, payment APIs,
email providers, and webhooks.

---

## Complex Use Case: SaaS Maintenance Runtime

Run maintenance tasks inside a Python web service without Redis, RabbitMQ,
Celery, or system cron.

```python
import kron

def compact_accounts(context):
    run_id = context["run_id"]
    compact_inactive_accounts(idempotency_key=run_id)

def refresh_billing_state(context):
    run_id = context["run_id"]
    refresh_billing(idempotency_key=run_id)

def send_usage_digest(context):
    run_id = context["run_id"]
    send_digest(idempotency_key=run_id)

kron.schedule("compact_accounts", cron="0 2 * * *", fn=compact_accounts)
kron.schedule("refresh_billing_state", every="15m", fn=refresh_billing_state)
kron.schedule("usage_digest", cron="0 8 * * MON", fn=send_usage_digest)

kron.start(data_dir="/var/lib/myapp/kron")
```

Operations:

```bash
kron --data-dir /var/lib/myapp/kron job list
kron --data-dir /var/lib/myapp/kron job status refresh_billing_state
kron --data-dir /var/lib/myapp/kron job history usage_digest
kron --data-dir /var/lib/myapp/kron log compact
```

---

## Complex Use Case: Delayed Application Requests

Applications often need to save a request and execute it later:

- send a reminder in 24 hours;
- retry an external API call in 10 minutes;
- expire a pending invite next week;
- run cleanup after a user deletes an account;
- schedule a delayed webhook.

Without Kron, this usually becomes a database table plus a polling worker:

```text
delayed_requests table
poll every N seconds
claim row
run handler
retry row
mark done
clean old rows
```

With Kron, the delayed request is a timer:

```python
import kron

def send_invite_reminder(context):
    invite_id = context["timer_id"].replace("invite_reminder:", "")
    send_reminder(invite_id, idempotency_key=context["run_id"])

def schedule_invite_reminder(invite_id: str):
    kron.schedule(
        f"invite_reminder:{invite_id}",
        after="24h",
        fn=send_invite_reminder,
        max_attempts=3,
    )

kron.start(data_dir=".kron")
```

Kron stores the timer, next execution time, run events, retry state, and final
status. The application stores only its business data.

---

## Complex Use Case: Distributed Worker Tasks

Start a server:

```bash
kron --data-dir .kron-n1 server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 127.0.0.1:7380 \
  --cluster-token dev-secret
```

Create a serializable timer:

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

Run a worker:

```python
import kron

worker = kron.Worker("http://127.0.0.1:7379", token="dev-secret")

@worker.task("send_digest")
def send_digest(payload):
    send_email_digest(payload["list"])

worker.run()
```

Worker execution flow:

```text
register -> heartbeat -> poll -> claim run -> execute task -> succeed/fail
```

The server assigns each run to one active owner and attaches a monotonic fencing
token to protect external systems from stale workers.

---

## Complex Use Case: Tenant-Scoped Workers

Create role-scoped tokens:

```json
{
  "tokens": [
    {
      "name": "admin",
      "token": "admin-secret",
      "role": "admin"
    },
    {
      "name": "tenant-a-worker",
      "token": "worker-a-secret",
      "role": "worker",
      "tenant_id": "tenant-a"
    },
    {
      "name": "tenant-a-reader",
      "token": "reader-a-secret",
      "role": "reader",
      "tenant_id": "tenant-a"
    }
  ]
}
```

Save it as:

```text
.kron/kron.tokens.json
```

Effects:

- `tenant-a-reader` sees only tenant A timers and history.
- `tenant-a-worker` claims only tenant A runs.
- token changes are picked up online on the next request.
- security decisions are written to `kron.audit.jsonl`.

---

## Server Security

Server mode supports:

- bearer token authentication;
- online token reload;
- role-based authorization;
- tenant-scoped timer visibility;
- tenant-scoped worker polling;
- append-only JSONL audit events;
- separate public API and Raft API listeners.

Roles:

| Role | Access |
|---|---|
| `reader` | list/status/history/cluster status |
| `worker` | register/heartbeat/poll/succeed/fail |
| `operator` | create timers and read state |
| `admin` | all public API operations |
| `raft` | internal Raft endpoints |

Audit path:

```text
.kron/kron.audit.jsonl
```

Example audit event:

```json
{
  "ts": "2026-06-10T10:00:00Z",
  "node_id": "n1",
  "action": "worker.poll",
  "outcome": "ok",
  "status": 200,
  "actor": "tenant-a-worker",
  "role": "worker",
  "tenant_id": "tenant-a"
}
```

Deploy server mode on a private network and terminate TLS/mTLS with a reverse
proxy or service mesh.

---

## Architecture

```text
Python API / CLI / HTTP API
          |
          v
Rust core engine
          |
          +-- timer heap
          +-- schedule parser
          +-- retry policy
          +-- append-only log
          +-- snapshot/compaction
          +-- local IPC
          +-- OpenRaft adapter
```

Workspace layout:

```text
crates/kron-core   Rust engine, log, scheduler, IPC, OpenRaft adapter
crates/kron-cli    CLI for observe/admin/server mode
crates/kron-py     Python bindings through PyO3
docs/              design notes, ADRs, usage docs
examples/          runnable Python examples
tests/python       Python integration tests
```

---

## Enterprise Stress And Reliability Checks

The current test matrix covers the parts companies usually care about in a
scheduler: durability, retry behavior, crash recovery, locking, IPC security,
distributed worker ownership, leader failover, stale completion rejection, and
storage corruption handling.

Latest local stress run:

```text
cargo fmt --check                                           PASS
cargo clippy --workspace --all-targets -- -D warnings       PASS
cargo test --workspace                                      PASS
cargo test -p kron-core --test engine_integration -- --ignored --nocapture
                                                              PASS
pytest -q tests/python                                      PASS
```

Observed results:

```text
Rust core unit tests:             33 passed
Rust integration tests:           17 passed
Manual core stress test:           1 passed
Python integration tests:         20 passed
```

Stress areas covered:

| Area | What is tested |
|---|---|
| Scheduler pressure | 1000 due timers execute once without duplicate firing |
| Retry correctness | failed callback retries and terminal failure paths |
| Crash recovery | persisted timer metadata survives process restart |
| Compaction | snapshot preserves status/history after AOF compaction |
| AOF replay | truncated final tail is handled deterministically |
| Data directory locking | second writer is rejected and lock releases on drop |
| IPC security | token-authenticated IPC rejects bad tokens |
| TCP fallback | local IPC fallback works when Unix socket path is too long |
| Shutdown | graceful wait and timeout behavior are tested |
| Raft storage | reopen, truncate, purge, snapshots, corrupted records, tail truncation |
| Distributed cluster | single-node client/worker roundtrip |
| 3-node cluster | join, replication, follower write rejection |
| Leader failover | leader kill, new election, write after failover |
| Worker recovery | abandoned run is reclaimed after leader kill and lease expiry |
| Fencing tokens | stale worker completions are rejected |
| Security model | RBAC role rules and tenant matching are tested |
| Python async wrapper | async start/status/list/shutdown wrapper behavior |

The manual stress command exercises the embedded scheduler with 1000 due timers:

```bash
cargo test -p kron-core --test engine_integration -- --ignored --nocapture
```

The Python suite exercises embedded mode, crash recovery, async wrappers, and
distributed server mode through real subprocesses:

```bash
python3 -m venv .venv
.venv/bin/pip install -U pip maturin pytest
VIRTUAL_ENV=$PWD/.venv PATH=$PWD/.venv/bin:$PATH .venv/bin/maturin develop
.venv/bin/python -m pytest -q tests/python
```

---

## Build And Test

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Python:

```bash
python3 -m venv .venv
.venv/bin/pip install -U pip maturin pytest
.venv/bin/maturin develop
.venv/bin/python -m pytest -q tests/python
```

Build wheel:

```bash
.venv/bin/maturin build --release
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

Kron is released under the [BSD 3-Clause License](LICENSE).
