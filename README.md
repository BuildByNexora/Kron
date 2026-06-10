<div align="center">

# Kron

### Reliable application time, embedded first.

Cron runs commands.  
Kron makes time observable, persistent, and coordinated.

[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)
[![Status: Alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#project-status)
[![Rust](https://img.shields.io/badge/Rust-core-black.svg)](crates/kron-core)
[![Python](https://img.shields.io/badge/Python-bindings-blue.svg)](crates/kron-py)

</div>

---

## What Is Kron?

Kron is a small runtime for application scheduling.

It is built around one idea:

> Time should be state, not a blind background command.

Classic cron can run something at 08:00, but it cannot tell your application much about what happened. Kron gives timers persistent state, history, retries, crash recovery, observability, and a path from embedded mode to a standalone server.

Kron starts as an embedded time engine.  
The server is a deployment mode, not the product.

---

## What It Does Today

Kron currently provides an embedded Python runtime backed by a Rust engine.

It can:

- register timers from Python callbacks;
- run timers from `cron`, `every`, `after`, or `at` schedules;
- start in the background without blocking the Python process;
- persist timer metadata in a local `.kron/` data directory;
- recover timer metadata after process restart;
- mark persisted timers as `orphaned` until their Python function is re-registered;
- record every run as events in an append-only log;
- retry failed callbacks with configurable max attempts;
- expose timer status and history through Python and the CLI;
- compact the append-only log into a JSON snapshot;
- prevent two runtimes from writing the same data directory at the same time;
- let the CLI inspect an active runtime through local IPC.

It also includes an experimental server mode that can:

- run as a standalone process;
- accept JSON task schedules over HTTP;
- register Python workers by task name;
- assign runs to workers;
- attach fencing tokens to claimed runs;
- persist server state through an OpenRaft-backed file store.

Server mode is not the recommended production path yet.

---

## Common Use Cases

Kron is useful when an application needs scheduled work but a full job stack is too heavy.

Good v0.1 use cases:

- send periodic email digests from a Python app;
- clean local or application-owned resources on a schedule;
- run small maintenance callbacks;
- retry transient application tasks;
- keep observable history for scheduled work;
- replace ad-hoc `while True: sleep(...)` loops;
- prototype scheduling without Redis, RabbitMQ, Celery, or cloud schedulers.

Not recommended yet:

- critical payment processing;
- high-volume distributed task queues;
- multi-tenant hosted scheduling;
- replacing Temporal, Airflow, Prefect, or a production message broker;
- depending on stable storage compatibility across alpha releases.

---

## Feature Matrix

| Capability | Status | Notes |
|---|---:|---|
| Embedded Python scheduling | Works | Primary v0.1 path |
| `cron`, `every`, `after`, `at` schedules | Works | Timezone support exists; DST needs continued hardening |
| Non-blocking `kron.start()` | Works | Runs in a background thread |
| Persistent timer metadata | Works | Stored under `.kron/` |
| Append-only event log | Works | NDJSON, fsync on append |
| Snapshot and compaction | Works | Format is not stable before v1.0 |
| Retry on callback failure | Works | Max attempts supported |
| CLI status/list/history | Works | Uses IPC when runtime is active, read-only fallback otherwise |
| Data directory locking | Works | Prevents two local writers |
| Python `Client`/`Worker` server API | Experimental | For serializable task payloads |
| OpenRaft server mode | Experimental | See [distributed readiness](docs/design/distributed-production-readiness.md) |
| Async Python wrapper | Works | `await kron.astart()`, sync callbacks only |
| PyPI release | Ready to publish | Manual/Trusted Publishing workflow prepared |

---

## Why Kron?

Most applications eventually need scheduled work:

- send an email digest every morning;
- clean temporary files every 30 minutes;
- retry a failed payment later;
- run a maintenance task once at a specific time;
- know whether yesterday's scheduled job actually succeeded.

Today this usually means choosing between system cron, Celery, Sidekiq, Airflow, Temporal, cloud schedulers, custom Redis locks, or a homemade loop.

Kron aims for the missing middle:

- embedded like SQLite;
- observable like a real runtime;
- persistent without an external database;
- simple enough to use in a small app;
- structured enough to grow into server mode.

---

## Install

```bash
pip install kron-scheduler
```

> The package name is `kron-scheduler` because `kron` is already occupied on PyPI.
> The Python import remains `import kron`.

```bash
python -m venv .venv
.venv/bin/pip install maturin pytest
.venv/bin/maturin develop
```

Run an example:

```bash
.venv/bin/python examples/email_digest.py
```

---

## Quickstart: Embedded Python

```python
import kron

def send_digest():
    print("sending digest")

def cleanup_temp_files():
    print("cleaning temp files")

kron.schedule("email_digest", cron="0 8 * * *", fn=send_digest)
kron.schedule("cleanup", every="30m", fn=cleanup_temp_files)

kron.start(data_dir=".kron")  # non-blocking, runs in a background thread
```

No broker.  
No external database.  
No worker stack.  
No cloud scheduler.

Timers persist in `.kron/`, and every run writes events to an append-only log.

Embedded public API:

```python
kron.schedule(name, fn=callable, cron="0 8 * * *")
kron.schedule(name, fn=callable, every="30m")
kron.schedule(name, fn=callable, after="10s")
kron.schedule(name, fn=callable, at=datetime_obj)

kron.start(data_dir=".kron")
kron.status(name)
kron.list()
kron.shutdown(timeout=5.0)

await kron.astart(data_dir=".kron")
await kron.astatus(name)
await kron.alist()
await kron.ashutdown(timeout=5.0)
```

Callbacks can accept no arguments, or one context dictionary:

```python
def task(context):
    print(context["timer_id"], context["run_id"])
```

---

## Observe Timers

```bash
kron job list
kron job status email_digest
kron job history email_digest
```

Example status:

```text
status:    scheduled
fn:        send_digest (registered)
next_run:  2026-06-11 08:00:00 Europe/Rome
last_run:  2026-06-10 08:00:01
duration:  340ms
retries:   0
```

Example history:

```text
2026-06-10 08:00:01  RUN_STARTED
2026-06-10 08:00:01  RUN_SUCCEEDED  340ms
2026-06-09 08:00:01  RUN_FAILED     timeout
2026-06-09 08:00:33  RUN_RETRYING   attempt 2
2026-06-09 08:00:34  RUN_SUCCEEDED  280ms
```

---

## Core Ideas

### Persistent Timers

A timer is not just a callback. It has state:

- schedule;
- next run time;
- last run result;
- retry count;
- history;
- registration status.

### Event Log First

Kron records state transitions as events:

```text
TIMER_CREATED
RUN_DUE
RUN_STARTED
RUN_SUCCEEDED
RUN_FAILED
RUN_RETRYING
RUN_DEAD
```

State is derived from the log and can be inspected by the CLI.

### Honest Execution Semantics

Kron can make scheduling reliable.

Kron does not promise exactly-once side effects.

External effects such as emails, payments, webhooks, and database writes must still be idempotent.

---

## Safety

Scheduled functions can perform real side effects: send emails, write databases, call APIs, create cloud resources, or charge users. Treat scheduled callbacks like production code.

- Make callbacks idempotent.
- Keep external effects guarded by application-level idempotency keys.
- Test timers with short local schedules before using real schedules.
- Use embedded mode for v0.1 production experiments.
- Keep experimental server mode away from critical workloads until the distributed test matrix is stronger.

---

## Experimental Server Mode

The stable path for v0.1 is embedded Python. Kron also includes an experimental standalone server mode for serializable worker tasks.

Do not use server mode for critical production workloads yet. It still needs more 3-node failure testing, leader redirect hardening, and a production-grade Raft storage backend.

```bash
kron --data-dir .kron-n1 server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 127.0.0.1:7380 \
  --cluster-token dev-secret
```

Server mode is for serializable worker tasks, not embedded Python callbacks.

```python
import kron

client = kron.Client("http://127.0.0.1:7379", token="dev-secret")

client.schedule(
    "email_digest",
    every="30m",
    task="send_digest",
    payload={"list": "daily"},
)

worker = kron.Worker("http://127.0.0.1:7379", token="dev-secret")

@worker.task("send_digest")
def send_digest(payload):
    print(payload)

worker.run()
```

Distributed mode uses OpenRaft for committed state, leader election, log replication, and membership. The current file-backed Raft store is intended for alpha testing, not long-term production storage.

Every claimed run has:

```http
X-Kron-Timer: email_digest
X-Kron-Run: run_01J...
X-Kron-Idempotency-Key: email_digest:2026-06-10T08:00:00Z
X-Kron-Fencing-Token: 42
```

---

## Project Status

Kron is **alpha software** moving toward a credible `v0.1`.

The primary product today is **embedded Python scheduling** backed by the Rust
core. That path is the most mature part of the project and is suitable for
experimentation, local tools, small services, and early adopters who can accept
alpha storage compatibility.

Working today:

- Rust core engine;
- Python embedded API with PyO3;
- non-blocking `kron.start()`;
- controlled shutdown;
- data directory locking;
- append-only event log;
- snapshot and compaction;
- CLI observe/admin commands;
- local IPC with token auth;
- Python callback execution and retry;
- Python callback context with `timer_id` and `run_id`;
- Python asyncio wrapper API for non-blocking use in async apps;
- local crash recovery for persisted timer metadata;
- wheel build and clean wheel import checks;
- experimental OpenRaft-backed server mode;
- Python `Client` and `Worker` APIs for server mode;
- single-node distributed client/worker roundtrip test;
- 3-node distributed smoke test covering join, timer replication, and follower write rejection;
- 3-node leader failover test covering leader kill, new leader election, and writes after failover;
- worker recovery test covering abandoned run reclaim after leader kill and lease expiry;
- majority-continuation test after follower loss;
- stale completion rejection tests after lease expiry and after a replacement worker succeeds;
- segmented OpenRaft storage tests for reopen, purge, truncate, legacy-store rejection, corrupted records, and truncated final records;
- BSD 3-Clause license.

Distributed mode status:

- uses OpenRaft for committed state, leader election, log replication, and membership;
- has token-authenticated public and internal Raft HTTP endpoints;
- supports serializable worker tasks and JSON payloads;
- has fencing tokens for claimed runs;
- has a real 3-node subprocess smoke test;
- has a real leader-kill failover test;
- has a worker recovery test after leader kill and lease expiry;
- has a majority-continuation test after follower loss;
- rejects stale worker completions with committed fencing validation;
- uses a segmented file-backed OpenRaft store with manifest, checksummed log records, and deterministic tail-truncation handling;
- remains **experimental**, not production-ready.

Still not mature enough for enterprise production:

- full network partition tests are still missing;
- client/CLI/worker leader redirect is still basic;
- Raft storage is segmented and crash-tested at unit level, but not yet a long-running enterprise log store;
- no stable storage compatibility promise yet;
- no native TLS/mTLS yet; use the documented reverse-proxy or service-mesh deployment model;
- async Python callbacks are not supported yet;
- PyPI publication still requires running the release workflow with a real PyPI project;

Current honest claim:

> Kron embedded mode is the primary alpha product. Distributed mode is an
> OpenRaft-backed experimental server with early 3-node validation, not yet an
> enterprise production scheduler.

---

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

Run Rust tests:

```bash
cargo test --workspace
```

Run linting:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Run Python tests:

```bash
.venv/bin/python -m pytest -q tests/python
```

Build a Python wheel:

```bash
.venv/bin/maturin build --release
```

Check a release artifact:

```bash
.venv/bin/pip install twine
.venv/bin/twine check target/wheels/*
```

Publish manually:

```bash
.venv/bin/maturin publish
```

---

## Repository Layout

```text
crates/kron-core   Rust engine, log, scheduler, IPC, OpenRaft adapter
crates/kron-cli    CLI for observe/admin/server mode
crates/kron-py     Python bindings via PyO3
docs/              design notes, ADRs, usage docs
examples/          small runnable Python examples
tests/python       Python integration tests
```

---

## What Kron Is Not

Kron is not a workflow engine.  
Kron does not have DAGs.  
Kron does not replace Temporal, Airflow, or Prefect.  
Kron does not require Redis, RabbitMQ, Postgres, or Kubernetes.

Kron does one thing:

> It makes application time reliable, observable, and coordinated.

---

## Design Docs

- [ADR 0001: Embedded Time Engine First](docs/adr/0001-embedded-time-engine.md)
- [Kron v0 Engine Design](docs/design/v0-engine.md)
- [Multiprocess IPC](docs/design/multiprocess-ipc.md)
- [Snapshot and Compaction](docs/design/snapshot-compaction.md)
- [Storage Format](docs/reference/storage-format.md)
- [Python Usage](docs/usage/python.md)
- [CLI Usage](docs/usage/cli.md)
- [Security Guide](docs/usage/security.md)
- [Release Checklist](docs/usage/release.md)

## Community Files

- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
- [Security Policy](SECURITY.md)

---

## License

Kron is released under the [BSD 3-Clause License](LICENSE).
