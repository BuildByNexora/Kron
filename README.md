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
pip install kron
```

> Kron is currently alpha. Until the package is published on PyPI, use the local development flow below.

```bash
python -m venv .venv
.venv/bin/pip install maturin pytest
.venv/bin/maturin develop
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

## Server Mode

Kron also has an experimental standalone server mode:

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

Distributed mode uses OpenRaft for committed state, leader election, log replication, and membership.

Every claimed run has:

```http
X-Kron-Timer: email_digest
X-Kron-Run: run_01J...
X-Kron-Idempotency-Key: email_digest:2026-06-10T08:00:00Z
X-Kron-Fencing-Token: 42
```

---

## Project Status

Kron is **alpha software**.

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
- experimental OpenRaft-backed server mode;
- Python `Client` and `Worker` APIs for server mode;
- BSD 3-Clause license.

Still not mature:

- distributed mode needs more real 3-node failure tests;
- client/CLI leader redirect is still basic;
- Raft storage is file JSON, not a production log segment store;
- no stable storage compatibility promise yet;
- no async Python API yet;
- no published PyPI release yet;
- no security hardening beyond local token auth.

This is a serious prototype moving toward pre-alpha release quality, not a finished 1.0 system.

---

## Development

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

---

## Repository Layout

```text
crates/kron-core   Rust engine, log, scheduler, IPC, OpenRaft adapter
crates/kron-cli    CLI for observe/admin/server mode
crates/kron-py     Python bindings via PyO3
docs/              design notes, ADRs, usage docs
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

---

## License

Kron is released under the [BSD 3-Clause License](LICENSE).

This is the same permissive family of license used by the original Redis project.
