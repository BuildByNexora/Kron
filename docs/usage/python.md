# Python Usage

Embedded Python is the primary Kron API for `0.1.x`.

Install the PyPI package as `kron-scheduler`; import it as `kron`:

```bash
pip install kron-scheduler
```

```python
from datetime import datetime, timedelta, timezone
import kron

def send_digest():
    print("digest")

kron.schedule(
    "email_digest",
    fn=send_digest,
    at=datetime.now(timezone.utc) + timedelta(seconds=10),
    max_attempts=3,
)
kron.start(data_dir=".kron")
```

`kron.start()` is non-blocking. `kron.shutdown(timeout=5.0)` stops new runs and waits for active runs.

Functions are not serialized. Re-register timers on application startup.

Callbacks may accept no arguments:

```python
def cleanup():
    ...
```

or one context dictionary:

```python
def cleanup(context):
    print(context["timer_id"])
    print(context["run_id"])
```

The v0.1 context is intentionally small. Future versions may add attempt metadata and idempotency keys.

## Public API Contract

- `kron.schedule(name, fn=callable, cron=... | every=... | after=... | at=..., timezone="UTC", max_attempts=3, overlap="delay")` registers or re-registers a timer.
- `kron.start(data_dir=".kron")` opens storage, starts the runtime in a background thread, and fails if another writer owns the data directory.
- `kron.shutdown(timeout=5.0)` is safe to call even when the runtime is not started.
- `kron.status(name)` returns a dictionary or `None`.
- `kron.list()` returns a list of timer dictionaries.

## Overlap Control

Use `overlap="skip"` when a timer should not start a new invocation while the
previous one is still running:

```python
kron.schedule(
    "long_import",
    every="10m",
    fn=long_import,
    overlap="skip",
)
```

Supported values:

- `delay`: default behavior; schedule from the previous finish time.
- `skip`: keep the schedule, but write `RUN_SKIPPED_OVERLAP` if the timer is still running.
- `allow`: allow concurrent invocations of the same timer.

## Asyncio Wrapper

Kron also exposes async wrappers for applications that already run an asyncio
event loop:

```python
await kron.astart(data_dir=".kron")
status = await kron.astatus("email_digest")
timers = await kron.alist()
await kron.ashutdown(timeout=5.0)
```

These functions run the synchronous Kron API in a thread so they do not block
the event loop. Timer callbacks are still synchronous in `0.1.x`; native async
callbacks are a later feature.

## Safety

Callbacks can produce real side effects. Make them idempotent and safe to retry. Kron can make scheduling observable and durable, but it does not guarantee exactly-once side effects.

## Distributed Worker Alpha

Distributed mode is experimental. It uses serializable task names and JSON payloads instead of embedded Python callbacks.

Do not use distributed mode for critical workloads until the 3-node failure test matrix and Raft storage backend are stronger.

```python
import kron

client = kron.Client("http://127.0.0.1:7379", token="...")
client.schedule(
    "email_digest",
    every="30m",
    task="send_digest",
    payload={"list": "daily"},
)

worker = kron.Worker("http://127.0.0.1:7379", token="...", worker_id="worker_1")

@worker.task("send_digest")
def send_digest(payload):
    print(payload)

worker.run()
```

`Worker.run_once()` is useful in tests. `Worker.run()` loops forever.
