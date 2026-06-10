# Python Usage

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

## Distributed Worker Alpha

Distributed mode uses serializable task names and JSON payloads instead of embedded Python callbacks.

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
