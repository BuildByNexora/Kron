import time
from datetime import datetime, timedelta, timezone

import kron


attempts = {"count": 0}


def flaky_task():
    attempts["count"] += 1
    print(f"attempt {attempts['count']}")
    if attempts["count"] < 2:
        raise RuntimeError("temporary failure")
    print("task recovered")


kron.schedule(
    "flaky_task",
    fn=flaky_task,
    at=datetime.now(timezone.utc) + timedelta(seconds=1),
    max_attempts=3,
)

kron.start(data_dir=".kron-example")

try:
    time.sleep(5)
    print(kron.status("flaky_task"))
finally:
    kron.shutdown()
