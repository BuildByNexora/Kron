import time
from datetime import datetime, timedelta, timezone

import kron


def task():
    print("task ran")


kron.schedule(
    "cli_observe",
    fn=task,
    at=datetime.now(timezone.utc) + timedelta(seconds=10),
    max_attempts=1,
)

kron.start(data_dir=".kron-example")

print("runtime started")
print("try in another terminal:")
print("  cargo run -q -p kron-cli -- --data-dir .kron-example job status cli_observe")

try:
    time.sleep(15)
finally:
    kron.shutdown()
