import time
from datetime import datetime, timedelta, timezone

import kron


def send_digest():
    print("sending daily digest")


kron.schedule(
    "email_digest",
    fn=send_digest,
    at=datetime.now(timezone.utc) + timedelta(seconds=2),
    max_attempts=1,
)

kron.start(data_dir=".kron-example")

try:
    time.sleep(3)
    print(kron.status("email_digest"))
finally:
    kron.shutdown()
