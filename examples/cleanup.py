import time

import kron


def cleanup_temp_files():
    print("cleaning temporary files")


kron.schedule("cleanup", fn=cleanup_temp_files, every="2s", max_attempts=1)
kron.start(data_dir=".kron-example")

try:
    time.sleep(5)
    print(kron.status("cleanup"))
finally:
    kron.shutdown()
