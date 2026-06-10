from datetime import datetime, timedelta, timezone
import os
import signal
import subprocess
import sys
import textwrap
import time

import kron


def test_subprocess_kill_recovers_timer_metadata(tmp_path):
    script = tmp_path / "writer.py"
    script.write_text(
        textwrap.dedent(
            f"""
            from datetime import datetime, timedelta, timezone
            import time
            import kron

            def future():
                pass

            kron.schedule(
                "future_after_kill",
                fn=future,
                at=datetime.now(timezone.utc) + timedelta(seconds=60),
                max_attempts=1,
            )
            kron.start(data_dir={str(tmp_path)!r})
            time.sleep(30)
            """
        )
    )

    proc = subprocess.Popen([sys.executable, str(script)])
    time.sleep(0.5)
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=5)

    status = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "kron-cli",
            "--",
            "--data-dir",
            str(tmp_path),
            "job",
            "status",
            "future_after_kill",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "future_after_kill" in status.stdout
    assert "orphaned" in status.stdout


def test_compaction_preserves_cli_status(tmp_path):
    calls = []

    def task():
        calls.append("ran")

    kron.schedule(
        "compact_cli",
        fn=task,
        at=datetime.now(timezone.utc) + timedelta(milliseconds=100),
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        deadline = time.time() + 5
        while time.time() < deadline and not calls:
            time.sleep(0.02)
        assert calls == ["ran"]
        subprocess.run(
            [
                "cargo",
                "run",
                "-q",
                "-p",
                "kron-cli",
                "--",
                "--data-dir",
                str(tmp_path),
                "log",
                "compact",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert (tmp_path / "kron.snapshot").exists()
    finally:
        kron.shutdown()

    status = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "kron-cli",
            "--",
            "--data-dir",
            str(tmp_path),
            "job",
            "status",
            "compact_cli",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "OK" in status.stdout
