from datetime import datetime, timedelta, timezone
import os
import subprocess
import time

import pytest

import kron


def wait_until(predicate, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition was not met before timeout")


def test_callback_runs_and_status_is_visible(tmp_path):
    calls = []

    def task():
        calls.append("ran")

    kron.schedule(
        "py_once",
        fn=task,
        after="1s",
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        wait_until(lambda: calls == ["ran"])
        wait_until(lambda: (kron.status("py_once") or {}).get("last_status") == "OK")
        status = kron.status("py_once")
        assert status["state"] == "scheduled"
        assert status["fn_name"] == "task"
        assert status["last_status"] == "OK"
    finally:
        kron.shutdown()


def test_callback_can_receive_timer_context(tmp_path):
    contexts = []

    def task(context):
        contexts.append(context)

    kron.schedule(
        "py_context",
        fn=task,
        after="1s",
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        wait_until(lambda: len(contexts) == 1)
        assert contexts[0]["timer_id"] == "py_context"
        assert contexts[0]["run_id"].startswith("run_")
    finally:
        kron.shutdown()


def test_list_returns_registered_timers(tmp_path):
    def task():
        pass

    kron.schedule(
        "py_listed",
        fn=task,
        at=datetime.now(timezone.utc) + timedelta(seconds=60),
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        timers = kron.list()
        names = {timer["id"] for timer in timers}
        assert "py_listed" in names
    finally:
        kron.shutdown()


def test_double_start_raises_runtime_error(tmp_path):
    kron.start(data_dir=str(tmp_path))
    try:
        with pytest.raises(RuntimeError, match="already started"):
            kron.start(data_dir=str(tmp_path))
    finally:
        kron.shutdown()


def test_python_exception_becomes_failed_run(tmp_path):
    def bad_task():
        raise ValueError("boom")

    kron.schedule(
        "py_failure",
        fn=bad_task,
        after="1s",
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        def has_failure_status():
            last_status = (kron.status("py_failure") or {}).get("last_status") or ""
            return last_status.startswith("DEAD") or last_status.startswith("FAILED")

        wait_until(has_failure_status)
        status = kron.status("py_failure")
        assert status["state"] == "dead"
    finally:
        kron.shutdown()


def test_shutdown_is_noop_when_not_started():
    kron.shutdown()


def test_lock_conflict_mentions_socket(tmp_path):
    kron.start(data_dir=str(tmp_path))
    try:
        code = (
            "import kron, sys; "
            f"kron.start(data_dir={str(tmp_path)!r})"
        )
        result = subprocess.run(
            [os.sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "kron.sock" in (result.stderr + result.stdout)
    finally:
        kron.shutdown()


def test_cli_reads_status_while_python_runtime_is_active(tmp_path):
    calls = []

    def task():
        calls.append("ran")

    kron.schedule(
        "cli_visible",
        fn=task,
        at=datetime.now(timezone.utc) + timedelta(milliseconds=100),
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        wait_until(lambda: (kron.status("cli_visible") or {}).get("last_status") == "OK")
        result = subprocess.run(
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
                "cli_visible",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "cli_visible" in result.stdout
        assert "OK" in result.stdout
    finally:
        kron.shutdown()


def test_cli_reads_history_while_python_runtime_is_active(tmp_path):
    def task():
        pass

    kron.schedule(
        "cli_history_visible",
        fn=task,
        at=datetime.now(timezone.utc) + timedelta(milliseconds=100),
        max_attempts=1,
    )
    kron.start(data_dir=str(tmp_path))
    try:
        wait_until(
            lambda: (kron.status("cli_history_visible") or {}).get("last_status") == "OK"
        )
        result = subprocess.run(
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
                "history",
                "cli_history_visible",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "RUN_STARTED" in result.stdout
        assert "RUN_SUCCEEDED" in result.stdout
    finally:
        kron.shutdown()
