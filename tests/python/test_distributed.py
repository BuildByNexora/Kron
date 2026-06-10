from datetime import datetime, timedelta, timezone
import json
import socket
import subprocess
import time
import urllib.request

import kron


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for(path, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"{path} did not appear")


def request(server, token, method, path, body=None):
    data = None
    if body is not None:
        data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://{server}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode())


def test_distributed_client_worker_roundtrip(tmp_path):
    http_port = free_port()
    raft_port = free_port()
    server = f"127.0.0.1:{http_port}"
    proc = subprocess.Popen(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "kron-cli",
            "--",
            "--data-dir",
            str(tmp_path),
            "server",
            "start",
            "--node-id",
            "n1",
            "--http",
            server,
            "--raft",
            f"127.0.0.1:{raft_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(tmp_path / "kron.token")
        token = (tmp_path / "kron.token").read_text().strip()
        client = kron.Client(f"http://{server}", token)
        client.schedule(
            "distributed_once",
            at=(datetime.now(timezone.utc) + timedelta(milliseconds=200)).isoformat(),
            task="send_digest",
            payload={"kind": "daily"},
            max_attempts=1,
        )

        calls = []
        worker = kron.Worker(f"http://{server}", token, worker_id="py_worker")

        @worker.task("send_digest")
        def send_digest(payload):
            calls.append(payload)

        time.sleep(0.4)
        assert worker.run_once(timeout=3.0) is True
        assert calls == [{"kind": "daily"}]

        history = client.history("distributed_once")
        event_types = [entry["type"] for entry in history]
        assert "RUN_CLAIMED" in event_types
        assert "RUN_SUCCEEDED" in event_types
    finally:
        try:
            token_path = tmp_path / "kron.token"
            if token_path.exists():
                request(server, token_path.read_text().strip(), "POST", "/v1/runtime/shutdown", {})
        finally:
            proc.wait(timeout=10)
