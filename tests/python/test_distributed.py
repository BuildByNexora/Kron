from datetime import datetime, timedelta, timezone
import json
import os
import socket
import subprocess
import time
import urllib.request
import urllib.error
from pathlib import Path

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


def wait_for_http(server, token, timeout=45.0, proc=None):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            _, stderr = proc.communicate(timeout=1)
            raise AssertionError(
                f"{server} process exited with {proc.returncode}: {stderr}"
            )
        try:
            return request(server, token, "GET", "/v1/cluster/status")
        except Exception as err:
            last_error = err
            time.sleep(0.05)
    raise AssertionError(f"{server} did not become ready after {timeout}s: {last_error}")


def wait_until(predicate, timeout=10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.1)
    raise AssertionError(f"condition was not met before timeout; last={last!r}")


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


def request_status(server, token, method, path, body=None):
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
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read().decode())


def start_server(
    data_dir: Path,
    node_id: str,
    http: str,
    raft: str,
    token: str,
    leader_id=None,
    extra_env=None,
):
    args = [
        "cargo",
        "run",
        "-q",
        "-p",
        "kron-cli",
        "--",
        "--data-dir",
        str(data_dir),
        "server",
        "start",
        "--node-id",
        node_id,
        "--http",
        http,
        "--raft",
        raft,
        "--cluster-token",
        token,
    ]
    if leader_id is not None:
        args.extend(["--leader-id", leader_id])
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def shutdown_server(server, token):
    try:
        request(server, token, "POST", "/v1/runtime/shutdown", {})
    except Exception:
        pass


def build_three_node_cluster(tmp_path, token, extra_env=None):
    nodes = []
    processes = []
    for name in ["n1", "n2", "n3"]:
        http = f"127.0.0.1:{free_port()}"
        raft = f"127.0.0.1:{free_port()}"
        data_dir = tmp_path / name
        nodes.append({"name": name, "http": http, "raft": raft, "data_dir": data_dir})

    processes.append(
        start_server(
            nodes[0]["data_dir"],
            nodes[0]["name"],
            nodes[0]["http"],
            nodes[0]["raft"],
            token,
            extra_env=extra_env,
        )
    )
    wait_for_http(nodes[0]["http"], token, proc=processes[-1])

    for node in nodes[1:]:
        processes.append(
            start_server(
                node["data_dir"],
                node["name"],
                node["http"],
                node["raft"],
                token,
                leader_id="n1",
                extra_env=extra_env,
            )
        )
        wait_for_http(node["http"], token, proc=processes[-1])

    for node in nodes[1:]:
        joined = request(
            nodes[0]["http"],
            token,
            "POST",
            "/v1/cluster/join",
            {
                "node_id": node["name"],
                "http_addr": node["http"],
                "raft_addr": node["raft"],
            },
        )
        assert joined["joined"] is True

    def membership_has_three():
        status = request(nodes[0]["http"], token, "GET", "/v1/cluster/status")
        text = json.dumps(status)
        return status if "3" in text or "n3" in text else None

    wait_until(membership_has_three, timeout=15.0)
    return nodes, processes


def stop_cluster(nodes, processes, token):
    for node in nodes:
        shutdown_server(node["http"], token)
    for proc in processes:
        if proc.poll() is not None:
            continue
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def current_leader(nodes, token):
    for node in nodes:
        status_code, body = request_status(
            node["http"],
            token,
            "GET",
            "/v1/cluster/status",
        )
        if status_code == 200 and body.get("role", "").lower() == "leader":
            return node, body
    return None


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
        cluster_status = request(server, token, "GET", "/v1/cluster/status")
        assert cluster_status["raft"] == "openraft"
        assert cluster_status["role"].lower() in {
            "leader",
            "candidate",
            "follower",
            "learner",
        }

        status, body = request_status(
            server,
            "bad-token",
            "GET",
            "/v1/cluster/status",
        )
        assert status == 401
        assert body["error"] == "unauthorized"

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


def test_three_node_cluster_replicates_timer_to_followers(tmp_path):
    token = "test-cluster-token"
    nodes = []
    processes = []
    try:
        nodes, processes = build_three_node_cluster(tmp_path, token)

        client = kron.Client(f"http://{nodes[0]['http']}", token)
        client.schedule(
            "replicated_timer",
            at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
            task="send_digest",
            payload={"kind": "replicated"},
            max_attempts=1,
        )

        for node in nodes:
            def timer_visible(server=node["http"]):
                status_code, body = request_status(
                    server,
                    token,
                    "GET",
                    "/v1/timers/replicated_timer",
                )
                if status_code != 200:
                    return None
                return body if body and body.get("id") == "replicated_timer" else None

            wait_until(timer_visible, timeout=15.0)

        follower_status, follower_body = request_status(
            nodes[1]["http"],
            token,
            "POST",
            "/v1/timers",
            {
                "name": "write_on_follower",
                "at": (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
                "task": "send_digest",
                "payload": {},
                "max_attempts": 1,
            },
        )
        assert follower_status == 409
        assert follower_body["error"] == "not_leader"
        assert follower_body["leader_id"] is not None
        assert follower_body["leader_http"] is not None

        leader_node, _ = wait_until(lambda: current_leader(nodes, token), timeout=15.0)
        follower_node = next(node for node in nodes if node["name"] != leader_node["name"])
        redirected_client = kron.Client(f"http://{follower_node['http']}", token)
        redirected_client.schedule(
            "redirected_timer",
            at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
            task="send_digest",
            payload={"kind": "redirected"},
            max_attempts=1,
        )
        wait_until(
            lambda: kron.Client(f"http://{leader_node['http']}", token).status(
                "redirected_timer"
            ),
            timeout=15.0,
        )
    finally:
        stop_cluster(nodes, processes, token)


def test_worker_connected_to_follower_follows_leader_redirect(tmp_path):
    token = "test-cluster-token"
    nodes = []
    processes = []
    try:
        nodes, processes = build_three_node_cluster(tmp_path, token)
        leader_node, _ = wait_until(lambda: current_leader(nodes, token), timeout=15.0)
        follower_node = next(node for node in nodes if node["name"] != leader_node["name"])

        client = kron.Client(f"http://{leader_node['http']}", token)
        client.schedule(
            "worker_redirect_timer",
            at=(datetime.now(timezone.utc) + timedelta(milliseconds=100)).isoformat(),
            task="send_digest",
            payload={"kind": "worker-redirect"},
            max_attempts=1,
        )

        calls = []
        worker = kron.Worker(
            f"http://{follower_node['http']}",
            token,
            worker_id="redirect_worker",
        )

        @worker.task("send_digest")
        def send_digest(payload):
            calls.append(payload)

        wait_until(lambda: worker.run_once(timeout=3.0), timeout=10.0)
        assert calls == [{"kind": "worker-redirect"}]
        history = client.history("worker_redirect_timer")
        event_types = [entry["type"] for entry in history]
        assert "RUN_CLAIMED" in event_types
        assert "RUN_SUCCEEDED" in event_types
    finally:
        stop_cluster(nodes, processes, token)


def test_three_node_cluster_elects_new_leader_after_leader_kill(tmp_path):
    token = "test-cluster-token"
    nodes = []
    processes = []
    try:
        nodes, processes = build_three_node_cluster(tmp_path, token)
        leader_node, _ = wait_until(lambda: current_leader(nodes, token), timeout=15.0)
        leader_index = nodes.index(leader_node)

        processes[leader_index].kill()
        processes[leader_index].wait(timeout=10)

        remaining = [node for index, node in enumerate(nodes) if index != leader_index]

        def new_leader():
            found = current_leader(remaining, token)
            if found is None:
                return None
            node, status = found
            return (node, status) if node["name"] != leader_node["name"] else None

        new_leader_node, _ = wait_until(new_leader, timeout=20.0)

        client = kron.Client(f"http://{new_leader_node['http']}", token)
        client.schedule(
            "after_failover",
            at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
            task="send_digest",
            payload={"kind": "failover"},
            max_attempts=1,
        )

        for node in remaining:
            def timer_visible(server=node["http"]):
                status_code, body = request_status(
                    server,
                    token,
                    "GET",
                    "/v1/timers/after_failover",
                )
                if status_code != 200:
                    return None
                return body if body and body.get("id") == "after_failover" else None

            wait_until(timer_visible, timeout=15.0)
    finally:
        stop_cluster(nodes, processes, token)


def test_worker_run_recovers_after_leader_kill_and_lease_expiry(tmp_path):
    token = "test-cluster-token"
    nodes = []
    processes = []
    try:
        nodes, processes = build_three_node_cluster(
            tmp_path,
            token,
            extra_env={"KRON_WORKER_LEASE_SECONDS": "2"},
        )
        leader_node, _ = wait_until(lambda: current_leader(nodes, token), timeout=15.0)
        leader_index = nodes.index(leader_node)

        client = kron.Client(f"http://{leader_node['http']}", token)
        client.schedule(
            "recover_after_leader_kill",
            at=(datetime.now(timezone.utc) + timedelta(milliseconds=100)).isoformat(),
            task="send_digest",
            payload={"kind": "recover"},
            max_attempts=3,
        )

        request(
            leader_node["http"],
            token,
            "POST",
            "/v1/workers/register",
            {
                "worker_id": "worker_before_kill",
                "tasks": ["send_digest"],
                "lease_seconds": 2,
            },
        )

        def claimed_run():
            response = request(
                leader_node["http"],
                token,
                "POST",
                "/v1/workers/poll",
                {
                    "worker_id": "worker_before_kill",
                    "tasks": ["send_digest"],
                },
            )
            return response if response else None

        first_run = wait_until(claimed_run, timeout=10.0)
        assert first_run["attempt"] == 1

        processes[leader_index].kill()
        processes[leader_index].wait(timeout=10)
        remaining = [node for index, node in enumerate(nodes) if index != leader_index]

        def new_leader():
            found = current_leader(remaining, token)
            if found is None:
                return None
            node, status = found
            return (node, status) if node["name"] != leader_node["name"] else None

        new_leader_node, _ = wait_until(new_leader, timeout=20.0)

        calls = []
        worker = kron.Worker(
            f"http://{new_leader_node['http']}",
            token,
            worker_id="worker_after_failover",
        )

        @worker.task("send_digest")
        def send_digest(payload):
            calls.append(payload)

        def recovered():
            try:
                return worker.run_once(timeout=3.0)
            except RuntimeError:
                return False

        wait_until(recovered, timeout=15.0)
        assert calls == [{"kind": "recover"}]

        history = kron.Client(f"http://{new_leader_node['http']}", token).history(
            "recover_after_leader_kill"
        )
        event_types = [entry["type"] for entry in history]
        assert "RUN_LEASE_EXPIRED" in event_types
        assert "RUN_SUCCEEDED" in event_types
    finally:
        stop_cluster(nodes, processes, token)


def test_majority_continues_after_follower_loss(tmp_path):
    token = "test-cluster-token"
    nodes = []
    processes = []
    try:
        nodes, processes = build_three_node_cluster(tmp_path, token)
        leader_node, _ = wait_until(lambda: current_leader(nodes, token), timeout=15.0)
        follower_index = next(
            index for index, node in enumerate(nodes) if node["name"] != leader_node["name"]
        )
        processes[follower_index].kill()
        processes[follower_index].wait(timeout=10)

        client = kron.Client(f"http://{leader_node['http']}", token)
        client.schedule(
            "majority_after_follower_loss",
            at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
            task="send_digest",
            payload={"kind": "majority"},
            max_attempts=1,
        )

        remaining = [node for index, node in enumerate(nodes) if index != follower_index]
        for node in remaining:
            def timer_visible(server=node["http"]):
                status_code, body = request_status(
                    server,
                    token,
                    "GET",
                    "/v1/timers/majority_after_follower_loss",
                )
                if status_code != 200:
                    return None
                return body if body and body.get("id") == "majority_after_follower_loss" else None

            wait_until(timer_visible, timeout=15.0)
    finally:
        stop_cluster(nodes, processes, token)


def test_stale_completion_rejected_after_lease_expiry(tmp_path):
    token = "test-cluster-token"
    server = f"127.0.0.1:{free_port()}"
    proc = start_server(
        tmp_path,
        "n1",
        server,
        f"127.0.0.1:{free_port()}",
        token,
        extra_env={"KRON_WORKER_LEASE_SECONDS": "1"},
    )
    try:
        wait_for_http(server, token, proc=proc)
        client = kron.Client(f"http://{server}", token)
        client.schedule(
            "stale_after_lease",
            at=(datetime.now(timezone.utc) + timedelta(milliseconds=100)).isoformat(),
            task="send_digest",
            payload={"kind": "stale"},
            max_attempts=3,
        )
        request(
            server,
            token,
            "POST",
            "/v1/workers/register",
            {"worker_id": "old_worker", "tasks": ["send_digest"], "lease_seconds": 1},
        )

        first_run = wait_until(
            lambda: request(
                server,
                token,
                "POST",
                "/v1/workers/poll",
                {"worker_id": "old_worker", "tasks": ["send_digest"]},
            )
            or None,
            timeout=10.0,
        )

        time.sleep(1.5)
        wait_until(
            lambda: "RUN_LEASE_EXPIRED"
            in [entry["type"] for entry in client.history("stale_after_lease")],
            timeout=10.0,
        )

        status, body = request_status(
            server,
            token,
            "POST",
            f"/v1/runs/{first_run['run_id']}/succeed",
            {
                "worker_id": "old_worker",
                "fencing_token": first_run["fencing_token"],
            },
        )
        assert status == 400
        assert "run not active" in body["error"] or "stale fencing token" in body["error"]
    finally:
        shutdown_server(server, token)
        proc.wait(timeout=10)


def test_stale_completion_rejected_after_new_worker_succeeds(tmp_path):
    token = "test-cluster-token"
    server = f"127.0.0.1:{free_port()}"
    proc = start_server(
        tmp_path,
        "n1",
        server,
        f"127.0.0.1:{free_port()}",
        token,
        extra_env={"KRON_WORKER_LEASE_SECONDS": "1"},
    )
    try:
        wait_for_http(server, token, proc=proc)
        client = kron.Client(f"http://{server}", token)
        client.schedule(
            "stale_after_success",
            at=(datetime.now(timezone.utc) + timedelta(milliseconds=100)).isoformat(),
            task="send_digest",
            payload={"kind": "stale-success"},
            max_attempts=3,
        )
        request(
            server,
            token,
            "POST",
            "/v1/workers/register",
            {"worker_id": "old_worker", "tasks": ["send_digest"], "lease_seconds": 1},
        )
        first_run = wait_until(
            lambda: request(
                server,
                token,
                "POST",
                "/v1/workers/poll",
                {"worker_id": "old_worker", "tasks": ["send_digest"]},
            )
            or None,
            timeout=10.0,
        )

        time.sleep(1.5)
        calls = []
        worker = kron.Worker(f"http://{server}", token, worker_id="new_worker")

        @worker.task("send_digest")
        def send_digest(payload):
            calls.append(payload)

        wait_until(lambda: worker.run_once(timeout=3.0), timeout=10.0)
        assert calls == [{"kind": "stale-success"}]

        status, body = request_status(
            server,
            token,
            "POST",
            f"/v1/runs/{first_run['run_id']}/succeed",
            {
                "worker_id": "old_worker",
                "fencing_token": first_run["fencing_token"],
            },
        )
        assert status == 400
        assert "run not active" in body["error"] or "stale fencing token" in body["error"]
    finally:
        shutdown_server(server, token)
        proc.wait(timeout=10)
