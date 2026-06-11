# Distributed Production Readiness

Kron distributed mode is the path that matters for larger companies, but it must
earn trust with tests and explicit guarantees. The existence of OpenRaft is not
enough by itself; the product must prove recovery, failover, security, and
operational behavior.

## Current Status

The current server mode is suitable for alpha testing of serializable worker
tasks.

It already has:

- OpenRaft-backed committed state;
- separate public HTTP and internal Raft endpoints;
- token authentication for public and Raft traffic;
- single-node bootstrap;
- a subprocess-based 3-node smoke test for join, replication, and follower write rejection;
- a subprocess-based leader-kill failover test with writes after new leader election;
- a worker recovery test after leader kill and lease expiry;
- a majority-continuation test after follower loss;
- worker registration and polling;
- committed run claims with fencing tokens;
- local segmented file-backed OpenRaft store;
- Python `Client` and `Worker` APIs.

It is not yet enterprise-ready.

## Enterprise Bar

Distributed mode becomes credible for larger organizations only when these are
true and continuously tested:

- 3-node clusters elect a leader without manual intervention.
- Writes to followers return structured `not_leader` responses with a usable
  leader address.
- Clients, workers, and CLI follow one-hop leader redirects safely.
- Killing the leader during active worker runs does not lose committed state.
- A new leader expires old worker leases and reclaims abandoned runs.
- Stale worker completions are rejected by committed state, not only by a
  pre-write local check.
- Minority partitions reject writes.
- Majority partitions continue accepting writes.
- Nodes that restart behind the log catch up from log replication or snapshot.
- Store corruption in the middle of the Raft log fails loudly.
- Tail truncation after a crash is handled only when safe.
- Public and Raft traffic have a clear deployment security model.
- Storage format has versioning and migration policy.
- Benchmarks cover replay, snapshot, claim latency, and worker polling.

## Hard Blockers Before Calling It Production

1. Multi-node subprocess test harness.

   Status: partially covered.

   The current harness starts three real `kron server start` processes on random
   ports, joins them, creates a timer on the leader, verifies the timer is
   visible on followers, and verifies writes to a follower are rejected with
   `not_leader`.

   Still missing: restart nodes, network partitions, and deeper recovery
   assertions.

2. Redirect correctness.

   Every write endpoint must return:

   ```json
   {
     "error": "not_leader",
     "leader_id": "n1",
     "leader_http": "127.0.0.1:7379"
   }
   ```

   Status: covered for Python `Client` and CLI request paths; Python `Worker`
   uses the same redirect-aware request path for register, poll, succeed, and
   fail. Redirects are intentionally one-hop to avoid loops during elections.

3. Apply-time fencing validation.

   Status: covered for committed OpenRaft apply.

   `CompleteRun` and `FailRun` commands must carry worker id and fencing token
   and the OpenRaft state machine must reject stale completions during apply.
   Pre-write validation is still kept for fast feedback, but the committed
   state machine now also validates ownership and fencing before making a run
   terminal.

4. Storage hardening.

   Status: partially covered.

   The old monolithic JSON OpenRaft store has been replaced by a segmented
   file-backed store with a manifest, persisted vote/commit files, checksummed
   log records, and deterministic final-tail truncation behavior. Unit tests
   cover reopen, truncate, purge, legacy JSON rejection, corrupted records, and
   truncated final records.

   Still missing before enterprise production: long-running crash tests,
   snapshot-install crash tests, performance benchmarks, and storage migration
   policy across releases.

5. Security model.

   Token auth is acceptable for local/private alpha deployments. Enterprise
   deployment should terminate TLS/mTLS with a reverse proxy or service mesh and
   load tokens from a secret manager. See `docs/usage/security.md`.

## Recommended Roadmap

### Phase 1: Honest Alpha

- Keep embedded mode as stable path.
- Keep server mode marked experimental.
- Add single-node distributed tests for auth, worker execution, stale tokens,
  and restart recovery.

### Phase 2: Cluster Confidence

- Add 3-node subprocess harness.
- Test leader kill and follower catch-up.
- Implement complete leader redirects.
- Add client/worker redirect handling.

### Phase 3: Fault Tolerance

- Add partition tests.
- Add crash-during-write tests.
- Add snapshot install/recovery tests.
- Move stale completion checks into committed state machine apply.

### Phase 4: Enterprise Preview

- Add deployment guide.
- Add operational metrics.
- Add security guide.
- Add storage migration policy.
- Publish distributed mode as beta only after the matrix is green in CI.

## Positioning

The correct public claim today is:

> Kron embedded mode is the primary alpha product. Distributed mode is an
> OpenRaft-backed experimental server for serializable worker tasks.

The target claim after the readiness matrix is green:

> Kron distributed mode provides single-owner scheduled runs with committed
> claims, fencing tokens, worker leases, and Raft-backed recovery.
