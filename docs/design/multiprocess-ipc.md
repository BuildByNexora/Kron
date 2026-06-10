# Multiprocess IPC

Kron v0 uses a single-writer model.

One embedded runtime owns `kron.lock` and exposes `.kron/kron.sock` on Unix. Other processes and the CLI send newline-delimited JSON commands to the socket for status, history, compaction, doctor, and shutdown.

This is not multi-writer scheduling and not distributed consensus. The local runtime remains the only writer.

## Distributed server

Kron also has a standalone distributed server mode:

```bash
kron --data-dir .kron-server server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 127.0.0.1:7380
```

The distributed server exposes HTTP JSON for clients and workers, and uses OpenRaft internally for leader election, log replication, membership, committed run claims, and fencing tokens.

Nodes are started with stable `--node-id`, `--http`, and `--raft` addresses. Membership is changed through `server join` and `server leave`.
