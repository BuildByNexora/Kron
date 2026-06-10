# CLI Usage

```bash
kron --data-dir .kron job list
kron --data-dir .kron job status email_digest
kron --data-dir .kron job history email_digest --limit 20
kron --data-dir .kron doctor
kron --data-dir .kron log compact
kron --data-dir .kron runtime status
kron --data-dir .kron runtime shutdown
```

The CLI uses the active runtime socket when available. If the runtime is not active, read-only commands replay `kron.snapshot` or `kron.aof`.

## Server Distributed Mode

```bash
kron --data-dir .kron-server server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 127.0.0.1:7380 \
  --cluster-token dev-secret

kron --data-dir .kron-server server status
kron --data-dir .kron-server server shutdown
```

The server writes its HTTP endpoint to `kron.http` and token to `kron.token`. `job list`, `job status`, and `job history` use the HTTP server when `kron.http` exists.

To add another node:

```bash
kron --data-dir .kron-n2 server start \
  --node-id n2 \
  --http 127.0.0.1:8379 \
  --raft 127.0.0.1:8380 \
  --cluster-token dev-secret

kron --data-dir .kron-n1 server join \
  --node-id n2 \
  --http 127.0.0.1:8379 \
  --raft 127.0.0.1:8380
```
