# Security Guide

Kron `0.1.x` is alpha software. Embedded mode is local by design. Distributed
mode should be deployed only on trusted private networks until the security
model is hardened further.

## Embedded Mode

Embedded mode stores local state in `data_dir`.

- Keep `data_dir` owned by the application user.
- Do not share one `data_dir` between unrelated applications.
- Treat `kron.token`, `kron.aof`, `kron.snapshot`, and callback output as
  sensitive application data.
- Scheduled functions can perform real side effects, so make callbacks
  idempotent and safe to retry.

On Unix, Kron attempts to use restrictive permissions for the data directory and
runtime/token files.

## Distributed Mode

Distributed mode uses token-authenticated HTTP for public API traffic and
internal Raft traffic. Native TLS/mTLS is not built into Kron `0.1.x`.

Recommended enterprise deployment:

1. Bind Kron HTTP and Raft addresses to a private network.
2. Put Kron behind a reverse proxy or service mesh for TLS/mTLS.
3. Store the cluster token in a secret manager.
4. Inject the token at process start with environment/config management.
5. Rotate tokens with a controlled rolling restart.
6. Restrict firewall rules so only trusted clients, workers, and peer nodes can
   reach Kron.

Do not expose Kron's HTTP or Raft ports directly to the public internet.

## Token Model

Every public API request must send:

```http
Authorization: Bearer <token>
```

Internal Raft traffic also uses the cluster token. The token protects against
accidental local access and basic private-network misuse. It is not a substitute
for TLS, mTLS, network policy, or secret rotation.

## Current Limits

- No native TLS/mTLS.
- No online token rotation.
- No role-based authorization.
- No multi-tenant isolation.
- No audit log designed for compliance use.

These limits are acceptable for alpha testing and private development clusters,
but they should be treated as blockers for regulated production environments.
