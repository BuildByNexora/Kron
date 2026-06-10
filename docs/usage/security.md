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
internal Raft traffic. Kron now supports role-scoped bearer tokens, online token
reload, tenant-scoped timer visibility, and an append-only audit log.

Native TLS/mTLS is still not built into Kron `0.1.x`. Use a reverse proxy,
private network, or service mesh for transport security.

Recommended enterprise deployment:

1. Bind Kron HTTP and Raft addresses to a private network.
2. Put Kron behind a reverse proxy or service mesh for TLS/mTLS.
3. Store the cluster token in a secret manager.
4. Inject the token at process start with environment/config management.
5. Rotate tokens by updating `kron.tokens.json`; the server reloads it on each
   request.
6. Restrict firewall rules so only trusted clients, workers, and peer nodes can
   reach Kron.

Do not expose Kron's HTTP or Raft ports directly to the public internet.

## Token And Role Model

Every public API request must send:

```http
Authorization: Bearer <token>
```

By default, Kron keeps compatibility with the legacy single-token model:
`.kron/kron.token` acts as an admin token.

For stronger deployments, create `.kron/kron.tokens.json`:

```json
{
  "tokens": [
    {
      "name": "admin",
      "token": "replace-with-secret-admin-token",
      "role": "admin"
    },
    {
      "name": "reader",
      "token": "replace-with-secret-reader-token",
      "role": "reader"
    },
    {
      "name": "worker-a",
      "token": "replace-with-secret-worker-token",
      "role": "worker",
      "tenant_id": "tenant-a"
    },
    {
      "name": "raft-peer",
      "token": "replace-with-secret-raft-token",
      "role": "raft"
    }
  ]
}
```

Kron reloads this file for every request. Replacing the file rotates tokens
online without restarting the server. Keep this file readable only by the Kron
process user.

Roles:

- `reader`: list timers, read status/history, read cluster status.
- `worker`: register, heartbeat, poll, and complete/fail assigned runs.
- `operator`: create timers and perform reader actions.
- `admin`: all API actions.
- `raft`: internal Raft replication endpoints.

Internal Raft traffic must also include a valid bearer token. The default admin
token is accepted for compatibility, but production deployments should use a
dedicated `raft` token.

## Tenant Isolation

Tokens may include `tenant_id`. When present:

- reads only return timers/history for that tenant;
- worker polling only claims runs for that tenant;
- created timers inherit the token tenant unless the request explicitly sets one.

Tenant support is an application-level isolation boundary for alpha server
mode. It is not yet a full hosted multi-tenant security model with quotas,
per-tenant encryption, billing isolation, or separate Raft groups.

## Audit Log

Distributed mode appends security-relevant API decisions to:

```text
.kron/kron.audit.jsonl
```

Each JSON line includes:

- timestamp;
- node id;
- actor/token name;
- role;
- tenant id;
- action;
- outcome;
- HTTP-style status;
- reason when available.

This is designed to be easy to ship into a SIEM or log pipeline. It is not yet a
compliance-certified immutable audit subsystem.

## Current Limits

- No native TLS/mTLS inside Kron yet.
- No token hashing in `kron.tokens.json` yet; protect the file with filesystem
  permissions and a secret manager.
- Tenant isolation is alpha and application-level, not a complete hosted
  multi-tenant platform.
- Audit logging is append-only JSONL, but not WORM storage or compliance
  certified.
- No enterprise secret rotation API yet; rotation is file-based.

These limits are acceptable for alpha testing and private development clusters,
but they should be treated as blockers for regulated production environments.
