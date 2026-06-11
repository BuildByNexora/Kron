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

## Secure Deployment Pattern

Recommended production-style topology:

```text
clients / workers
      |
      v
TLS or mTLS proxy
      |
      v
Kron public HTTP API on 127.0.0.1 or private subnet

Kron node <-> private Raft network <-> Kron node
```

Use this pattern for every node:

- bind Kron public HTTP to `127.0.0.1` when a local proxy is used;
- bind Kron Raft to a private interface only;
- terminate TLS/mTLS at Nginx, Envoy, HAProxy, Caddy, or a service mesh;
- keep bearer tokens in a secret manager or root-owned environment file;
- use separate tokens for readers, workers, operators, admins, and Raft peers;
- allow only workers/clients to reach the public API proxy;
- allow only peer Kron nodes to reach the Raft listener;
- ship `kron.audit.jsonl` to a central log system.

## Nginx TLS Example

This protects the public API with HTTPS while Kron listens locally:

```nginx
server {
    listen 443 ssl http2;
    server_name kron.internal.example.com;

    ssl_certificate     /etc/ssl/kron/fullchain.pem;
    ssl_certificate_key /etc/ssl/kron/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    location / {
        proxy_pass http://127.0.0.1:7379;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $remote_addr;
    }
}
```

Start Kron behind it:

```bash
kron --data-dir /var/lib/kron server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 10.0.10.11:7380
```

## Envoy mTLS Sketch

Use Envoy or a service mesh when clients/workers must present certificates.
Kron still sees bearer tokens; Envoy enforces transport identity.
This is a structural example, not a complete production Envoy configuration.

```yaml
static_resources:
  listeners:
    - name: kron_https
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 443
      filter_chains:
        - transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              require_client_certificate: true
              common_tls_context:
                tls_certificates:
                  - certificate_chain: { filename: /etc/envoy/tls/server.crt }
                    private_key: { filename: /etc/envoy/tls/server.key }
                validation_context:
                  trusted_ca: { filename: /etc/envoy/tls/ca.crt }
          filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: kron
                route_config:
                  virtual_hosts:
                    - name: kron
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route: { cluster: kron_local }
  clusters:
    - name: kron_local
      connect_timeout: 1s
      type: STATIC
      load_assignment:
        cluster_name: kron_local
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: 127.0.0.1
                      port_value: 7379
```

## Firewall Rules

Example intent for a 3-node private cluster:

```text
allow workers -> proxy:443
allow operators -> proxy:443
allow kron nodes -> raft:7380
deny internet -> kron:7379
deny internet -> raft:7380
deny workers -> raft:7380
```

The Raft port is an internal consensus port. It should not be reachable by
normal clients or workers.

## Systemd Hardening Example

```ini
[Service]
User=kron
Group=kron
EnvironmentFile=/etc/kron/kron.env
ExecStart=/usr/local/bin/kron --data-dir /var/lib/kron server start \
  --node-id n1 \
  --http 127.0.0.1:7379 \
  --raft 10.0.10.11:7380
Restart=on-failure
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/kron
RestrictAddressFamilies=AF_INET AF_INET6
```

`/etc/kron/kron.env` should be owned by root and readable only by root:

```text
KRON_TOKEN=replace-with-bootstrap-secret
```

Prefer `kron.tokens.json` for ongoing role-scoped access and rotation.

## Token Rotation Runbook

Kron reloads `kron.tokens.json` on each request, so rotation does not require a
server restart.

1. Generate a new high-entropy token.
2. Add the new token beside the old token in `kron.tokens.json`.
3. Prefer `token_sha256` for the new entry.
4. Set `not_before` to the start of the rotation window.
5. Set `expires_at` on the old token to the end of the overlap window.
6. Deploy the new token to clients/workers/peers.
7. Confirm successful requests in `kron.audit.jsonl`.
8. Remove the old token after it expires.
9. Verify old-token requests fail.

For Raft peer token rotation, roll the new token to every node before removing
the old peer token. During alpha deployments, keep a short maintenance window
for token rotation so failed peer authentication is easy to diagnose.

Compute a SHA-256 token hash:

```bash
# Linux
printf '%s' "$KRON_NEW_TOKEN" | sha256sum | awk '{print $1}'

# macOS
printf '%s' "$KRON_NEW_TOKEN" | shasum -a 256 | awk '{print $1}'
```

## Transport Security Boundary

Kron's built-in security model covers application authorization:

- bearer token authentication;
- role authorization;
- tenant scoping;
- audit records;
- hash-chain verification.

The deployment layer should cover transport security:

- TLS certificates;
- client certificates for mTLS;
- certificate rotation;
- public/private network boundaries;
- L4/L7 firewalling;
- service mesh policy.

This split keeps Kron small and portable while still allowing secure enterprise
deployment through standard infrastructure components.

## Cluster Endpoint Metadata

Leader redirects use endpoint metadata replicated through Raft during node join.
For static clusters, keep node HTTP/Raft addresses stable across restarts.
Changing node addresses should be treated as a controlled membership operation:

1. remove or stop the old node address;
2. join the node with the new HTTP/Raft address;
3. verify follower `not_leader` responses include `leader_http`;
4. verify Python Client, Python Worker, and CLI requests follow the redirect.

Future Kron versions may add automatic endpoint reconciliation during leadership
changes. In `0.1.x`, explicit join metadata is the source of truth.

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
      "token_sha256": "replace-with-64-char-sha256-hex",
      "role": "admin"
    },
    {
      "name": "reader",
      "token_sha256": "replace-with-64-char-sha256-hex",
      "role": "reader"
    },
    {
      "name": "worker-a",
      "token_sha256": "replace-with-64-char-sha256-hex",
      "role": "worker",
      "tenant_id": "tenant-a"
    },
    {
      "name": "raft-peer",
      "token_sha256": "replace-with-64-char-sha256-hex",
      "role": "raft"
    }
  ]
}
```

Kron reloads this file for every request. Replacing the file rotates tokens
online without restarting the server. Keep this file readable only by the Kron
process user.

Tokens can be stored in plaintext with `token` or as a SHA-256 hex digest with
`token_sha256`. Hashes reduce accidental secret exposure in config backups and
reviews. Plaintext tokens remain supported for local development and backward
compatibility.

Token entries can also define an activation window:

```json
{
  "name": "worker-a-v2",
  "token_sha256": "1f2d...64-hex-chars",
  "role": "worker",
  "tenant_id": "tenant-a",
  "not_before": "2026-06-11T10:00:00Z",
  "expires_at": "2026-06-18T10:00:00Z"
}
```

Rules:

- `not_before` rejects a token before its activation time.
- `expires_at` rejects a token at or after its expiration time.
- omitted times mean active immediately and no configured expiry.
- each entry must set exactly one of `token` or `token_sha256`.
- `token_sha256` must be a 64-character SHA-256 hex digest.
- plaintext `token` remains available for development and backward
  compatibility, but hashed entries are preferred.

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

- sequence number;
- timestamp;
- node id;
- actor/token name;
- role;
- tenant id;
- action;
- outcome;
- HTTP-style status;
- reason when available.
- `prev_hash`;
- `hash`.

Audit records are hash-chained:

```text
hash = SHA256(canonical JSON of every audit field except hash)
```

The canonical hash input includes `prev_hash`, `seq`, `ts`, `node_id`, `actor`,
`role`, `tenant_id`, `action`, `outcome`, `status`, and `reason`.

The first record uses an empty `prev_hash`. Every following record stores the
previous record's `hash` as its `prev_hash`.

Verify the chain:

```bash
kron audit verify
```

Inspect records:

```bash
kron audit tail
kron audit tail --no-follow --limit 50
kron audit query --actor "tenant-a-worker"
kron audit query --action "worker.poll" --from "2026-06-01" --to "2026-06-10"
```

This is designed to be easy to ship into a SIEM or log pipeline. It is not yet a
compliance-certified immutable audit subsystem.

## Current Limits

- No native TLS/mTLS inside Kron yet.
- Token hashes are supported, but bearer tokens are still shared secrets; protect
  clients, environment files, and deployment logs.
- Tenant isolation is alpha and application-level, not a complete hosted
  multi-tenant platform.
- Audit logging is append-only JSONL, but not WORM storage or compliance
  certified.
- No enterprise secret rotation API yet; rotation is online through
  `kron.tokens.json`.

These limits are acceptable for alpha testing and private development clusters,
but they should be treated as blockers for regulated production environments.
