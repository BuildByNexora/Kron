# Storage Format

Kron storage is pre-stable in `0.1.x`.

The storage format is not stable before v1.0. Do not rely on direct file compatibility across alpha releases unless the release notes explicitly say the format is unchanged.

## Files

- `kron.aof`: newline-delimited JSON `LogEntry` records.
- `kron.snapshot`: JSON checkpoint of derived engine state.
- `kron.aof.old`: previous AOF after compaction.
- `kron.lock`: exclusive writer lock.
- `kron.token`: local IPC token.
- `kron.sock` / `kron.port`: local IPC endpoint metadata.
- `kron.cluster.json`: experimental server-mode local cluster metadata.

Server mode uses a separate experimental OpenRaft store:

```text
raft/
  manifest.json
  vote.json
  committed.json
  state.json
  log/
    0000000000000001-0000000000000042.seg
  snapshots/
```

`kron.openraft.store.json` was the old alpha OpenRaft store. Kron refuses to
open it automatically and reports that manual migration is required.

## Versioning

`LogEntry.v`, `Snapshot.v`, `Snapshot.format_version`, and `Snapshot.engine_version` are written explicitly.

Backward compatibility begins after the storage format is declared stable. Until then, breaking changes must bump `format_version` and fail with a clear error instead of silently replaying incompatible data.

Experimental server-mode storage is less stable than embedded storage. Treat it
as disposable during `0.1.x` testing unless a release note explicitly says a
format is compatible.

## OpenRaft Segmented Store

The distributed store is file-backed and dependency-light.

- `manifest.json` records the storage format version, active log segments,
  active snapshot metadata, and last purged log id.
- `vote.json` persists the current Raft vote.
- `committed.json` persists the committed log id.
- `state.json` persists the current applied state machine.
- `log/*.seg` files contain append-only binary records.

Each log record contains:

- magic bytes;
- format version;
- Raft term;
- Raft index;
- payload length;
- serialized entry payload;
- checksum.

Only a truncated final log record may be ignored during recovery. A bad magic
value, checksum mismatch, unsupported version, or corruption in the middle of a
segment is fatal.

The current `0.1` implementation writes a segmented on-disk format and tests
reopen, purge, truncate, corruption, and truncated-tail behavior. It is not yet
claiming the same operational maturity as an etcd/RocksDB-style production log
store.

## Snapshot + AOF Tail

After compaction, `kron.snapshot` contains the complete derived state and `kron.aof` is recreated. Startup loads the snapshot and replays `kron.aof` from `last_aof_offset`.

Only a truncated final AOF line may be ignored. Corruption in the middle of the log is fatal.
