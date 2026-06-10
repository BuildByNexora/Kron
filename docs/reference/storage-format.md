# Storage Format

Kron storage is pre-stable in `0.1.x`.

## Files

- `kron.aof`: newline-delimited JSON `LogEntry` records.
- `kron.snapshot`: JSON checkpoint of derived engine state.
- `kron.aof.old`: previous AOF after compaction.
- `kron.lock`: exclusive writer lock.
- `kron.token`: local IPC token.
- `kron.sock` / `kron.port`: local IPC endpoint metadata.

## Versioning

`LogEntry.v`, `Snapshot.v`, `Snapshot.format_version`, and `Snapshot.engine_version` are written explicitly.

Backward compatibility begins after the storage format is declared stable. Until then, breaking changes must bump `format_version` and fail with a clear error instead of silently replaying incompatible data.

## Snapshot + AOF Tail

After compaction, `kron.snapshot` contains the complete derived state and `kron.aof` is recreated. Startup loads the snapshot and replays `kron.aof` from `last_aof_offset`.

Only a truncated final AOF line may be ignored. Corruption in the middle of the log is fatal.
