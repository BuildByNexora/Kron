# Changelog

All notable changes to Kron will be documented in this file.

Kron follows semantic versioning after the first stable release. During `0.1.x`, storage and distributed-mode APIs are still allowed to change.

## 0.1.0-alpha.1 - Unreleased

### Added

- Rust embedded scheduling engine.
- Python bindings via PyO3.
- `kron.schedule`, `kron.start`, `kron.shutdown`, `kron.status`, and `kron.list`.
- Append-only event log with crash-tail handling.
- Snapshot and compaction support.
- Data directory locking.
- CLI observe/admin commands.
- Local IPC for active runtime inspection.
- Python integration tests.
- Experimental OpenRaft-backed server mode.
- Experimental Python `Client` and `Worker` APIs.

### Known Limitations

- Distributed mode is experimental.
- Storage format is not stable before v1.0.
- Async Python API is not implemented yet.
- PyPI release is not published yet.
