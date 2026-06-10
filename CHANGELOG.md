# Changelog

All notable changes to Kron will be documented in this file.

Kron follows semantic versioning after the first stable release. During `0.1.x`, storage and distributed-mode APIs are still allowed to change.

## 0.1.2-alpha.1 - Unreleased

### Changed

- Improved README structure with a user-friendly quickstart and a separate technical overview.
- Added Ubuntu/Debian `externally-managed-environment` troubleshooting for `pip`.
- Documented the current PyPI package name more clearly: install `kron-scheduler`, import `kron`.
- Stabilized Python timing tests on Windows by avoiding sub-second scheduling assumptions.
- Cleaned up platform-specific Raft directory syncing for Windows CI.

### Verification

- GitHub Actions CI passes for Rust, Python, and wheel builds across Linux, macOS, and Windows.

## 0.1.1 - 2026-06-10

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
- Async Python wrapper API: `astart`, `ashutdown`, `astatus`, and `alist`.
- Python callback context with `timer_id` and `run_id`.
- Segmented OpenRaft file store tests and cross-platform wheel checks.

### Known Limitations

- Distributed mode is experimental.
- Storage format is not stable before v1.0.
- Native async Python callbacks are not implemented yet.
