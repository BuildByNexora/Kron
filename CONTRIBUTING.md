# Contributing to Kron

Kron is currently an embedded-first alpha project.

The main stable path for `0.1.x` is:

- Rust core engine;
- Python embedded API;
- CLI observe/admin commands;
- local storage, IPC, snapshot, and compaction.

Distributed server mode is experimental. Contributions there are welcome, but they need tests that cover failure behavior.

## Development Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -U pip maturin pytest twine
.venv/bin/maturin develop
```

## Required Checks

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
.venv/bin/python -m pytest -q tests/python
```

## Contribution Guidelines

- Keep embedded mode simple and dependency-light.
- Do not promise exactly-once side effects.
- Add tests for scheduler, persistence, crash recovery, and retries.
- Keep distributed-mode changes clearly marked as experimental unless they include multi-node failure tests.
- Do not commit generated files such as `target/`, `.venv/`, `.kron/`, or `.pytest_cache/`.
