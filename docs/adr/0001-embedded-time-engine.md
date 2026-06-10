# ADR 0001: Embedded Time Engine First

## Status

Accepted

## Decision

Kron starts as an embedded time engine.

The server is a deployment mode, not the product.

## Context

Kron's strongest promise is not that it can run as another scheduler daemon. Its strongest promise is that application time can become reliable and observable inside the application process, with no broker, no external database, and no worker stack.

This means the first usable version must behave more like SQLite than like a cloud scheduler.

## Consequences

- The core engine is implemented as an embeddable Rust crate.
- The first public API targets Python through PyO3.
- `kron.start()` always starts a non-blocking background runtime in v0.
- Timer metadata and run history persist locally in a Kron data directory.
- Python functions are registered in memory on application startup and are not serialized into the event log.
- `kron-server` may exist later, but it must use the same core semantics as embedded mode.

## Non-goals

- No distributed mode in v0.
- No workflow DAGs.
- No native asyncio integration in v0.
- No server-first architecture.
