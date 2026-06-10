# Asyncio

Kron v0 intentionally exposes a synchronous Python API with a Rust background runtime.

Native asyncio support is deferred because it changes callback execution semantics, cancellation behavior, and shutdown guarantees. A future API should add explicit async entrypoints such as `await kron.astart()` and async callback support without changing the current synchronous API.
