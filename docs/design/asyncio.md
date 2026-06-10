# Asyncio

Kron v0.1 exposes a synchronous Python API with a Rust background runtime, plus
small asyncio wrappers for applications that already run an event loop.

Implemented wrapper API:

```python
await kron.astart(data_dir=".kron")
await kron.ashutdown(timeout=5.0)
await kron.astatus("email_digest")
await kron.alist()
```

The wrappers delegate to the synchronous API with `asyncio.to_thread()`. This
keeps the current callback model unchanged and avoids blocking the Python event
loop.

Native async callbacks are still deferred because they change callback execution
semantics, cancellation behavior, and shutdown guarantees.
