# Snapshot And Compaction

`kron.aof` is the append-only source of recent events.

`kron.snapshot` is a JSON checkpoint of derived state. Compaction writes `kron.snapshot.tmp`, fsyncs it, renames it atomically, rotates `kron.aof` to `kron.aof.old`, and creates a new empty AOF.

Startup prefers `kron.snapshot` when present and falls back to full AOF replay when no snapshot exists.
