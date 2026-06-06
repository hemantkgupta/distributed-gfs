---
title: "Flows — index"
type: flows-index
updated: 2026-05-31
---

# Flows

End-to-end sequences that cross multiple modules. Each page has a Mermaid sequence diagram, a step-by-step walkthrough, and a failure-modes table.

## Index

| Flow | What it shows | Modules |
|---|---|---|
| [`write-path.md`](./write-path.md) | Byte-range write: locate → lease → PushBytes daisy chain → CommitWrite | client, master, chunkserver |
| [`record-append.md`](./record-append.md) | At-least-once append: primary picks offset, padding, RETRY_ON_NEXT_CHUNK | client, master, chunkserver |
| [`read-path.md`](./read-path.md) | Locate → ReadChunk against nearest replica → CRC32C verify | client, master, chunkserver |
| [`re-replication.md`](./re-replication.md) | Chunkserver death → reaper detects under-replication → CopyChunk | master, chunkserver |
| [`master-recovery.md`](./master-recovery.md) | Master crash → checkpoint load → op-log replay → resume | master, oplog |

## The two write APIs

GFS has two ways to put bytes into a chunk, and they behave differently:

- **Byte-range write** (`write-path.md`) — the client specifies the offset. Used rarely in production.
- **Record append** (`record-append.md`) — the *primary* picks the offset (the chunk's current end). At-least-once; padding and duplicates are visible. This is what MapReduce and crawls actually use.

Read both to understand why GFS optimizes for append.
