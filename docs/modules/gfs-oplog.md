---
title: "gfs-oplog — module guide"
module: gfs-oplog
phase: 1
updated: 2026-05-31
---

# `gfs-oplog`

## 1. Role

The master's durability boundary. An append-only operation log of metadata mutations plus periodic full-state checkpoints. Together they let a crashed master rebuild its entire in-memory state. The master writes here before acking any client mutation.

## 2. Plan anchor

- §5.4 — Op-log entries (sealed ADT)
- §7.1 — Operation log file format
- §7.2 — Checkpoint format
- ADR 0008 — op-log `fsync` before client ack

## 3. Public API surface

| Class | Kind | Purpose |
|---|---|---|
| `OperationLog` | class | `append(LogEntry)` (with `fsync` semantics), `replay(callback)` over all entries since a sequence |
| `Checkpoint` | class | `write(state)` serializes the master's full in-memory state to one file; `load()` rehydrates it |
| `LogEntry` | sealed interface | exhaustive set of mutation kinds: `CreateFile`, `DeleteFile`, `Mkdir`, `AllocateChunk`, `SetChunkReplicas`, `GrantLease`, `RenewLease`, `RevokeLease`, `MarkReplicaStale`, `DeleteChunk` |

## 4. Internal structure

`OperationLog` is a length-prefixed append-only file writer/reader (`length | type | seq | payload`). `append` calls `fsync` before returning so durability precedes the client ack — the single non-negotiable invariant of the design. `Checkpoint` writes a sectioned binary file (namespace / chunk map / leases), `fsync`s a `.tmp`, then renames atomically. `LogEntry` is a `sealed interface` so adding a new mutation kind forces every replay site to be updated at compile time — the Java equivalent of an exhaustive algebraic data type.

## 5. Key tests

| Test | What it locks down |
|---|---|
| `OperationLogTest` | append N entries → replay → get N entries back in order; entries survive a simulated restart |
| `CheckpointTest` | checkpoint a state → load → deep-equal the original state |

## 6. Where it fits

```
gfs-common ──► gfs-oplog ──► gfs-master
```

Depends only on `gfs-common`. Consumed by `gfs-master` for durability and recovery.

## 7. Stubs and departures from production

- **Op-log "replication" is local** — the plan's N remote mirror directories are simulated as directories on the same disk. The bytes-on-wire are identical; only the remote-ness is mocked.
- Checkpoint is a single file written by one thread under a snapshot lock; production GFS forks a copy-on-write snapshot to avoid pausing mutations.

## 8. Related

- Module index: [`README.md`](./README.md)
- Flow: [`../flows/master-recovery.md`](../flows/master-recovery.md)
- ADR: [0008 op-log fsync before ack](../decisions/0008-oplog-fsync-before-ack.md)
