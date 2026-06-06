---
title: "Code companion — implementation plan § → code"
type: code-companion
updated: 2026-06-07
---

# Code Companion

Maps each section of the implementation plan (`~/CSE-Raw/raw-blog/distributed-file-system/gfs-implementation-plan.md`) to the modules and key classes that realize it — and names the explicit gaps where the plan describes a mechanism the code does not (yet) implement.

## Plan § → code

| Plan § | Topic | Module(s) | Key classes | Status |
|---|---|---|---|---|
| §3 | Repo layout (7 modules) | all | — | ✅ built (see departure on `gfs-replication`) |
| §4 | Wire format (framing, message types) | `gfs-common` | `WireMessage`, `WireCodec`, `MessageType` | ✅ built |
| §5.1 | Value types | `gfs-common` | `ChunkHandle`, `ChunkVersion`, `ChunkserverId`, `FilePath`, `Bytes`, `LeaseToken` | ✅ built |
| §5.2 | Master state | `gfs-master` | `Master` (4 in-memory maps), `ChunkMetadata`, `NamespaceEntry`, `ChunkserverState` | ✅ built |
| §5.3 | Chunkserver state | `gfs-chunkserver` | `Chunkserver`, `ChunkStore` | ✅ built |
| §5.4 | Op-log entries (sealed ADT) | `gfs-oplog` | `LogEntry` | ✅ built |
| §6.1 | Client read | `gfs-client` + `gfs-chunkserver` | `GfsClient.read`, `OffsetMath`, `ChunkserverRpcServer` | ✅ built — see [`flows/read-path.md`](./flows/read-path.md) |
| §6.2 | Client write (byte-range) | `gfs-client` + `gfs-chunkserver` | `GfsClient.write`, `ChainReplicationDriver` | ✅ built — see [`flows/write-path.md`](./flows/write-path.md) |
| §6.3 | Record append | `gfs-client` + `gfs-chunkserver` | `GfsClient.recordAppend`, `Chunkserver` | ✅ built — see [`flows/record-append.md`](./flows/record-append.md) |
| §6.4 | Lease grant / renew / revoke | `gfs-master` | `Master` lease map, `GrantLease`, `RevokeLease` | ✅ built |
| §6.5 | Chunkserver heartbeat | `gfs-master` + `gfs-common` | `Heartbeat`, `HeartbeatAck`, `Master` handler | ✅ built |
| §6.6 | Re-replication | `gfs-master` | `Master` reaper, `CopyCommand`, `CopyChunk` | ✅ built — see [`flows/re-replication.md`](./flows/re-replication.md) |
| §6.7 | Garbage collection | `gfs-master` | `Master` (deleted-file retention) | ⚠️ partial — see gaps below |
| §6.8 | Master crash recovery | `gfs-master` + `gfs-oplog` | `Master` boot, `OperationLog.replay`, `Checkpoint.load` | ✅ built — see [`flows/master-recovery.md`](./flows/master-recovery.md) |
| §6.9 | Shadow master | `gfs-master` | `ShadowMaster` | ✅ built |
| §7.1 | Operation log file format | `gfs-oplog` | `OperationLog` | ✅ built |
| §7.2 | Checkpoint format | `gfs-oplog` | `Checkpoint` | ✅ built |
| §7.3 | Chunk file format (CRC32C sidecar) | `gfs-chunkserver` | `ChunkStore` | ✅ built |
| §8 | Phasing (21 CPs) | all | — | ✅ shipped as a single consolidated repo (not per-CP commits) |
| §11 | Test plan | all | 12 test classes, 63 tests | ✅ unit coverage; integration in `gfs-simulator` |

## Gaps (plan describes it, code does not fully implement it)

| Plan claim | Code reality | Action |
|---|---|---|
| §3.3 `gfs-replication` holds `ChainNode` / `MutationBatch` / `MutationApplier` | `gfs-replication` is an empty placeholder; chain logic lives in `gfs-chunkserver` + `gfs-client` | Either populate the module or fold it away; documented in [`modules/gfs-replication.md`](./modules/gfs-replication.md) |
| §6.7 two-phase GC with 3-day retention + chunk orphan cleanup over heartbeats | Deleted-file retention exists; full orphan-sweep-over-heartbeat may be simplified | Confirm against `MasterTest`; expand if a GC integration test is wanted |
| §8 21 checkpoints, green build at each | Repo shipped as one consolidated commit | Cosmetic; history could be replayed as phase commits if desired |
| package root `com.hkg.gfs.*` (sibling-repo convention) | actual root is `gfs.*` | See [ADR 0009](./decisions/0009-java17-gradle-multimodule.md); rename is a mechanical follow-up |

## Related

- [`architecture.md`](./architecture.md) — the decision → code table at a glance
- [`modules/README.md`](./modules/README.md) — per-module detail
