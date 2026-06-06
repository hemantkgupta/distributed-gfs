---
title: "gfs-master — module guide"
module: gfs-master
phase: 2
updated: 2026-05-31
---

# `gfs-master`

## 1. Role

The single, authoritative GFS master — the control plane. Holds **all** metadata in RAM: namespace, chunk-to-replica map, chunkserver registry, lease state. It hands clients chunk locations and lets them talk to chunkservers directly; it never touches chunk payload bytes. Durability comes from `gfs-oplog`.

## 2. Plan anchor

- §1.1 — Single in-memory master
- §5.2 — Master state (four in-memory maps)
- §6.4 — Lease grant / renew / revoke
- §6.5–6.6 — Heartbeat handling + re-replication
- §6.8 — Master crash recovery
- §6.9 — Shadow master
- ADR 0001 — in-memory master

## 3. Public API surface

| Class | Kind | Purpose |
|---|---|---|
| `Master` | class | the long-running master; namespace ops, chunk allocation, lease grant, heartbeat handling, reaper |
| `MasterConfig` | record/class | tunables: replication factor, lease duration, heartbeat thresholds, paths |
| `MasterRpcServer` | class | socket front end; frames requests, dispatches to `Master`, writes responses |
| `ChunkserverState` | record | per-chunkserver runtime state: last heartbeat, disk free, reported chunks |
| `ShadowMaster` | class | tails the live op log; serves read-only metadata with a "may be stale" marker |
| `MutableClock` | class | injectable clock so tests can advance time (lease expiry, GC retention) deterministically |

## 4. Internal structure

`Master` keeps four `ConcurrentHashMap`s — `namespace` (`String → NamespaceEntry`, a flat map, not a tree), `chunkMap` (`ChunkHandle → ChunkMetadata`), `chunkservers` (`ChunkserverId → ChunkserverState`), and `leases` (`ChunkHandle → LeaseToken`) — plus monotonic counters (`nextChunkHandle`, `opLogSequence`). Every mutation is written to `gfs-oplog` (`fsync`) before the client is acked. A reaper loop scans for under-replicated chunks (priority: 1 replica before 2), dead chunkservers (missed-heartbeat threshold), and GC-eligible deleted files, emitting `CopyCommand` / `DeleteChunk` instructions piggybacked on `HeartbeatAck`. On boot, the master loads the latest checkpoint then replays op-log entries past it. `MutableClock` makes lease expiry and the 3-day GC window testable without real waiting.

## 5. Key tests

| Test | What it locks down |
|---|---|
| `MasterTest` | namespace create/stat/delete; chunk allocation respects replication factor; lease grant → renew → expire; heartbeat registration; missed-heartbeat → dead-marking; re-replication assignment appears in heartbeat ack; checkpoint + replay restores state |

## 6. Where it fits

```
gfs-common ─┐
gfs-oplog ──┴─► gfs-master ──► gfs-simulator
```

Control plane. Depends on `gfs-common` + `gfs-oplog`. Never depends on `gfs-chunkserver` — the planes meet only at the wire format.

## 7. Stubs and departures from production

- **No master election.** Bring-up is manual / watchdog-driven; Chubby (the paper's election service) is out of scope. The shadow master is for stale reads, not automatic failover.
- **No rack/datacenter placement awareness** — replica selection picks healthy chunkservers but doesn't model topology.
- **GC** (§6.7) implements deleted-file retention; the full orphan-sweep-over-heartbeat may be simplified relative to the plan.

## 8. Related

- Module index: [`README.md`](./README.md)
- Flows: [`../flows/re-replication.md`](../flows/re-replication.md), [`../flows/master-recovery.md`](../flows/master-recovery.md)
- ADRs: [0001 in-memory master](../decisions/0001-in-memory-master.md), [0004 lease-based primary-copy](../decisions/0004-lease-based-primary-copy.md)
