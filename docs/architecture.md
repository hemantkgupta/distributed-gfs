---
title: "Architecture — distributed-gfs"
type: architecture
updated: 2026-06-07
---

# Architecture

The big-picture map of this Java repo: how the modules fit together, how data flows, and how the implementation plan's decisions map to packages and classes.

## The shape in one paragraph

A client asks the **master** where a chunk lives. The master — which holds all metadata in RAM and never touches payload bytes — returns the chunk handle, its replica set, and (for writes) the current lease-holding **primary**. The client then talks directly to **chunkservers**: it pushes bytes down a **daisy chain** (`client → A → B → C`), then sends a commit to the primary, which assigns a serial number and forwards an apply to the secondaries. Durability on the master side comes from an **operation log** (`fsync` before ack) plus periodic **checkpoints**; durability on the chunkserver side comes from `fsync`'d chunk files with per-64 KB **CRC32C** sidecars. Background reaper loops re-replicate under-replicated chunks and garbage-collect deleted ones. Every module is pure Java with in-memory or local-disk substrates.

## Module dependency graph

```
                       ┌─────────────┐
                       │ gfs-common  │  ← value types + wire format, no deps
                       └──────┬──────┘
                              │ (everyone depends on it)
        ┌─────────────────────┼──────────────────────┬───────────────┐
        │                     │                      │               │
  ┌─────▼─────┐        ┌──────▼──────┐        ┌──────▼──────┐  ┌──────▼──────┐
  │ gfs-oplog │        │gfs-replication│      │ gfs-client  │  │ (others …)  │
  └─────┬─────┘        └──────┬──────┘        └─────────────┘  └─────────────┘
        │                     │
  ┌─────▼─────┐        ┌──────▼────────┐
  │ gfs-master│        │ gfs-chunkserver│
  └───────────┘        └───────────────┘

  ┌──────────────┐
  │ gfs-simulator│  ← depends on ALL others (integration harness only)
  └──────────────┘
```

The architectural invariant: **control-plane modules never depend on data-plane modules, and vice versa.** `gfs-master` (control) depends on `gfs-oplog` + `gfs-common`; `gfs-chunkserver` (data) depends on `gfs-replication` + `gfs-common`. The two planes meet only at the wire format defined in `gfs-common`, and only `gfs-simulator` is allowed to depend on everything.

## How plan decisions map to code

| Plan decision (§ of the plan) | Module | Key class |
|---|---|---|
| Single in-memory master (§1.1) | `gfs-master` | `Master` |
| Op log + checkpoint durability (§7) | `gfs-oplog` | `OperationLog`, `Checkpoint` |
| Op-log entry ADT (§5.4) | `gfs-oplog` | `LogEntry` (sealed interface) |
| 64 MB chunks (§1.1) | `gfs-common` | `GfsConstants` / chunk geometry |
| Lease-based primary-copy (§6.4) | `gfs-master` | `LeaseToken`, lease map in `Master` |
| Pipelined daisy-chain (§6.2) | `gfs-client` + `gfs-chunkserver` | `ChainReplicationDriver`, `ChunkserverRpcServer` |
| At-least-once record append (§6.3) | `gfs-chunkserver` | `Chunkserver` (offset pick + padding) |
| One chunk = one file + CRC32C (§7.3) | `gfs-chunkserver` | `ChunkStore` |
| Hand-rolled binary RPC (§4) | `gfs-common` | `WireCodec`, `WireMessage`, `MessageType` |
| Heartbeats + re-replication (§6.5–6.6) | `gfs-master` | `Master` reaper, `ChunkserverState` |
| Shadow master (§6.9) | `gfs-master` | `ShadowMaster` |
| Master crash recovery (§6.8) | `gfs-master` + `gfs-oplog` | `Master` boot + `OperationLog.replay` |

## Planes

The system separates into planes, colour-coded the way the wiki/blog diagrams are:

- **Client plane** — `gfs-client` (`GfsClient`, `MetadataCache`, `OffsetMath`, `ChainReplicationDriver`).
- **Control plane** — `gfs-master` (namespace, chunk map, lease manager, reaper, shadow). Watches and decides; never moves payload bytes.
- **Data plane** — `gfs-chunkserver` (`ChunkStore`, chain participation). Moves and persists bytes.
- **Foundation** — `gfs-common` (value types + wire format), `gfs-oplog` (durability primitives), `gfs-replication` (chain-node placeholder).

## Key sequences

See [`flows/`](./flows/) for detailed sequence diagrams. The headline flows:

1. **Write path** — `flows/write-path.md`
2. **Record append** — `flows/record-append.md`
3. **Read path** — `flows/read-path.md`
4. **Re-replication** — `flows/re-replication.md`
5. **Master recovery** — `flows/master-recovery.md`

## Build & test

- Java 17, Gradle multi-module. 7 modules, 12 test classes, 63 tests.
- `./gradlew build` — compiles + runs all tests.
- `./gradlew :gfs-simulator:test` — end-to-end scenarios.
- See [`getting-started.md`](./getting-started.md).

## Where the bodies are buried (gotchas)

- **`gfs-replication` is an empty placeholder.** The plan (§3.3) put `ChainNode` / `MutationBatch` / `MutationApplier` here; in the built code the daisy-chain logic actually lives in `gfs-chunkserver` (`ChunkserverRpcServer`) and `gfs-client` (`ChainReplicationDriver`). The module is kept on the dependency graph so the intended seam is visible. See [`modules/gfs-replication.md`](./modules/gfs-replication.md).
- **Namespace is a flat `Map<String, NamespaceEntry>`** in `Master`, not a tree — exactly as GFS does it. Directory operations are prefix operations on path strings.
- **The master is off the write path** once a lease is granted. A common misread is to expect the master to mediate each write; it does not. That is the whole point of leases (ADR 0004).
- **Padding and duplicates are visible** to the application after record append, by design (ADR 0003). Readers must dedupe via record IDs.
