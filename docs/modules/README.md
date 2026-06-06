---
title: "Modules — index"
type: modules-index
updated: 2026-05-31
---

# Modules

One page per module. Each follows the same 7-section template: Role → Plan anchor → Public API surface → Internal structure → Key tests → Where it fits → Stubs and departures from production.

## Index

| Module | Phase | Role |
|---|---|---|
| [`gfs-common`](./gfs-common.md) | 1 | Value types + hand-rolled binary wire format |
| [`gfs-oplog`](./gfs-oplog.md) | 1 | Operation log + checkpoint (master durability) |
| [`gfs-master`](./gfs-master.md) | 2 | Single in-memory master: namespace, chunk map, leases, reaper |
| [`gfs-chunkserver`](./gfs-chunkserver.md) | 3 | CRC32C chunk store, heartbeats, lease holder, chain participation |
| [`gfs-replication`](./gfs-replication.md) | 3 | Chain-replication node (placeholder — see page) |
| [`gfs-client`](./gfs-client.md) | 4 | Client library: read / write / recordAppend, metadata cache |
| [`gfs-simulator`](./gfs-simulator.md) | 5 | Single-JVM cluster harness + fault injection + integration tests |

## Dependency rule

`gfs-common` is the leaf — everyone depends on it, it depends on nothing. Control-plane (`gfs-master`) and data-plane (`gfs-chunkserver`) never depend on each other; they meet only at the `gfs-common` wire format. `gfs-simulator` is the only module allowed to depend on everything. See [`../architecture.md`](../architecture.md) for the full graph.
