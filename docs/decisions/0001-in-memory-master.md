---
title: "ADR 0001: In-memory master with op log + checkpoint"
adr: 1
status: accepted
updated: 2026-05-31
---

# ADR 0001: In-memory master with op log + checkpoint

## Status

Accepted. Faithful to the 2003 GFS paper.

## Context

The master owns all metadata: namespace, file→chunk maps, chunk→replica maps, lease state, chunkserver registry. These are read on every client request and mutated on every namespace/allocation operation. The design must be fast enough that a single master serves a whole cluster.

## Decision

Keep **all** metadata in the master's RAM, in plain `ConcurrentHashMap`s. Get durability from an **operation log** (append-only, `fsync`'d before ack) plus periodic **checkpoints** (full-state snapshots). On crash, load the latest checkpoint and replay the log tail.

## Consequences

**Positive:**
- Metadata operations are RAM-speed; no KV store on the hot path.
- Recovery is simple and fast (checkpoint load + short replay).
- The design is easy to reason about — one authoritative copy.

**Negative:**
- A structural ceiling: total metadata must fit in one machine's RAM (~tens of PB / ~100 M files for 64 MB chunks and ~64 bytes/chunk). This is exactly the ceiling Colossus later removed by sharding the master.
- Single point of failure; recovery causes a brief unavailability window.

## Alternatives considered

- **Sharded master** — rejected here; that is the Colossus implementation plan, a separate paper in the arc.
- **Embedded KV (RocksDB) for metadata** — rejected: predates GFS and obscures the op-log + checkpoint pattern that is the whole point of studying this design.

## Related

- Module: [`gfs-master`](../modules/gfs-master.md), [`gfs-oplog`](../modules/gfs-oplog.md)
- Flow: [`../flows/master-recovery.md`](../flows/master-recovery.md)
