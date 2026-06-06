---
title: "ADR 0004: Lease-based primary-copy serialization"
adr: 4
status: accepted
updated: 2026-05-31
---

# ADR 0004: Lease-based primary-copy serialization

## Status

Accepted. The mechanism that keeps the master off the write path.

## Context

Mutations to a chunk must be applied in the same order on all replicas. The naive design routes every mutation through the master to assign order — but that makes the master a per-write bottleneck.

## Decision

The master grants a **60-second lease** on a chunk to one replica (the **primary**). For the lease window, that primary assigns serial numbers to all mutations and forwards them to the secondaries. There is **one consensus per lease, not one per write**. The primary requests renewal by piggybacking on its heartbeats; a silent primary simply loses the lease when it expires.

## Consequences

**Positive:**
- The master is consulted once per lease, then is off the write path entirely — the central scalability move.
- Lease expiry is a natural failure detector: a dead primary's lease lapses and the master can grant a new one safely.

**Negative:**
- A 60 s window where a partitioned-but-alive primary could still believe it holds the lease; bounded by the lease duration and chunk version numbers.

## Alternatives considered

- **Master assigns order per write** — rejected: master becomes the bottleneck.
- **Leaderless / quorum writes** — rejected: that is RADOS/Dynamo territory, a later paper in the arc.

## Related

- Module: [`gfs-master`](../modules/gfs-master.md)
- Value type: `LeaseToken` in [`gfs-common`](../modules/gfs-common.md)
- Flow: [`../flows/write-path.md`](../flows/write-path.md)
