---
title: "ADR 0008: Op-log fsync before client ack"
adr: 8
status: accepted
updated: 2026-05-31
---

# ADR 0008: Op-log fsync before client ack

## Status

Accepted. The single non-negotiable durability invariant.

## Context

The master mutates in-memory state on every namespace/allocation operation. If it acknowledges a client before the mutation is durable and then crashes, recovery would silently lose an acked operation — the worst kind of data loss.

## Decision

Every metadata mutation is appended to the operation log and **`fsync`'d before the master replies** to the client RPC that triggered it. Durability precedes the ack, always.

## Consequences

**Positive:**
- Crash recovery (ADR 0001) is correct by construction: every acked mutation is on disk and gets replayed.
- Simple mental model — "acked means durable."

**Negative:**
- Each mutating RPC pays one `fsync` of latency. GFS amortizes this by batching several log records per `fsync` when operations queue up; the workload is append-heavy, not metadata-churn-heavy, so this is rarely the bottleneck.

## Alternatives considered

- **Async / periodic flush** — rejected: opens a window where acked operations vanish on crash.
- **Group commit only (no per-op guarantee)** — partially adopted: batching is fine *as long as* the ack waits for the batch's `fsync`. The invariant is "ack after durable," not "one fsync per op."

## Related

- Module: [`gfs-oplog`](../modules/gfs-oplog.md) (`OperationLog`)
- Flow: [`../flows/master-recovery.md`](../flows/master-recovery.md)
