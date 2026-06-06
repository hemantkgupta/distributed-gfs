---
title: "ADR 0005: Pipelined daisy-chain replication"
adr: 5
status: accepted
updated: 2026-05-31
---

# ADR 0005: Pipelined daisy-chain replication

## Status

Accepted. The bandwidth argument behind GFS's write throughput.

## Context

A write must reach three replicas. The topology that delivers the bytes determines how the available network bandwidth is used.

## Decision

Push bytes down a **daisy chain**: `client → A → B → C`. Each node receives, buffers in RAM, and immediately forwards to the next while still receiving. Data flow (the bytes) is decoupled from control flow (the serial-number assignment, which goes only to the primary).

## Consequences

**Positive:**
- Each machine's **outbound** NIC is saturated independently. The primary forwards to exactly one downstream node, not to two — its uplink isn't divided.
- Chain order is chosen by network distance (nearest first), minimizing latency.

**Negative:**
- Latency is the sum of per-hop forwarding delays, not a single broadcast hop — but pipelining hides most of it for large payloads.

## Alternatives considered

- **Star / broadcast** (primary sends to B and C directly) — rejected: the primary's uplink is split in half, halving throughput.
- **Client sends to all three** — rejected: the client's uplink becomes the bottleneck and it pays 3× egress.

## Related

- Flow: [`../flows/write-path.md`](../flows/write-path.md)
- Modules: [`gfs-client`](../modules/gfs-client.md) (`ChainReplicationDriver`), [`gfs-chunkserver`](../modules/gfs-chunkserver.md)
- See also: [`gfs-replication`](../modules/gfs-replication.md) (intended home of the chain node)
