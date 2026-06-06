---
title: "ADR 0003: At-least-once record append"
adr: 3
status: accepted
updated: 2026-05-31
---

# ADR 0003: At-least-once record append

## Status

Accepted. The defining semantic choice of GFS.

## Context

The dominant workload is many producers concurrently appending records to one file (crawl output, MapReduce intermediates). Offering exactly-once, in-order append across replicas would require a consensus round per record — far too expensive at GFS's throughput.

## Decision

`recordAppend` is **at-least-once**. The primary picks the offset (the chunk's current end), and on any failure the client retries. Retries may produce **duplicates** at different offsets, and chunk **padding** (zero regions) is left visible when a record won't fit. The system makes these visible to the application rather than hiding them.

## Consequences

**Positive:**
- No per-record consensus; appends run at chain bandwidth.
- Concurrent appenders are serialized cheaply by the single primary.

**Negative:**
- Applications must carry a unique **record ID + checksum** and **dedupe + skip padding** on read. The correctness burden moves up the stack.

## Alternatives considered

- **Exactly-once append** — rejected: needs consensus per record; defeats the throughput goal.
- **Client-specified offsets for append** — rejected: reintroduces coordination between concurrent appenders.

## Related

- Flow: [`../flows/record-append.md`](../flows/record-append.md)
- Modules: [`gfs-client`](../modules/gfs-client.md), [`gfs-chunkserver`](../modules/gfs-chunkserver.md)
