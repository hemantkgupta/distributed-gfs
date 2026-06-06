---
title: "ADR 0002: 64 MB chunks"
adr: 2
status: accepted
updated: 2026-05-31
---

# ADR 0002: 64 MB chunks

## Status

Accepted. The paper's value, used verbatim.

## Context

Files are split into fixed-size chunks, each independently placed and replicated. The chunk size trades two scarce resources against each other: master metadata (smaller chunks → more chunks → more RAM) and parallelism / client-master chatter (larger chunks → fewer placement choices, more contention on a hot chunk).

## Decision

Use **64 MB** chunks (`CHUNK_SIZE_BYTES = 67108864`), with CRC32C verification at **64 KB** block granularity (1024 blocks per chunk).

## Consequences

**Positive:**
- Master metadata stays tiny — a few dozen bytes per chunk means a full cluster's chunk map fits in RAM (ADR 0001 depends on this).
- A client doing a large sequential read/append talks to the master once, then streams from a chunkserver for a long time — minimal master load.

**Negative:**
- Small files waste a chunk's worth of placement granularity and can create hot spots (many clients on one chunk). GFS accepts this because its workload is large append-mostly files.

## Alternatives considered

- **16 MB** — more parallelism but ~4× the master metadata; rejected.
- **256 MB** — even less metadata but worse hot-spotting and coarser placement; rejected.

## Related

- Module: [`gfs-common`](../modules/gfs-common.md) (geometry), [`gfs-chunkserver`](../modules/gfs-chunkserver.md)
