---
title: "ADR 0006: One chunk = one file (no LSM / embedded KV)"
adr: 6
status: accepted
updated: 2026-05-31
---

# ADR 0006: One chunk = one file on the local filesystem

## Status

Accepted. Paper-faithful; deliberately *not* what later storage backends do.

## Context

A chunkserver must persist 64 MB chunks and verify their integrity. Modern object stores (BlueStore) use a log-structured / embedded-KV backend; GFS predates all of that.

## Decision

Store each chunk as a plain file on the local filesystem: `<handle>.chunk` for data, `<handle>.crc` for the per-64 KB-block CRC32C sidecar. Writes seek to an offset, write, recompute the touched blocks' CRCs, and `fsync` both files.

## Consequences

**Positive:**
- Dead simple; leans on the local FS for naming, allocation, and caching.
- Easy to inspect and reason about — a chunk is literally a file.

**Negative:**
- Inherits the local FS's small-write and metadata overheads, and double-journaling (FS journal + our durability). This is precisely the pain BlueStore later removed by going to raw block — the next-but-one paper in the arc.

## Alternatives considered

- **LSM / embedded RocksDB per chunkserver** — rejected: anachronistic for GFS and obscures the design being studied (it's BlueStore's answer, ADR territory for that repo).
- **Raw block device** — rejected for the same reason; also needs JNI (ADR 0010).

## Related

- Module: [`gfs-chunkserver`](../modules/gfs-chunkserver.md) (`ChunkStore`)
