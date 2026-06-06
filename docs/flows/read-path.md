---
title: "Flow: read path"
flow: read-path
modules: [gfs-client, gfs-master, gfs-chunkserver]
updated: 2026-05-31
---

# Flow: Read Path

How `GfsClient.read(path, offset, size)` turns into bytes. Reads are side-effect-free: they touch no master state and no chunkserver state.

## Sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master
    participant S as Nearest replica

    C->>M: GetChunkLocations(path, offset, size)
    M-->>C: handle, version, replicas[A,B,C]
    Note over C: cache (path, chunkIndex) → locations
    C->>S: ReadChunk(handle, offsetInChunk, size)
    Note over S: read bytes; recompute CRC32C<br/>over spanning 64 KB blocks; compare
    S-->>C: bytes + status OK
```

## Steps

1. **Offset math** — `OffsetMath` maps the file offset to (chunk index, offset-in-chunk). A read that straddles a 64 MB boundary is split into per-chunk reads.
2. **Locate (cached)** — the client checks `MetadataCache` first; on a miss it calls `GetChunkLocations`. The cache TTL is aligned to the lease window, so repeated reads of one file cause a single master RPC.
3. **Read nearest** — the client reads directly from the nearest replica (`ChainReplicationDriver`'s distance ordering applies to reads too).
4. **Verify** — the chunkserver recomputes CRC32C over the spanning 64 KB blocks and compares against the sidecar before returning bytes.

## State mutations

None — on master or chunkservers. Read is pure.

## Failure modes

| Failure | What happens |
|---|---|
| Stale cache (chunk moved) | chunkserver returns `CHUNK_NOT_FOUND`; client invalidates cache, re-fetches from master |
| CRC32C mismatch | chunkserver returns `CHECKSUM_FAILURE` and reports corruption; client retries against a different replica; master re-replicates the bad one |
| Read past EOF | master returns `found=false`; client gets a short/empty read |

## Related

- Modules: [`gfs-client`](../modules/gfs-client.md), [`gfs-chunkserver`](../modules/gfs-chunkserver.md)
- Companion: [`write-path.md`](./write-path.md)
