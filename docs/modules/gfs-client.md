---
title: "gfs-client — module guide"
module: gfs-client
phase: 4
updated: 2026-05-31
---

# `gfs-client`

## 1. Role

The client library. Exposes the public file API — `read`, `write`, `recordAppend`, `create`, `delete`, `mkdir`, `stat` — and hides the two-step "ask the master, then talk to chunkservers" dance, the per-file metadata cache, the offset math across 64 MB chunk boundaries, and the daisy-chain ordering.

## 2. Plan anchor

- §3.3 — client classes (`GfsClient`, `MetadataCache`, `OffsetMath`, `ChainReplicationDriver`)
- §6.1 — client read
- §6.2 — client write
- §6.3 — record append

## 3. Public API surface

| Class | Kind | Purpose |
|---|---|---|
| `GfsClient` | class | `read(path, offset, size)`, `write(path, offset, bytes)`, `recordAppend(path, bytes)`, `create/delete/mkdir/stat` |
| `MetadataCache` | class | caches chunk-handle + replica locations per (file, chunk index), TTL aligned to lease duration |
| `OffsetMath` | class | pure functions: `chunkIndex(offset)`, `offsetInChunk(offset)`, span across chunk boundaries |
| `ChainReplicationDriver` | class | orders the replica set into a daisy chain by topological distance to the client |

## 4. Internal structure

`GfsClient` asks the master for chunk locations (consulting `MetadataCache` first), then drives the data plane directly. For a write it picks a chain order via `ChainReplicationDriver`, sends `PushBytes` to the head of the chain, then `CommitWrite` to the primary. For record append it loops: push, commit, and on `RETRY_ON_NEXT_CHUNK` re-fetch the (now newly allocated) last chunk and retry — which is where at-least-once duplicates can arise. `OffsetMath` is the pure arithmetic that turns a file offset into a (chunk index, offset-in-chunk) pair and splits reads/writes that straddle a 64 MB boundary.

## 5. Key tests

| Test | What it locks down |
|---|---|
| `OffsetMathTest` | chunk index / offset-in-chunk arithmetic; boundary-straddling spans |
| `ChainReplicationDriverTest` | chain order picks nearest replica first |
| `MetadataCacheTest` | hit / miss / invalidation; repeated reads of one file cause one master RPC |
| `GfsClientTest` | read/write/recordAppend round-trips against in-process master + chunkservers |

## 6. Where it fits

```
gfs-common ──► gfs-client ──► gfs-simulator
                 (test scope: gfs-master, gfs-chunkserver, gfs-oplog)
```

Depends on `gfs-common` at compile time; pulls in `gfs-master` + `gfs-chunkserver` + `gfs-oplog` only as **test** dependencies for end-to-end client tests.

## 7. Stubs and departures from production

- Chain ordering uses a simple distance heuristic, not real network-topology measurement.
- The metadata cache TTL is aligned to the lease window; production clients also invalidate on explicit error responses, which this models on `CHUNK_NOT_FOUND`.

## 8. Related

- Module index: [`README.md`](./README.md)
- Flows: [`../flows/write-path.md`](../flows/write-path.md), [`../flows/record-append.md`](../flows/record-append.md), [`../flows/read-path.md`](../flows/read-path.md)
- ADR: [0003 at-least-once record append](../decisions/0003-at-least-once-record-append.md)
