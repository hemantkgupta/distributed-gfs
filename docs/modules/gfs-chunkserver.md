---
title: "gfs-chunkserver — module guide"
module: gfs-chunkserver
phase: 3
updated: 2026-05-31
---

# `gfs-chunkserver`

## 1. Role

The data plane. A chunkserver stores chunks as files on local disk, verifies them with CRC32C, participates in the daisy-chain replication of writes, holds leases granted by the master, and reports its state via periodic heartbeats. This is where payload bytes actually live.

## 2. Plan anchor

- §5.3 — Chunkserver state
- §6.1–6.3 — read / write / record-append participation
- §7.3 — Chunk file format (one chunk = one file + CRC32C sidecar)
- ADR 0006 — one chunk = one file
- ADR 0005 — pipelined daisy-chain

## 3. Public API surface

| Class | Kind | Purpose |
|---|---|---|
| `Chunkserver` | class | the long-running chunkserver: holds leases, buffers pushed bytes, applies mutations, picks append offsets, pads chunks |
| `ChunkStore` | class | local disk store: `read/writeAt/lengthOf/verify/delete`; one `<handle>.chunk` + `<handle>.crc` per chunk |
| `ChunkserverRpcServer` | class | socket front end; handles `ReadChunk`, `PushBytes`, `CommitWrite`, `CommitRecordAppend`, `ApplyMutation`, `CopyChunk`, `DeleteChunk` |

## 4. Internal structure

`ChunkStore` writes bytes at an offset into `<handle>.chunk`, recomputes the CRC32C of each touched 64 KB block into the `<handle>.crc` sidecar, and `fsync`s both before acking. Reads recompute CRC32C over the spanning blocks and compare; a mismatch returns `CHECKSUM_FAILURE` and is reported to the master for re-replication. `Chunkserver` buffers `PushBytes` payloads in RAM keyed by chunk, and on `CommitWrite` / `CommitRecordAppend` (when it holds the lease) assigns the serial number and forwards `ApplyMutation` down the remaining chain. For record append it picks the offset as the chunk's current size, pads to 64 MB and returns `RETRY_ON_NEXT_CHUNK` when a record won't fit.

## 5. Key tests

| Test | What it locks down |
|---|---|
| `ChunkStoreTest` | write + read round-trip; CRC32C catches single-byte corruption; length tracking; delete |
| `ChunkserverTest` | lease holding; push-then-commit serialization; append offset selection + padding behavior |

## 6. Where it fits

```
gfs-common ──┐
gfs-replication ─┴─► gfs-chunkserver ──► gfs-simulator
```

Data plane. Depends on `gfs-common` + `gfs-replication`. Never depends on `gfs-master`.

## 7. Stubs and departures from production

- **Chain logic lives here**, not in `gfs-replication` (which is an empty placeholder) — a departure from plan §3.3.
- Network is assumed fast and reliable; failures are driven explicitly by the simulator rather than emerging from real congestion/jitter.
- Single in-flight request per connection; no pipelining of distinct RPCs.

## 8. Related

- Module index: [`README.md`](./README.md)
- Flows: [`../flows/write-path.md`](../flows/write-path.md), [`../flows/record-append.md`](../flows/record-append.md), [`../flows/read-path.md`](../flows/read-path.md)
- ADRs: [0005 pipelined daisy-chain](../decisions/0005-pipelined-daisy-chain.md), [0006 one chunk one file](../decisions/0006-one-chunk-one-file.md)
