---
title: "gfs-common — module guide"
module: gfs-common
phase: 1
updated: 2026-05-31
---

# `gfs-common`

## 1. Role

Foundational value types and the hand-rolled binary wire format used by every other module. No business logic; no I/O. This is the vocabulary the rest of the cluster speaks, plus the protocol they speak it over.

## 2. Plan anchor

- §4 — Wire format (framing, message types, encoding primitives)
- §5.1 — Value types
- The shared geometry of the system: 64 MB chunks, 64 KB CRC blocks, 3× replication, 60 s leases.

## 3. Public API surface

| Class | Kind | Purpose |
|---|---|---|
| `ChunkHandle` | record | 64-bit monotonic chunk id, zero-padded display |
| `ChunkVersion` | record | monotonic version; bumped on lease grant for staleness detection |
| `ChunkserverId` | record | `host:port` identity with host/port accessors |
| `FilePath` | record | absolute namespace path; decomposes into parts/parent/name |
| `Bytes` | record/class | immutable byte payload with value equality + defensive copies |
| `LeaseToken` | record | (chunk, primary, grantedAt, expiresAt); `isValid(now)` |
| `ChunkMetadata` | record | (handle, version, replicas, size, primary, lease) — master's per-chunk record |
| `NamespaceEntry` | record | a file or directory entry in the master's flat namespace |
| `LocatedChunk` | record | a chunk handle + its replica locations, returned to clients |
| `MessageType` | enum | every RPC tag, grouped by plane (0x1x client→master … 0x7x master→cs) |
| `WireMessage` | record | a framed message: length + type + reqId + payload |
| `WireCodec` | class | encode/decode for every message type; positional, big-endian |
| request/response records | records | `GetChunkLocationsRequest/Response`, `CreateFileRequest/Response`, `DeleteFileRequest/Response`, `MkdirRequest/Response`, `StatRequest/Response`, `ReadChunkRequest/Response`, `PushBytesRequest/Response`, `CommitWriteRequest`, `CommitRecordAppendRequest`, `CommitResponse`, `ReplicateBytes`, `ApplyMutation`, `ApplyAck`, `Heartbeat`, `HeartbeatAck`, `ReplicaStateReport`, `GrantLease`, `RevokeLease`, `CopyChunk`, `CopyCommand`, `DeleteChunk`, `ErrorResponse` |

## 4. Internal structure

Pure value types — Java `record`s with defensive copies and invariant checks in compact constructors (e.g. `FilePath` requires a leading `/`; `ChunkserverId` requires a `:`). The wire format is positional and tag-free: `WireCodec` reads/writes big-endian primitives in a fixed field order, so a mis-encoded byte surfaces immediately as a wrong value or an underflow exception. No reflection, no protobuf, no Java serialization.

## 5. Key tests

| Test | What it locks down |
|---|---|
| `ValueTypesTest` | equals/hashCode round-trips; `FilePath` decomposition; `Bytes` defensive copy; lease validity window; chunk geometry constants |
| `WireCodecTest` | byte-level encode→decode round-trip for every message type; big-endian byte order; unique message-type codes; frame length integrity |

## 6. Where it fits

```
gfs-common ← (everyone)
```

Depended on by every module; depends on nothing. The leaf of the dependency graph.

## 7. Stubs and departures from production

- These are real value types and a real wire codec — nothing stubbed.
- Simplifications: `Bytes` holds the whole array on heap (production would slice off-heap `ByteBuffer`s for 64 MB payloads); the wire format has a single in-flight request slot per connection (no pipelining); CRC is computed in `gfs-chunkserver`, not here.

## 8. Related

- Module index: [`README.md`](./README.md)
- Architecture: [`../architecture.md`](../architecture.md)
- Glossary: [`../glossary.md`](../glossary.md)
- ADR: [0007 hand-rolled binary RPC](../decisions/0007-hand-rolled-binary-rpc.md)
