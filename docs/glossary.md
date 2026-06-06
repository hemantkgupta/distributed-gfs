---
title: "Glossary — distributed-gfs"
type: glossary
updated: 2026-05-31
---

# Glossary

Vocabulary lookup for the GFS reimplementation. Each term links to the module that implements it.

| Term | Definition | Implemented in |
|---|---|---|
| **Chunk** | A 64 MB unit of allocation, placement, and replication. Files are composed of one or more chunks. | `gfs-common` (geometry), `gfs-chunkserver` (`ChunkStore`) |
| **Chunk handle** | A 64-bit monotonic identifier for a chunk, assigned by the master at allocation time. Stable for the chunk's lifetime. | `gfs-common` (`ChunkHandle`) |
| **Chunk version** | Monotonic version bumped on lease grant; lets the master detect replicas that missed mutations while partitioned. | `gfs-common` (`ChunkVersion`) |
| **Chunkserver** | A process that stores chunks on local disk and serves reads/writes to clients. | `gfs-chunkserver` (`Chunkserver`) |
| **Master** | The single process that owns all metadata: namespace, chunk-to-replica maps, lease state. In-memory; durable via op log + checkpoint. | `gfs-master` (`Master`) |
| **Lease** | A bounded (60 s) delegation from the master to one chunkserver, granting it the right to serialize mutations on a specific chunk. | `gfs-common` (`LeaseToken`), `gfs-master` |
| **Primary replica** | The chunkserver currently holding the lease for a chunk. Picks serial numbers; forwards mutations to secondaries. | `gfs-chunkserver` |
| **Secondary replica** | The other chunkservers holding copies of a chunk. Apply mutations in the order the primary specifies. | `gfs-chunkserver` |
| **Operation log** | An append-only file of metadata mutations. The master's durability boundary. `fsync`'d before every client ack. | `gfs-oplog` (`OperationLog`) |
| **Log entry** | One metadata mutation in the op log (create file, allocate chunk, grant lease, …). A sealed interface — exhaustive at compile time. | `gfs-oplog` (`LogEntry`) |
| **Checkpoint** | A periodic full snapshot of the master's in-memory state to disk. Combined with the op-log tail, lets a crashed master recover. | `gfs-oplog` (`Checkpoint`) |
| **Shadow master** | A separate process that tails the live master's op log and serves read-only metadata queries. Not a hot standby. | `gfs-master` (`ShadowMaster`) |
| **Record append** | The native GFS write API. Client supplies a record; the primary picks the offset (the chunk's current end). At-least-once semantics. | `gfs-client` (`GfsClient`), `gfs-chunkserver` |
| **Padding** | Zero bytes the primary writes to fill a chunk when an incoming record won't fit. Visible to readers. | `gfs-chunkserver` |
| **Re-replication** | The master's background process of detecting under-replicated chunks (<3 copies) and instructing healthy chunkservers to copy them. | `gfs-master` (reaper) |
| **Heartbeat** | Periodic message (every 5 s) from chunkserver to master carrying chunk list, disk state, and lease-renewal requests. | `gfs-common` (`Heartbeat`), `gfs-master` |
| **Daisy chain** | Replication topology where bytes flow `client → A → B → C` in sequence, so each node's outbound NIC is saturated independently. | `gfs-client` (`ChainReplicationDriver`) |
| **Chain order** | The replica ordering picked for a daisy chain — nearest replica to the client first. | `gfs-client` (`ChainReplicationDriver`) |
| **CRC32C** | Castagnoli CRC32, the per-block checksum stored alongside chunk data at 64 KB block granularity. | `gfs-chunkserver` (`ChunkStore`) |
| **Stale replica** | A replica the master has marked not-up-to-date (a write failed there, or it missed heartbeats). Re-replication targets these. | `gfs-master` |
| **Wire format** | The hand-rolled, big-endian, length-prefixed binary RPC framing. No protobuf/JSON/Java-serialization. | `gfs-common` (`WireCodec`, `WireMessage`) |

## Related

- [`architecture.md`](./architecture.md) — how these fit together
- [`modules/README.md`](./modules/README.md) — module index
