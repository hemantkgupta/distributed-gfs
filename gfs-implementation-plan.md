---
title: "GFS Implementation Plan — Java 17 / Gradle Multi-Module"
slug: gfs-implementation-plan
type: implementation-plan
tags: [implementation-plan, gfs, java, distributed-systems, storage, paper-implementation]
paper: "Ghemawat, Gobioff, Leung — The Google File System (SOSP 2003)"
arc: distributed-file-system
companion_blog: gfs.md
status: spec
updated: 2026-05-23
---

# GFS Implementation Plan — Java 17 / Gradle Multi-Module

## 0. How to use this document

This document is a **complete specification** an LLM (or a careful engineer) can use to build a faithful Java implementation of the 2003 Google File System. Every architectural decision is named, every wire format is laid out, every data structure has a class skeleton, every protocol has a step-by-step trace, and every failure mode has a recovery path. It is pedagogical — the goal is that reading this and then reading the original SOSP paper makes the design "click" — but it is also constructively complete: a builder following this spec end-to-end produces a working multi-process cluster file system that exhibits every behavior the paper describes.

Two reading orders work:
- **Top to bottom** if you want the paper's design unfolded as a build plan.
- **§3 (Repo Layout) → §6 (Protocols) → §8 (Phasing) → everything else** if you want to jump straight to "what would I type into IntelliJ."

The companion narrative deep-dive lives at [gfs.md](gfs.md). It carries the *why*; this document carries the *what* and the *how*.

---

## 1. Goals and non-goals

### 1.1 Goals (the things that MUST be faithfully implemented)

The implementation must reproduce these architectural commitments of the original paper, even when modern Java idioms would suggest otherwise:

1. **Single in-memory master.** All metadata — namespace tree, file-to-chunk maps, chunk-to-replica maps, lease state, chunkserver heartbeat records — lives in one `Master` process's RAM. Durability comes from an operation log + periodic checkpoint.
2. **64 MB chunks** as the unit of allocation, replication, and placement.
3. **3× replication** by default; configurable.
4. **Lease-based primary-copy serialization** with a 60-second lease window. Exactly one consensus per lease per chunk, not one per write.
5. **Pipelined daisy-chain replication.** Bytes flow `client → A → B → C`; each replica's outbound NIC runs at full link speed.
6. **At-least-once record append** semantics. Padding and duplicates are *allowed*; the system makes the trade visible rather than hiding it.
7. **Control / data plane decoupling.** The master returns metadata; the master never touches payload bytes. Clients open direct TCP connections to chunkservers.
8. **Operation log → checkpoint → shadow master** for master durability and failover.
9. **Chunkserver-driven re-replication** when replica counts fall below 3.
10. **Lazy garbage collection** of deleted files: hidden for 3 days, then chunks orphaned, then chunkservers retire them on heartbeat.

### 1.2 Non-goals (the things we deliberately don't do)

- **No sharded master.** That's the Colossus implementation plan, not this one.
- **No erasure coding.** GFS used 3× replication; so do we.
- **No CRUSH-like deterministic placement.** Master picks replica sets explicitly.
- **No file-level ACLs / POSIX permissions** beyond "file exists / file doesn't exist."
- **No snapshots.** The paper's snapshot mechanism is genuinely interesting but not load-bearing for understanding the rest; we punt.
- **No real cross-host networking required.** Modules run in separate JVMs by default, but the included `gfs-simulator` runs the whole cluster in one JVM for testing.
- **No JNI dependencies** (no embedded RocksDB, no Intel ISA-L, no BouncyCastle). Pure JDK + Gradle. A reader can `gradle build` on a laptop, no native toolchain.
- **No external KV store** (no embedded RocksDB, no BigTable). GFS's design predates both. The master holds everything in `HashMap`s and durability comes from the op log + checkpoint pattern — *that is the design* we are studying.

### 1.3 Faithful-to-paper departures

Some shortcuts are unavoidable in a pedagogical single-machine build. They are called out explicitly here so the reader knows where the spec diverges from the 2003 production system:

| What 2003 GFS did | What this spec does | Why the departure is OK |
|---|---|---|
| Op log replicated to N remote machines via real network | Op log written to N local directories on the same disk | Bytes-on-the-wire are identical; only the "remote-ness" is mocked |
| Master + chunkservers + clients on separate hosts | Optionally one JVM via `gfs-simulator`, or separate JVMs on the same host | Wire format is real; only physical separation is mocked |
| Network failures, packet loss, asymmetric partitions | Simulated via deliberate timeouts and explicit `Disconnect()` calls in the simulator | We can drive failures from tests rather than wait for hardware |
| Heartbeats over actual networking with backoff and jitter | TCP heartbeats with simple fixed-interval timing | The mechanism is faithful; the timing constants are pedagogical |

The departures are mechanical, not architectural. Every architectural choice from the paper is implemented.

---

## 2. The 2003 context — what we are reproducing

The Google File System paper's design is the answer to a specific 2003 problem statement. The implementation feels right only when you hold that frame:

- **Workload:** crawl + MapReduce. Multi-PB to multi-TB files, written once by a producer, scanned many times by N consumers. Random small-file access is rare; sequential bulk reads and appends are the norm.
- **Hardware:** commodity Intel servers, 64 GB RAM per host, gigabit Ethernet, SATA disks. Drive failures, host failures, and rack failures are routine events — not exceptional.
- **Software constraint:** no commercial cluster file systems existed at this scale. Lustre and HDFS predecessors targeted research clusters or single sites; nothing handled Google's scale on commodity hardware.
- **Reliability via software:** a hard disk fails every day in a fleet of 1000s; the file system is the thing that makes that invisible. No RAID, no enterprise SAN, no hardware-level redundancy. Pure software replication.

The design decisions in this spec all fall out of those constraints. When in doubt, re-anchor: a master with 64 GB of RAM, gigabit links, commodity drives, append-heavy workload.

---

## 3. Repo layout

### 3.1 Gradle module structure

Seven modules, organized by concern:

```
distributed-gfs/
├── settings.gradle           # multi-project settings
├── build.gradle              # root build script — versions, common config
├── gradle.properties         # Java 17, jenv hint
├── gradlew, gradlew.bat
├── README.md
├── docs/
│   ├── architecture.md       # module dependency graph + plane mapping
│   ├── adr/
│   │   ├── 0001-in-memory-master.md
│   │   ├── 0002-64mb-chunks.md
│   │   ├── 0003-at-least-once-record-append.md
│   │   ├── 0004-lease-based-primary-copy.md
│   │   ├── 0005-pipelined-daisy-chain.md
│   │   ├── 0006-one-chunk-one-file.md
│   │   ├── 0007-hand-rolled-binary-rpc.md
│   │   ├── 0008-oplog-fsync-before-ack.md
│   │   └── 0009-java17-gradle-multimodule.md
│   ├── wire-format.md        # RPC schema reference
│   ├── failure-modes.md      # what each failure looks like
│   └── glossary.md
├── gfs-common/               # shared value types + wire format
├── gfs-master/               # control plane
├── gfs-chunkserver/          # data plane
├── gfs-client/               # client library
├── gfs-oplog/                # persistence: op log + checkpoint
├── gfs-replication/          # chain-replication helper used by chunkserver
└── gfs-simulator/            # multi-process chaos harness
```

### 3.2 Module dependency rules

The graph must be **acyclic**. The Gradle build enforces this via the `dependsOn` declarations.

```
gfs-common (no deps)
    ↑
    ├── gfs-oplog
    │       ↑
    ├── gfs-master ──── (depends on gfs-oplog)
    │       ↑
    ├── gfs-replication
    │       ↑
    ├── gfs-chunkserver ──── (depends on gfs-replication)
    │       ↑
    ├── gfs-client
    │
    └── gfs-simulator ──── (depends on ALL others — integration harness only)
```

The architectural invariant is the same as in the synthesis implementation: **control-plane modules never depend on data-plane modules, and vice versa.** The Gradle dependency edges encode this as a *build-time* constraint, not a convention.

### 3.3 Per-module purpose and key classes

#### `gfs-common`
- Value types (records): `ChunkHandle`, `ChunkVersion`, `FilePath`, `ChunkserverId`, `ClientId`, `LeaseToken`, `Bytes`.
- Wire format: `WireMessage`, `WireEncoder`, `WireDecoder`. Enum `MessageType`.
- Constants: `CHUNK_SIZE_BYTES = 64 * 1024 * 1024`, `BLOCK_SIZE_BYTES = 64 * 1024`, `LEASE_DURATION_SECONDS = 60`, `HEARTBEAT_INTERVAL_SECONDS = 5`, `MISSED_HEARTBEATS_THRESHOLD = 3`, `REPLICATION_FACTOR = 3`.

#### `gfs-master`
- `Master` — the long-running master process. Boots from checkpoint + log, listens on a port for client + chunkserver RPCs.
- `Namespace` — in-RAM namespace tree, `ConcurrentHashMap<String, NamespaceEntry>`.
- `ChunkMap` — in-RAM `ConcurrentHashMap<ChunkHandle, ChunkMetadata>`.
- `ChunkserverRegistry` — `ConcurrentHashMap<ChunkserverId, ChunkserverState>` with last-heartbeat timestamps.
- `LeaseManager` — `ConcurrentHashMap<ChunkHandle, LeaseRecord>` + a `ScheduledExecutorService` that expires leases.
- `Reaper` — background thread: scans for under-replicated chunks, schedules re-replication; scans for dead chunkservers; scans for GC-eligible chunks.
- `ShadowMaster` — separate process; tails the live op log over a socket; serves read-only metadata RPCs.
- `MasterRpcHandler` — dispatches incoming RPCs to handler methods.

#### `gfs-chunkserver`
- `Chunkserver` — long-running process; manages local chunk files; participates in chain replication.
- `ChunkStore` — wraps a directory on local disk; one file per chunk; CRC32C sidecar.
- `LeaseHolder` — tracks which chunks this server holds the lease for; serializes mutations on those chunks.
- `ReplicationParticipant` — implements one node of the daisy chain (in `gfs-replication`).
- `ChunkserverRpcHandler` — handles `ReadChunk`, `WriteChunk`, `RecordAppend`, `ApplyMutation`, `CopyChunk`, etc.

#### `gfs-client`
- `GfsClient` — public API: `read(path, offset, size)`, `write(path, offset, bytes)`, `recordAppend(path, bytes)`, `create(path)`, `delete(path)`, `mkdir(path)`.
- `MetadataCache` — caches chunk-handle + replica-locations per file, with TTL aligned to lease duration.
- `OffsetMath` — pure functions: `chunkIndex(offset)`, `offsetInChunk(offset)`, `bytesNeededAcrossChunks(offset, size)`.
- `ChainReplicationDriver` — picks chain order by topological distance to client.

#### `gfs-oplog`
- `OperationLog` — append-only file writer + reader. `append(LogEntry)`, `replay(callback)`. `fsync()` semantics built in.
- `Checkpoint` — serializes / deserializes the master's full in-memory state to a single file.
- `LogEntry` — sealed interface with `CreateFile`, `DeleteFile`, `AllocateChunk`, `GrantLease`, `RenewLease`, `RevokeLease`, `MarkReplicaStale`, `DeleteChunk` variants.
- `LogReplicator` — writes each appended entry to N local "remote" directories before signaling durability complete.

#### `gfs-replication`
- `ChainNode` — implements one node of the daisy chain. Receives bytes from upstream, buffers in RAM, forwards to downstream.
- `MutationBatch` — represents a pending mutation (bytes + serial number + chunk + offset) before commit.
- `MutationApplier` — applies a sealed batch to the underlying `ChunkStore`.

#### `gfs-simulator`
- `Cluster` — boots a master + N chunkservers + M clients in one JVM (for tests) or coordinates external processes (for integration runs).
- `FaultInjector` — programmatic API to drop heartbeats, kill primaries, partition replicas, fill disks.
- `WorkloadDriver` — runs the canonical workloads (crawl write, MapReduce read, concurrent record append).

---

## 4. Wire format — hand-rolled binary framing

Every RPC over the wire is a length-prefixed binary message. No protobuf, no JSON, no Java serialization (the latter is a security and stability landmine and obscures the bytes-on-wire). We hand-roll a small encoder/decoder so the format is inspectable and a reader can verify it against `tcpdump` output if curiosity strikes.

### 4.1 Frame format

```
+----------------+--------+----------+-------------------+
| length (4 B)   | type   | reqId    | payload (length-1-4 bytes) |
| big-endian     | (1 B)  | (4 B)    |                   |
+----------------+--------+----------+-------------------+
```

- `length` is the total frame length **including** itself.
- `type` is the message type tag (see §4.2).
- `reqId` is a per-connection sequence number for request/response correlation.
- `payload` format depends on `type`.

### 4.2 Message type enumeration

```java
public enum MessageType {
    // Client → Master
    GET_CHUNK_LOCATIONS_REQUEST(0x10),
    CREATE_FILE_REQUEST(0x11),
    DELETE_FILE_REQUEST(0x12),
    MKDIR_REQUEST(0x13),
    STAT_REQUEST(0x14),

    // Master → Client (responses)
    GET_CHUNK_LOCATIONS_RESPONSE(0x20),
    CREATE_FILE_RESPONSE(0x21),
    DELETE_FILE_RESPONSE(0x22),
    MKDIR_RESPONSE(0x23),
    STAT_RESPONSE(0x24),
    ERROR_RESPONSE(0x2F),

    // Client → Chunkserver
    READ_CHUNK_REQUEST(0x30),
    PUSH_BYTES_REQUEST(0x31),
    COMMIT_WRITE_REQUEST(0x32),
    COMMIT_RECORD_APPEND_REQUEST(0x33),

    // Chunkserver → Client (responses)
    READ_CHUNK_RESPONSE(0x40),
    PUSH_BYTES_RESPONSE(0x41),
    COMMIT_RESPONSE(0x42),

    // Chunkserver → Chunkserver (replication chain)
    REPLICATE_BYTES(0x50),
    APPLY_MUTATION(0x51),
    APPLY_ACK(0x52),

    // Chunkserver → Master
    HEARTBEAT(0x60),
    REPLICA_STATE_REPORT(0x61),

    // Master → Chunkserver
    GRANT_LEASE(0x70),
    REVOKE_LEASE(0x71),
    COPY_CHUNK(0x72),         // re-replication command
    DELETE_CHUNK(0x73),       // GC command
    HEARTBEAT_ACK(0x74);
}
```

### 4.3 Per-message payload format

The pattern: each payload is a sequence of fixed-width primitive types followed by length-prefixed variable-width fields. No tags inside payloads — the schema is positional. This is exactly the discipline that hand-rolled protocols enforce, and the cost of a mis-encoded byte is immediate.

#### Example: `GET_CHUNK_LOCATIONS_REQUEST`

```
+--------------+----------+----------+----------+
| path length  | path     | offset   | size     |
| (2 B uint)   | (UTF-8)  | (8 B le) | (8 B le) |
+--------------+----------+----------+----------+
```

#### Example: `GET_CHUNK_LOCATIONS_RESPONSE`

```
+-------+----------+------------------------+----------+----------+----+
| count | chunk[0] | chunk[0] replica count | replica[]| chunk[1] | …  |
| (1 B) |  handle  |        (1 B)           | (variable)  handle  |    |
|       | (8 B le) |                        |          | (8 B le) |    |
+-------+----------+------------------------+----------+----------+----+

Each replica is encoded as:
  +----------+----------+-----+
  | ip (4 B) | port(2B) | … |
  +----------+----------+-----+
```

#### Example: `HEARTBEAT`

```
+--------------+----------+----------+-------------------+
| chunkserverId| diskFree | numChunks| chunk handles []  |
| (length + UTF-8 host:port) (8 B)   | (4 B)             |
+--------------+----------+----------+-------------------+
```

The full schema lives in `docs/wire-format.md` of the eventual build; this spec gives a complete description by referring to the types and primitives. The encoder/decoder in `gfs-common` is `~200 lines of Java`.

### 4.4 Encoding primitives

- All multi-byte integers: **big-endian** (network byte order).
- Strings: 2-byte unsigned-length-prefixed UTF-8.
- Byte arrays: 4-byte unsigned-length-prefixed.
- Booleans: 1 byte, 0 or 1.
- `ChunkHandle`: 8-byte signed (long).
- `ChunkVersion`: 4-byte signed (int).

### 4.5 Transport

- One TCP connection per (client → master), one per (client → chunkserver), one per (master → chunkserver), and one per (chunkserver-pair) in the replication chain.
- Connections are **long-lived** by default; reconnect on EOF.
- Each connection has a single in-flight request slot **per direction** (no pipelining in v1; a v2 could add it).

---

## 5. Data structures

### 5.1 Value types (`gfs-common`)

```java
public record ChunkHandle(long id) implements Comparable<ChunkHandle> {
    public static ChunkHandle of(long id) { return new ChunkHandle(id); }
    @Override public int compareTo(ChunkHandle o) { return Long.compare(id, o.id); }
}

public record ChunkVersion(int v) {
    public static ChunkVersion ZERO = new ChunkVersion(0);
    public ChunkVersion next() { return new ChunkVersion(v + 1); }
}

public record ChunkserverId(String hostPort) {
    public static ChunkserverId of(String host, int port) {
        return new ChunkserverId(host + ":" + port);
    }
}

public record FilePath(String path) {
    public FilePath { Objects.requireNonNull(path); }
    public boolean isAbsolute() { return path.startsWith("/"); }
    public List<String> parts() { return Arrays.asList(path.split("/")).subList(1, path.split("/").length); }
}

public record LeaseToken(
    ChunkHandle chunk,
    ChunkserverId primary,
    Instant grantedAt,
    Instant expiresAt
) {
    public boolean isValid(Instant now) { return now.isBefore(expiresAt); }
}

public final class Bytes {
    private final byte[] data;
    public Bytes(byte[] data) { this.data = data.clone(); }
    public byte[] toByteArray() { return data.clone(); }
    public int length() { return data.length; }
    // value-based equality on byte content
    @Override public boolean equals(Object o) { return o instanceof Bytes b && Arrays.equals(data, b.data); }
    @Override public int hashCode() { return Arrays.hashCode(data); }
}
```

### 5.2 Master state

The master holds everything in RAM. Persistence is via the op log + checkpoint.

```java
public class Master {
    // Namespace tree — flat map by path string
    private final ConcurrentHashMap<String, NamespaceEntry> namespace = new ConcurrentHashMap<>();

    // chunk handle → chunk metadata
    private final ConcurrentHashMap<ChunkHandle, ChunkMetadata> chunkMap = new ConcurrentHashMap<>();

    // chunkserver id → chunkserver runtime state
    private final ConcurrentHashMap<ChunkserverId, ChunkserverState> chunkservers = new ConcurrentHashMap<>();

    // chunk handle → current lease (if any)
    private final ConcurrentHashMap<ChunkHandle, LeaseToken> leases = new ConcurrentHashMap<>();

    // Monotonic counters
    private final AtomicLong nextChunkHandle = new AtomicLong(1);
    private final AtomicInteger opLogSequence = new AtomicInteger(0);

    private final OperationLog opLog;
    private final CheckpointStore checkpointStore;
    private final Reaper reaper;

    public Master(MasterConfig config) { … }
}
```

```java
public record NamespaceEntry(
    String path,
    boolean isDirectory,
    List<ChunkHandle> chunks,     // null if directory
    long sizeBytes,
    Instant ctime,
    Instant mtime,
    Optional<Instant> deletedAt   // for GC
) {}

public record ChunkMetadata(
    ChunkHandle handle,
    ChunkVersion version,
    Set<ChunkserverId> replicas,
    long sizeBytes,               // current length, <= 64 MB
    Optional<ChunkserverId> primary,
    Optional<Instant> leaseExpiresAt
) {}

public record ChunkserverState(
    ChunkserverId id,
    Instant lastHeartbeat,
    long diskFreeBytes,
    Set<ChunkHandle> reportedChunks
) {}
```

### 5.3 Chunkserver state

```java
public class Chunkserver {
    private final ChunkserverId id;
    private final ChunkStore store;             // local disk-backed
    private final ConcurrentHashMap<ChunkHandle, LeaseToken> heldLeases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, ReplicationParticipant> pendingMutations = new ConcurrentHashMap<>();
}

public class ChunkStore {
    private final Path dataDir;       // e.g. /var/lib/gfs/chunks/
    // One file per chunk: <handle>.chunk
    // One sidecar: <handle>.crc — 4-byte CRC32C per 64 KB block
    public byte[] read(ChunkHandle h, long offset, int size) { … }
    public void writeAt(ChunkHandle h, long offset, byte[] bytes) { … }
    public long lengthOf(ChunkHandle h) { … }
    public boolean verify(ChunkHandle h, long offset, int size) { … }
    public void delete(ChunkHandle h) { … }
}
```

### 5.4 Operation log entries

```java
public sealed interface LogEntry permits
    LogEntry.CreateFile, LogEntry.DeleteFile, LogEntry.Mkdir,
    LogEntry.AllocateChunk, LogEntry.SetChunkReplicas,
    LogEntry.GrantLease, LogEntry.RenewLease, LogEntry.RevokeLease,
    LogEntry.MarkReplicaStale, LogEntry.DeleteChunk {

    int sequence();
    Instant timestamp();

    record CreateFile(int sequence, Instant timestamp, String path) implements LogEntry {}
    record DeleteFile(int sequence, Instant timestamp, String path) implements LogEntry {}
    record Mkdir(int sequence, Instant timestamp, String path) implements LogEntry {}
    record AllocateChunk(int sequence, Instant timestamp, String path, ChunkHandle chunk, ChunkVersion version, Set<ChunkserverId> replicas) implements LogEntry {}
    record SetChunkReplicas(int sequence, Instant timestamp, ChunkHandle chunk, Set<ChunkserverId> replicas) implements LogEntry {}
    record GrantLease(int sequence, Instant timestamp, ChunkHandle chunk, ChunkserverId primary, Instant expiresAt) implements LogEntry {}
    record RenewLease(int sequence, Instant timestamp, ChunkHandle chunk, Instant newExpiresAt) implements LogEntry {}
    record RevokeLease(int sequence, Instant timestamp, ChunkHandle chunk) implements LogEntry {}
    record MarkReplicaStale(int sequence, Instant timestamp, ChunkHandle chunk, ChunkserverId stale) implements LogEntry {}
    record DeleteChunk(int sequence, Instant timestamp, ChunkHandle chunk) implements LogEntry {}
}
```

The `sealed interface` makes the set of entry types exhaustive at compile time — adding a new mutation kind forces touching every replay site. This is the Java equivalent of an exhaustive ADT.

---

## 6. Protocols

This section is the heart of the spec. Each protocol is given as a numbered step-by-step trace with explicit RPCs, state mutations, and durability checkpoints.

### 6.1 Client read

```
Client                       Master                       Chunkserver(s)
  |                            |                                |
  | GetChunkLocations(/a.dat,  |                                |
  |   offset=150 MB, size=10K) |                                |
  |--------------------------->|                                |
  |                            | chunkIndex = 150 MB / 64 MB = 2|
  |                            | offsetInChunk = 150 mod 64 = 22|
  |                            | look up chunkMap[handle 7]     |
  |   (handle 7, version 4,    |                                |
  |    replicas [A, B, C])     |                                |
  |<---------------------------|                                |
  |                            |                                |
  | cache: (path, idx) -> ...  |                                |
  |                            |                                |
  | ReadChunk(handle 7,        |                                |
  |   offset=22 MB, size=10K)  |                                |
  |--------------------------------->                           |  (to nearest replica)
  |                                                             |
  |   bytes[10K] + CRC32C per 64 KB block                       |
  |<-----------------------------------                         |
```

**State mutations:** none on master, none on chunkservers. Read is side-effect-free.

**Error paths:**
- Chunkserver doesn't have the chunk (stale cache) → returns `CHUNK_NOT_FOUND`; client invalidates cache, re-fetches from master.
- CRC32C mismatch on a block → chunkserver returns `CHECKSUM_FAILURE`; client retries against a different replica.

### 6.2 Client write (non-append, byte-range)

This is the more elaborate path. The paper rarely uses byte-range overwrites in production (everyone uses record append), but the mechanism is the foundation of everything else.

```
Client            Master            Primary (A)        Secondary (B)    Secondary (C)
  |                 |                    |                    |               |
  | GetChunk        |                    |                    |               |
  | Locations       |                    |                    |               |
  |---------------->|                    |                    |               |
  |    (handle, primary=A,                                                    |
  |     replicas [A,B,C], lease)                                              |
  |<----------------|                    |                    |               |
  |                                                                           |
  |  pick chain by topological distance: client -> A -> B -> C                |
  |                                                                           |
  |  PushBytes(handle, bytes[10K])                                            |
  |-------------------------------------->|                                   |
  |                                       |  buffer in RAM                    |
  |                                       |--------- PushBytes ------->|      |
  |                                       |                            | buf  |
  |                                       |                            |--->| |
  |                                       |                            |    |buf
  |                                       |<---------ack-----------------|<---|
  |                                       |<---ack---|                   |    |
  |<------------- ack ----------------|                                       |
  |                                                                           |
  |  CommitWrite(handle, offset=22M, size=10K)                                |
  |-------------------------------------->|                                   |
  |                                       | assigns serial = 47               |
  |                                       |--ApplyMutation(handle,            |
  |                                       |     serial=47, off=22M, size=10K)->|
  |                                       |                                   |
  |                                       |                            |--->| |
  |                                       |                            | apply|
  |                                       |                            |<---| |
  |                                       |<---ack(serial=47, ok)-------|     |
  |                                       |<---ack(serial=47, ok)-------------|
  |<----------- ok (serial=47) ----------|                                    |
```

**State mutations:**
- **Master:** none during the write itself. The master is only consulted at lease grant time.
- **Chunkservers:** each replica's `<handle>.chunk` file is extended (if necessary) and the bytes written to the file region [22 MB, 22 MB + 10 KB). CRC32C of each touched 64 KB block is recomputed in the `<handle>.crc` sidecar.
- **Durability:** the bytes are `fsync`'d on each chunkserver **before** that chunkserver acks the `ApplyMutation`.

**Error paths:**
- Lease expired between PushBytes and CommitWrite → Primary rejects with `LEASE_EXPIRED`; client re-fetches metadata from master.
- One secondary fails to apply → Primary still acks the client *if* both other replicas applied (configurable; default is "all-or-fail"). The failed replica is reported to master via `REPLICA_STATE_REPORT`, master marks it stale, re-replication starts.
- Primary crashes between PushBytes and CommitWrite → secondaries time out the buffered bytes after 60 s. Client gets a connection failure on commit; retries.

### 6.3 Record append

This is the GFS-native write path. It is what MapReduce uses, what crawls use, what every production workload uses.

```
Client            Master            Primary (A)        Secondary (B)    Secondary (C)
  |                 |                    |                    |               |
  | GetChunkLocations for LAST CHUNK of /foo                                  |
  |---------------->|                    |                    |               |
  |<----------------|                    |                    |               |
  |                                                                           |
  | PushBytes(record, 100 KB)                                                 |
  |-------------------------------------->|------>|------>|                   |
  |<--- ack ---|<---|<---|                                                    |
  |                                                                           |
  | CommitRecordAppend(handle)                                                |
  |-------------------------------------->|                                   |
  |                                       | currentSize = 63.95 MB            |
  |                                       | currentSize + 100 KB > 64 MB ?   |
  |                                       |   YES → pad chunk to 64 MB        |
  |                                       |   reply RETRY_ON_NEXT_CHUNK       |
  |<--- RETRY_ON_NEXT_CHUNK -------------|                                    |
  |                                                                           |
  | GetChunkLocations for NEW (allocated) LAST CHUNK                          |
  |---------------->|                    |                    |               |
  |    (master allocates new chunk, grants lease)                             |
  |<----------------|                    |                    |               |
  |                                                                           |
  | PushBytes(record, 100 KB) — same bytes                                    |
  |-------------------------------------->|------>|------>|                   |
  |<--- ack ---|<---|<---|                                                    |
  |                                                                           |
  | CommitRecordAppend(handle')                                               |
  |-------------------------------------->|                                   |
  |                                       | assign serial=N, append at off=0  |
  |                                       |--ApplyMutation(...)-->|--->|      |
  |                                       |<--- ack ---------------|<---|     |
  |<--- OK(handle', offset=0, size=100K)-|                                    |
```

**Key behaviors:**
1. The client does not specify the offset. The primary picks: `currentSize` of the chunk.
2. If the record won't fit (`currentSize + recordSize > 64 MB`), the primary pads the chunk to 64 MB on all replicas, then returns `RETRY_ON_NEXT_CHUNK` to the client. The client asks the master for the next chunk and retries.
3. On any failure mid-append, the client retries. Retries may cause **duplicates** at different offsets on different replicas. The paper acknowledges this; the spec must acknowledge it too.
4. The client is responsible for **idempotency at the application layer**: each record should carry a unique ID and a checksum, and readers must dedupe + verify.

**Padding:** when the primary pads, the secondaries pad too. The chunk now contains `currentSize` bytes of real data followed by `64 MB - currentSize` bytes of zeroes. Readers must tolerate this (a record-aware reader sees the zero region and skips to the next valid record header).

### 6.4 Lease grant

```
Client → Master: GetChunkLocations(...)

If no lease exists OR lease expired:
    Master picks one replica from ChunkMetadata.replicas (typically the
    one with the lowest current load).
    Master writes LogEntry.GrantLease to op log (fsync first).
    Master sends GRANT_LEASE message to chosen primary.
    Master updates ChunkMetadata.primary + leaseExpiresAt.
    Master returns metadata to client with primary indicated.

If lease still valid:
    Master returns metadata with existing primary.
```

**Lease renewal:** primaries piggyback a `lease_renewal_request` flag on outbound heartbeats. If all replicas are healthy, master extends the lease by 60 s and logs `LogEntry.RenewLease`. The master never *push*-renews; it only renews on primary request, so a silent primary loses its lease naturally.

**Lease revocation:** master writes `LogEntry.RevokeLease` and sends `REVOKE_LEASE` to the holder. Used when the master decides to migrate the lease (rare — typically only during planned maintenance).

### 6.5 Chunkserver heartbeat

Every 5 seconds, each chunkserver sends to master:

```java
record Heartbeat(
    ChunkserverId id,
    long diskFreeBytes,
    int numChunksHeld,
    List<ChunkHandle> recentlyAddedOrChangedChunks,
    Map<ChunkHandle, Long> chunkSizes,        // current size per held chunk
    boolean leaseRenewalRequested,
    List<ChunkHandle> staleReplicaReports     // replicas this server thinks are stale (e.g. discovered via failed ApplyMutation)
) {}
```

Master:
1. Updates `chunkservers[id].lastHeartbeat = now`.
2. Updates `chunkservers[id].diskFreeBytes`.
3. Reconciles reported chunk list with `chunkMap`. Discrepancies trigger correction RPCs:
   - Chunk on chunkserver not in master's view → master ignores (probably leftover after GC; master will send `DELETE_CHUNK`).
   - Chunk in master's view not on chunkserver → master marks that replica stale and triggers re-replication.
4. Renews lease if `leaseRenewalRequested` is true and all replicas healthy.
5. Sends `HEARTBEAT_ACK` containing: list of chunks to delete (GC), list of new chunks to copy (re-replication assignments).

### 6.6 Re-replication

The `Reaper` thread inside the master runs every 30 s:

```java
for (ChunkMetadata cm : chunkMap.values()) {
    if (cm.replicas().size() < REPLICATION_FACTOR) {
        ChunkserverId source = pickHealthyReplica(cm.replicas());
        ChunkserverId target = pickHealthyChunkserverNotIn(cm.replicas());
        appendOpLog(new SetChunkReplicas(...));   // record the intent
        sendCopyChunk(target, cm.handle(), source);
        // target will read the chunk from source over the wire, write to local disk,
        // then send REPLICA_STATE_REPORT confirming it has the chunk.
        // Master then updates cm.replicas() to include target.
    }
}
```

**Priority:** chunks with `replicas.size() == 1` are repaired first (in danger of data loss). Then `replicas.size() == 2`. Then balancing-driven moves (not in scope for v1).

### 6.7 Garbage collection

Two phases:

**Phase 1 — file delete:** when a client calls `Delete(/foo)`, master:
1. Renames file to `/__deleted/<timestamp>/foo` (in-RAM rename; logged).
2. After 3 days (`GC_RETENTION_DURATION_DAYS = 3`), master scans `/__deleted/` and removes namespace entries older than the retention window. Logs `DeleteFile`.

**Phase 2 — chunk cleanup:** chunkservers periodically include their full chunk list in heartbeats (every 10th heartbeat, configurable). Master compares against `chunkMap`. Chunks held by chunkservers but not in `chunkMap` are orphans. Master sends `DELETE_CHUNK` in the next `HEARTBEAT_ACK`.

### 6.8 Master crash recovery

```
1. Master process dies (crash, kill, hardware fault).
2. A watchdog (external to this spec; can be systemd, K8s, or a manual restart)
   starts a new master process.
3. New master boots:
   a. Open the latest checkpoint file. Deserialize into RAM (HashMap rehydration).
   b. Open the operation log. Read entries with sequence > checkpoint's last sequence.
   c. For each, replay into the in-memory state.
   d. Start listening for RPCs.
4. Recovery time: dominated by checkpoint load (single-digit seconds for a
   ~100 MB checkpoint) + log replay (proportional to recent activity, typically
   << 1 second).
5. During recovery, clients see connection refusal. They retry; their cached
   metadata is still valid for the lease window (60 s), so cached reads continue
   to succeed against chunkservers without touching the master.
```

### 6.9 Shadow master replay

The shadow master:
1. Connects to the live master on boot.
2. Requests a "checkpoint stream" — full state dump (one-time).
3. Subscribes to op log entries as they are written.
4. Replays each entry into its own in-memory copy.
5. Serves **read-only** RPCs (`GetChunkLocations`, `Stat`, `Mkdir`-no-op) with a "shadow may be stale" marker in the response.
6. If the shadow's view falls more than `SHADOW_LAG_THRESHOLD_SECONDS = 30` behind the live master, it stops serving and waits to catch up.

The shadow is **not** promoted to live master on failure. A human (or external watchdog) brings up a fresh master with the same checkpoint and log; the design's primary purpose for the shadow is "stale reads during master outage," not automatic failover.

---

## 7. Persistence

### 7.1 Operation log file format

```
+----------+--------+--------+---------------+
| length   | type   | seq    | payload bytes |
| (4 B le) | (1 B)  | (4 B)  | (variable)    |
+----------+--------+--------+---------------+
```

Each log entry is one such record. The file is append-only. After each append, `fsync()` is called *before* the master replies to the client RPC that triggered the mutation.

**Rotation:** when a checkpoint completes, the current log file is closed and a new one started. Old log files are deleted (after a safety margin to ensure all shadow masters have caught up).

**Replicated log:** on each `fsync`, the bytes are also written to N "remote" directories (in our pedagogical setup, just other directories on the same disk). In production these would be remote machines; the bytes-on-wire format is identical to the local file format.

### 7.2 Checkpoint format

A checkpoint is a single binary file:

```
+----------+----------+----------+----------+----------+
| version  | seq      | namespace| chunkMap | leases   |
| (4 B)    | (4 B)    | section  | section  | section  |
+----------+----------+----------+----------+----------+
```

Each section is length-prefixed. Inside:
- **Namespace section:** count + (path-length + path + isDir + chunkHandles + sizeBytes + ctime + mtime + deletedAt) per entry.
- **ChunkMap section:** count + (chunk handle + version + replica count + replicas + sizeBytes + primary + leaseExpiresAt) per entry.
- **Leases section:** count + (chunk + primary + grantedAt + expiresAt) per active lease.

**Checkpointing protocol:**
1. Master forks a checkpoint thread.
2. Thread acquires read locks on the three maps (snapshot semantics).
3. Thread serializes to a *new* file (`checkpoint.<seq>.bin.tmp`).
4. Thread `fsync`'s the file, renames to `checkpoint.<seq>.bin`.
5. Master updates `latestCheckpointSequence` pointer.
6. Old checkpoint files retained for 2 hours (safety margin), then deleted.

**Trigger:** every 100,000 op log entries OR every 6 hours, whichever first.

### 7.3 Chunk file format

Each chunk on disk is two files:

```
/var/lib/gfs/chunks/
    00000001.chunk     # raw bytes, 0 to 64 MB
    00000001.crc       # one CRC32C per 64 KB block, packed 4 bytes each
    00000002.chunk
    00000002.crc
    ...
```

**Writing:**
1. Open `<handle>.chunk` with append mode.
2. Write bytes at the specified offset.
3. Recompute CRC32C for each 64 KB block that was touched.
4. Update the corresponding 4-byte entries in `<handle>.crc`.
5. `fsync` both files.

**Reading:**
1. Open `<handle>.chunk` read-only.
2. Read bytes at offset.
3. Compute CRC32C of the spanning 64 KB blocks.
4. Compare against the stored CRC32C.
5. On mismatch: return `CHECKSUM_FAILURE`; the chunkserver also reports the corruption to the master via the next heartbeat (master triggers re-replication from a healthy replica).

---

## 8. Phasing — checkpoint-by-checkpoint build plan

The implementation is broken into 5 phases of 4–5 checkpoints each, for 21 CPs total. Each CP is a self-contained piece of working, tested code. The build is green after every CP — `./gradlew build` passes at every step.

### Phase 1: Foundation (CPs 1–4)

| CP | Module | Deliverable |
|---|---|---|
| 1 | `gfs-common` | Value types (records). Tests: equals/hashCode roundtrip. |
| 2 | `gfs-common` | Wire format encode/decode for all 18 message types. Tests: byte-level roundtrip per message type. |
| 3 | `gfs-oplog` | `OperationLog.append()` + `replay()`. Tests: append N entries, replay, get N entries. fsync verified via `tmpfs` + truncate test. |
| 4 | `gfs-oplog` | `Checkpoint.write()` + `Checkpoint.load()`. Tests: checkpoint a state, load, deep-equal the state. |

### Phase 2: Master (CPs 5–9)

| CP | Module | Deliverable |
|---|---|---|
| 5 | `gfs-master` | Namespace operations (CreateFile, Delete, Mkdir, Stat). In-memory only; logs to op log. Tests: create + stat roundtrip, delete + GC eligibility. |
| 6 | `gfs-master` | Chunk allocation. `AllocateChunkForAppend(file)` picks 3 chunkservers, allocates handle, logs entry, returns metadata. |
| 7 | `gfs-master` | Lease grant. Tests: grant lease, check primary, renew, expire after 60 s. |
| 8 | `gfs-master` | Heartbeat handler. Tests: chunkserver registers, sends heartbeat, gets ack; missed heartbeats > 3 → marked dead. |
| 9 | `gfs-master` | Reaper. Tests: kill a chunkserver, verify re-replication assignments appear in subsequent heartbeat acks. |

### Phase 3: Chunkserver (CPs 10–13)

| CP | Module | Deliverable |
|---|---|---|
| 10 | `gfs-chunkserver` | `ChunkStore` — create/read/write/delete chunk files on local disk, with CRC32C. Tests: write + read + verify CRC. |
| 11 | `gfs-chunkserver` | Heartbeat client (chunkserver side). Periodic timer, sends heartbeat to master, applies HEARTBEAT_ACK (chunk deletions, copy commands). |
| 12 | `gfs-chunkserver` | Lease holder logic. Receives GRANT_LEASE, holds it, requests renewal via heartbeat, respects REVOKE_LEASE. |
| 13 | `gfs-chunkserver` + `gfs-replication` | `ReplicationParticipant` — implements chain replication node. PushBytes buffers; ApplyMutation commits. |

### Phase 4: Client + Protocols (CPs 14–17)

| CP | Module | Deliverable |
|---|---|---|
| 14 | `gfs-client` | `GfsClient.read()`. Tests: write a chunk via low-level interfaces, read with offset math, verify bytes. |
| 15 | `gfs-client` | `GfsClient.write()` — full daisy chain through PushBytes + CommitWrite. Tests: write 1 MB, read it back, 3 replicas have same bytes. |
| 16 | `gfs-client` | `GfsClient.recordAppend()`. Tests: 1000 concurrent appenders, all records present, may include padding and duplicates (paper-compliant). |
| 17 | `gfs-client` | `MetadataCache`. Tests: 100 reads of the same file → 1 master RPC (cached). |

### Phase 5: Resilience + Sim (CPs 18–21)

| CP | Module | Deliverable |
|---|---|---|
| 18 | `gfs-master` | Master crash recovery. Tests: write 1000 entries, kill master, restart, verify state matches pre-crash. |
| 19 | `gfs-master` | Shadow master. Tests: shadow tails live log, serves Stat, lags within threshold. |
| 20 | `gfs-master` | Garbage collection. Tests: delete file, verify chunks not orphaned for 3 days; advance time; verify chunkservers told to delete. |
| 21 | `gfs-simulator` | Multi-process chaos harness. Tests: 4 chunkservers, kill one mid-write, append succeeds eventually; kill master, restart, full state recovered; partition primary, lease expires, new lease granted. |

**Test count target:** ~80–120 unit tests + ~20 integration tests by Phase 5 completion.

---

## 9. Failure modes and recovery

| Failure | Detection | Recovery |
|---|---|---|
| Primary chunkserver dies mid-write | Secondaries' `PushBytes` timeout (5 s); client's `CommitWrite` connection drop | Lease expires after 60 s; master grants new lease to another healthy replica; client retries |
| Secondary chunkserver dies during `ApplyMutation` | Primary's `ApplyMutation` timeout (3 s) | Primary reports stale replica to master in next heartbeat; master marks stale + triggers re-replication |
| Chunkserver disk full | Reported via heartbeat (`diskFreeBytes < 5%`) | Master stops assigning new chunks to that server; existing chunks remain |
| Chunkserver process dies | Master's missed-heartbeat counter exceeds 3 (15 s total) | Master marks all replicas on that server stale; triggers re-replication for affected chunks |
| Chunkserver host network partition | Same as process death from master's perspective | When partition heals: chunkserver is treated as fresh (no replicas claimed); existing chunks on its disk become orphans; master tells it to delete them |
| Master process dies | RPC connection refused | Watchdog brings up new master; new master loads checkpoint + replays log; resumes service (typically < 5 s); clients see brief unavailability |
| Master disk corruption — op log unreadable | New master crashes during replay | Bootstrap from latest checkpoint only; mutations between checkpoint and crash are LOST. The spec accepts this and documents it; the production answer is to replicate the op log to remote machines (we mock this) |
| Master host network partition (split brain) | Not in scope for v1 — single-master design has no resolution mechanism | Production GFS uses external coordination (Chubby) for master election; out of scope here |
| Bit rot on chunk disk | Chunkserver CRC32C check at read time | Read returns error; chunkserver reports corruption; master triggers re-replication from a healthy replica |
| Network blip during heartbeat | Single missed heartbeat | Tolerated; only 3+ consecutive misses trigger dead-marking |
| Client crashes mid-write | Connection drops at primary | Primary's buffered bytes are discarded after 60 s; partial chunk state is the chunk's current `sizeBytes` — no rollback needed |
| Append retry causes duplicates | Application-layer detection | Paper-compliant: client treats as "at-least-once"; readers dedupe via record IDs |

---

## 10. ADRs to be authored

Each ADR is a single markdown file in `docs/adr/` following the standard ADR template (Context / Decision / Consequences). The list:

| ADR | Decision |
|---|---|
| 0001 | In-memory master with op log + checkpoint for durability — accepts the structural ceiling at ~50 PB / ~100 M files for pedagogical clarity |
| 0002 | 64 MB chunks — small enough to parallelize, large enough that master metadata fits in RAM |
| 0003 | At-least-once record append — padding and duplicates are visible to the application, by design |
| 0004 | Lease-based primary-copy serialization — one consensus per lease, not per write |
| 0005 | Pipelined daisy-chain replication — each NIC saturated independently; star/broadcast would divide bandwidth |
| 0006 | One chunk = one file on local FS (no LSM, no embedded KV) — paper-faithful; LSM is BlueStore's answer, not GFS's |
| 0007 | Hand-rolled binary RPC framing — no protobuf, no JNI, no Java serialization |
| 0008 | Op log fsync before client ack — durability before reply; the single non-negotiable invariant |
| 0009 | Java 17 + Gradle multi-module + JUnit 5 + AssertJ — matches the synthesis implementation conventions |
| 0010 | No JNI dependencies — readers can open the JAR and trace every architectural claim to the source |

---

## 11. Test plan summary

### 11.1 Unit tests (per module)

Each module ships ~10–25 unit tests. Highlights:

- **`gfs-common`:** wire format roundtrip for every message type; value type equality; offset math edge cases.
- **`gfs-master`:** namespace operations atomicity; lease grant + expiry + renewal; chunk allocation respects replication factor.
- **`gfs-chunkserver`:** chunk write + read roundtrip; CRC32C catches single-byte corruption; lease holder serializes mutations.
- **`gfs-client`:** offset math; chain order picking; metadata cache hit/miss/invalidation.
- **`gfs-oplog`:** entries survive process restart; checkpoint + replay = original state; fsync verified.
- **`gfs-replication`:** chain order honored; mutation batch atomicity.

### 11.2 Integration tests (in `gfs-simulator`)

These exercise the cluster end-to-end:

1. **Basic write + read** — single client, single chunk, 1 KB write, read back.
2. **Multi-chunk file** — write 200 MB → 4 chunks; read random offsets; verify CRC.
3. **Concurrent record append** — 100 clients appending to the same file; verify all records present (allowing for padding); verify primary serialization (no overlap).
4. **Primary failure mid-write** — start a write; kill the primary chunkserver between PushBytes and CommitWrite; verify lease expires; verify client's retry succeeds against new primary.
5. **Secondary failure during ApplyMutation** — kill one secondary mid-apply; verify the other two replicas commit; verify master re-replicates the missing replica.
6. **Chunkserver death + re-replication** — kill a chunkserver holding 10 chunks; verify within 60 s those chunks are re-replicated to other healthy chunkservers; verify replication factor restored.
7. **Master crash recovery** — write 1000 entries; kill master mid-write; restart master; verify in-flight write's outcome matches one of (committed | not-committed) and is consistent across replicas.
8. **Shadow master** — bring up shadow; perform 100 mutations on live master; verify shadow's state matches live master after replay.
9. **Garbage collection** — delete a file; verify chunks remain for 3 days; advance simulated time past 3 days; verify chunkservers receive `DELETE_CHUNK` commands.
10. **Lease expiration without holder failure** — primary stops sending heartbeats but doesn't crash; verify lease expires at 60 s; verify master grants new lease cleanly.

### 11.3 Performance smoke tests

Not for v1, but the spec hooks for these are:

- **Write throughput** — 100 MB/s sustained across daisy chain on a single gigabit link.
- **Read throughput** — N clients × 100 MB/s, scales linearly with chunkserver count.
- **Master RPC rate** — ~5 k Stat/sec + ~1 k GetChunkLocations/sec on a single-threaded master implementation.
- **Recovery time** — < 5 s for master crash recovery on a 100 K-file namespace.

---

## 12. Configuration knobs

```properties
# gfs.properties — all values shown are defaults

# Cluster-wide
gfs.replication-factor = 3
gfs.chunk-size-bytes = 67108864          # 64 MB
gfs.block-size-bytes = 65536             # 64 KB CRC granularity

# Lease
gfs.lease-duration-seconds = 60

# Heartbeat
gfs.heartbeat-interval-seconds = 5
gfs.heartbeat-missed-threshold = 3       # 3 misses (15 s) = dead

# Op log
gfs.oplog.dir = /var/lib/gfs/oplog
gfs.oplog.remote-mirror-dirs = /var/lib/gfs/oplog-mirror-{1,2,3}
gfs.oplog.fsync-on-append = true

# Checkpoint
gfs.checkpoint.dir = /var/lib/gfs/checkpoints
gfs.checkpoint.entry-threshold = 100000
gfs.checkpoint.time-threshold-hours = 6

# Reaper
gfs.reaper.scan-interval-seconds = 30
gfs.gc.retention-days = 3

# Chunkserver storage
gfs.chunkserver.data-dir = /var/lib/gfs/chunks

# Shadow master
gfs.shadow.lag-threshold-seconds = 30
```

---

## 13. What's stubbed for pedagogy

Explicit list of departures from a production GFS, in the order a reader will encounter them:

1. **Op log "replication" is local** — same disk, different directories. Production GFS replicates to remote machines. The wire format is identical; only the destination is mocked.
2. **Master election** — there is no master election. Bring-up is manual or via systemd-style restart. Chubby (the paper's election service) is out of scope.
3. **Cross-rack placement awareness** — replica selection picks healthy chunkservers but doesn't model racks/datacenters. A `ChunkserverState` field for rack ID is reserved for v2.
4. **Network is fast and reliable** — no congestion, no jitter, no asymmetric latencies. Simulator drives explicit failures.
5. **Snapshots** — out of scope. Paper's snapshot mechanism is interesting but orthogonal.
6. **Quotas, namespace ACLs, audit logging** — all out of scope.
7. **Performance is not a goal** — clean code over fast code. A JIT warm-up isn't even needed for the tests.

These departures are explicit so a reader can see exactly where the pedagogical version stops short of production, and so a future v2 can target each one if desired.

---

## 14. Glossary

| Term | Definition |
|---|---|
| **Chunk** | A 64 MB unit of allocation and replication. Files are composed of one or more chunks. |
| **Chunk handle** | A 64-bit monotonic identifier for a chunk, assigned by the master at allocation time. Stable for the chunk's lifetime. |
| **Chunkserver** | A process that stores chunks on local disk and serves reads/writes to clients. |
| **Master** | The single process that owns all metadata: namespace, chunk-to-replica maps, lease state. |
| **Lease** | A bounded delegation from the master to one chunkserver, granting that chunkserver the right to serialize mutations on a specific chunk for a fixed duration (60 s). |
| **Primary replica** | The chunkserver currently holding the lease for a chunk. Picks serial numbers; forwards mutations to secondaries. |
| **Secondary replica** | The other chunkservers holding copies of a chunk. Apply mutations in the order the primary specifies. |
| **Operation log** | An append-only file of metadata mutations. The master's durability boundary. Replicated to N remote locations. `fsync`'d before every client ack. |
| **Checkpoint** | A periodic full snapshot of the master's in-memory state to disk. Combined with the op log tail, lets a crashed master recover. |
| **Shadow master** | A separate process that tails the live master's op log and serves read-only metadata queries. Not a hot standby. |
| **Record append** | The native GFS write API. Client supplies a record; the primary picks the offset (the chunk's current end). At-least-once semantics. |
| **Padding** | Zero bytes the primary writes to fill a chunk when an incoming record won't fit. Visible to readers. |
| **Re-replication** | The master's background process of detecting under-replicated chunks (<3 copies) and instructing healthy chunkservers to copy them. |
| **Heartbeat** | Periodic message from chunkserver to master (every 5 s) carrying the chunkserver's chunk list, disk state, and lease renewal requests. |
| **Daisy chain** | Replication topology where bytes flow `client → A → B → C` in sequence. Each node's outbound NIC is saturated independently; primary's NIC is not divided. |
| **Chain order** | The replica ordering picked for a daisy chain. Chosen by topological distance: nearest replica to the client is first. |
| **CRC32C** | Castagnoli CRC32, the per-block checksum stored alongside chunk data. 64 KB block granularity. |
| **Stale replica** | A replica the master has marked as not-up-to-date — either because a write failed there, or because the chunkserver missed heartbeats. Re-replication targets these. |

---

## 15. Connection to the buildup arc

This implementation plan exists inside a four-paper deep-dive arc:

- [**GFS deep-dive**](gfs.md) (this paper's narrative companion) — explains *why* each decision was made and where each one aged.
- [**Ceph RADOS deep-dive**](ceph-rados.md) — the next paper; removes the central master via CRUSH + monitors.
- [**Colossus deep-dive**](colossus.md) — the production answer; shards the master over BigTable.
- [**Ceph BlueStore deep-dive**](ceph-bluestore.md) — the storage-backend rewrite; replaces POSIX FS with raw block.
- [**Synthesis**](distributed-file-system.md) — what the design would look like today.

The companion implementation plans for the other three papers are future work. Each will inherit the same conventions (Java 17 / Gradle multi-module / hand-rolled wire format / no JNI), with adjustments for each paper's specific architectural choices — Ceph RADOS adds CRUSH, Colossus uses an embedded KV store as the metadata backend, BlueStore uses an embedded RocksDB for its metadata layer.

Reading the four plans in chronological order is the same pedagogical arc as reading the four deep-dives in chronological order: each plan reuses what the previous plan established, and each one adds exactly one architectural move.

---

## 16. Sanity checks before you start building

A builder following this spec end-to-end should be able to answer these without scrolling back:

1. What does the master keep in RAM, and what does it write to disk?
2. When the client writes 10 KB at offset 150 MB to a file, what bytes go where, in what order, and which fsyncs happen before the client gets ack'd?
3. If the primary chunkserver dies mid-write, what is the worst-case data outcome the client sees, and how does the system reconverge?
4. Why is the chunk size 64 MB and not 16 MB or 256 MB? What changes if you pick differently?
5. Why is the replication topology a daisy chain and not a star?
6. What's the difference between "lease grant" and "lease renewal"? Why does the master never push-renew?
7. What's the structural ceiling of this design, and where does it come from?

If any of these is unclear after reading this spec, the spec is incomplete and should be improved. The bar is: a Java engineer who has read the GFS paper and this document together has zero design ambiguity left.

---

*This implementation plan is companion to [gfs.md](gfs.md). The plan is the *how*; the deep-dive is the *why*.*
