---
title: "Flow: byte-range write path"
flow: write-path
modules: [gfs-client, gfs-master, gfs-chunkserver]
updated: 2026-05-31
---

# Flow: Byte-Range Write Path

How a byte-range write (`GfsClient.write(path, offset, bytes)`) travels from client to three durable replicas. This is the foundation of every mutation; record append (see [`record-append.md`](./record-append.md)) is a variation on it.

## Sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master
    participant A as Primary (A)
    participant B as Secondary (B)
    participant D as Secondary (C)

    C->>M: GetChunkLocations(path, offset, size)
    Note over M: grant lease if none valid;<br/>fsync GrantLease to op log
    M-->>C: handle, version, replicas[A,B,C], primary=A, lease
    Note over C: pick chain by distance: C → A → B → C
    C->>A: PushBytes(handle, bytes, chain=[B,C])
    A->>B: PushBytes(... chain=[C])
    B->>D: PushBytes(... chain=[])
    D-->>B: ack
    B-->>A: ack
    A-->>C: ack (bytes buffered on all 3)
    C->>A: CommitWrite(handle, offset, size)
    Note over A: assign serial = N
    A->>B: ApplyMutation(serial=N, offset, size)
    A->>D: ApplyMutation(serial=N, offset, size)
    Note over A,D: write bytes, recompute CRC32C, fsync
    D-->>A: ApplyAck(N, ok)
    B-->>A: ApplyAck(N, ok)
    A-->>C: CommitResponse(OK, serial=N)
```

## Steps

1. **Locate** — client calls `GetChunkLocations`; `OffsetMath` has already turned the file offset into a (chunk index, offset-in-chunk) pair. If no valid lease exists, the master picks a primary, logs `GrantLease` (fsync), and returns the metadata.
2. **Push** — bytes flow down the daisy chain `client → A → B → C`. Each node buffers in RAM and forwards; this saturates each outbound NIC independently instead of dividing the primary's bandwidth (ADR 0005).
3. **Commit** — client sends `CommitWrite` to the primary only. The primary assigns a serial number (the mutation order) and forwards `ApplyMutation` to the secondaries.
4. **Apply + durable** — each replica writes the buffered bytes at the offset, recomputes the CRC32C of every touched 64 KB block, and `fsync`s **before** acking.
5. **Respond** — once all replicas ack, the primary returns `OK` with the serial.

## State mutations

- **Master:** only at lease-grant time (logged). None during the write itself — the master is off the write path.
- **Chunkservers:** `<handle>.chunk` extended + bytes written; `<handle>.crc` updated; both `fsync`'d.

## Failure modes

| Failure | What happens |
|---|---|
| Lease expired between Push and Commit | primary rejects `LEASE_EXPIRED`; client re-fetches metadata and retries |
| One secondary fails to apply | primary reports the stale replica to the master via heartbeat; master marks it stale and triggers re-replication |
| Primary crashes before Commit | secondaries time out the buffered bytes (60 s); client's commit connection drops; client retries against the new primary after the lease expires |
| CRC32C mismatch on later read | chunkserver returns `CHECKSUM_FAILURE`, reports corruption; master re-replicates from a healthy replica |

## Related

- Modules: [`gfs-client`](../modules/gfs-client.md), [`gfs-chunkserver`](../modules/gfs-chunkserver.md), [`gfs-master`](../modules/gfs-master.md)
- Next: [`record-append.md`](./record-append.md)
- ADR: [0005 pipelined daisy-chain](../decisions/0005-pipelined-daisy-chain.md)
