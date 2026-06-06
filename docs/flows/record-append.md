---
title: "Flow: record append"
flow: record-append
modules: [gfs-client, gfs-master, gfs-chunkserver]
updated: 2026-05-31
---

# Flow: Record Append

The GFS-native write path — what MapReduce, crawls, and every production workload use. The client supplies a record; the **primary** picks the offset (the chunk's current end). Semantics are **at-least-once**: padding and duplicates can appear, and the application is responsible for deduping.

## Sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master
    participant A as Primary (A)
    participant B as Secondaries (B,C)

    C->>M: GetChunkLocations(LAST chunk of path)
    M-->>C: handle, primary=A, replicas
    C->>A: PushBytes(record, chain)
    A->>B: PushBytes(...)
    B-->>A: ack
    A-->>C: ack
    C->>A: CommitRecordAppend(handle)
    Note over A: currentSize + recordSize > 64 MB ?
    alt record won't fit
        Note over A,B: pad chunk to 64 MB on all replicas
        A-->>C: RETRY_ON_NEXT_CHUNK
        C->>M: GetChunkLocations (master allocates NEW last chunk)
        M-->>C: new handle, primary, lease
        C->>A: PushBytes(record) ; CommitRecordAppend(handle')
    end
    Note over A: offset = currentSize ; serial = N
    A->>B: ApplyMutation(serial=N, offset, append)
    B-->>A: ApplyAck(N, ok)
    A-->>C: OK(handle, offset, size)
```

## Steps

1. **Locate last chunk** — the client fetches the file's *last* chunk and its primary.
2. **Push** — the record flows down the daisy chain, same as a byte-range write.
3. **Fit check** — on `CommitRecordAppend`, the primary checks `currentSize + recordSize` against 64 MB.
4. **Pad + retry** — if it won't fit, the primary pads the chunk to exactly 64 MB **on all replicas** and returns `RETRY_ON_NEXT_CHUNK`. The client asks the master for the next chunk (the master allocates one and grants a lease) and retries.
5. **Append** — otherwise the primary sets the offset to the chunk's current size, assigns a serial, forwards `ApplyMutation`, and returns the chosen offset to the client.

## Why at-least-once

On any mid-append failure the client retries. A retry may land the record at a different offset on different replicas, producing **duplicates**. The paper accepts this; so does this implementation. Padding leaves zero regions inside chunks. Therefore:

- Each record should carry a **unique ID + checksum**.
- Readers must **skip padding** (zero regions) and **dedupe** by record ID.

## Failure modes

| Failure | What happens |
|---|---|
| Record larger than 64 MB | rejected — a single record must fit in one chunk |
| Primary crash mid-append | client retries; may create a duplicate at a new offset (at-least-once) |
| Secondary lags | primary still serializes; lagging replica is marked stale and re-replicated |
| Concurrent appenders | primary serializes all of them; every record lands exactly once *per successful attempt*, in primary-chosen order |

## Related

- Modules: [`gfs-chunkserver`](../modules/gfs-chunkserver.md), [`gfs-client`](../modules/gfs-client.md)
- Prereq: [`write-path.md`](./write-path.md)
- ADR: [0003 at-least-once record append](../decisions/0003-at-least-once-record-append.md)
