---
title: "ADR 0007: Hand-rolled binary RPC framing"
adr: 7
status: accepted
updated: 2026-05-31
---

# ADR 0007: Hand-rolled binary RPC framing

## Status

Accepted.

## Context

Every component talks over TCP: client↔master, client↔chunkserver, chunkserver↔chunkserver, chunkserver↔master. The messages need a serialization format.

## Decision

Hand-roll a small, big-endian, length-prefixed binary format (`WireMessage` = `length | type | reqId | payload`; `WireCodec` for each message type). The payload schema is **positional and tag-free**. No protobuf, no JSON, no Java serialization.

## Consequences

**Positive:**
- Every byte on the wire is accounted for and inspectable (verifiable against `tcpdump`).
- No dependencies; no codegen step.
- A mis-encoded byte surfaces immediately as a wrong value or an underflow exception — errors are loud and local, which is pedagogically the point.

**Negative:**
- No schema evolution / backward compatibility (positional format). Acceptable for a single-version teaching system.
- Hand-written encode/decode is more code than an annotated schema.

## Alternatives considered

- **Protobuf / gRPC** — rejected: hides the bytes, adds a codegen toolchain and dependencies.
- **Java serialization** — rejected: a security and stability landmine; obscures the wire format entirely.

## Related

- Module: [`gfs-common`](../modules/gfs-common.md) (`WireCodec`, `WireMessage`, `MessageType`)
