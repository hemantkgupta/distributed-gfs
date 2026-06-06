---
title: "ADR 0010: No JNI dependencies"
adr: 10
status: accepted
updated: 2026-05-31
---

# ADR 0010: No JNI dependencies

## Status

Accepted.

## Context

A faithful storage system is tempting to build on native libraries: embedded RocksDB (`rocksdbjni`), Intel ISA-L for erasure coding, a raw block device via JNI. Each brings a native toolchain requirement.

## Decision

**No JNI.** Pure JDK + Gradle. Chunks are plain files (ADR 0006); CRC32C uses `java.util.zip.CRC32C`; the wire format is hand-rolled (ADR 0007); the master holds state in `HashMap`s (ADR 0001). A reader can `./gradlew build` on any laptop with a JDK and no native toolchain.

## Consequences

**Positive:**
- Zero-friction build; every architectural claim traces to readable Java source.
- Portable across macOS / Linux / any JDK 17.

**Negative:**
- Cannot reproduce native-speed paths (raw block I/O, hardware Reed-Solomon). For GFS this costs nothing — the paper's design is FS-file-based anyway.

## Note for later papers in the arc

This is a default, not a dogma. The **BlueStore** implementation plan deliberately *breaks* it: BlueStore is built on RocksDB, so reimplementing it without `rocksdbjni` would obscure the very thing being studied. Such exceptions get their own ADR in their own repo. For GFS there is no such forcing function — pure JDK is faithful.

## Alternatives considered

- **Embedded RocksDB for the master / chunkserver** — rejected: anachronistic for GFS (ADR 0001, ADR 0006) and pulls in JNI.

## Related

- ADRs: [0001 in-memory master](./0001-in-memory-master.md), [0006 one chunk one file](./0006-one-chunk-one-file.md), [0007 hand-rolled binary RPC](./0007-hand-rolled-binary-rpc.md)
