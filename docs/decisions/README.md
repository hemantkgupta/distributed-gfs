---
title: "Architecture Decision Records"
type: decisions-index
updated: 2026-05-31
---

# Architecture Decision Records

ADRs capture the load-bearing design choices in this repo — what was decided, why, and what was rejected. Each is short and focused on one decision. They are drawn from §10 of the implementation plan; together they ARE the GFS design, decision by decision.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](./0001-in-memory-master.md) | In-memory master with op log + checkpoint | Accepted |
| [0002](./0002-64mb-chunks.md) | 64 MB chunks | Accepted |
| [0003](./0003-at-least-once-record-append.md) | At-least-once record append | Accepted |
| [0004](./0004-lease-based-primary-copy.md) | Lease-based primary-copy serialization | Accepted |
| [0005](./0005-pipelined-daisy-chain.md) | Pipelined daisy-chain replication | Accepted |
| [0006](./0006-one-chunk-one-file.md) | One chunk = one file (no LSM / embedded KV) | Accepted |
| [0007](./0007-hand-rolled-binary-rpc.md) | Hand-rolled binary RPC framing | Accepted |
| [0008](./0008-oplog-fsync-before-ack.md) | Op-log fsync before client ack | Accepted |
| [0009](./0009-java17-gradle-multimodule.md) | Java 17 + Gradle multi-module + JUnit 5 / AssertJ | Accepted |
| [0010](./0010-no-jni.md) | No JNI dependencies | Accepted |
