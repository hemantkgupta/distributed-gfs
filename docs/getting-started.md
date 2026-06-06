---
title: "Getting started — distributed-gfs"
type: getting-started
updated: 2026-06-07
---

# Getting Started

First 30 minutes with the repo: build it, run the tests, explore one module.

## Prerequisites

- **Java 17** (the repo targets Java 17 via a Gradle toolchain; `jenv local 17.0` if you use jenv).
- No other tooling — the Gradle wrapper is vendored (`./gradlew`).

## Build everything

```bash
./gradlew build
```

Compiles all 7 modules and runs every test (63 tests). Should be green.

## Run the tests for one module

```bash
./gradlew :gfs-common:test
./gradlew :gfs-master:test
```

## Run the end-to-end simulator

```bash
./gradlew :gfs-simulator:test
```

The simulator boots a master + N chunkservers + clients in a single JVM and drives the canonical scenarios: multi-chunk write/read, concurrent record append, chunkserver death + re-replication, master crash recovery.

## Explore — suggested order

1. **`gfs-common`** — the vocabulary: `ChunkHandle`, `LeaseToken`, the hand-rolled wire format (`WireCodec`, `MessageType`). Start here.
2. **`gfs-oplog`** — the master's durability boundary: `OperationLog` (append + replay) and `Checkpoint`.
3. **`gfs-master`** — the single in-memory master: namespace, chunk map, lease manager, reaper.
4. **`flows/write-path.md`** and **`flows/record-append.md`** — how a write and an append travel through the cluster.

## Where to look when …

| Question | File |
|---|---|
| What does this module do? | `modules/<module>.md` |
| How does a write work? | `flows/write-path.md` |
| How does record append avoid one-consensus-per-write? | `flows/record-append.md` + ADR 0004 |
| Why this design? | `decisions/<adr>.md` |
| What does this term mean? | `glossary.md` |
| Which class implements X from the plan? | `code-companion.md` |

## A note on package naming

Source lives under the `gfs.*` package root (e.g. `gfs.common`, `gfs.master`). This differs from the `com.hkg.*` convention used by some sibling repos; see [ADR 0009](./decisions/0009-java17-gradle-multimodule.md).

## Related

- [`README.md`](./README.md) — doc hub
- [`architecture.md`](./architecture.md) — big picture
- Implementation plan: `~/CSE-Raw/raw-blog/distributed-file-system/gfs-implementation-plan.md`
