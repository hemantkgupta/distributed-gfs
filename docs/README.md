# distributed-gfs — documentation hub

> **Canonical narrative:** the per-paper deep-dive at `~/CSE-Raw/raw-blog/distributed-file-system/gfs.md` and the companion implementation plan at `~/CSE-Raw/raw-blog/distributed-file-system/gfs-implementation-plan.md`. The deep-dive explains *why* the 2003 Google File System was designed the way it was; the implementation plan is the *what* and the *how*. This `docs/` folder describes what is **actually built** in this Java repo and maps every architectural decision from the plan to a file.
>
> Last reconciled with the repo on 2026-06-07.

## What this repo is

A pedagogical, paper-faithful Java 17 reimplementation of the **2003 Google File System** (Ghemawat, Gobioff, Leung — SOSP 2003). It reproduces the load-bearing architecture of the original: a single in-memory master with op-log + checkpoint durability, 64 MB chunks, 3× replication, lease-based primary-copy serialization, pipelined daisy-chain replication, at-least-once record append, chunkserver heartbeats with re-replication, and a shadow master for stale reads. Every module is pure Java with in-memory or local-disk substrates — no JNI, no protobuf, no embedded KV store.

## Pick your path

| If you want to … | Start with | Then |
|---|---|---|
| Understand what this repo IS in 5 minutes | [`getting-started.md`](./getting-started.md) → [`architecture.md`](./architecture.md) | the deep-dive `gfs.md` |
| Build, run tests, and explore one module | [`getting-started.md`](./getting-started.md) | the module page for your area |
| Trace a write end-to-end through the modules | [`flows/write-path.md`](./flows/write-path.md) | [`modules/gfs-client.md`](./modules/gfs-client.md) + downstream pages |
| Understand record append (the GFS-native write) | [`flows/record-append.md`](./flows/record-append.md) | [`modules/gfs-chunkserver.md`](./modules/gfs-chunkserver.md) |
| Map a design decision to code | [`code-companion.md`](./code-companion.md) | the specific module page |
| Understand a module's contract | [`modules/<module>.md`](./modules/) | its tests under `<module>/src/test/` |
| Understand why a design choice was made | [`decisions/README.md`](./decisions/README.md) | the relevant ADR |
| Look up an unfamiliar term | [`glossary.md`](./glossary.md) | the deep-dive section it appears in |
| Read about a sequence (write, append, repair, recovery) | [`flows/README.md`](./flows/README.md) | the involved modules |

## The doc tree

### Reference

- [`getting-started.md`](./getting-started.md) — first 30 minutes: build, test, explore.
- [`architecture.md`](./architecture.md) — the big picture: module dependency graph, planes, how plan decisions map to packages.
- [`glossary.md`](./glossary.md) — vocabulary lookup (chunk, lease, primary, op log, daisy chain, …).
- [`code-companion.md`](./code-companion.md) — per-section map: every section of the implementation plan → modules + key classes; explicit gaps where the plan describes a mechanism the code does not (yet) implement.

### Modules — [`modules/`](./modules/)

One page per module. Every page follows a strict 7-section template: Role → Plan anchor → Public API surface → Internal structure → Key tests → Where it fits → Stubs and departures from production.

7 modules across 5 phases. See [`modules/README.md`](./modules/README.md) for the full index.

| Phase | Modules |
|---|---|
| 1 — Foundation | `gfs-common`, `gfs-oplog` |
| 2 — Master (control plane) | `gfs-master` |
| 3 — Chunkserver (data plane) | `gfs-chunkserver`, `gfs-replication` |
| 4 — Client + protocols | `gfs-client` |
| 5 — Resilience + simulation | `gfs-simulator` |

### Flows — [`flows/`](./flows/)

End-to-end sequences crossing multiple modules. Mermaid sequence diagrams + step walkthroughs + failure modes. See [`flows/README.md`](./flows/README.md) for the index.

| Flow | What it shows |
|---|---|
| [`write-path.md`](./flows/write-path.md) | Byte-range write: GetChunkLocations → lease → PushBytes daisy chain → CommitWrite |
| [`record-append.md`](./flows/record-append.md) | At-least-once append: primary picks offset, padding, RETRY_ON_NEXT_CHUNK |
| [`read-path.md`](./flows/read-path.md) | GetChunkLocations → ReadChunk against nearest replica → CRC32C verify |
| [`re-replication.md`](./flows/re-replication.md) | Chunkserver death → reaper detects under-replication → CopyChunk |
| [`master-recovery.md`](./flows/master-recovery.md) | Master crash → checkpoint load → op-log replay → resume |

### Decisions — [`decisions/`](./decisions/)

Architecture Decision Records. Why each load-bearing choice was made — and what alternatives were rejected. See [`decisions/README.md`](./decisions/README.md) for the index. The ADRs are drawn from §10 of the implementation plan.

## How docs and code stay in sync

The **mapping rule**: every load-bearing decision in the implementation plan that this code implements must have exactly one module page that names it, and every module page must declare in its "Plan anchor" section which plan decisions it realizes. If the code adds behavior the plan doesn't describe, the module page's "Stubs and departures" section says so; if the plan describes a mechanism the code doesn't implement, the same section records the gap.

Two failure modes the rule protects against:

1. The plan describes a mechanism that has no code (false advertising).
2. The code does something the plan never mentions (orphaned behavior with no architectural reasoning).

When changing module code, update the matching `modules/<m>.md` and any `flows/*.md` that touch it. New design decision? Add an ADR in `decisions/`.

## Running the code

```bash
./gradlew build                 # compile all 7 modules + run all tests (63 tests)
./gradlew :gfs-common:test      # one module
./gradlew :gfs-simulator:test   # end-to-end cluster scenarios
```

See [`getting-started.md`](./getting-started.md) for the full first-30-minutes path.
