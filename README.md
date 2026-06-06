# distributed-gfs

Java 17 / Gradle companion implementation of the 2003 Google File System design.

This repo is a pedagogical multi-module build, not a production filesystem. It implements the load-bearing GFS mechanisms in local JVMs and local disk directories:

- single in-memory master with operation log and checkpoints
- 64 MB chunks and configurable replication
- lease-based primary-copy writes
- pipelined daisy-chain byte replication
- at-least-once record append with retry-on-next-chunk behavior
- chunkserver heartbeats, lazy chunk garbage collection, and re-replication
- shadow master catch-up for stale metadata reads

## Build

```bash
./gradlew build
```

Useful focused commands:

```bash
./gradlew test
./gradlew :gfs-master:test
./gradlew :gfs-chunkserver:test
./gradlew :gfs-simulator:test
```

## Module Map

| Module | Role |
|---|---|
| `gfs-common` | Shared value types and hand-rolled binary wire codec |
| `gfs-oplog` | Master operation log and checkpoint persistence |
| `gfs-master` | Namespace, chunk metadata, leases, recovery, reaper, shadow master |
| `gfs-chunkserver` | Local chunk store, heartbeats, lease-holder mutation path, copy tasks |
| `gfs-client` | Client API, metadata cache, read/write/record-append flows |
| `gfs-simulator` | Single-JVM cluster harness and integration tests |
| `gfs-replication` | Reserved placeholder; chain logic currently lives in `gfs-chunkserver` and `gfs-client` |

## Documentation

Start with [docs/README.md](docs/README.md), then use:

- [docs/architecture.md](docs/architecture.md) for the system and module dependency graph
- [docs/code-companion.md](docs/code-companion.md) for plan-section-to-code mapping and explicit gaps
- [docs/flows/](docs/flows/) for read, write, append, recovery, and re-replication sequences
- [docs/modules/](docs/modules/) for per-module contracts

## Known Gaps

- `gfs-replication` is intentionally empty for now. The current build keeps the module as a documented placeholder while the implemented chain behavior remains in `gfs-chunkserver` and `gfs-client`.
- The simulator is local-process/local-disk only; it models GFS architecture and failure paths without cross-host deployment.
