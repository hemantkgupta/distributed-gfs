---
title: "gfs-simulator — module guide"
module: gfs-simulator
phase: 5
updated: 2026-05-31
---

# `gfs-simulator`

## 1. Role

The integration harness. Boots a master + N chunkservers + M clients in a single JVM, drives the canonical GFS workloads, and injects faults (kill the primary mid-write, partition a replica, crash the master) so the resilience paths are exercised by tests rather than by waiting for real hardware to fail.

## 2. Plan anchor

- §3.3 — `Cluster`, `FaultInjector`, `WorkloadDriver`
- §11.2 — integration test scenarios
- §1.3 — failures simulated via deliberate timeouts and explicit disconnects

## 3. Public API surface

| Class | Kind | Purpose |
|---|---|---|
| `Cluster` | class | boots and wires a master + chunkservers + clients in one JVM; lifecycle control |
| `FaultInjector` | class | programmatic faults: drop heartbeats, kill a primary, partition a replica, fill a disk |

## 4. Internal structure

`Cluster` constructs the real `Master`, real `Chunkserver` instances, and real `GfsClient`s, connected over loopback (or in-process) so the wire format is genuinely exercised — only physical host separation is mocked. `FaultInjector` reaches into that running cluster to simulate failures deterministically. `IntegrationTest` composes them into scenarios: multi-chunk write/read, concurrent record append (allowing padding + duplicates), chunkserver death + re-replication, and master crash recovery.

## 5. Key tests

| Test | What it locks down |
|---|---|
| `IntegrationTest` | end-to-end write+read across chunk boundaries; concurrent appenders; kill-a-chunkserver → re-replication restores factor; kill-the-master → restart recovers full state |

## 6. Where it fits

```
(all modules) ──► gfs-simulator
```

The only module allowed to depend on everything. Integration harness — nothing depends on it.

## 7. Stubs and departures from production

- Runs the "cluster" in one JVM by default; the plan's separate-JVM / separate-host modes are mocked.
- Faults are injected programmatically and synchronously, so scenarios are reproducible — real clusters see asynchronous, partial, and ambiguous failures.

## 8. Related

- Module index: [`README.md`](./README.md)
- All flows under [`../flows/`](../flows/) are validated here.
