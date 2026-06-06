---
title: "gfs-replication — module guide"
module: gfs-replication
phase: 3
updated: 2026-05-31
---

# `gfs-replication`

## 1. Role

Intended home of the daisy-chain replication primitives — one node of the `client → A → B → C` chain. **As built, this module is an empty placeholder**: it has a `build.gradle` and sits on the dependency graph, but contains no source. The chain logic it was meant to hold currently lives in `gfs-chunkserver` (`ChunkserverRpcServer`, `Chunkserver`) and `gfs-client` (`ChainReplicationDriver`).

## 2. Plan anchor

- §3.3 — the plan placed `ChainNode`, `MutationBatch`, `MutationApplier` here
- §6.2 — pipelined daisy-chain replication
- ADR 0005 — pipelined daisy-chain

## 3. Public API surface

None yet. Reserved for:

| Planned class | Kind | Purpose |
|---|---|---|
| `ChainNode` | class | one node of the daisy chain: receive upstream bytes, buffer, forward downstream |
| `MutationBatch` | record | a pending mutation (bytes + serial + chunk + offset) before commit |
| `MutationApplier` | class | applies a sealed batch to the underlying `ChunkStore` |

## 4. Internal structure

Empty. The dependency edge `gfs-chunkserver → gfs-replication` is retained so the intended seam between "chunk storage" and "chain forwarding" is visible in the build graph, even though the forwarding code has not yet been factored out of `gfs-chunkserver`.

## 5. Key tests

None (no source). Chain behavior is exercised indirectly through `ChunkserverTest`, `ChainReplicationDriverTest`, and the simulator's integration tests.

## 6. Where it fits

```
gfs-common ──► gfs-replication ──► gfs-chunkserver
```

Depends only on `gfs-common`.

## 7. Stubs and departures from production

**This whole module is the departure.** Two clean ways to resolve it, recorded here so the choice is explicit:

1. **Populate it** — move the chain-forwarding code out of `gfs-chunkserver` (`ChunkserverRpcServer`) into `ChainNode` / `MutationApplier` here, matching plan §3.3.
2. **Fold it away** — delete the module and the dependency edge, and document that the chain lives in `gfs-chunkserver`.

Until one is chosen, the placeholder is kept and this page is the source of truth for why.

## 8. Related

- Module index: [`README.md`](./README.md)
- Module that actually holds the logic: [`gfs-chunkserver`](./gfs-chunkserver.md), [`gfs-client`](./gfs-client.md)
- ADR: [0005 pipelined daisy-chain](../decisions/0005-pipelined-daisy-chain.md)
