---
title: "ADR 0009: Java 17 + Gradle multi-module + JUnit 5 / AssertJ"
adr: 9
status: accepted
updated: 2026-05-31
---

# ADR 0009: Java 17 + Gradle multi-module + JUnit 5 / AssertJ

## Status

Accepted.

## Context

The repo needs a build system, a language level, and a test stack — consistent with the other CSE companion repos so a reader moving between them isn't re-learning conventions.

## Decision

- **Java 17** via a Gradle toolchain (`languageVersion = 17`).
- **Gradle multi-module** — one module per architectural concern (7 modules), with `implementation project(...)` edges encoding the dependency rules at build time.
- **JUnit 5 + AssertJ** for tests; one focused test class per concept.
- **Records** for value objects; **sealed interfaces** for exhaustive ADTs (e.g. `LogEntry`).

## Consequences

**Positive:**
- The acyclic module graph makes the control/data-plane separation a *compile-time* constraint, not a convention.
- Records + sealed interfaces give value equality and exhaustive pattern handling with minimal code.

**Negative / known divergence:**
- The package root here is **`gfs.*`** (e.g. `gfs.common`, `gfs.master`), whereas some sibling repos use **`com.hkg.*`**. This is a cosmetic departure; a rename to `com.hkg.gfs.*` is a mechanical follow-up if cross-repo consistency is wanted. Recorded here so the divergence is explicit rather than accidental.
- Pattern-matching `switch` is preview in Java 17 — `instanceof` chains are used instead for portability.

## Alternatives considered

- **Maven** — rejected: Gradle matches the sibling repos and has cleaner multi-module ergonomics.
- **Single module** — rejected: loses the build-time plane separation that documents the architecture.

## Related

- [`../architecture.md`](../architecture.md) — the module graph
- [`code-companion.md`](../code-companion.md) — the `gfs.*` vs `com.hkg.*` gap row
