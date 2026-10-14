# What works in this release candidate

- One `acyclic-harness` crate owning operation identities, authority, durable
  reducer semantics, typed interactions, effects, atomic fork manifests,
  resumable state machines, structured scheduling, and replaceable execution.
- Direct Stream-backed Agent, Conversation, Session, Turn, and Task histories,
  with deterministic native/WASM replay and snapshot-assisted restoration.
- Provider-neutral model, context-stage, tool definition/executor/projection,
  and custom-executor APIs; Stream-backed memory/retrieval/skills revisions,
  digest-bound compaction, and a complete coding host-adapter bundle remain
  ordinary code-defined compositions.
- A framework-neutral `@acyclic-labs/harness` WASM/client package with reconnect,
  generation-fenced cursors, an explicit-namespace atomic bounded IndexedDB cursor/outbox store,
  bounded hydration, and
  bidirectional pagination; thin React and Svelte bindings remain separate.
- One transport-neutral Rust server port with separate HTTP/SSE/WebSocket and
  gRPC adapter crates; embedded, JSONL/stdio, browser/Bun WebSocket, HTTP/SSE,
  and gRPC clients negotiate the same descriptor and capabilities first. The
  same owner- and scope-bound operation status and exact-retry cancellation
  contract is available on every transport, including atomic recursive cancel.
- Explicit Filesystem and Machines integration crates. Filesystem workspaces
  support exact-generation reads and atomic idempotent mutation batches;
  Machines references remain opaque and provider-bound.
- Native dependency-free OpenAI-compatible streaming in `@acyclic-labs/harness`,
  plus a separately versioned Pi bridge package.
- In-memory Filesystem, Stream, and Objects providers, plus a deterministic
  process-local Machines simulator.
- A single canonical Filesystem engine over the public Stream and Objects
  provider traits, with memory and durable-local compositions, sparse
  content-addressed generations, source capture, safe rebase and join,
  S3 workspace views, native watchers and mounts, core-owned journaled
  materialization, browser WASM persistence, a TypeScript facade, and an N-API
  embedded engine.
- A hierarchical Stream v1 Rust contract, bounded structural-sharing memory
  provider, checksummed crash-recoverable local provider, authenticated gRPC
  client/server adapter, exact retry semantics, immutable-prefix forks, gapless
  follow, and atomic optimistic commits. The memory and local providers execute
  the same semantic state machine; the local feature adds only bounded durable
  publication and recovery.
- An Objects v1 logical current-key contract with Rust and TypeScript gRPC/HTTP
  clients, conditions, exact retry receipts, bounded listing and multipart
  publication. Native memory and durable-local providers share one state machine;
  local recovery authenticates inline small bodies and immutable larger bodies,
  with grouped native batches and private compaction. Native Filesystem
  compositions use the sole Objects v1 contract. Roots with obsolete format discriminators are rejected without conversion or
  overwrite.
- Actors v1 with Rust validation, generated TypeScript types and authenticated gRPC/HTTP clients. Workers deploy stateless modules through these Actors operations, with code stored in Objects. Hosted acceptance requires separate qualification.
- An Inference v1 Rust client with immutable item-addressed Context revisions,
  independent forks, exact edit/compact/transfer, recoverable Runs, inclusive
  event replay, cancellation, four work meters, and admitted warm commitments.
- A generated-contract TypeScript Inference client covering the same Context,
  warm-retention, recoverable Run, watch, and cancellation lifecycle through an
  authenticated protobuf-JSON transport.
- A shape-free Machines v1 contract with immutable image qualification, exact
  idempotency, checkpoints, fork sets, lifecycle recovery, stable endpoints,
  events, usage receipts, a mutual-TLS/Unix client, and one bounded deterministic
  simulator.
- An executable bounded recursive workload and package-bound cross-language
  conformance runner that retains per-case evidence, an immutable source identity,
  and a deterministic qualification receipt with the exact release archives.
- TypeScript contract facades for families whose public contract includes JavaScript.
- Protobuf package boundaries ready for audited service schemas.
