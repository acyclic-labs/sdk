# Acyclic SDK

The Acyclic SDK is the open contract and composition layer for building recursive,
fork-join agent systems. It contains the Rust harness, public service interfaces,
customer-machine implementations, conformance suites, and TypeScript packages.

This repository contains a public release candidate. APIs remain pre-release
until their family version is published and tagged.

## What works in this release candidate

- Rust contracts for operation identity, admission, completion, cancellation,
  capabilities, protocol versions, and descriptor digests.
- A Tokio-based recursive task harness with bounded concurrency.
- In-memory Filesystem, Stream, Objects, Machines, and Inference providers.
- A single canonical Filesystem engine over the public Stream and Objects
  provider traits, with memory and durable-local compositions, sparse
  content-addressed generations, source capture, safe rebase and join,
  S3 workspace views, native watchers and mounts, an optional local daemon,
  browser WASM persistence, a TypeScript facade, and an N-API embedded engine.
- A hierarchical Stream v2 Rust contract, bounded structural-sharing memory
  provider, checksummed crash-recoverable local provider, authenticated gRPC
  client/server adapter, exact retry semantics, immutable-prefix forks, gapless
  follow, and atomic optimistic commits. The memory and local providers execute
  the same semantic state machine; the local feature adds only bounded durable
  publication and recovery.
- An Objects v1 Rust gRPC client plus bounded memory and durable-local providers
  with permanent versions, BLAKE3 validators, delete markers, conditions, exact
  idempotency, stable listing views, multipart publication, and whole-bucket
  snapshots/forks. Local bodies use authenticated digest-sharded chunks;
  range reads touch only intersecting chunks and shared bodies are never copied.
- An Inference v1 Rust client with immutable item-addressed Context revisions,
  independent forks, exact edit/compact/transfer, recoverable Runs, inclusive
  event replay, cancellation, four work meters, and admitted warm commitments.
- A shape-free Machines v1 contract with immutable image qualification, exact
  idempotency, checkpoints, fork sets, lifecycle recovery, stable endpoints,
  events, usage receipts, a mutual-TLS/Unix client, and one bounded deterministic
  simulator.
- An executable bounded recursive workload and black-box provider conformance tests.
- TypeScript contract facades for families whose public contract includes JavaScript.
- Protobuf package boundaries ready for audited service schemas.

## Open-source boundary

Everything shipped to or executed on a customer's machine belongs in this
Apache-2.0 repository. That includes embedded and durable-local engines, local
daemons and processes, browser implementations, native mounts, language
bindings, local recovery, model adapters, and customer-hosted providers. Each
component has one public source of truth; private repositories consume released
SDK contracts and code instead of keeping copies.

Private repositories contain only Acyclic-operated infrastructure such as
multi-tenant control planes, distributed replication and consensus, cloud
placement and scheduling, internal administration, billing, and private
qualification evidence.

Run the local profile without an Acyclic account:

```sh
cargo run -p acyclic-cli
```

Use the same high-level Inference API with either a local provider or an
authenticated service; placement, batching, KV movement, and rebalancing remain
provider internals:

```rust,no_run
use inference_sdk::Inference;

# async fn example() -> inference_sdk::Result<(), inference_sdk::Error> {
let inference = Inference::connect(
    "https://inference.example",
    "account-token",
    include_bytes!("trusted-ca.pem"),
).await?;
let base = inference
    .context("model/revision")
    .instructions("Answer from the supplied evidence.")
    .create()
    .await?;
let branch = base.fork().send().await?;
let run = branch.generate("Summarize the findings.", 1_024).send().await?;
let run_id = run.id();
let result = run.inspect().await?;
let recovered = inference.recover_run(run_id);
let events = recovered.watch(0).await?;
# let _ = (result, events);
# Ok(())
# }
```

Run all implemented checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
bun install --frozen-lockfile
bun run check
bun test
```

The included memory providers are deterministic, process-local implementations.
`SimulatedMachines` explicitly reports `ProcessLocalSimulation`: it runs no guest
or operating-system process and provides no crash durability, isolation,
distributed consistency, or production availability. Applications can bind a
conforming customer-hosted provider or the Acyclic managed service without
changing orchestration code.

See [ARCHITECTURE.md](ARCHITECTURE.md), [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and
[provenance/README.md](provenance/README.md) before importing source.
Open-source Acyclic SDK and in-memory reference providers
