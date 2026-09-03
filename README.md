# Acyclic SDK

The Acyclic SDK is the open contract and composition layer for building recursive,
fork-join agent systems. It contains the Rust harness, public service interfaces,
customer-machine implementations, conformance suites, and TypeScript packages.

This repository is **staged privately for disclosure review**. Nothing here is a
published API or package until the repository passes its disclosure gate and a
release is tagged.

## What works in the scaffold

- Rust contracts for operation identity, admission, completion, cancellation,
  capabilities, protocol versions, and descriptor digests.
- A Tokio-based recursive task harness with bounded concurrency.
- In-memory Filesystem, Stream, Objects, Machines, and Inference providers.
- A hierarchical Stream v2 Rust contract, bounded structural-sharing memory
  provider, authenticated gRPC client/server adapter, exact retry semantics,
  immutable-prefix forks, gapless follow, and atomic optimistic commits.
- An Objects v1 Rust gRPC client and bounded reference provider with permanent
  versions, BLAKE3 validators, delete markers, conditions, exact idempotency,
  stable listing views, multipart publication, and whole-bucket snapshots/forks.
- An Inference v1 Rust client/provider family with immutable item-addressed
  Context revisions, independent forks, explicit edit/compact/transfer,
  recoverable Runs, inclusive event replay, cancellation, four work meters,
  and zero-copy `Bytes` payloads across its Rust and protobuf boundary.
- A shape-free Machines v1 contract with immutable image qualification, exact
  idempotency, checkpoints, fork sets, lifecycle recovery, stable endpoints,
  events, usage receipts, a mutual-TLS/Unix client, and one bounded deterministic
  simulator.
- A complete recursive example and black-box provider conformance tests.
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
use acyclic_inference::Inference;

# async fn example() -> acyclic_contracts::Result<()> {
let inference = Inference::connect("https://inference.example", "account-token").await?;
let base = inference
    .context("model/revision")
    .instructions("Answer from the supplied evidence.")
    .create()
    .await?;
let branch = base.fork().await?;
let run = branch.generate("Summarize the findings.").await?;
let run_id = run.id().to_owned();
let result = run.result().await?;
let recovered = inference.recover(run_id).await?;
let events = recovered.events_from(0).await?;
# let _ = (result, events);
# Ok(())
# }
```

Run all implemented checks:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
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
