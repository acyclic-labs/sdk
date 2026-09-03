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
- A complete recursive example and black-box provider conformance tests.
- TypeScript contract facades for the same public concepts.
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

Run all implemented checks:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bun install --frozen-lockfile
bun run check
bun test
```

The included providers are deterministic, process-local implementations. They do
not claim crash durability, isolation, distributed consistency, or production
availability. Applications can bind conforming customer providers or Acyclic
services without changing orchestration code.

See [ARCHITECTURE.md](ARCHITECTURE.md), [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and
[provenance/README.md](provenance/README.md) before importing source.
Open-source Acyclic SDK and in-memory reference providers
