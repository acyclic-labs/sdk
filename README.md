# Acyclic SDK

**Rapid agent swarms. Make any agentic workflow exponentially faster.**

Acyclic Labs builds rapid agent swarms. Hand a task to a swarm and it fans the work out across many agents, each in its own sandbox, then merges what they produce, so long agentic workflows finish exponentially faster. The harness is open source and works without managed hosting; for managed swarms, see [acyclic.dev](https://acyclic.dev/agents).

[Managed Agents](https://acyclic.dev/agents) is the hosted composition of the open-source Acyclic harness. It does not introduce a separate agent loop, and nothing about forking, recursion or durable execution is reserved for the hosted version.

The Acyclic SDK is the open contract and composition layer for building recursive,
fork-join agent systems. It contains the Rust harness, public service interfaces,
customer-machine implementations, conformance suites, and TypeScript packages.

This repository contains a public release candidate. APIs remain pre-release
until their family version is published and tagged.

## Open-source boundary

Everything shipped to or executed on a customer's machine belongs in this
Apache-2.0 repository. Private repositories contain only Acyclic-operated
infrastructure such as multi-tenant regional control planes, distributed
replication and consensus, internal administration, billing, and private
qualification evidence.

Run all implemented checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
bun install --frozen-lockfile
bun run check
bun test
```

- [What works in this release candidate](docs/release-candidate.md)

See [ARCHITECTURE.md](ARCHITECTURE.md), [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and
[provenance/README.md](provenance/README.md) before importing source.

[TypeScript service examples](typescript/packages/sdk/README.md) · [Rust service examples](rust/README.md) · [llms.txt](llms.txt)
