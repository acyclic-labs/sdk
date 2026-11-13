# Repository architecture

The SDK minimizes the combined public and private codebase. Moving a customer
surface here means making it canonical here and deleting the superseded copy
from its private service after compatibility passes. An extraction should remove
more code, schemas, wrappers, and tests across both repositories than it adds.

The dependency graph is one way:

1. The versioned Protobuf modules registered in `buf.yaml` own stable wire
   schemas; the Rust crate for each family owns its public lifecycle semantics
   and generated bindings.
2. Each `rust/crates/<family>` owns that family's public types, provider trait,
   customer client adapter, and deterministic in-memory implementation.
3. Each `typescript/packages/<family>` owns its idiomatic facade and generated
   transport glue. Bun is the only JavaScript workspace tool.
4. Each family's memory provider is constructed directly in qualification code;
   no public profile or umbrella crate sits between applications and providers.
5. Each published primitive owns its public `conformance` harness and canonical
   vectors. Downstream backends run the same assertions through `Backend` and
   `suite!`; the private `acyclic-conformance` crate supplies provider adapters
   and retains historical proof/report producers.
6. `acyclic-harness` owns durable agent/task semantics, the replaceable runtime,
   and opaque cross-family references. Host adapters consume public provider
   traits; no family crate depends on harness internals.
7. The `acyclic-plugin` CLI and examples are composition leaves.
8. `acyclic-harness` exposes one remote entry point, `server::serve`, over the
   Stream session protocol on gRPC. Managed and self-deployed hosts share the
   same `hosting::Server` for admission, routing, approvals and spawn; durable
   reducer, replay and scheduling semantics remain in the harness core.
9. The optional `acyclic-harness` filesystem and machines modules are genuine
   cross-family adapter boundaries. Provider-specific types remain feature-gated
   out of the pure/WASM harness core.

The versioned modules in `buf.yaml` are the canonical schema inputs.
Hosted services must qualify against their public contracts before availability
is claimed. `buf.gen.yaml` configures ESM-ready JavaScript and declaration
generation; Rust crates compile their schema bindings through `build.rs`.
`bun run generate` runs the schema and Rust contract producers, using
`scripts/generated-bindings.mjs` for descriptors, compatibility digests and
package destinations. Package `generated/proto/`, generated Rust source and
WASM bindings are ignored build outputs, not committed copies. Publication
generates those outputs before packaging them.
Rust-owned TypeScript contracts (limits, routes, enum maps) are
rendered by `scripts/generate-contracts.mjs` from one Cargo build.
`node scripts/check-generated.mjs` regenerates ignored products from current
source and checks tracked projections, contract baselines and the public RPC
matrix. Rust WASM exports are built from the Rust crates and packaged beside
the TypeScript facades. A package's `./proto` export points directly to its
generated module. Handwritten TypeScript should provide idiomatic APIs and
runtime boundaries, not duplicate protobuf message definitions.

## Consumption rules

- A family may depend on its generated wire schema, transport libraries, and
  ordinary third-party libraries. It must not depend on a sibling family's
  implementation.
- Cross-family values travel as public IDs, immutable references, or caller-owned
  provider traits. A family never reaches through one client into another service.
- The harness accepts family provider traits and composes them. Service clients
  and providers never depend on the harness or umbrella SDK.
- Applications depend on the individual family packages they use. Switching providers changes bindings
  and capability checks, not orchestration code.
- Private services pin an exact public SDK commit or release, implement its public
  contract, and run its black-box conformance suite. Customer contract changes
  originate here; the private repository deletes its superseded copy.
- Private services may call other private services only through authenticated,
  versioned service or administration APIs. Fleet consumes immutable private
  service releases and admin APIs; it never imports SDK or service source.
- A runtime handshake binds protocol version, schema digest, and capabilities.
  Unsupported combinations fail before work is admitted.

The execution boundary determines ownership. All code shipped to or run on a
customer machine is public here, including embedded and durable-local storage,
browser backends, native mounts, local daemons and processes, language bindings,
local recovery, model adapters, and customer-hosted providers.

Private services consume their exact SDK family commit or release and keep only
Acyclic-operated infrastructure: multi-tenant control planes, distributed
  replication and consensus, multi-tenant regional control planes, tenant authority,
internal admin protocols, billing, operations, and private qualification
evidence. The SDK never depends on a private path, package, registry, namespace,
descriptor, or implementation.

In-memory providers stay in their family crate. They are deterministic, bounded,
and process-local. Durable-local and other customer-machine providers also stay
with their family rather than moving into a parallel implementation layer.
Capabilities state which durability, isolation, distribution, and availability
guarantees each provider supports. Public providers and private servers pass the
same applicable conformance cases.

Before merging an extraction, report the public and private commits, descriptor
and suite digests, exact test results, and the net files/crates/lines removed and
added. Any remaining duplicate contract or semantic implementation needs an
explicit compatibility reason and a deletion milestone.
