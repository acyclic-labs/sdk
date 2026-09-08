# Repository architecture

The SDK minimizes the combined public and private codebase. Moving a customer
surface here means making it canonical here and deleting the superseded copy
from its private service after compatibility passes. An extraction should remove
more code, schemas, wrappers, and tests across both repositories than it adds.

The dependency graph is one way:

1. `proto/<family>` owns stable wire schemas; the Rust crate for that family owns
   its public lifecycle semantics and generated bindings.
2. Each `rust/crates/<family>` owns that family's public types, provider trait,
   customer client adapter, and deterministic in-memory implementation.
3. Each `typescript/packages/<family>` owns its idiomatic facade and generated
   transport glue. Bun is the only JavaScript workspace tool.
4. `acyclic-memory` only assembles family providers into a profile. Family
   semantics never live in the profile crate.
5. `acyclic-conformance` and `conformance/vectors` own black-box assertions used
   unchanged against memory, customer, and Acyclic implementations.
6. `acyclic-harness` owns durable agent/task semantics, the replaceable runtime,
   and opaque cross-family references. Host adapters consume public provider
   traits; no family crate depends on harness internals.
7. `acyclic-sdk`, the CLI, and examples are composition leaves.
8. `acyclic-harness-http` and `acyclic-harness-grpc` contain framing and server
   dependencies only. Both delegate to the same `HarnessWireApi`; neither owns
   reducer, admission, replay, or scheduling semantics.
9. `acyclic-harness-filesystem` and `acyclic-harness-machines` are genuine
   cross-family adapter boundaries. Provider-specific types never enter the
   pure/WASM harness core.

## Consumption rules

- A family may depend on its generated wire schema, transport libraries, and
  ordinary third-party libraries. It must not depend on a sibling family's
  implementation.
- Cross-family values travel as public IDs, immutable references, or caller-owned
  provider traits. A family never reaches through one client into another service.
- The harness accepts family provider traits and composes them. Service clients
  and providers never depend on the harness or umbrella SDK.
- Applications depend on `acyclic-sdk` for a tested profile or on individual
  family packages for a smaller surface. Switching providers changes bindings
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
