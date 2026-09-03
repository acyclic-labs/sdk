# Repository architecture

The SDK minimizes the combined public and private codebase. Moving a customer
surface here means making it canonical here and deleting the superseded copy
from its private service after compatibility passes. An extraction should remove
more code, schemas, wrappers, and tests across both repositories than it adds.

The dependency graph is one way:

1. `proto/<family>` and `acyclic-contracts` own public wire and lifecycle types.
2. Each `rust/crates/<family>` owns that family's public types, provider trait,
   customer client adapter, and deterministic in-memory implementation.
3. Each `typescript/packages/<family>` owns its idiomatic facade and generated
   transport glue. Bun is the only JavaScript workspace tool.
4. `acyclic-memory` only assembles family providers into a profile. Family
   semantics never live in the profile crate.
5. `acyclic-conformance` and `conformance/vectors` own black-box assertions used
   unchanged against memory, customer, and Acyclic implementations.
6. `acyclic-harness` may consume public provider traits. No family crate depends
   on harness internals.
7. `acyclic-sdk`, the CLI, and examples are composition leaves.

The execution boundary determines ownership. All code shipped to or run on a
customer machine is public here, including embedded and durable-local storage,
browser backends, native mounts, local daemons and processes, language bindings,
local recovery, model adapters, and customer-hosted providers.

Private services consume their exact SDK family commit or release and keep only
Acyclic-operated infrastructure: multi-tenant control planes, distributed
replication and consensus, cloud placement and scheduling, tenant authority,
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
