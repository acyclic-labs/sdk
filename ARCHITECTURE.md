# Repository architecture

The SDK minimizes the combined public and private codebase. Moving a customer
surface here means making it canonical here and deleting the superseded copy
from its private service after compatibility passes. An extraction should remove
more code, schemas, wrappers, and tests across both repositories than it adds.

The dependency graph is one way:

1. `proto/<family>` and `acyclic-contracts` own public wire and lifecycle types.
2. Each `rust/crates/<family>` owns that family's public types, provider trait,
   customer client adapter, and deterministic in-memory implementation.
3. Each `typescript/packages/<family>` owns its idiomatic facade and private
   generated transport glue. Bun is the only JavaScript workspace tool.
4. `acyclic-memory` only assembles family providers into a profile. Family
   semantics never live in the profile crate.
5. `acyclic-conformance` and `conformance/vectors` own black-box assertions used
   unchanged against memory, customer, and Acyclic implementations.
6. `acyclic-harness` may consume public provider traits. No family crate depends
   on harness internals.
7. `acyclic-sdk`, the CLI, and examples are composition leaves.

Private services consume their exact SDK family commit or release and keep
durable storage, distribution, scheduling, tenant authority, admin protocols,
operations, and qualification evidence private. The SDK never depends on a
private path, package, registry, namespace, descriptor, or implementation.

In-memory providers stay in their family crate. They are deterministic, bounded,
and process-local. They make no durability, isolation, distribution, or
availability claim. They and private servers must pass the same public
conformance cases.

Before merging an extraction, report the public and private commits, descriptor
and suite digests, exact test results, and the net files/crates/lines removed and
added. Any remaining duplicate contract or semantic implementation needs an
explicit compatibility reason and a deletion milestone.
