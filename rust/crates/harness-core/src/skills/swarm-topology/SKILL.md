---
name: swarm-topology
description: Choose and adapt a dependency graph when coordinating several agents with shared artifacts or messages.
---

# Choose the dependency graph

Split independent work when it saves more than the coordination costs. Sequence
only real dependencies, and name the artifact that joins each branch. Choose a
pattern below, then give each participant its inputs, output and ownership.

| Pattern | Use it for |
| --- | --- |
| [Pipeline](../../plugins/spawn/topologies/pipeline.md) | Successive transformations of one artifact |
| [Mesh](../../plugins/spawn/topologies/mesh.md) | Independent participants sharing observations |
| [Debate ring](../../plugins/spawn/topologies/debate-ring.md) | Ordered challenges to a proposal |
| [Blackboard](../../plugins/spawn/topologies/blackboard.md) | Contributions to shared, versioned knowledge |
| [Market](../../plugins/spawn/topologies/market.md) | Choosing among competing offers |
| [Hierarchy](../../plugins/spawn/topologies/hierarchy.md) | Independent work with local integration |
| [Service](../../plugins/spawn/topologies/service.md) | Reacting to a durable request log |
| [Search-derived graph](../../plugins/spawn/topologies/search-derived.md) | Discovering dependencies before execution |

These are ordinary programs and instructions for the `acyclic` module. They
describe data flow; permissions come from admitted grants and issuer policy.
Install the complete policy, configuration and input cut before starting work
when isolation or an experimental condition depends on them. A prompt naming an
edge does not enforce it. Shared-volume access and conversation inheritance are
separate choices; choose each explicitly.

Create a fixed set of participants with one `atomically` call when they must all
exist with their initial files before any starts. Each child's `files` are paths
in its own home. A `fork` copies conversation and starts paused; `spawn` uses a
template. Select budgets explicitly: `null` shares the parent's budget. Group
names are subtrees, and iteration uses the paged `list` API.

Make a final wait the code result (`export default wait(...)` or
`export default until(...)`). The durable read can then release the VM. Waiting
for quiescence is not a success check: inspect the original results before
integrating them. Use the primitive's terminal outcome if a producer fails;
do not replace it with repeated reads. A deadline, when wanted, is an explicit
caller choice, and a timeout does not cancel the producer.

The examples use concrete names and tasks to show the composition. They impose
no population, depth, round or history limit. Creating a graph costs work for
its vertices and edges once; route a record only to its actual recipients.
After-hooks receive one page, and commit returned changes with the original
delivery cursor. Do not reconstruct a graph or scan its history per delivery.
