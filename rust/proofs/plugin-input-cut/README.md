# Original plugin input decisions

Run `node rust/proofs/qualify.mjs plugin-input-cut`.

`plugins::load` and `load_decision` share the original enabled-plugin loader.
It folds the layered `plugins.ts`, evaluates each enabled manifest and reads each supported
hook form once. Its decision binds that composition, the enabler, each
manifest's evaluator/dependency identity and each original hook observation.
Absent manifests and hooks, invalid module bytes and invalid UTF-8 retain
their original identities. Reads that fail operationally return their original
outer error, including when the tree's error variant is `Invalid`.

The existing generated settings oracle and `config_evaluation` fuzz target
call this production loader with generated settings and hook contents. They
check independent selected names, hook bytes and decoded settings; distinct
input identities across absence, malformed dependencies and unsupported or
invalid hooks; stable repeated selections; and the exact read count. Generated
read failures reach manifest, import and absent-hook reads. The three retained
mutations erase original bytes, cache a failed read or repeat the original
hook read. Each must fail the unchanged oracle before restored source passes.
The patches are captured from actual changes to the production loader.

The loader adds one stack hasher and fixed-size updates for each original
manifest evaluation. A hook is hashed once over its original borrowed bytes;
hashing work is proportional to the admitted source bytes. Observation
bookkeeping is constant work per original read. There are zero additional
file reads, round trips, durability barriers or payload copies. The shared
`compose_with_evidence` boundary allocates one erased future and retains its
ordered layer/module evidence; this loader adds no second composition path.
Existing `load` callers retain no evidence and add no hashing or allocations.
`evaluate_decision` exposes the existing inline evaluator path, with no
additional future allocation per enabled manifest;
`prepare` remains available when retained scripts need another evaluation.
No history, second dependency collector, cache or wait is introduced.

This cut describes inputs and grants no caller or publication authority.
Authenticated receipt adoption, native session integration, full platform
qualification and instrumented fuzz coverage remain separate obligations.
