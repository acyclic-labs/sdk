# Implementation status

Base: 31b9ff52d63c91f2b9bf87e16b78ad682d26546f.
Branch: codex/graphcoder-sdk. No merge/publication.

## Completed foundation
- Shared versioned model-input admission in stock executor and live task dispatch.
- Ordered manifests bind message bytes, roles, file references, model and tool revisions.
- Aggregate input bounds reject overflow without truncation.
- Exact persisted model prefixes reject content/order/binding mutations and corruption.
- Fork-prefix capture rejects incomplete tool exchanges.
- Stock executor persists input manifests before model dispatch.
- Provider-capture integration verifies actual received input against the persisted manifest.
- Provider admission enforces pinned prefixes before dispatch and recovered attempts.
- Memory and persistent compositions share the same provider-neutral storage implementation.
- Persistent session descriptors pin identities, authority keys, model and limits.
- Reopening replays completed turns without model redispatch; changed prompt/configuration fails.
- Full model requests are pinned beside their ordered manifests.
- Completed tool batches pin one boundary after all results; child context is an explicit suffix stage.
- Production executor tests preserve inherited inputs across three child levels with real file effects.
- Assistant text in tool-bearing responses now reaches the next model request.
- Reconciliation verifies the original admitted request and its digest; guarded providers refuse identity-only recovery.
- Pre-dispatch refusals are durable, scoped, non-secret observations distinct from dispatched failures.
- Completed-batch publication has a pinned implementation/guarantee and immutable admission.
- Publication uncertainty prevents the next parent request; reconciliation uses the original admission.
- Publication retry is permitted only for the declared idempotent guarantee.

## Verification
- Existing Harness baseline: 176 passed.
- Shared-input integration: 181 passed.
- Filesystem-local suite after manifest and provider-capture integration: 195 passed.
- Durable local composition and prefix admission: 197 passed.
- Completed-batch and recursive input composition: 198 passed.
- Existing journal E2E: 6 passed; fork-preparer recovery: 1 passed.
- Existing native durable recursive-workspace E2E: 2 passed.
- Full native Harness regression at f2fc0f4c: 215 passed, zero ignored.
- Latest request-bound recovery/refusal changes: 198 library + 6 journal tests passed.
- Native library lint gate passed; WASM compilation passed (execution not tested).
- Completed publication recovery: 200 library + 6 persistent-journal tests passed.
- Final publication source: native lint and WASM compilation passed.
- Source-bound checkpoint receipts: checkpoint-recovery.json and checkpoint-publication.json.
- None of these results qualify the complete swarm or terminal product.

## Next
Connect the completed-batch publisher to existing typed workspace fork publication
and child task admission. The publisher seam alone does not create or run children.
Extend durable composition with scoped swarm communication and git integration,
effect recovery, terminal app, and installed-artifact acceptance evidence.

The locked requirements matrix remains authoritative. No Cloud, web UI,
production models, migration or sandbox. Arbitrary host commands and root
writeback require approval; workspace routing is not process confinement.
