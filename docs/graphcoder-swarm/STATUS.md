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
- Authoritative history publishes the exact complete exchange, including assistant text and rejected-call feedback.
- Versioned text artifacts restore the original model text representation with no regenerated content.
- Follow-up history preserves intermediate text once and publishes only the final step's text as the final reply.
- Completed-conversation boundaries reject stale histories before publication file writes.
- Child prefix/model binding is supplied by the shared Harness inherited constructor.

- Model tools receive checked turn/step provenance through dispatch and reconciliation, separately from model input.
- Batch publication uses a distinct identity domain from model-owned tool calls.
- Stock executor v3 fences the previous publication identity semantics before replay/dispatch.

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
- Exact authoritative history: 201 library + 6 journal + 1 fork recovery + 2 native fork tests passed.
- Two native sibling forks receive byte-identical completed prefixes and declared suffixes.
- A native stale-boundary test confirms refusal does not change the private workspace generation.
- Source-bound checkpoint receipts: checkpoint-recovery.json, checkpoint-publication.json and checkpoint-history.json.
- Model tool provenance checkpoint: 204 library + 6 journal + 1 fork recovery + 2 native fork tests passed, zero ignored.
- Native library lint passed after documentation repair; WASM compilation passed, with execution still unverified.
- checkpoint-provenance.json binds these results to source, suite and test executable digests.
- None of these results qualify the complete swarm or terminal product.

## Next
Implement production fork intent tools, session budget admission and durable child activation.
Pinned task prerequisites and restart-safe dependency scheduling are now implemented.
The native fixture now connects the stock publisher to existing typed workspace
forks and child models; application-facing swarm activation remains pending.
Extend durable composition with scoped swarm communication and git integration,
effect recovery, terminal app, and installed-artifact acceptance evidence.

The locked requirements matrix remains authoritative. No Cloud, web UI,
production models, migration or sandbox. Arbitrary host commands and root
writeback require approval; workspace routing is not process confinement.

## Durable prerequisite checkpoint

Task admissions pin sorted operation prerequisites; empty admissions retain canonical v2 bytes,
while dependent admissions use v3. Native owner admission rejects missing or foreign dependencies
before staging payloads. The existing scheduler controls readiness and cancellation failure.
Two real-provider scenarios verify blocked child dispatch across reopening, exact reattachment,
changed dependency refusal, and cancelled prerequisites. This is not full fork activation.

Focused native regression: 205 library + 2 dependency + 6 journal + 1 fork preparer + 2 boundary
cases passed. Native library and dependency-test lint passed after repairs. A freshly built WASM
runtime passed 69 TypeScript tests (382 assertions), before final native lint-only repairs.
Installed package equivalence remains pending: directly invoking that fixture from source failed
because its package had not been installed. No distributable qualification is claimed.

Five persistent slice owners now work in separate managed worktrees: communication/waits,
workspace facade/integration/writeback, approved execution, terminal/generic UI, and qualification.
The root owns exact model inputs, fork activation, shared composition and combined integration.
Workers continue validation and maintainability iterations after initial delivery; verified commits
are cherry-picked to this branch without merging branches.

## Strict history and attachment admission checkpoint

Default turn selection now keeps every authoritative model-visible message through the current
user, or rejects overflow. It never chooses a suffix automatically. Retained selections omitting
prior history are fenced before replay or dispatch. Projection rejects excess selected attachments
instead of adding an omission note. Explicit selection remains a separate public contract.

Native focused gates: 209 library + 2 dependencies + 6 journal + 1 fork preparer + 2 boundary =
220 passed. Native library lint passed. Fresh WASM production code passed 101 TypeScript tests
with 582 assertions across runtime, projection and memory-conversation suites. The first new replay
test omitted completion of its prior turn and was repaired. A highly contended run exceeded the
5-second default for the 1,030-attachment fixture; its explicit budget is now 15 seconds. The next
full run passed, with that fixture completing in about 2.1 seconds. Reliability still requires final
qualification; these observations are retained rather than treated as final product evidence.

Persistent slice owners received review feedback and continue native effects, model-facing tools,
recovery and package qualification work. Initial worker commits remain under review.

## Admitted model fork boundary checkpoint

HarnessStorage now verifies the exact persisted batch publication admission, original
request, completed exchange, and authoritative conversation before entering the existing
typed workspace fork engine. Identity, request-reference, and publisher substitutions
are rejected before workspace preparation. The two native real-filesystem boundary
scenarios pass. This remains a checkpoint: production recursive activation, persisted
child suffix/replay consistency, and exact inherited content grants remain unqualified.

Seven persistent implementation owners and two independent reviewers now cover the
slices. The installed terminal PTY demo passed in its worker branch, but its fake
transport does not qualify the production swarm. The qualification owner's installed
Harness baseline passed npm/WASM checks; its Rust package phase failed and provider
conformance did not run. Worker changes remain separate until reviewed integration.

## Published fork exact-grant checkpoint

HarnessStorage::from_published_fork verifies the existing typed published parent seed and
child binding, then signs only the seed's immutable read capabilities in the child's scope.
Its journal, content reader, and builder share those exact grants. Ordinary provider
composition remains owner-private. Limits and provider mismatches are checked before binding.
The inherited-conversation namespace permits exact reads; journal internals still require
owner volume authority.

The native two-child boundary suite passed 2/2 (11.78 seconds) after the final source change,
and strict library/test lint passed. Assertions cover inherited primary bytes, unchanged
historical reads after parent mutation, denied later parent files, forged seed rejection,
reopened child grants, attached-reader inherited-prefix access, denied missing grants,
denied private scratch, and denied internal request files. Helpers separate these security
checks from publication setup. A refactor initially lost iteration borrows and was repaired;
strict complexity checks prompted the helper separation.

This is not full recursive qualification. Ancestor grant propagation at deeper levels,
declared suffix/authoritative replay consistency, real swarm integration, fault injection,
WASM/package/platform lanes, and production terminal qualification remain required.

## Final context and allocation authority checkpoint

Checkpoint 8b9c7496 verifies every final model-context file reference through the
journal resolver before provider admission. A custom context stage injecting an
existing sibling private file is rejected before any prepared/started/model event
or provider request. Published child composition also validates all inherited
file providers and private/project allocation provenance before binding.

Windows native execution_journal passed 6/6 and model_fork_boundary passed 2/2,
with no failures or ignored cases. Fresh unbound-child negatives cover foreign
file provider, changed operation allocation, and altered project seed. The last
case fails the private allocation whole-seed digest before the project guard;
it does not independently qualify missing project allocation recovery.

checkpoint-final-context-authority.json records scoped source and executable
digests. This is focused checkpoint evidence, not full-tree, recursive, WASM,
packaged terminal, fault-matrix, or final qualification.

## Recursive declaration and WASM admission checkpoint

prepareModelRequest shares PreparedModelInput admission and completed exchange
validation with TypeScript. Actual wasm32 compilation and wasm-release artifact
generation passed at 50e334ad. Generated public declarations expose the export.
Provider dispatch parity and lossless normalized request consumption remain pending.

The recursive fork helper now accepts an explicit InheritedModelContext and checks
the complete frozen prefix, exact declared suffix, and all authoritative own messages.
The root helper retains exact full conversation equality. The negative composition
test passed; real native fork boundary scenarios passed 2/2 (8.20 seconds).
Production persistence of that declaration and recursive activation remain required.

Broader model_input unit selection produced 8 passes and 1 required failure:
production_batch_pins_text_and_all_ordered_results rejects exact file read authority.
Its old recursion loop creates independent child stores then inherits references
without published seed grants. The new final-context verifier exposes this gap.
No bypass or skip was introduced; replacement with typed published recursion and
ancestor reference propagation remains an unmet acceptance gate.
