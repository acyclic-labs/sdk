# Context composition and admission

This extends the existing ordered pipeline and executor journal. It introduces
no context engine, revision store, lifecycle, scheduler or provider adapter.
Tests and the finite model are bounded evidence, not an unrestricted proof.

| Invariant | Production mechanism | Assumptions | Verification |
| --- | --- | --- | --- |
| Replay reconstructs exact admitted input without loading sources or transforming it again. | `ModelStarted` retains canonical private bytes; `load_json`, descriptor verification, `PreparedModelRequest` and recorded digest validation run before cached output or reconciliation. | Linearizable authenticated journal, retained immutable artifacts, collision-resistant digests. | Source disappears after admission; replay still succeeds without renderer/provider dispatch. Missing artifact fails closed. Existing request/prefix corruption and restart integration tests remain mandatory. |
| Custom loops use the same admission and recovery engine. | Public `StockExecutor::model_step` validates turn/step, pins configuration and calls the same internal transition as `execute`. The custom executor example no longer publishes a completion without model admission. | The embedding host supplies authorized providers/journals and task-level cancellation/shared accounting. This primitive does not replace those host responsibilities. | Public-step replay, out-of-bound step rejection, existing model/tool schema and uncertainty tests. |
| File/span and attribute projections have explicit pinned inputs. | `ContextSelection`, typed renderer and `SelectionStage` reuse existing content verification and stage ordering. Full/summary/reference choices are explicit. | Linked renderer implementations are trusted, pure and immutable; their contracts include all dependencies, including separately admitted summary references. Read verifiers enforce host authority. Model-suggested selections require host approval before installation. | Schema negative controls, exhaustive span properties over generated cases, representation tests and native/WASM contract correspondence. |
| Updates and rebuilt prompt components derive from the same state. | Immutable `ContextAttribute` contains type/schema/definition revision/state revision/value. Update mode always appends; prompt mode uses declared placement. | Host durably records authoritative state before selecting it; linked renderers honor the declared representation. | Pinned old/new state, rebuilt/update identity, input immutability and output-bound tests. |
| Reload cannot change in-flight definitions. | Validated replacement returns a new `ContextPipeline`; old clones retain `Arc` implementations. Invalid replacement leaves prior inspectable contracts intact. `Started` rejects different configuration for an existing operation. | Custom stages implement replacement validation and immutable contracts. Compiled changes require restart or explicitly supplied replacements. | Invalid configuration rejection, original contract/state preservation and existing configuration-retry rejection tests. |
| Mandatory context is never silently truncated to fit a bound. | `run_bounded` validates initial and every intermediate view; standalone projection applies the same limits. Final `PreparedModelRequest` validates complete exchanges and historical tool schemas before dispatch. | Renderers honor finite work/input bounds; output validation limits materialized values, not arbitrary allocations inside trusted custom code. | Tiny render limit, invalid attribute/extent controls, existing incomplete exchanges and aggregate request-limit tests. |
| Normal durable-source lookup does not hydrate all revisions. | Capture Stream tail; read exactly one record, or two for immediate compaction provenance. Shared record decoder enforces format, identity, canonical bytes and content residency. Explicit `revisions()` remains bounded archival validation. | Stream implements bounded immutable reads; pinned latest lookup verifies the requested revision and immediate provenance, not unrelated archival history. | Retained counts 1/16/128 with fixed active content: one verified reference and constant active wire bytes. Existing compaction replay/provenance/reopen tests. Wire size is not a heap-memory proof. |
| Native/browser projection semantics stay identical. | Rust-owned Tsify contracts and WASM selection/projection exports; TypeScript only normalizes/freezes the returned value. | Fresh matching WASM module, authorized host reads, JS representation conversions. | Native and WASM schema/placement/bound tests; real Chromium browser smoke. |

Provider reuse is a consumer policy over the exact admitted manifests and
bindings. Reuse may cover only equal ordered message identities under compatible
bindings; a changed projection must be admitted and cannot be replaced by stale
content for a cache hit. No provider cache or model-family heuristic is added.

The standalone `tests/context_admission_model.py` exhausts 26 reachable states
for two payloads and one operation. It models claim-before-dispatch, a crash
after claim, projection changes, retained-storage loss, and durable completion.
Negative controls using fresh projections on replay or ignoring missing storage
must fail. It assumes linearizable admission and immutable authenticated storage;
it does not establish liveness, budgets, cancellation or routing for an unbounded
system. Those remain the existing owning runtimes' contracts.

Retained complexity is required by distinct boundaries: schema validation for
dynamic attributes; immutable references for residency/authority; stage validation
for reload; projection bounds for standalone consumers; final request validation
for complete tool exchanges; private journal artifacts for exact recovery.
Shared placement, record decoding and live-task bounds replace duplicated paths.
No old-data migration or alternate execution path is introduced.

Qualification receipts identify a source snapshot in the focused PR. Failed,
skipped or flaky required checks do not qualify a source snapshot.
