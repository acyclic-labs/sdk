# Qualification evidence index

These are checkpoint results, not final product qualification.

[checkpoint-isolated-local-storage-2026-10-04.json](checkpoint-isolated-local-storage-2026-10-04.json) records six passing current native local-storage cases and a failed unknown-operation fencing case. Completed-turn replay is verified; full effect recovery remains incomplete.

[checkpoint-terminal-lifecycle-failure-2026-10-04.json](checkpoint-terminal-lifecycle-failure-2026-10-04.json) records the integrated process-backed terminal fixtures passing 9/14, plus seven strict type failures. Missing bridge lifecycle support and fixture shape errors remain under repair; scripted hosts do not replace native PTY qualification.

[checkpoint-isolated-native-2026-10-04.json](checkpoint-isolated-native-2026-10-04.json) records a fresh dedicated native build: two frozen-input cases and two read-projection cases pass, while the real local swarm fault suite passes one and fails four. Rust tree and executable hashes avoid shared-output provenance ambiguity; runtime fork recovery remains blocked by observed failures.

[checkpoint-frozen-input-types-2026-10-04.json](checkpoint-frozen-input-types-2026-10-04.json) records strict TypeScript verification and 30 repeated WASM assertions after repairing the frozen-vector test's types. Byte expectations remain unchanged; native equivalence remains pending.

[checkpoint-cold-conflict-pass-2026-10-03.json](checkpoint-cold-conflict-pass-2026-10-03.json) records two passing recursive native workspace cases, including actual cold provider reopening after a typed conflict and replay of the same immutable merge operation. [checkpoint-cold-replay-communication-2026-10-03.json](checkpoint-cold-replay-communication-2026-10-03.json) records completed-turn replay without dispatch and two durable local communication cases. [checkpoint-model-wait-cold-conflict-2026-10-03.json](checkpoint-model-wait-cold-conflict-2026-10-03.json) separately records the passing production model-invoked wait and the earlier retained-handle conflict failure, subsequently repaired.

[checkpoint-central-manifest-wasm-2026-10-03.json](checkpoint-central-manifest-wasm-2026-10-03.json) records the fresh Rust-owned manifest validator build and generated declarations: TypeScript source and test types pass, with 239 runtime tests and 1,196 assertions passing. TypeScript no longer duplicates the manifest validation rules.

[checkpoint-native-prepared-input-2026-10-03.json](checkpoint-native-prepared-input-2026-10-03.json) records 17 passing native model-input cases and the separate cold replay fixture failure at its host-owned content reader. [checkpoint-native-journal-conflict-pass-2026-10-03.json](checkpoint-native-journal-conflict-pass-2026-10-03.json) records six passing execution-journal cases and two passing recursive workspace/conflict cases. Conflict journal reopening does not yet prove cold provider reopening after conflict.

[checkpoint-harness-wasm-typescript-pass-2026-10-03.json](checkpoint-harness-wasm-typescript-pass-2026-10-03.json) records the repaired TypeScript gate at `aa927b74`: source and test types pass, and all 239 runtime tests pass against the fresh WASM artifact. The transport fixtures derive their protocol identity from the same artifact as the validators; strict peer checks remain intact. Earlier failures below retain their historical scope.

[checkpoint-wasm-build-2026-10-03.json](checkpoint-wasm-build-2026-10-03.json) records the fresh policy ABI build from `9fa97138`. Against that artifact, [checkpoint-harness-wasm-typescript-2026-10-03.json](checkpoint-harness-wasm-typescript-2026-10-03.json) records source `bf5b14ed`: source types passed, test types failed, and runtime tests passed 223/239. Sixteen transport cases reject an unsupported event wire version. [checkpoint-graphcoder-typescript-2026-10-03.json](checkpoint-graphcoder-typescript-2026-10-03.json) records 49 passing transport and terminal tests. These do not prove full process cleanup or installed native/PTY qualification.

Process cleanup remains an unchecked gate covering owned children and readers, all exit paths, and durable uncertainty. Pending patches are under independent review; detached readers and unbounded reap paths prevent acceptance. Simplification must reuse the owning library's admission, journal, and lifecycle contracts rather than introduce parallel state.

[checkpoint-local-composition-2026-10-03.json](checkpoint-local-composition-2026-10-03.json) records four passing native composition tests, including durable project/key reopening and exact recorded operator decision. [checkpoint-recursive-restart-2026-10-03.json](checkpoint-recursive-restart-2026-10-03.json) records the real recursive restart followup: one passed, one failed at the later stale-target assertion after provider reopening succeeded.

[checkpoint-cli-composition-2026-10-03.json](checkpoint-cli-composition-2026-10-03.json) records the simplified native CLI: six tests passed and one stage retry failed because the reopened execution path omitted its registered model-option policy. This source-level test run does not qualify installed artifacts or PTY interaction.

The latest broader run is [checkpoint-integration-2026-10-03.json](checkpoint-integration-2026-10-03.json), source `c44432dc`: compilation passed; recovery 7/7, execution journal 5/6, recursive workspace 1/2, model-fork boundary 1/3, and model-selected swarm 0/1. Fork attestation, command/revision binding, restart handle cleanup, and one exact approval fixture remain failed gates.

Latest focused native results are in [checkpoint-host-execution-2026-10-03.json](checkpoint-host-execution-2026-10-03.json): source `91214d3a`, 18 execution tests passed and one Windows descendant-process fixture failed. Both previously stalled execution cases completed successfully. This is a focused source-bound result, not full qualification.

[checkpoint-native-repair-2026-10-03.json](checkpoint-native-repair-2026-10-03.json) retains earlier results: source `c3a98adc` recovery 7/7, recursive workspace 1/2, budget boundaries 1/2, scheduler 0/2; library run terminated incomplete. It also records the source `813d2523` recursive followup and source `a9ca8ff4` integration compilation failure, where no tests executed. See [STATUS.md](STATUS.md) for the current scope; older results below apply only to their recorded source and artifacts.


| Checkpoint | Suite | Result | Required scope remaining |
|---|---|---|---|
| 2f96c3c3 | Harness library, filesystem-local | 195 passed | Recursive runtime, persistent host, terminal, artifacts |
| Durable composition checkpoint | Harness library, filesystem-local | 197 passed, zero ignored | Recursive runtime, terminal, artifacts |

Command: cargo test -p acyclic-harness --locked --lib --features filesystem-local.
Windows native execution on 2026-10-03.

New evidence: model_input::tests::prefix_provider_rejects_before_downstream_dispatch;
filesystem::local::tests::reopen_recovers_completed_turn_without_dispatch.
The latter uses real LocalStream and LocalFs storage and reopens the providers.

Implementation iterations exposed an unbound conversation initialization check; it
was repaired by testing the bound agent, and the full 197-case suite rerun passed.
Final source/suite/artifact digests and full acceptance receipts remain pending.

## Completed-batch checkpoint

Harness library with filesystem-local: 198 passed, zero ignored.
Production model-input scenario: two edits, all ordered tool results, retained
assistant text, then three explicitly composed child levels through the same
StockExecutor and prefix guard. This does not yet exercise a model-facing fork tool.

Existing integration gates rerun on Windows:
- execution_journal: 6 passed.
- fork_preparer: 1 passed (lost reply reconciles without another child allocation).
- local_recursive_fork: 2 passed (native persistent providers, restart and parent controls).

Command: cargo test -p acyclic-harness --locked --features filesystem-local
--test fork_preparer --test local_recursive_fork --test execution_journal.

The journal regression's former four-record assertion was updated to assert the
fifth input-admission record and verify the persisted request digest. Its
exactly-once dispatch and private-content assertions remain intact.

## Recovery admission checkpoint

See checkpoint-recovery.json for source and descriptor hashes and exact gates.
Native library: 198 passed; journal integration: 6 passed; library lint: passed.
WASM: compilation passed, runtime/serialization parity still pending.

The full native Harness regression at f2fc0f4c passed 215 tests, zero ignored,
including 1,024 recursive forks and 32 sibling forks. It predates the final
recovery-admission/refusal changes and is not a final-source qualification claim.

Recovery tests reject identity-only prefix reconciliation, corrupt request
digests and changed inherited content. Tool refusal replay retains the same
journal, invokes no executor and sends no additional model request.

## Publication admission checkpoint

See checkpoint-publication.json for hashes and exact gates.
Final-source Windows native library: 200 passed; journal integration: 6 passed;
zero ignored. Native library lint passed. WASM compilation passed; no WASM
execution or serialization parity is claimed.

completed_batch_publication_blocks_next_request_until_reconciled exercises the
stock executor: lost publication response blocks the second model request, exact
completed call/result bytes survive recovery, and neither tool execution nor
publication is duplicated on replay. The second request equals the pinned boundary.

batch_publication_recovery_preserves_admission_and_retry_guarantee covers all
three effect guarantees, observed/unresolved outcomes, original admission identity,
and altered-boundary refusal. Unknown at-most-once/exactly-once outcomes remain
indeterminate; only the declared idempotent guarantee allows redispatch.

The concrete adapter to existing workspace forks and child task admission is
still pending. This checkpoint does not qualify the recursive swarm or terminal.

## Authoritative history checkpoint

See checkpoint-history.json for source/suite/descriptor hashes.
Windows native: 201 library, 6 execution-journal, 1 fork-preparer recovery and
2 model-fork-boundary tests passed, with zero ignored. Native library lint and
WASM compilation passed. WASM execution/parity is still unverified.

The native fork fixture supplies a publisher to the production stock loop.
It publishes a complete exchange through Harness, performs two real workspace
forks with FilesystemForkPreparer and StreamAggregate.spawn_from_report, then
runs child models using HarnessStorage.inherited_builder. Actual child provider
requests are captured and their inherited serialized message bytes compared to
the pinned parent prefix. Parent history includes exact whitespace/Unicode,
malformed-call feedback, an actual private-file effect and ordered results.
A follow-up request proves earlier assistant text is not duplicated.

The first run caught text-to-file representation drift and was repaired with
the versioned canonical model-text artifact. Negative artifact tests reject
noncanonical JSON, corruption, invalid encoding/type and render overflow.
A stale-boundary fault scenario interrupts publication, appends concurrent user
input and proves repeated refusal leaves the private workspace generation unchanged.

These are real native provider effects, but activation is supplied by the test
fixture. Production fork tools, task scheduling, complete recursive swarm recovery,
git facade, terminal, generated bindings and fresh package qualification remain
required and pending.

## Model tool provenance checkpoint

The stock executor now passes checked turn/step provenance outside model content
to tool execution and reconciliation. An interrupted-tool test captures both
calls, proves they retain the same exact invocation, and checks actual serialized
provider requests for absence of runtime metadata. It then interrupts batch
publication and confirms recovery never repeats the tool effect.

A separate identity domain prevents a model call ID such as publication from
colliding with batch publication. Cross-turn/step/call routing is refused.
The executor revision changes to v3; a v2 replay regression proves refusal
occurs before dispatch, without new journal observations.

The first focused compilation found a test-only serde error conversion mismatch;
the test was corrected. This checkpoint supplies provenance needed for the fork
tool, not production fork admission, budgets or activation. Those gates remain
pending, along with the complete swarm, terminal and installed-artifact matrix.

Final checkpoint run: 204 library + 6 execution-journal + 1 fork-preparer +
2 native model-fork tests passed (213 total), zero failed or ignored. Native
library lint passed after doc formatting repair; WASM compilation passed, with
no claim of WASM execution. See checkpoint-provenance.json for source, suite,
descriptor and tested executable digests. No distributables are qualified.

## Durable prerequisites checkpoint

The native dependency scenarios use actual LocalStream and LocalFs providers, close and reopen
both, and prove no child lease is available until the pinned prerequisite completes. Missing and
foreign prerequisites do not publish admissions; changed prerequisites conflict; cancellation
propagates as a scheduler failure. Canonical negative tests cover malformed, duplicate, unsorted,
empty-v3, downgraded and self-dependent records. Existing v2 fixture bytes remain unchanged.

The focused native suite passed 216 cases; native library and dependency-test lint passed after
fixing unchecked indexing and a test lookup. Adding a proto field changed the descriptor fingerprint;
the wire fixture refresh changes only that fingerprint, and native equality passes. The rebuilt WASM
runtime passed 69 tests and 382 assertions before final lint-only native source repairs. Running the
installed equivalence fixture without installation failed module resolution; packaged execution
remains required. See checkpoint-dependencies.json. These checks do not qualify a complete swarm.

## Strict input omission checkpoint

Removed automatic old-history draining and synthetic attachment omission text from shared
Rust selection/projection. Tests prove exact-limit order and parts, over-limit refusal, retained
selection fencing and actual persistent session refusal across restart with no extra provider call.
Fresh WASM tests exercise the changed public TypeScript behavior. The native focused suite passed
220 tests and the three TypeScript suites passed 101 tests (582 assertions). Native library lint passed.

Observed repair history: replay fixture initially left its prior turn unresolved; adding the real
assistant completion made it reach the intended omitted-selection assertion. One contended run
reported a 7.03-second timeout for the 1,030-attachment fixture under its 5-second default. Its explicit
15-second fixture budget retains all semantic assertions. A subsequent full run passed (2.09 seconds
for that case). Final reliability and packaged qualification remain pending. See checkpoint-input-omission.json.

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

## Canonical fork and terminal checkpoint

checkpoint-canonical-fork-terminal.json binds focused Windows native, actual WASM, captured TypeScript model-dispatch, and GraphCoder package checks to listed source/suite/artifact digests. It does not qualify the full production swarm or final packaged matrix.

## Exact-input and facade followup

[checkpoint-input-facade-followup-2026-10-03.json](checkpoint-input-facade-followup-2026-10-03.json) records 24 passing native executor tests, 242 passing TypeScript tests with type checks, and a passing real recursive facade integration fixture. It also retains the broader facade rebase failure and held fork compile failure. Existing WASM was used for this TypeScript run; no fresh distributable qualification is claimed.

## Projection, execution, and regenerated bindings checkpoint

See checkpoint-projection-execution-bindings.json for scoped digests, actual pass counts, required failures, and limitations. Fresh WASM admission: 12 passed; focused TypeScript runtime: 8 passed; TypeScript contracts pass after generating the Objects dependency; qualification validation: 21 passed. Native receipt reopen and actual subprocess fault checks each passed one case. These do not qualify the full recursive runtime or production approval recovery.

The native malformed-call continuation test currently fails because cumulative typed rejection evidence is missing. Host-owned journal reader separation and the full published-fork fixture remain required failures. Dynamic model-selected fork routing and final installed native terminal/ConPTY qualification remain pending. The goal is active; no merge occurred.

## Native terminal and host-history checkpoint

Source 5cefdcc5002860196660c146d2cc9b2b4e152178 has three passing native CLI tests, including real staged-file restart and exact SDK reads. Host execution artifacts remain private; authoritative conversation stores validated model projections. Actual provider-manifest capture, repeated-call rejection replay, and orphaned tool-role rejection have focused passing evidence. The synthetic recursive test still fails missing inherited grants. This is not installed-package, recursive swarm, or final qualification. See checkpoint-native-terminal-host-history.json for digests and limitations.

## Authoritative replay and interactive native checkpoint

See checkpoint-authoritative-replay-interactive-native.json for scoped source and artifact digests. Seventeen native executor regressions pass at e76ff0be; four native CLI regressions pass at 384cdeaa, including a response with stdin kept open. An actual hidden native process and installed SDK consumer pass staged-file operation retry, exact read, and reopen. The installed package was prepared in a separate validation worktree, so this is focused behavior evidence rather than final package qualification. The test discards a completed response; it does not prove a mid-effect crash. Recursive swarm, authenticated effects, budgets, writeback, PTY, regenerated WASM, and the full matrix remain required.

The same checkpoint now records two actual LocalStream communication cases and sixteen native host-execution cases. Three protected local-provider cases pass at cdf28fe0, including real authenticated execution reopen and Unknown/operator-resolution recovery with zero replay runner calls. Earlier protected approval failures exposed model-reader use in internal receipt/interaction admission and were repaired through exact host classes and a private interaction verifier. Full fault-window coverage remains open.

## Integrated native fork, workspace, and budget checkpoint

See checkpoint-integrated-native-fork-budget.json for source-bound suite/artifact digests and required failures. Actual native results: journal 6/6, Git facade 7/9, local recursive workspaces 1/2, typed model fork boundary 1/2, budget unit selection 21/23. Source GraphCoder dispatcher/UI tests pass 25/25; provenance validator passes 2/2. Required failures remain open and prevent qualification. The goal stays active; production swarming, hard resource ceilings, effect faults and fresh installed PTY evidence remain outstanding. No merge occurred.

## Current scoped verification, 2026-10-04

- [Actual fork suite results](checkpoint-fork-native-2026-10-04.json): held source `6c6ae5444`; nine recovery cases pass, two fork-boundary cases and the real model swarm case fail.
- [Actual cleanup follow-up](checkpoint-cleanup-followup-native-2026-10-04.json): held source `a5188016a`; full Windows execution passes 24/28. Required failures include operator uncertainty resolution and intermittent output-limit behavior. The earlier focused run is retained separately in the same receipt.
- [Request scope pinning](checkpoint-scope-pinning-2026-10-04.json): 84 TypeScript cases, 478 assertions, and test type checks pass using the explicitly identified existing WASM artifact.
- [Fresh installed GraphCoder package](checkpoint-installed-graphcoder-build-2026-10-04.json): clean consumer passes 21 host calls including authenticated operator approval. Native runtime and installed ConPTY qualification remain open.

These receipts qualify only their stated source and scope. They do not constitute final qualification or replace the locked 68-entry matrix.

