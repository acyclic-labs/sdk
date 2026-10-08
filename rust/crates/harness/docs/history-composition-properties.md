# History and compaction qualification

Slice B starts at main `4e3ed22bdc2137a93cccdd66a3cbb815acf897cc`.
This is an implementation/evidence map in progress, not a qualification claim.

| Invariant | Mechanism | Assumptions | Verification | Evidence |
| --- | --- | --- | --- | --- |
| Identity and turn-state lookup do not rescan retained history. | The existing `ConversationState` maintains rebuildable identity, model-position and turn indexes on append; decoding rebuilds them from ordered records. External mutation uses `append`. | Trusted reducer admission; immutable records; cold decode still reads all loaded history. | Production append/planner regressions, malformed wire negatives and roundtrip tests. | Windows native library suite: 233 passed in `target-b-native-compaction-final-iteration.log` before the content accessor/refactor. Prior hashes: `target/b-native-source-hashes-before-parts.json`. Current strict native consumer lint passes in `target-b-clippy-projection.log`; current hashes: `target/b-native-iteration-source-before.json`. Integration/platform qualification remains open. |
| Preparing or replaying a selected turn does not copy complete history. | `prepare_turn` indexes the existing state and selects against one candidate; Filesystem projection reads the pinned selection revision directly. | The existing committed selection and exact request artifacts remain authoritative; provider grant checks are unchanged. | Native conversation/execution replay and fork fixtures. | Source implementation; platform execution pending. |
| Archive pages exclude later parent changes and never cap total traversal. | `ConversationState::page(after, through, maximum)` seeks by sequence and returns a bounded slice. The caller retains `through` across pages; WASM page rendering uses the same primitive. | Ordered immutable history; caller pins the logical tail; a page bound is not a lifetime bound. | Reopen, multiple pages, later-append exclusion, invalid cursor and zero-bound negatives. | Source implementation and bounded regression; no performance or unrestricted-memory proof. |
| Warm native turns and selected-context checks advance one existing authoritative projection. | The execution journal retains its `StreamAggregate`; `refresh_through` verifies a caller-sized contiguous page up to a captured tail. Turn admission releases the shared projection before executor entry. | Committed prefix remains immutable; reducer validates every new event. Resident reducer history and cold replay remain linear. The helper currently drains all pending pages; explicit total-work progress remains open. | Production refresh through three pages, exclusion of a later append, invalid cursors, exact cold-reopen equality, native turn/retry fixtures. | `target-b-native-refresh.log`: 234 passed, zero failed or ignored; strict lint passes in `target-b-clippy-refresh.log`. |
| An inherited projection does not permanently fix future requests to the original model binding. | Prefix binding digest remains original admission provenance; prefix resolution validates immutable ordered messages and grants, then prepares a new request against the caller-selected model/tools. | This is explicit prefix reuse, not the default logical-history fork policy. Required exchange schemas and media grants must remain compatible. | Depth-three chain, changed-model preservation and new binding, incompatible-tool rejection, parent-message substitution, missing-grant and corrupt-media negatives. | `target-b-native-prefix-policy.log`: 234 passed, zero failed or ignored; strict consumer lint passes in `target-b-clippy-prefix-policy.log`; source hashes `target/b-prefix-policy-source-hashes.json`. |
| A recent-history selection cannot silently drop half a tool exchange. | Indexed selection returns an explicit budget failure if a selected result's call lies outside the selection. | Canonical reducer validates call/result identities; admitted requests use their original retained artifact. | Tool-exchange boundary negatives required. | Source implementation; tests pending. |
| A summary shares ordinary model admission and reconciles uncertainty without another dispatch. | `StockExecutor::summarize` reuses `model_step`, retains the existing task binding, binds the source digest into its source-stage revision, and stages text plus provenance through the execution journal. | Caller-selected output budget, trusted provider reconciliation, immutable journal artifacts. This is not a separate journal or lifecycle. | The existing interrupted provider emits 70 retained observations; changed source fails before reconciliation, exact retry stages one output, completed replay invokes no provider. | `summary_reconciles_exact_source_and_stages_durable_output`, Windows native 233-test suite. In-memory journal/provider evidence; real storage/platform/accounting-fault qualification remains open. |
| Compaction preserves required instructions/current input/native media and cannot forge its retained projection. | One `DurableContextProvider::compact` implementation serves the existing stage and durable revision checks; the reference retains the caller budget and summary provenance, and replay reconstructs the exact projection. | Finite selected input; immutable verified output file; default conservative mandatory retention. Configurable retention/automatic thresholds remain open. | All six message budgets through the production transformation, incomplete-pair and mandatory-budget negatives, missing-summary rejection and retained-content substitution negative. | `compaction_keeps_mandatory_messages_and_never_splits_tool_pairs`, Windows native 233-test suite. Bounded production-path checking, not a universal proof. |
| Consumer retention policy is part of the exact compaction admission. | `CompactionRetention` declares roles and native-media preservation; current input is always mandatory. The persisted reference reconstructs using that exact policy. Context records use format 3 and compaction stage contract revision 2; no migration path is added. | Explicitly changing mandatory roles/media is a consumer policy decision. Tool-pair validation remains unconditional. | Default mandatory-budget failure, imported User-role policy, policy substitution and repeated-role negatives. | `target-b-native-retention.log`: 234 passed; strict lint passes in `target-b-clippy-retention.log`. |
| A pending summary cannot hide behind a settled response in the same execution. | Typed response/summary purposes distinguish admissions and observations in the existing execution journal. Existing owner publication and replay verify purpose-derived shared-budget attempt identities. The same quiescence projection observes both purposes. | Trusted provider events and immutable request artifacts; task lease and capabilities remain unchanged. | Both start orders and both completion orders through production `ExecutionSummary`, duplicate/settled negatives; exhausted task ceiling rejects summary claims. | `target-b-native-summary-quiescence.log`: 235 passed. Strict native consumers pass `target-b-clippy-summary-purpose.log`. Additional parent-summary API/source pinning tests are not covered by this prior result. |
| Parent summary recovery retains the exact source and never aliases the response admission. | `summarize_in_turn` shares the parent's Started record, owner and journal. Source bytes are pinned before inference; retained summary request equality is checked before reconciliation. Source extent identifies the covered prefix; every uncovered message remains mandatory in compaction. | Caller-provided instruction, output and source bounds; provider reconciliation returns the existing attempt. Automatic thresholds are not yet installed. | Interrupted summary and response in one parent journal; source/instruction substitution fails before reconciliation; exact retry makes two total generations/two reconciliations; response claim cannot publish an uncharged summary; prefix extent/budget negatives. | `target-b-native-parent-summary-fixed.log` and `target-b-native-summary-prefix.log`: 236 passed each on their respective prior-main sources. Strict native consumers pass `target-b-clippy-summary-prefix.log`. |

Reuse/cuts: retain StreamAggregate snapshots, canonical conversation events,
`DurableContextProvider::latest`, A's source/selection/renderer pipeline, the
existing execution journal and `StockExecutor::model_step`. Remove complete
conversation cloning/truncation from native selected replay and turn planning;
remove per-request reconstruction of identity maps. Both tail compaction paths
now use the same transformation. Permanent model-binding equality is removed
from explicit inherited projections. Remaining cuts include checkpointed cold
hydration and logical fork-policy integration.

The indexes remain O(retained loaded messages) resident metadata. Cold decode
and canonical serialization remain linear. Indexed lookup is O(log N); recent
selection is O(log N + K log N) for K selected messages, including histories
containing non-model events. Archive page seek is O(log N) plus returned records.
These are source-derived work bounds, not measured latency/memory evidence.
No claim of unrestricted constant memory or 10,000-agent performance is made.

Open acceptance obligations: admitted summary provenance and uncertainty,
default replaceable threshold compaction with explicit model capacity/reserve,
full/summary/reference fork policies, immutable media retention, checkpointed
cold history, generated/current consumers, actual native/WASM provider captures,
fault/restart/malformed/oversize/corruption negatives, bounded production-reducer
checks and final-source Windows/Linux/macOS/Chromium/installed qualification.
No PR merge or completion claim until those obligations are closed.

Failed receipts are retained in the worktree: `target-b-first.log` records a
vendored protobuf compiler launch failure; `target-b-native-retry.log` records
a test constructor compile error corrected before the next source run.
`target-b-native-compaction.log` and `target-b-native-compaction-current.log`
retain test iterator compile failures; corrected source passed the later suite.
Strict-lint failures remain in `target-b-clippy.log` and
`target-b-clippy-fixed.log`; the centralized accessor/projection source passes
`target-b-clippy-projection.log` with warnings denied. Native integration
execution in `target-b-native-integrations.log` passed local conversation and
recursive restart tests, but its process handle disappeared before the final
recursive test result. This run is incomplete, not a full-suite pass.
`target-b-native-summary-binding.log` retains a removed sibling-journal draft's
compile failure. The draft was removed because parent quiescence would not see
its pending summary. No journal factory is retained. `target-b-native-parent-summary.log`
records the extracted request constructor's return-type error; its corrected
source is being checked separately in `target-b-native-parent-summary-fixed.log`.

Landings integrated: origin/main
`5d2336e299704fee0d9f6113dd0ac258b4ef368a` (#266) and preceding #271,
then `0a2312d1c603adcf0353def29b3698c8f8a6097c` (#272), and
`08a5b8ea796a3b85aeee3e56a79d0439ae68247f` (#273).
Shared store/core/contract observability, provider/generated consumers and
development compilation profile changed. All receipts above precede this
integration and must not be promoted to merged-source qualification.

`target-b-native-merged-main.log` completed successfully against commit
`3c9c18bea62d2e6388f14708647835a932d14b09`, tree
`a0589d992e6c04d8742c367435dfbfa0e80fa986`, including main through #266:
240 unit and 46 integration tests passed, with no failures or ignored tests.
This verifies the native filesystem-local test surface at that source; it
does not qualify #272, later history fixes, generated consumers, installed
packages, other platforms or the remaining composition mechanisms.
The unpublished development commit was subsequently rebased onto #272 with
the required DCO sign-off and a rationale in its commit body.

Sparse conversation read views remain readable but reject both relative and
absolute-next appends; dense reopened history still admits its next message.
The focused regression passed in `target-b-native-sparse-history.log` on #272.
`ModelDispatch` supplies operation ID, logical step and exact request digest
separately from canonical model-visible bytes. Summary IDs remain purpose-derived;
`ModelAttempt::dispatch()` exposes the same triple during reconciliation.
Interrupted-provider tests capture fresh dispatches and require reconciliation
to match them, including distinct summary/response IDs at one parent step.
All native targets pass strict lint in `target-b-clippy-dispatch-fixed.log`.
The preceding `target-b-clippy-dispatch.log` retains an integration-test import
failure corrected before that pass. The unit/recovery run in
`target-b-native-dispatch.log` passed all 240 unit tests on its prior-main
source. Browser/generated qualification remains open.

The independent dispatch seam was isolated on #273 as signed commit
`fbaea4d355e2937cdaaa1bafef722f78bd2d7a80`, tree
`dfbaa7401742ca668885cf226463b4f203ccb162`: ten native contract/consumer paths,
without B's summary/context dependency chain. Strict all-target lint and all
234 unit tests passed. Receipts are retained in
`target/b-independent-dispatch-receipts/{clippy.log,native.log}`; its managed
worktree is archived after I integrated the seam. I's browser adaptation and
its reported running checks are separate evidence, not B qualification.

Context history now uses a positive per-page allowance instead of a lifetime
revision cap. `revisions(after, through)` verifies one pinned page, excludes
later appends, and validates compaction against the preceding source even at
a page boundary. Latest reads remain at most two records beyond that allowance.
Malformed identity, noncanonical/oversized records and missing compaction-source
negatives use the production replay/decoder. Summary owner preflight now rejects
a stale task before adding records or artifacts; this does not claim atomic
fencing of every staging write against a concurrent cancellation.
`target-b-clippy-context-pages-owner-fixed.log` passes strict native all-target
lint. `target-b-native-context-pages-owner-fixed.log` passes all 241 unit tests
on #273 plus the two source files recorded in
`target/b-context-pages-owner-fixed-source-hashes.json`.
The preceding native run retained a legacy lifetime-cap assertion failure;
its corrected assertion checks latest and one-record pages beyond the allowance.
The earlier page check's Stream generated-wire mismatch is retained in
`target-b-clippy-context-pages-negatives.log`: an older source saw newer generated
frames after a shared target directory was used across worktrees. B was refreshed
to matching #273 source, and builds are now isolated by worktree.

Warm native reopening has been replaced by a retained aggregate and bounded
verified refresh. Next implementation loop: checkpointed cold hydration and
default threshold policy with explicit provider/model capacity and
counting semantics (no guessed tokenizer, reserve/recent budget violations or
raw summary provider path); add fork/fresh/summary policies over pinned logical
history; expose Rust-owned contracts to generated/WASM/current consumers.
Coordinate I's browser/provider-binding seam through the coordinator, keeping
its runtime timer/spawn regions untouched. No gated owners may be awakened.

Active input retention now uses `Context.current_input_index`, initialized from
the validated selected turn before prior model/tool observations are appended.
Shared source/selection placement shifts the marker on prepend and preserves it
on append. Compaction keeps the marked input and reindexes it in the output;
stages that remove it and projections with out-of-range markers fail explicitly.
Standalone imported sources carry no active-input marker. This avoids deriving
turn identity from whichever message most recently has the User role.
Generated summaries now have the Assistant role, allowing a subsequent admitted
summary to replace them while the default policy retains original System
instructions. These semantics use context record format 4 and compaction stage
contract revision 3. Earlier format-3/revision-2 receipts above remain prior-source.
`target-b-clippy-input-marker-final.log` passes strict native all-target lint;
source hashes are in `target/b-input-marker-source-hashes.json`.
`target-b-native-input-marker.log` passes all 244 native unit and recovery tests,
with no failures or ignored tests. Production transformations cover repeated
compaction, trailing tool results, prepended/appended User-role facts and marker
removal/range negatives. Generated/WASM consumers and remaining composition
mechanisms are still unqualified.

An isolated WASM build at `6387d3dbf0` passed in
`target-b-wasm-input-marker.log`. Its first generated declaration made the marker
required; the Rust `tsify(optional)` annotation corrected that exported shape.
The rebuilt declaration in `target/b-context-wasm` contains
`current_input_index?: number`; `target-b-wasm-input-marker-optional.log` passes.
Shared generated consumer files have not been replaced by this isolated check.

The next source freezes each response projection before any model dispatch in
the existing execution journal's `ContextPrepared` observation. Its private
artifact retains the full context (including the input marker and metadata).
The existing Started composition pins model/tools/options, so the artifact does
not duplicate those fields or message bodies. Reattachment before `ModelStarted`
loads that artifact, and admitted/reconciled model requests retain their existing exact
request path. The existing owner classifies context publication as a fresh write;
the journal rejects duplicate preparation and preparation after response dispatch.
This is a prerequisite for resuming an automatic summary without rereading mutable
context sources, not an implementation claim for automatic thresholds.
`target-b-clippy-prepared-context-fixed.log` passes native all-target strict lint
before the final duplicate-preparation negative. The first lint receipt retains
the corrected match-arm duplication failure. The current source hashes are in
`target/b-prepared-context-source-hashes.json`; native tests in
`target-b-native-prepared-context.log` passed 243 tests and failed two: an old
record-count assertion, and a payload-bound regression caused by duplicating
messages in the first capture format. The duplicate representation was removed
and the count assertion updated; corrected hashes are in
`target/b-prepared-context-fixed-source-hashes.json`. Corrected native execution,
final strict lint, generated consumption and default capacity/counting remain open.
The corrected native all-target strict lint passes in
`target-b-clippy-prepared-context-compact.log`; the corrected full native suite
passes all 245 tests, with no failures or ignored tests, in
`target-b-native-prepared-context-compact.log`. All four recorded production
source hashes match the tested source. The mutable-source recovery fixture
captures before dispatch, reattaches, then interrupts/reconciles the model with
one source read, one generation and one reconciliation. The bounded native
storage fixture passes with its existing payload allowance. Context lookup by
operation already uses `operation_intents`; no additional selection index was
needed. This checkpoint does not close default compaction/fork, cold hydration,
total refresh work, generated/current consumers or final qualification.

The current development source installs ordinary `CompactionPolicy` configuration
in both `StockExecutor` and `HarnessBuilder`, defaulting to a threshold with
16,384 response-reserve tokens and 20,000 recent tokens. `ModelProvider` supplies
the selected model's actual input/output capacities and deterministic additive
request token upper bounds. The SDK supplies no model-name catalog or tokenizer;
unsupported capacity/counting fails explicitly. Capacity and impossible budgets
are validated before the root turn's Started observation. Disabled policy is an
explicit imported alternative used by the older unrelated synthetic fixtures.

Provider accounting binds the exact prepared request digest and message count,
and is persisted beside the frozen context in the same ContextPrepared record.
Recent suffix planning expands a split tool exchange, and uses the same mandatory
positions as deterministic compaction. Oversized mandatory input fails before
summary inference. The summary uses the original turn's journal and shared model
accounting; its finite output ceiling comes from the validated reserve/output
capacity. A retained uncertain summary is reconciled without recounting its
already admitted input or rereading mutable context sources. Final response
accounting validates the actual compacted request before response dispatch.
Explicit same-message-count compression is supported, since a large old message
may be replaced by a shorter summary without reducing message count.

`target-b-clippy-default-compaction-recovery.log` passes native all-target strict
lint. The new native fixtures use a synthetic provider with declared byte-based
token units, capture both summary/response requests, check impossible default
capacity before records/artifacts/generation, and interrupt/reconcile an automatic
summary over a changing source. They do not claim a production provider's tokenizer
or media support. Source hashes are in `target/b-default-compaction-source-hashes.json`;
`target-b-native-default-compaction.log` is in progress. Native integrations,
malformed accounting/policy replacement/mandatory-bound negatives, generated and
WASM runtime consumption, incremental projection reuse across later turns, default
fork policies, cold checkpoints and final qualification remain open.

The first default-policy unit run completed 244 passes and four failures in
`target-b-native-default-compaction.log`: two automatic-summary fixtures lacked
staged-file verification, and two local-default fixtures lacked selected-capacity
advertisements. The synthetic journal now verifies its staged summary file via
its real load/descriptor path. Local-default synthetic providers now advertise
their capacities and bound referenced bytes/framing, keeping automatic compaction
enabled for those default-consumer checks. Unrelated discovery/journal/recursive
fixtures and the custom-executor example explicitly disable compaction; the
durable journal fixture also checks the additional ContextPrepared record.
`target-b-clippy-default-compaction-fixtures.log` passes native all-target strict
lint. Corrected unit and integration execution completed in
`target-b-native-default-compaction-fixtures.log`, with source hashes recorded in
`target/b-default-compaction-fixtures-source-hashes.json`: 248 unit tests passed,
as did the preceding integration binaries, before `task_workflow` reported 12
passes and eight failures. Its synthetic InterruptedModel lacked the new selected
capacity/accounting hooks, so default admission failed before the intended
file-grant and uncertain-recovery checks. That test provider now advertises actual
fixture capacities and counts serialized messages/framing plus referenced bytes,
keeping default compaction enabled and the existing 8,192-token output contract.
The targeted rerun in `target-b-native-default-compaction-workflow.log` passed
18 workflow tests and failed two publication-fault scenarios. Their positional
fault targeted tail 2, now ModelStarted instead of the first observed event.
The fault remains on first-event publication at tail 3, retaining the existing
before/visible/hidden acknowledgement, exact-generation/reconciliation and
cancelled-owner checks. The corrected rerun is recorded in
`target-b-native-default-compaction-workflow-fault-position.log`, with hashes in
`target/b-default-compaction-workflow-fault-position-source-hashes.json`: all 20
workflow tests passed, with zero failures or ignored tests. Strict native
all-target lint passed in `target-b-clippy-default-compaction-workflow.log`.
These receipts precede integration of main's observation-span changes.

Main `b94284f885cfde8a5961d62e9a435b2e7d80d0cf` (#275) is now integrated.
The model-step and execution wrappers retain observation spans while preserving
purpose-specific summary/response admission and shared owner verification.
The recursive model-step wrapper returns the existing platform boxed future,
keeping native Send and browser future contracts intact. The new native journal
span fixture selects Response explicitly; the unrelated span-only model fixture
uses the existing disabled-policy test constructor. Fresh strict native
all-target lint passed in `target-b-clippy-main275-compaction-fixed.log`.
Integrated native execution is recorded separately in
`target-b-native-main275-compaction.log`; earlier receipts do not qualify this
integrated source. This integrated run passed 250 unit and 46 integration tests,
with zero failures or ignored tests, at commit
`76b88721fc7008e27889bd2c5a6ca7baa5184c93` (tree
`c3df1bba72cdd5d82953077221f25d8d144602eb`). Source hashes are recorded in
`target/b-main275-compaction-source-hashes.json`.
Additional policy/accounting negatives, durable
projection continuation, logical forks, cold checkpoints, generated consumption
and final platform qualification remain open.

The next consumer change forwards the existing CompactionPolicy through
LocalHarness::with_limits/with_tools, FilesystemTaskExecution's configuration
and the stock task model binding. LocalHarness::new retains the default policy.
The local consumer regression installs a 1,024-token reserve, then disables
compaction for a provider that implements no capacity/accounting hooks. The
durable reopen regression installs an 8,192-token reserve and checks the retained
output budget after exact reconciliation. These are current-source checks under
development, separate from the preceding 296-test receipt. The current consumer
run passed all 251 unit and 46 integration tests in
`target-b-native-compaction-convenience.log`; no failures or ignored tests.
Strict native all-target lint passed in
`target-b-clippy-compaction-convenience-final.log`. The four source hashes in
`target/b-compaction-convenience-source-hashes.json` were rechecked after the run
and all matched. This verifies native configuration consumption; generated and
browser consumption remain open.

Source inspection of later-turn continuation identifies the remaining boundary:
StockExecutor::compact_response stages the exact CompactionReference in its turn
journal, while MemoryHarnessStorage::run_conversation starts the next canonical
selection again. The selection's indexed suffix bounds message count but does
not load a retained summary projection. DurableContextProvider::latest provides
bounded durable projection reads, but the default builder currently binds the
journal/content providers without installing that provider as a continuing
context source. Thus the current automatic-threshold evidence covers one admitted
turn and its recovery, not persistent compression across later turns. The next
continuation change must reuse those existing projection/journal mechanisms,
retain authoritative history and exact prior admissions, and include a changed
source plus later-turn consumer regression.

Accounting admission negatives passed in
`target-b-native-accounting-negatives.log` (255 native library tests, zero failed
or ignored), with strict native all-target lint in
`target-b-clippy-accounting-negatives-final.log`. The executor hash in
`target/b-accounting-negatives-source-hashes.json` matched after completion.
Wrong digest and short/long dimensions reject before projection staging or model
publication. Unknown nested fields and negative, overflowing, fractional or null
token counts reject wire decoding. Mandatory instructions and native file
references exceeding the synthetic provider's declared input headroom reject
before summary inference. Excess actual compacted accounting rejects response
publication; an exact retry reuses the captured source and completed summary,
with one total summary generation and no response generation. These are typed
synthetic-provider admission tests, not production tokenizer/media qualification.

Generated consumers now expose the production Rust CompactionPolicy,
CompactionRetention, ThresholdCompaction, ModelContextCapacity and ModelTokenCount
types, plus pure default-policy, selected-capacity and exact-request count
validation. Host counts remain provider-owned; these exports do not implement a
second browser compactor or qualify automatic browser model admission.
The generated optional current-input marker is omitted from standalone JS
projections, while index zero is preserved. Native canonical context serialization
is unchanged. Strict WASM lint passed in
`target-b-clippy-generated-compaction-marker.log`. The regenerated consumer build
and type checks passed in `target-b-types-compaction-build-marker.log` and
`target-b-types-compaction-consumer-marker.log`. Affected actual WASM consumers
passed 26 tests and 213 expectations in
`target-b-wasm-compaction-affected-consumers-marker.log`; actual Chromium passed
16 contract checks in `target-b-browser-compaction-contracts-marker.log`.
Both generation output directories and installed JS/WASM/declarations matched
all four SHA-256 hashes in `target/b-compaction-generated-artifact-hashes.json`.
The eight compiled/consumer source hashes are recorded in
`target/b-compaction-generated-source-hashes.json`. The earlier failed optional
marker receipt is retained in `target-b-wasm-compaction-marker-output.log`.
These checks cover generated primitive consumption and wire validation; later-turn
projection reuse, cold checkpoints, default logical fork integration and complete
platform/runtime qualification remain open.

Local canonical-turn retry publication now uses ConversationState's existing
message-ID index for user, assistant and tool-message checks. These lookups no
longer scan retained history. The existing local storage/retry consumers passed
10 tests in `target-b-native-indexed-memory-lookups.log`; strict native all-target
lint passed in `target-b-clippy-indexed-memory-lookups.log`. The compiled memory
source SHA-256 is recorded in `target/b-indexed-memory-lookups-source-hash.json`.
This removes three warm-turn scans and does not qualify cold restoration or
complete operation work bounds.

Continuing projection publication now uses one existing Stream append CAS for
the exact source plus its compacted revision (`append_compaction`). Validation,
portable revision arithmetic and record-size checks finish before publication;
both single and paired appends require the complete expected receipt range.
`latest_revision` captures a revision pin, and `revision` verifies that pinned
context and its immediate compaction source with at most two record reads.
Neither operation traverses all retained context revisions.

The final context provider tests passed 10 cases in
`target-b-native-atomic-context-pair-receipts.log`. The new actual native consumer
passed three cases in `target-b-native-context-continuation-consumer-final.log`:
an ordinary admitted summary with real private Filesystem source/output,
lost-ack recovery of the paired publication, malformed start/end/tail receipts,
and later-turn SourceStage composition plus exact response recovery after a
changed source. New admissions see the changed source; an admitted retry retains
its captured request. Strict native all-target lint passed in
`target-b-clippy-context-continuation-consumer-final.log`, and strict WASM library
lint passed in `target-b-clippy-atomic-context-pair-wasm.log`. Three source hashes
are recorded in `target/b-atomic-context-pair-final-source-hashes.json`.
The existing durable workflow consumers of the shared fault fixture passed all
20 cases in `target-b-native-context-pair-shared-fault-consumers.log`; no failures
or ignored tests. All three source hashes were rechecked after completion and
matched.

This qualifies the explicit existing context-source path and atomic publication
primitive, not automatic canonical-conversation continuation. The default
integration must pin its context revision alongside the canonical selection and
select only later history after the checkpoint watermark. Its retained view must
not duplicate old current input or instruction stages. Durable context records
at this checkpoint accepted file parts only; inline instruction/skill text and
structured tool exchanges required immutable payload references before default
integration could use this primitive.

The next storage revision closes that representation gap. Stream context format
5 stores one immutable canonical context payload FileRef, plus bounded revision
identity and compaction proof. The resolved public ContextRevision contains its
typed Context and payload ref. There is no format-4 decoding fallback. The same
owner-bound ContentPublisher stages payloads, and the existing residency reader
checks descriptor, exact bytes, canonical encoding and every nested file ref.
Explicit Limits bound payload bytes, context messages, metadata and file paths.
Read-only reopen needs no writer; publication requires an installed publisher.
Single and paired publication reuse one payload encoder and the existing CAS.

The native source in `target/b-context-payload-final-source-hashes.json` passed
258 library tests in `target-b-native-context-payloads-final.log`. The final real
consumer fixture passed four cases in
`target-b-native-context-payloads-consumer-final.log`, including a Unicode
instruction larger than a Stream record and a typed tool exchange which survive
read-only reopen and ordinary PreparedModelRequest validation unchanged. Direct
inspection confirms the Stream record contains only the payload ref, not those
instruction bytes. Missing payload, changed bytes, excessive descriptor size,
render/file budget exhaustion and absent publisher produce explicit errors;
excessive descriptor size rejects before reading payload bytes, and failed
admission leaves both Stream and staged-file counts unchanged. Strict native
all-target lint passed in `target-b-clippy-context-payloads-native-final.log`;
strict WASM library lint passed in
`target-b-clippy-context-payloads-wasm-final.log`. Earlier format-4 receipts remain
scoped to that previous source. Automatic canonical continuation, cold history
checkpoints, default fork policies and final runtime/platform gates remain open.

The shared JSON snapshot repair from signed peer commit
`d8453a5a1701d5d6009b8aabd639709d49bd3189` is adopted only at its three string
conversion sites. String values, object keys and Map keys reuse the existing
checked UTF-16 converter; no new serializer or model/provider algorithm is
installed. B's actual accounting consumer proves that an invalid surrogate
request cannot be admitted using counts bound to its lossy U+FFFD replacement,
while a request containing the valid replacement character is accepted.
Replacement characters, paired astral characters and NUL remain unchanged.

Fresh strict WASM lint passed in `target-b-clippy-peer-json-unicode.log` and
regeneration in `target-b-wasm-peer-json-unicode.log`. Package build/type checks
passed in `target-b-types-peer-json-unicode.log` and
`target-b-types-peer-json-consumer.log`. Actual affected WASM consumers passed
27 tests and 252 expectations in `target-b-wasm-peer-json-consumers-final.log`.
Real Chromium passed all 42 pure JSON/accounting/compaction checks and the
existing broader wire/discovery page in
`target-b-browser-peer-json-and-discovery-final.log`. The discovery fixture now
awaits the existing asynchronous builder; its earlier missing-await failure is
retained in `target-b-browser-peer-json-consumers-built.log`. Its missing local
Filesystem browser build was supplied through the package's existing TypeScript
compiler, not a replacement runtime. Four installed JS/WASM/declaration hashes
match the generated directory in `target/b-peer-json-artifact-hashes.json`;
four source hashes are retained in `target/b-peer-json-source-hashes.json`.
These pages verify their exercised contract/discovery paths, not the broader
automatic Rust browser model runtime or default conversation checkpoint policy.


## Committed compacted projections

`ContextCompacted` records the exact compacted context, source/summary proof and
final provider accounting as three immutable FileRefs in the existing execution
journal. The stock executor publishes it after successful summary settlement and
capacity validation, before response admission. Journal admission rejects an
unprepared source, pending summary, duplicate checkpoint, wrong step or already
started response. Recovery reconstructs the proof against the frozen prepared
source and validates retained accounting against the exact reconstructed request;
it does not invoke the token counter or summary provider again.

The WASM decoder consumes the native Rust event enum directly. Tsify derives its
union and component enums; FileRef fields use the existing generated wire
contract and existing descriptor-number normalization. This decoder validates
canonical representation and typed references, not the journal's temporal
admission rules. Those remain the owning journal's responsibility.

Strict WASM lint passed in `target-b-clippy-compacted-context-wire-wasm.log`.
The frozen source hashes are in
`target/b-compacted-context-wire-source-hashes.json`. The first native run
passed 259 tests but failed the new recovery fixture: one oversized whole recent
message left no older source eligible for summarization. The fixture now has two
complete older messages. The final native rerun passed all 260 tests in
`target-b-native-compacted-context-wire-final.log`. Strict native all-target lint
passed in `target-b-clippy-compacted-context-wire-native.log`. Isolated generation
passed in `target-b-wasm-compacted-context-wire.log`; all four installed artifacts
match their generated counterparts in
`target/b-compacted-context-wire-artifact-hashes.json`. Package build passed in
`target-b-types-compacted-context-wire.log`, and final test type checking in
`target-b-types-compacted-context-consumer-final.log`. Final generated consumers
passed 28 tests and 265 expectations in
`target-b-wasm-compacted-context-consumers-final.log`. Real Chromium passed the
broader wire/discovery page and 54 pure checkpoint/accounting checks in
`target-b-browser-compacted-context-wire.log`. The earlier fixture literal-widening
type error is retained in `target-b-types-compacted-context-consumer.log`; it is
not final-source evidence. These checks qualify their described seams, not the
unimplemented default continuation or full browser automatic model path.

Default canonical continuation remains open. A post-pipeline checkpoint already
contains instruction/retrieval contributions, so replaying the pipeline blindly
would duplicate them, while skipping it would lose input-dependent stages and
explicit reloads. The canonical watermark and declared stage contribution
boundary must be integrated without inferring provenance from message equality.
Cold bounded restoration and configurable default fork policies also remain open.


## Explicit input-base transformation boundary

`ContextPipeline::base_context` builds the selected canonical input and in-turn
messages without executing any stage. `transform_bounded` applies the same
ordered stages to an explicitly supplied base and checks the current input marker
and all intermediate bounds. `run_bounded` composes these two operations; there
is no second pipeline or checkpoint engine. A continuation owner can therefore
supply a retained canonical base plus its delta, while rerunning input-dependent
stages and future reloads. The owner must establish that base's provenance; this
API does not infer contribution ownership from message equality or claim that a
post-pipeline summary can be unmerged.

The custom-transform fixture prepends new input-dependent instructions by
reordering the entire supplied base, tracks the active input, and changes the
instruction revision on explicit reload. It checks that stage construction/base
construction performs no source calls, each new transformation runs once, no old
stage instructions enter the retained base, and an absent input marker rejects
before stage execution. This is a transformation-boundary test, not a default
checkpoint admission or canonical-watermark qualification.

Strict native all-target lint passed in
`target-b-clippy-explicit-base-boundary.log`; strict WASM lint passed in
`target-b-clippy-explicit-base-boundary-wasm.log`. The exact context source hash
is retained in `target/b-explicit-base-boundary-source-hash.json`. The complete native
library passed 261 tests, and all four existing admitted-summary continuation
integration tests passed in `target-b-native-explicit-base-boundary.log`.
Generated browser artifacts have not been regenerated for this refactor yet;
these receipts qualify the Rust transformation seam, not browser adoption or
default canonical watermark publication.


## Pinned canonical checkpoint and indexed delta seam

`CanonicalContextCheckpoint` is an immutable, ref-only envelope over a source
selection, pre-stage source, retained base and admitted-summary proof. Its
selection revision is the coverage watermark. Validation binds the retained
projection to the actual compaction and summary operation; the owning journal
must establish committed publication and canonical source authority.
`select_turn_delta` verifies the pinned envelope descriptor and recorded source
selection, then uses the resident sequence index to select the complete delta.
Without a checkpoint it selects all model-visible history or rejects the bound;
it never silently drops an older prefix. Unresolved checkpoint selections fail
closed at pipeline base construction until the owner materializes the base.

The 10,000-record fixture qualifies resident indexed selection, exact delta
boundaries, insufficient bounds and altered-envelope rejection. It does not
qualify cold restoration, constant memory or default checkpoint publication.
The real storage integration publishes and reopens an envelope referencing
actual source/retained contexts and an admitted summary proof, with altered
retained projection and operation negatives. Its synthetic source identities do
not establish canonical history coverage.

Native library and continuation checks passed 262 plus five tests in
`target-b-native-canonical-checkpoint-seam.log`. Strict native and WASM lint
passed in `target-b-clippy-canonical-checkpoint-seam-final.log` and
`target-b-clippy-canonical-checkpoint-seam-wasm.log`. Fresh isolated generation
passed in `target-b-wasm-canonical-checkpoint-seam.log`; all four installed
artifacts match `target/b-canonical-checkpoint-seam-artifact-hashes.json`.
Package build and test type checking passed in
`target-b-types-canonical-checkpoint-consumers.log`. Projection/model/type
consumers passed 29 tests in `target-b-bun-canonical-checkpoint-consumers.log`,
and compaction accounting passed separately in
`target-b-bun-canonical-checkpoint-accounting.log`. Chromium passed the broader
wire page and 54 pure compaction checks in
`target-b-browser-canonical-checkpoint-consumers.log`. These checks also
regenerate the prior base/transform boundary. Default continuation, cold
restoration, fork policy and the automatic browser model path remain open.


## Default preparation rejects uncovered history overflow

Default `prepare_turn` now calls `select_turn_delta` without a checkpoint, so it
preserves all model-visible history or rejects an insufficient context bound.
The native memory caller always derives the canonical user identity; it no
longer detects an older alternate identity from history. The explicit custom
identity planner remains available to hosts that choose it deliberately.
Native and generated-WASM fixtures pin a complete two-message prior exchange:
a bound of two rejects the next user, while three includes the entire exchange
and current input without mutating history. This closes silent suffix selection
in default preparation, not automatic checkpoint publication/materialization.

The no-feature run passed 242 library tests but correctly ran no feature-gated
integration tests. The final all-feature run in
`target-b-native-default-complete-history-all-features.log` passed the library,
five continuation tests and both recursive-fork integration tests. Strict
native lint passed in `target-b-clippy-default-complete-history-native.log`.
The first WASM lint command incorrectly included native-only test dependencies;
the corrected library-only command passed in
`target-b-clippy-default-complete-history-wasm-final.log`. Fresh generation
passed in `target-b-wasm-default-complete-history.log`; four artifact hashes are
pinned in `target/b-default-complete-history-artifact-hashes.json`. Package and
test type checks passed. Generated consumers passed 25 tests/195 expectations in
`target-b-bun-default-complete-history.log`; Chromium passed both pages in
`target-b-browser-default-complete-history.log`. The new main commit
`0db1466cd0f2aef5327d3a81c0bae7cdad22a6bd` was fetched after these source pins;
these receipts do not qualify that upcoming merge.


## Native default canonical continuation

The native memory composition now materializes its latest committed canonical
checkpoint and selects the exact indexed delta. The pointer is maintained by
replay of existing selection events. First-step response capture stages its raw
canonical base independently from ordered stage transformations. Under pressure,
Stock v6 summarizes that base through the existing admitted summary path, stages
its retained base/proof envelope, reruns the stages, validates final capacity and
pins the exact transformed request/accounting. Existing prepared/compacted event
schemas are unchanged; their private projection payload separates these values.
No historical payload adapter is added. Already admitted supported requests still
recover their exact retained bytes.

After successful execution, the memory owner publishes the pin through its
existing selection action before the assistant closes the turn. Lost publication
acknowledgements compare the exact committed selection; retries of old operations
do not republish an older pin. Public journal-mediated lookup/materialization
functions expose the same mechanism to custom callers. Materialization verifies
committed execution publication, source/retained compaction proof and current
read admission for all retained file and metadata payloads. It loads only the
current envelope and bounded payloads, not the lifetime checkpoint chain.

The real default-path fixture executes ten turns while the message bound is
eight. Its arbitrary reordering/input-dependent stage contributes one fresh
instruction per response, explicit instruction reload applies to future turns,
and source/retained bases contain no stage instructions. Exact retry calls
neither the model nor stages. A separate real-journal fault fixture commits
compaction then loses its acknowledgement: the next turn remains blocked,
recovery admits one response with one summary and no recount/stage replay,
and normal continuation succeeds afterward. It also rejects a denied retained
payload read and an unpublished envelope. These fixtures qualify the described
native default path; they are not cold-restoration, 10,000-turn performance,
constant-memory or automatic browser-model qualifications.

Final strict native lint passed in
`target-b-clippy-default-canonical-continuation-owner-negatives.log`; final strict
WASM lint passed in
`target-b-clippy-default-canonical-continuation-grants-wasm.log`. The final library
and fork run passed 272 plus two tests in
`target-b-native-default-canonical-continuation-grants.log`; all seven continuation
tests, including owner-read/publication negatives, passed in
`target-b-native-default-canonical-continuation-owner-negatives.log`. Fresh isolated
generation passed in `target-b-wasm-default-canonical-continuation-grants.log`,
and four installed artifact hashes are recorded in
`target/b-default-canonical-continuation-artifact-hashes.json`. Package and test
types passed in `target-b-types-default-canonical-continuation.log`; the complete
Harness Bun suite passed 248 tests/1347 expectations in
`target-b-bun-default-canonical-continuation.log`. Chromium passed the wire page
and 54 pure compaction checks in
`target-b-browser-default-canonical-continuation.log`.

Qualification remains incomplete. In addition to browser default integration,
bounded cold hydration, fork policies, 10,000-turn performance and full release
gates, arbitrary custom checkpoint publication needs a stronger admission audit
binding the canonical source to its committed selection and prior covered base.
The healthy default producer already constructs that source through verified
selection/materialization; metadata-only publication membership is not itself
proof against an independently fabricated source/coverage claim. Do not treat
these passing fixtures as closure of that owner-admission requirement. Private
checkpoint JSON currently loads through the journal's own volume binding;
portable direct-parent checkpoint reads require the appropriate owner-mediated
content path rather than weakening that boundary.


## Canonical publication and local temporal admission

The native filesystem journal now applies its existing temporal reducer to local
executions as well as task-owned executions. A single active-operation index is
rebuilt from that operation's paged records when ownership switches; it does not
retain a lifetime operation registry. Append and compare-and-append share the
same retry/digest validation. Local publication uses the existing exact-tail
Stream CAS and checks the complete one-record receipt. Task-owned publication
continues to use its existing owner-fenced commit and shared budget checks.
Both paths decode retained model requests and bind their digest before admission.

Canonical checkpoint publication now reconstructs the source from the owning
conversation's exact committed selection, all indexed model-visible messages
since the immediate previous checkpoint watermark, and that previous retained
base. It rejects a sparse coverage claim or a changed source. It validates the
existing compaction proof and binds the summary to the retained prepared source,
actual admitted request prefix, completed model observations and exact output
bytes. This admission does not reconstruct arbitrary stage ownership by equality;
stages remain separate from the canonical base. Immediate prior checkpoints are
materialized through the existing owner-mediated read path, without recursively
loading the lifetime checkpoint chain.

The new adversarial continuation fixture substitutes immutable, descriptor-valid
source or summary payloads and recomputes a valid public compaction proof. Owning
history and model-observation admission must reject these publications before a
ContextCompacted record commits; the same operation then recovers with the
original admitted summary. The local temporal fixture independently exercises
premature/unsettled/duplicate compaction, stale-tail rejection, exact retries and
operation-index switching. Its opaque compaction payload qualifies temporal
ordering only; it is not canonical proof evidence.

This admission increment does not close automatic browser model integration,
bounded cold hydration, direct-parent fork read admission or full fork policies.
The reducer still retains lifetime events, first open still replays history, and
these fixtures do not demonstrate constant memory or 10,000 default turns. Full
slice and release qualification remain required before B's PR/admin merge.


Strict native lint passed in `target-b-clippy-canonical-admission-final.log`;
strict WASM lint passed in `target-b-clippy-canonical-admission-wasm-final.log`.
The final native library passed 272 tests in
`target-b-native-canonical-admission-library.log`; nine continuation and two fork
tests passed in `target-b-native-canonical-admission-integration.log`, including
both fabricated-payload rejection/recovery cases. Six owning source hashes are
recorded in `target/b-canonical-admission-source-hashes.json` and verified unchanged
after the terminal native run. Strengthening retained-request admission exposed
three authority/tracing fixtures that staged placeholder JSON; those fixtures now
stage valid prepared requests and retain their original owner, budget and tracing
assertions. Failed intermediate runs remain recorded separately.

Fresh isolated WASM generation passed in `target-b-wasm-canonical-tamper.log`.
The four installed artifacts match the hashes recorded in
`target/b-canonical-tamper-artifact-hashes.json`. Package/test type checks passed
in `target-b-types-canonical-tamper.log`, the complete Harness Bun suite passed
248 tests/1347 expectations in `target-b-bun-canonical-tamper.log`, and Chromium
passed the wire page and 54 pure compaction checks in
`target-b-browser-canonical-tamper.log`. Subsequent source edits changed only
native test fixtures; the WASM library owning inputs remain identical. These
consumer checks qualify the generated library and pure contracts, not automatic
browser canonical continuation or production tokenizer/media adapters.


Final-source isolated generation also passed in
`target-b-wasm-canonical-admission-final.log`. All four output hashes exactly
match the installed and consumer-tested artifacts; their final pins are in
`target/b-canonical-admission-final-artifact-hashes.json`.


## Pinned canonical history reads

`HistoryReader` opens an owner-bound handle to the existing aggregate Stream
without constructing a reducer, replaying any events or caching history. A
serializable `HistoryCursor` pins aggregate authority, consumed revision and
committed upper revision. Each page declares event and encoded-byte limits,
checks every record's contiguous sequence and authority/revision binding, and
verifies the existing keyed event-admission attestation. Later appends remain
outside the cursor. A malformed, oversized or empty nonterminal provider page
fails explicitly and cannot advance the supplied cursor. The reader performs
no writes, admissions or payload-body resolution.

Historical extension-migration replay/source lookup and published-fork child
binding now read exact canonical Stream positions through this reader, rather
than depending on resident lifetime event bodies. This uses the same owner
verifier and Stream path as aggregate admission. The pure reducer's resident
`events_after` now binary-seeks its ordered event vector; it no longer scans a
lifetime prefix for a small late page. This is an index/search change, not a
claim that the vector itself has bounded resident size.

The real-provider fixture builds 10,000 committed events, constructs a cold
reader without an aggregate reopen, and reads the final three at a pinned
boundary in exactly two record-read calls, each requesting at most two records.
A later append is excluded, serialized cursors resume at the same boundary,
terminal reads fetch no records, and write attempts remain zero. Fault pages
exercise gaps, excess records, missing records and corrupt wire data; byte and
cursor/authority bounds reject explicitly. A separate fixture rejects validly
encoded wrong-authority and changed-event-attestation records. These cases do
not substitute for 10,000 default model turns or cold reducer hydration.

Snapshot v2 still embeds/replays lifetime events; default aggregate open still
rehydrates from that history. Those paths remain incomplete. Compact derived
checkpoint state must retain exact owner authentication, installed definitions,
old-operation identity lookup and in-flight recovery, and normal hydration must
load bounded needed state. The archival reader is the existing canonical access
path for historical data excluded from a future compact resident checkpoint;
it is not a second context store or an independent lifecycle. Browser runtime
integration and full release qualification remain open.


Final strict native all-target/all-feature lint passed in
`target-b-clippy-history-reader-final.log`; strict WASM library lint passed in
`target-b-clippy-history-reader-wasm.log`. The final native run passed 274 library,
nine continuation and two fork tests in `target-b-native-history-reader.log`.
Four owning source hashes in `target/b-history-reader-source-hashes.json` remained
unchanged through the terminal runs. Fresh isolated generation passed in
`target-b-wasm-history-reader.log`; four installed artifact pins are in
`target/b-history-reader-artifact-hashes.json`. Package/test types passed in
`target-b-types-history-reader.log`; all 248 Bun tests/1347 expectations passed
in `target-b-bun-history-reader.log`; Chromium passed the wire page and 54 pure
compaction checks in `target-b-browser-history-reader.log`. The Rust reader is
portable and WASM-compiles, but these browser pages do not invoke its new cold
Stream reader through the runtime binding; that consumer evidence remains open.


## Archived operation locations

Aggregate event publication now uses the existing Stream coordinated commit to
append both the canonical event and a small derived operation-location record.
The location is keyed by exact aggregate authority and operation ID; its fields
bind the canonical revision and intent digest. Canonical-tail and absent-location
conditions are checked atomically. The caller's original retry key remains the
transaction key. Task-owned interaction publication adds the same location to its
existing owner-fenced transaction, retaining the current lease/coordinator fence.
Execution/workflow journals retain their existing owned publication mechanism.

Passive operation lookup reads one location and one canonical event. It checks
both sequence bounds, the single-record location tail, authority and operation
identity, exact intent digest, existing keyed event attestation and a shared
commit ID. Separate publication of an otherwise valid owner-attested event and
location is rejected. Empty nonterminal, missing or corrupt committed locations
fail explicitly. A resident cached operation is not an alternative proof if its
canonical location is missing. No derived record grants admission authority.

Turn selection/checkpoint admission, interaction retry planning and historical
extension migration now use owning asynchronous operation lookups. A cold
HistoryReader can resolve an admitted operation without restoring a reducer or
scanning an operation registry. Exact command replay rechecks its original scope
and whole intent digest; changing the caller retry key remains a conflict.
A stale projection may apply only the next exact indexed event; a larger gap
requires explicit bounded refresh before local state can advance.

This index is a prerequisite for releasing resident lifetime retry events during
compact hydration. That release, authenticated compact state checkpoints and
bounded normal cold restoration are not yet implemented. No old-data migration
or generic historical adapter is added; newly supported aggregate publications
include their location in the same atomic commit. The canonical event Stream
remains authoritative, and the index has no independent admission lifecycle.


The real-provider fault fixture covers no-publication failure, committed/lost
acknowledgement, temporarily hidden location reads and contradictory receipts
missing or changing the index mutation. Only the no-publication case needs a
second commit attempt; committed retries resolve the exact original event without
another publication. Exact old retries after a later event work with writes
disabled, changed retry identities conflict, and malformed/missing index reads
cannot fall back to resident cached events. The 10,000-event cold reader also
resolves an operation with exactly two record reads, each requesting one record,
and no writes. A valid owner-attested event and location published in separate
transactions fail their shared-commit check.

Final strict native lint passed in
`target-b-clippy-operation-index-final-source.log`; strict WASM library lint passed
in `target-b-clippy-operation-index-final-source-wasm.log`. Final native tests
passed 276 library, nine continuation and two fork tests in
`target-b-native-operation-index-final-source.log`. Nine source hashes in
`target/b-operation-index-final-source-hashes.json` remained unchanged through
terminal qualification. Fresh isolated generation passed without warnings in
`target-b-wasm-operation-index-qualified.log`; four matching installed artifact
hashes are in `target/b-operation-index-artifact-hashes.json`. Package/test types
passed in `target-b-types-operation-index.log`; Bun passed 248 tests/1347
expectations in `target-b-bun-operation-index.log`; Chromium passed wire and 54
pure compaction checks in `target-b-browser-operation-index.log`. An intermediate
native run failed the existing model-span capture assertion; the final full run
passed that unchanged assertion. The intermediate failure remains recorded in
`target-b-native-operation-index-uncertainty.log` rather than being counted as a
passing receipt.

Fork publication keeps its existing private fence held on ambiguous receipts or
failure to apply an attested committed event locally. Contradictory task-owned
interaction receipts report the interaction operation as indeterminate. The
index contract does not change model-provider/dispatch/request or event schemas.
Its Rust reader and publication paths WASM-compile, but are not yet exposed as a
browser runtime reader/publication bridge; the pure browser pages above do not
qualify that missing consumer. These receipts also precede adoption of main
`a10e2b24c53201a321ff567835ab960a80a309bc` (#280/#281); relevant final-source provider
and generated-contract checks must be refreshed after that integration.


## Main provider/contract reconciliation after operation indexing

Signed merge `d023c64da0837fa5c3ce8870456153aabd405a94` incorporates actual main
`a10e2b24c53201a321ff567835ab960a80a309bc` (#280 Objects bounded streaming deadlines
and #281 Rust-derived Filesystem transaction types) without Harness source
conflicts. The nine owning operation-index sources still match their recorded
hashes. Four changed provider/contract inputs are pinned in
`target/b-operation-index-main-provider-source-hashes.json`.

Merged-source strict native lint passed in
`target-b-clippy-operation-index-main.log`; native 276 library, nine continuation
and two fork tests passed in `target-b-native-operation-index-main.log`. Strict
WASM lint passed in `target-b-clippy-operation-index-main-wasm.log`. Fresh isolated
Filesystem and Harness generation passed in
`target-b-wasm-operation-index-main-filesystem.log` and
`target-b-wasm-operation-index-main-harness.log`. Eight matching installed artifact
hashes are in `target/b-operation-index-main-artifact-hashes.json`. The fresh Filesystem
JS/WASM pair differs from main's checked-in pair; the JS difference is an internal
closure shim index and public declarations match. Preserve the qualified matching pair together.

Filesystem package/type tests and Harness package/type tests passed in
`target-b-types-operation-index-main.log`; all 248 Harness Bun tests/1347
expectations passed in `target-b-bun-operation-index-main.log`. Chromium passed
Filesystem smoke, multitab, publication, OPFS publication, and Harness wire/54
pure compaction checks in `target-b-browser-operation-index-main.log`. That run's
Stream page initially failed to load missing `stream/dist` modules. After
`target-b-types-operation-index-main-stream.log` rebuilt that required dependency,
the unchanged Stream page passed in `target-b-browser-operation-index-main-stream.log`.
The failed full-page run remains a failure receipt; the separate passing page
receipt closes the dependency failure without weakening any assertions.

Actual-main handoff `91e6935b7727f8a47b7c6ac58ca26ca67d0c974e` (#283 Inference
terminal policy/routes and lockfile/generated contracts) arrived during these
checks. It is not included in the merged-source receipts above and must be
accounted for in subsequent final-source qualification. These component/main
integration checks do not close compact cold hydration, resident retry/history
release, a compiled browser history/publication bridge, default browser canonical
continuation, fork policies, 10,000 default turns or full release/PR qualification.

## Authenticated projection restoration (format 3; qualification in progress)

Snapshot format 3 captures the reducer projection and retained event cache under
one domain-separated keyed BLAKE3 attestation using the existing aggregate issuer.
The public canonical state digest covers the authority, revision, event cache and
projection. The keyed proof additionally binds issuer identity and audience.
Only the reducer's checkpoint constructor emits this proof. Restoration verifies
both digest and proof before installing the projection, and never invokes
historical payload transitions. The original installed registry bindings are
pinned by complete canonical binding digests (schema, implementation, fork
policy, dependencies and configuration schema); added namespaces are allowed.
Format 2 is unsupported. No historical restoration adapter is retained.

Structured-key configuration and fork maps use ordered entry arrays in the
private serialized projection. UUID-keyed maps retain their normal representation.
The public TypeScript projection remains opaque and is admitted by compiled Rust.
Retry intents rebuild from the authenticated event cache, avoiding a second
serialized copy of every event. No issuer key is serialized.

Bounded property tests now restore through canonical JSON and compare the result
with both the original reducer and full event replay. Existing configuration and
fork tests round-trip their real projections through JSON before checking pinned
activation and duplicate-fork behavior. Adversarial tests reject a forged
projection/revision despite recomputing the public digest, mismatched issuer
key/identity/audience, missing original bindings, and changed schema,
implementation, fork policy, dependency graph or configuration schema. These
are bounded tests. Authentication assumes the existing host-managed issuer key
is secret and admitted reducer state is the only checkpoint producer.

Current source includes signed main integration 06247354a6ee21bdcfa7fa47886196fb7784cb07
of #283/91e693. `target-b-clippy-authenticated-snapshot-qualified.log` passes strict
native lint; `target-b-native-authenticated-snapshot.log` passes 279 library,
nine continuation and two recursive-fork tests. Earlier unused-import and
redundant-clone failures remain in their original logs; neither required a
weakened check. `target-b-types-authenticated-snapshot-final.log` passes Harness
package/type tests after adding the shared consumer declaration. Eight current
source/lockfile pins in `target/b-authenticated-snapshot-source-hashes.json`
remained unchanged through the terminal native run. Shared installed-WASM
checkpoint tests have been added for Bun and Chromium, but fresh generation,
WASM lint and those consumer executions are pending the coordinated Windows
build window. The old installed artifact does not qualify this changed source.

Simplification: the restoration loop that applied every historical event and
its public-digest-only trust boundary are removed. A private typed projection
preserves each current reducer field; explicit construction keeps new reducer
fields visible to compilation. The remaining cache-derived retry index avoids
redundant serialized events. Cache release is deliberately still unfinished:
event retention, conversation serialization/index reconstruction, effects,
forks, interactions and initial Stream opening can still grow with lifetime.
This change removes transition replay, but does not establish bounded cold
hydration. Compact resident state, compiled browser history/publication,
default browser continuation, fork policies, 10,000 default-turn performance,
full release qualification and owned PR/verified landing remain open.

The subsequent normal-command audit found MemoryConversation.#apply constructing
an entire snapshot solely for authority/revision. WasmReducer.head and the typed
Harness.head now read only that pair, and command construction uses it. The shared
checkpoint consumer compares the cheap head with fresh and post-admission state.
The previous package/type receipt predates this new generated method and does not
qualify it. No lightweight head is represented as proof of full bounded turns:
MemoryConversation still has lifetime conversation/event consumers to replace.

The runtime owner confirmed its browser provider is the existing Rust
BrowserStream (rather than a JS provider callback). New wasm/history.rs therefore
forwards directly to HistoryReader and StreamAggregate over BrowserStream and
StreamClient. The aggregate executes the existing atomic event/location commit,
exact retry and reconciliation paths; the reader performs the existing cursor,
byte/count, event-attestation and shared-commit checks. A typed browser-history.ts
facade forwards data, without a JS reducer, cursor admission or publication
engine. Browser Aggregate opening accepts an optional format-3 checkpoint; model
content, fork and merge adapters remain explicit in the owning runtime. This
bridge does not itself install default model execution.

The authored Chromium consumer exercises indexed publication, pinning before a
later append, bounded/terminal pages, reopen from checkpoint plus canonical suffix,
old-operation retry/reconciliation without another revision, changed retry key,
cold operation lookup, failed-page cursor preservation and wrong-issuer rejection.
Both new .mjs consumers pass node syntax checks. Runtime execution is still
UNQUALIFIED: central shared-host scheduling holds subsequent heavy builds, the
tracked old artifact lacks these new exports, and generated/package/browser gates
must run against the fresh matching pair. The wasm feature now activates the
existing Stream wasm adapter; the changed feature graph also requires its affected
native check. Prior 279/9/2 native evidence remains valid for the unchanged core
checkpoint implementation only. Initial BrowserStream provider recovery remains
subject to that provider's bounded journal recovery and capacity, not a new
constant-work recovery claim. No competing provider codec or runtime owner edit
was introduced; remote/custom-provider transport stays an explicit future adapter.

The browser bridge also forwards all six MemoryLimits ceilings when supplied,
so it can reopen the runtime owner's journals with matching persisted limits.
Default memory ceilings are used only when the caller omits that configuration.
The browser fixture supplies smaller explicit capacities and reuses them across
reader/aggregate reopen; this is authored coverage awaiting execution, not a
passing receipt. No configuration change is silently substituted on reopen.

The pre-qualification reader audit found a fresh aggregate inconsistency:
StreamAggregate opens a missing canonical path at revision zero, while archival
pinning previously returned NotFound. HistoryReader now treats only that provider
NotFound tail as zero; other provider errors propagate. A cursor asserting a
positive committed boundary still fails against an absent path. The native
real-provider fixture pins an empty terminal cursor without writes/record reads,
publishes the first event, verifies the old zero boundary remains terminal and
reads the first event through a new boundary. The browser consumer authors the
same empty-to-first-publication case, plus two concurrent aggregate handles,
stale CAS rejection and a one-event refresh before a fresh command. These new
cases await the queued native/WASM/browser qualification; the earlier 279 library
receipt does not cover this reader fix.

### Bounded browser bridge qualification after main integration

The central functional window qualified the pending bridge on signed base
80ecf341a5e376871e643c1d88d0c61de95e71b8, which integrates main #279/1d79a736.
One BelowNormal build worker used only this checkout's caches and separate
windows-B-functional reservation. The initial 15-minute compile budget was
explicitly extended to 12:38:05 UTC; all work completed before that cutoff.

Strict native Clippy passed in 1m57s. The focused native empty-history test
passed (one run, 279 filtered out); its affected provider/Harness build took
13m39s. No unchanged full native suite was repeated. Strict WASM Clippy passed
in 39.62s, and isolated Harness generation passed in 1m46s. All four installed
files matched the isolated producer hashes, including JS
187DCE92BB6140C79CA55C153B9DEFE33E84BD3D0156E8895A066DE1AA98792C and WASM
BEB88CB8FA61A7D4D2D958FD346E84458A1DCF105F4120DE18BED26B2F6FF1C8.
Runtime artifacts remain build-generated as required by the Harness ignore rules;
the changed generated declarations are committed.

Final Harness package/type checks passed. Bun passed 249 tests with 1,347
assertions. The actual Chromium wire page passed the shared checkpoint-forgery
and browser-history assertions, including the empty pinned cursor, canonical
suffix restore, indexed retries/reconciliation, competing stale publication and
bounded refresh. The original sandbox dependency-resolution failure is retained
separately from the successful dependency-access run. Chromium's first failure
was opaque because its catch read only error.message. Preserving String(error)
exposed an extra authority field: the new facade incorrectly declared the wire
Command while forwarding the existing owner-bound core Command body. The facade
now declares CanonicalCommand without that field, the browser fixture uses that
body, and compile-time consumers check execute/reconcile signatures. Corrected
type, Bun and Chromium checks all passed without changing or rebuilding Rust.

Receipts are target-b-functional-{native-clippy,empty-history,wasm-clippy,
generation,types-canonical-command,bun-canonical-command,chromium-qualified}.log.
Failed browser receipts retain the opaque and diagnostic failures. Local
manifests record source/lock hashes, the conservative 603-file Rust inventory,
matching artifact hashes, rustc 1.98.1/48a229cea and its executable hash,
wasm-bindgen 0.2.117 and its executable hash, observed background load and live
session/process identities. All native/WASM source inputs remained unchanged
through their terminal runs. Known B process roots and children drained, the
finite browser runner closed and awaited Chrome without cleanup warnings, and
only B's verified separate reservation was removed.

This is focused functional evidence. Compact resident/cold state, runtime-owner
adoption and default browser continuation, complete fork policies, default-turn
10,000-operation evidence, full production/release gates, the owned PR and
verified landing remain required.

The final B/I join must also qualify the owning browser model-provider adapter.
At I's qualified d53723a5782470abd595241fb3ee22f054d4ff2f, HostModel forwards
generate/reconcile only. B's ModelProvider adds context_capacity for the exact
selected Model and count_tokens for the exact PreparedModelRequest; their trait
defaults return Unsupported. Consequently the joined source can compile while
default StockExecutor compaction fails before model dispatch. The owning adapter
must forward both actual provider hooks and prove the enabled default through
public browser and installed consumers on the final joined source. Disabled
compaction and fabricated byte-to-token counts do not satisfy this requirement.
I acknowledged this seam while preserving its already-qualified source; the bare
history bridge itself does not require a different dispatch/reconcile contract.

### Checkout recovery and subsequent retention work

The original 1656/sdk checkout was later found absent and unregistered in Git.
Its signed branch at 4007c54 survived and was recovered at the original path.
No verified archive or backup of ignored qualification receipts, producer
artifacts or build caches has been identified. The earlier qualification notes
are historical reports; they are not a substitute for inspectable retained
receipts. Final-source artifacts and gates must be regenerated and requalified
unless an exact source/hash-bound archive is recovered.

Subsequent unqualified source work removes the duplicate Event stored for each
retry identity: the operation index now points into the sole retained event
vector, including after snapshot restoration. The existing indexed lookup for
one admitted context selection is exposed to the WASM consumer. Local normal
turns, retries and indeterminate-turn checks use it instead of creating a complete
authenticated snapshot and scanning its events. The turn consumer fixture
allows only its explicit model audit to read a snapshot, so the prior hidden
history clones are detectable. Formatting and whitespace checks pass; native,
fresh generated declarations, Bun and actual Chromium gates are pending a
coordinated build grant. Conversation hydration, retained-event/index growth and
compact cold restoration remain unfinished.
