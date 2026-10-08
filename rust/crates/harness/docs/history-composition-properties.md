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
