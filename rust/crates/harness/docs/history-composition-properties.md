# History, checkpoints and compaction

The canonical Stream events remain the source of conversation history. Context
selection, projection and compaction change model input without deleting that
history. The existing reducer, execution journal, Filesystem publication and
provider reconciliation own admission and effects.

## Invariants and mechanisms

| Invariant | Mechanism | Assumptions and limits |
| --- | --- | --- |
| A history traversal excludes later appends. | `HistoryReader::pin` captures an authority and event cut. `read_page` advances only within that cut, charging event and encoded-byte allowances. | The provider retains committed immutable events. A page allowance bounds one read, not total archival traversal. |
| A cold lookup authenticates its result. | Operation, message-ID and message-sequence locators are published atomically with the canonical event and aggregate-tail condition. Reads verify the locator, event, authority, attestation and common atomic commit. | An unattested locator alone never authorizes an operation or message. Existing stores without these indexes require explicit handling; no migration fallback scans lifetime history. |
| A cold logical-message head has constant read work. | Binding and every ordinary or merge-notice append publish a head locator in the existing atomic event commit. `latest_conversation_message` pins its tail and verifies the locator and original event through the shared atomic-proof checker. | Two protocol-bounded record reads, with a combined explicit byte allowance, independent of retained history. Concurrent later appends are excluded. Missing head state for a nonempty aggregate fails rather than reporting empty history. This observation is separate from an older archival cursor. |
| Resident event eviction preserves retry identity. | `StreamAggregate` keeps a configurable event suffix and resolves evicted operations through the atomic locator. Its indexed planner follows that authenticated lookup. | Conversation messages and other reducer metadata are still lifetime-sized. This is a bounded event cache, not a constant-memory aggregate. |
| A snapshot cannot manufacture state. | The original issuer authenticates the snapshot's complete projection, cut and digest. Restore checks the original authority and schema registry before applying a canonical suffix. | Trust rests on the existing issuer key and provider integrity. Snapshot size remains proportional to its projection. |
| A selected context has explicit bounds. | Selection, rendering, whole-tool-batch checks and each stage validate the admitted limits. Sources and stage inputs use separate allowances. | Oversized mandatory content fails explicitly. Selection must not retain a tool result without its call or split a completed tool batch. |
| Immutable context imports preserve metadata and authority. | `PinnedContextStage::capture` validates the exact canonical context file, its descriptor, bounds and all referenced content through the supplied reader. Its contract binds the file reference. New admissions verify access again. | The reader must already carry the receiving authority. Construction grants no access and performs no model operation. |
| An admitted summary remains tied to its source. | Summary and response purposes share the existing execution journal while retaining distinct attempt identities. Summary source and prepared request are pinned before dispatch; retry reconciles the original attempt. | The model provider owns reconciliation and immutable staging. An uncertain attempt is not regenerated as a fresh request. |
| Default compaction uses actual model accounting. | `ThresholdCompaction` defaults to a 16,384-token response reserve and 20,000 recent tokens. The selected provider supplies capacity and request-bound additive token upper bounds. Consumers may replace the policy or disable it. | There is no model-name capacity catalog or SDK tokenizer. Missing accounting, impossible capacities and mandatory-budget overflow fail explicitly. |
| Compaction cannot substitute a retained projection. | The canonical checkpoint envelope binds original source, retained context, compaction proof, operation and covered logical history. Publication verifies the admitted settled summary before exposing the checkpoint. | Full canonical history is retained. Checkpoint imports verify original-owner publication, scope grants, event cut and bounded tail before model admission. |
| Checkpoint continuation does not rerun settled work. | Native canonical conversation execution reuses the committed retained projection and admitted response artifacts. Subsequent stages receive a fresh delta; a covered historical current-input marker is cleared. | The continuation must bind the exact original operation and checkpoint. Reconciliation and lost-ack recovery remain in existing owner journals. |
| Full, fresh and Summary forks select explicit history semantics. | `ForkHistoryPolicy` pins the original logical history cut, starts fresh, or binds a verified checkpoint projection. The Filesystem preparer copies private execution payloads into child-owned immutable files and binds the complete capture to its seed. `HistoryReader::summary_fork_stage` checks parent publication, the original causal child binding, signed receiving scope and exact reads before using the ordinary pinned stage. | Cold import uses two authenticated event lookups (four bounded records) within one explicit history byte allowance, without restoring either aggregate. Content has separate admitted limits. Full-prefix grant capture still scans parent history; explicit stage capture does not implement automatic receiving-task setup or browser qualification. |

## Verification surfaces

The native library tests exercise authenticated snapshot restoration, malformed
history and locator rejection, atomic publication and lost acknowledgements,
bounded pages, eviction, cold indexed lookup, constant two-record head reads at
1, 1,000 and 10,000 retained messages, a real atomic append racing a pinned head
read, forged separate-commit head locators, whole tool exchanges, source and
projection substitution, summary uncertainty and accounting bounds.

`tests/context_continuation.rs` exercises the real Filesystem execution journal:
default compaction across later canonical turns, retained replay, checkpoint and
tail verification, source and summary fabrication rejection, original-scope read
admission, pinned whole-context metadata, and read-only Summary preparation.
The other affected integrations cover journal replay and recursive full/fresh
fork behavior.

Browser consumers exercise the Rust/WASM history reader, aggregate snapshot,
compaction accounting, fork-policy wire and generated declarations. Native
success does not establish browser qualification. Generated declarations must
come from the current Rust producer, and required hosted SDK Qualification must
succeed on the exact PR head before merge.

Raw local build and test receipts record the source commit, tree, lock digest,
toolchain, command and terminal exit status under the owner's evidence directory.
Historical runs from earlier branches do not qualify this source. A running
compiler, successful source review or unchanged fixture is not a passing test.

## Remaining composition work

The TypeScript local stock loop does not yet invoke provider accounting or
automatic compaction. Its passing dispatch, wire and accounting-contract tests
do not establish that default. Browser default compaction must use admitted
summary operations and a portable owner journal, with reload and uncertainty
verification; a facade-only truncation or unjournaled summarizer is insufficient.

The broader slice still requires bounded lifetime conversation/reducer metadata
and bounded default cold hydration. The event cache and independent archival
reader do not establish either property. No unrestricted memory, throughput or
10,000-agent claim follows from bounded regression fixtures.

Summary-fork capture and explicit child-stage admission are exercised by
`filesystem::memory::summary_tests`: an actual admitted compaction is copied,
published, imported into its bound child and consumed by a replayable model
operation. Denied grants, changed seeds, narrow file bounds and independently
bound children reject. A read-only facade over the actual MemoryStream provider
checks cold import before and after 1,000 later parent messages: four reads of
at most one record each, exact combined-byte admission, a failed third read, and
a fresh reader succeeding after that failure. This bounds index proof reads,
not whole reducer residency or full-prefix fork capture. Automatic receiving-task
setup and restart/browser evidence remain separate work. `PreparedSummaryFork` authorizes no workspace allocation, model
dispatch or child activation. Native typed tool-result and file-policy contracts
must be supplied by their owning producer before their consumers can qualify.

The full composition goal remains open until all required owned work is qualified
and merged into `main`; a first focused landing is not full goal completion.
