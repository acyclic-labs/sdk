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
| Resident event eviction preserves retry identity. | `StreamAggregate` keeps a configurable event suffix and resolves evicted operations through the atomic locator. Its indexed planner follows that authenticated lookup. | The native canonical loop also retires closed checkpoint-covered conversation prefixes. Terminal reducer maps and cold opening still retain or replay lifetime state; this is not a constant-memory aggregate. |
| Retiring a closed conversation prefix preserves its logical history. | The bound conversation keeps the checkpoint cut, exact checkpoint reference and chained prefix digest, then only the resident suffix. Logical revision, append sequencing and indexed retries use the original canonical identities. Snapshot format 6 authenticates this prefix representation with the complete projection. | An unresolved user or incomplete tool exchange prevents retirement. Older pages and hashes require explicit archival reads. Archived full-fork grant materialization and arbitrary archived replies/outcomes remain unfinished. |
| A retired turn replays its original selection. | `HistoryReader::selected_conversation` verifies each selected ID through the original atomic locator and canonical event, charging all encoded records to one allowance. Native callers bind the selection to its original operation and reuse indexed assistant/tool outputs. | The selected view is read-only and grants no admission authority. Default cold aggregate construction is still unbounded. |
| A snapshot cannot manufacture state. | The original issuer authenticates the snapshot's complete projection, cut and digest. Restore checks the original authority and schema registry before applying a canonical suffix. | Trust rests on the existing issuer key and provider integrity. Snapshot size remains proportional to its projection. |
| A selected context has explicit bounds. | Selection, rendering, whole-tool-batch checks and each stage validate the admitted limits. Sources and stage inputs use separate allowances. | Oversized mandatory content fails explicitly. Selection must not retain a tool result without its call or split a completed tool batch. |
| Immutable context imports preserve metadata and authority. | `PinnedContextStage::capture` validates the exact canonical context file, its descriptor, bounds and all referenced content through the supplied reader. Its contract binds the file reference. New admissions verify access again. | The reader must already carry the receiving authority. Construction grants no access and performs no model operation. |
| An admitted summary remains tied to its source. | Summary and response purposes share the existing execution journal while retaining distinct attempt identities. Summary source and prepared request are pinned before dispatch; retry reconciles the original attempt. | The model provider owns reconciliation and immutable staging. An uncertain attempt is not regenerated as a fresh request. |
| Mandatory native data retains its complete exchange. | Compaction uses the shared typed `ModelContent::contains_native_media` traversal, including native data inside tool results, then closes the ordered call/result graph. | Explicit native byte/work/intent limits remain separate from token accounting. A consumer may explicitly replace the retention policy. |
| Default compaction uses actual model accounting. | `ThresholdCompaction` defaults to a 16,384-token response reserve and 20,000 recent tokens. The selected provider supplies capacity and request-bound additive token upper bounds. Consumers may replace the policy or disable it. | Canonical continuation also compacts before exhausting its declared message count, reserving two positions for the response and next user. The count allowance caps recent-token retention; mandatory roles/media and complete exchanges remain verbatim. Fixed custom contexts use their full declared allowance. Missing accounting, impossible capacities and mandatory-budget overflow fail explicitly; no model-name catalog or SDK tokenizer is used. |
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

The real native Summary fixture directly observes logical revision four,
archived cut three and one resident message after admitted compaction. A second
fixture runs twenty small-input turns under an eight-message bound, checks the
resident conversation after every turn and replays the first retired turn
without another model request. The count planner preserves pinned instructions,
selects a complete tool-exchange boundary and rejects an impossible mandatory
allowance. A fixed two-message custom pipeline consumes its entire allowance
and replays without an unnecessary summary. These cases qualify those paths;
they do not establish default cold restoration or retained-10,000-turn scaling.

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

The broader slice still requires bounded terminal reducer metadata, archived
full-fork/reply/outcome hydration and bounded default cold construction. Native
checkpoint-covered warm conversation retirement does not establish whole
aggregate residency or those cold paths. No unrestricted memory, throughput or
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
reuse the released model/tool producer. Summary, checkpoint and response reads
share its effective-scope and original-option validation before body IO; the
replaced partial validators are removed. Retained tool projections return the
already validated typed envelope rather than decoding it a second time. The
native retention fixture covers both call-message and tool-result media across
one to three calls, validates the actual prepared request before and after
compaction, and rejects a budget that would split the exchange.

The full composition goal remains open until all required owned work is qualified
and merged into `main`; a first focused landing is not full goal completion.
