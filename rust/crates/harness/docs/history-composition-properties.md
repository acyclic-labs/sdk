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
| Resident event eviction preserves retry identity. | `StreamAggregate` keeps a configurable event suffix and resolves evicted operations through the atomic locator. Its indexed planner follows that authenticated lookup. | The canonical loop retires closed checkpoint-covered conversation prefixes. Terminal effects default to a 64-entry cache; active and uncertain effects remain resident. This does not bound independently growing active state. |
| Retiring a closed conversation prefix preserves its logical history. | The bound conversation keeps the checkpoint cut, exact checkpoint reference and chained prefix digest, then only the resident suffix. Logical revision, append sequencing and indexed retries use original canonical identities. Snapshot format 6 authenticates the prefix representation. | An unresolved user or incomplete tool exchange prevents retirement. Earlier pages and hashes require explicit archival reads. Full and Summary grant capture after retirement require the original issuer's exact-seed archive witness. |
| A retired turn replays its original selection. | `HistoryReader::selected_conversation` verifies each selected ID through the original atomic locator and canonical event, charging all encoded records to one allowance. Callers bind the selection to its original operation and reuse indexed assistant/tool outputs. | The selected view grants no admission authority. Default cold restoration uses an authenticated durable checkpoint and bounded canonical suffix; active state must still fit the declared projection input allowance. |
| Default cold restoration has finite input work. | Every 64th canonical event atomically publishes a checkpoint pointer with the original event and indexes. The snapshot retains its canonical head and original issuer MAC. Opening charges pointer, chunks, canonical anchor and suffix to one byte allowance, and the anchor and suffix to one event allowance. Caller-supplied snapshots charge their bytes and cached events before provider IO. | Defaults allow 64 decoded events and 32 MiB total encoded input. Missing checkpoints for established histories fail explicitly; no lifetime replay fallback. Larger caller inputs require explicit limits. This bounds restoration input, not whole reducer residency. |
| Checkpoints reuse unchanged stored payloads. | Bounded content-defined chunks use immutable content-addressed Stream paths. Each chunk digest and length, the complete payload digest, original snapshot MAC and atomic canonical binding are verified on restore. | Chunks are at most one Stream record and interior chunks at least 16 KiB. The manifest fits one record. Payloads above the configured implementation ceiling fail explicitly. Snapshot construction still serializes the current projection; growing active or terminal state is not constant work. |
| Retired successful/failed effects keep their original identity and result. | The finite terminal cache retires effects with original atomic operation/effect indexes. Snapshot format 6 authenticates retirement eligibility and archived-state flags. `StreamAggregate::effect`, the owned effect host and native receipt consumers use the same bounded effect archive. Cold replay charges retirement-proof records to its cumulative input allowance. | Planned, dispatched and indeterminate effects remain resident. Missing original proof cannot turn a known effect into absence. The archive observation does not replace provider, attempt, task-owner or lease-fence validation. |
| A snapshot cannot manufacture state. | The original issuer authenticates the snapshot's complete projection, cut and digest. Restore checks the original authority and schema registry before applying a canonical suffix. | Trust rests on the existing issuer key and provider integrity. Snapshot size remains proportional to its projection. |
| A selected context has explicit bounds. | Selection, rendering, whole-tool-batch checks and each stage validate the admitted limits. Sources and stage inputs use separate allowances. | Oversized mandatory content fails explicitly. Selection must not retain a tool result without its call or split a completed tool batch. |
| Immutable context imports preserve metadata and authority. | `PinnedContextStage::capture` validates the exact canonical context file, its descriptor, bounds and all referenced content through the supplied reader. Its contract binds the file reference. New admissions verify access again. | The reader must already carry the receiving authority. Construction grants no access and performs no model operation. |
| An admitted summary remains tied to its source. | Summary and response purposes share the existing execution journal while retaining distinct attempt identities. Summary source and prepared request are pinned before dispatch; retry reconciles the original attempt. | The model provider owns reconciliation and immutable staging. An uncertain attempt is not regenerated as a fresh request. |
| Mandatory native data retains its complete exchange. | Compaction uses the shared typed `ModelContent::contains_native_media` traversal, including native data inside tool results, then closes the ordered call/result graph. | Explicit native byte/work/intent limits remain separate from token accounting. A consumer may explicitly replace the retention policy. |
| Default compaction uses actual model accounting. | `ThresholdCompaction` defaults to a 16,384-token response reserve and 20,000 recent tokens. The selected provider supplies capacity and request-bound additive token upper bounds. Consumers may replace the policy or disable it. | Canonical continuation also compacts before exhausting its declared message count, reserving two positions for the response and next user. The count allowance caps recent-token retention; mandatory roles/media and complete exchanges remain verbatim. Fixed custom contexts use their full declared allowance. Missing accounting, impossible capacities and mandatory-budget overflow fail explicitly; no model-name catalog or SDK tokenizer is used. |
| Compaction cannot substitute a retained projection. | The canonical checkpoint envelope binds original source, retained context, compaction proof, operation and covered logical history. Publication verifies the admitted settled summary before exposing the checkpoint. | Full canonical history is retained. Checkpoint imports verify original-owner publication, scope grants, event cut and bounded tail before model admission. |
| Checkpoint continuation does not rerun settled work. | Native canonical conversation execution reuses the committed retained projection and admitted response artifacts. Subsequent stages receive a fresh delta; a covered historical current-input marker is cleared. | The continuation must bind the exact original operation and checkpoint. Reconciliation and lost-ack recovery remain in existing owner journals. |
| Full, fresh and Summary forks select explicit history semantics. | `ForkHistoryPolicy` pins the logical head independently of residency, starts fresh, or binds a verified checkpoint projection. `HistoryReader::fork_history` produces a finite issuer-bound selection; the Filesystem preparer reuses the original grant/materialization checks and seals the complete seed. `summary_fork_stage` verifies parent publication, causal child binding and receiving scope. | Archive capture is an explicit bounded operation, not work repeated by normal turns. The witness binds every resource, reader, grant, manifest and inherited file; omission or rebinding rejects. Cold Summary import uses two event lookups/four bounded records plus separately bounded content. |

## Original-cut effect archive

Every canonical publication appends a mandatory certificate to one fixed cut
stream under the original aggregate-tail condition, with the canonical event
and operation/effect indexes in the same atomic commit. The existing issuer
MAC binds the complete effect-index root, original authority, canonical path,
revision and raw canonical digest. The private proof purpose is
`harness/v1/effect-history-root`; paths derive the current authority domain.
The certificate supplies observations, never scope or effect admission.

The immutable compressed binary radix index selects the exact last dense
effect ordinal at the requested canonical cut, including genuine absence.
Only the update path changes; non-effect publications retain the preceding
root. Content hashes bind nodes, and a read requires exactly one original
record at each node path. Missing nodes, changed bytes, invalid branch order,
wrong authority/cut, non-atomic certificates and missing original transitions
reject. A missing mandatory certificate requires explicit handling. No later
index tail or speculative future event determines a historical result.

| Mechanism considered | Cost and correctness at an arbitrary old cut |
| --- | --- |
| Original dense effect index alone | Authenticates existing transitions but cannot prove the terminal transition was not omitted. The actual admitted negative control returned older Dispatched state; that candidate was removed. |
| A current per-effect locator | Does not identify its value at arbitrary older cuts or unrelated-event gaps. Updating all retained locators at every canonical cut costs work proportional to retained effects. |
| A complete copied map per cut | Provides completeness but serializes and reads the lifetime map, exceeding finite default publication/read work. |
| Authenticated compressed radix root | Preserves complete original-cut membership and absence with at most 128 ordered branch bits plus one leaf. Updates and reads follow only that bounded path; original typed effect transitions still use the shared core reducer. |

The caller's single byte allowance includes the certificate, raw canonical
anchor, all traversed nodes and every original index/canonical transition
record, charged before decoding. Only selected original effect events consume
the decoded-event allowance. For three transitions, pinning and reading a
maximum-depth path require 137 record reads and 131 tail reads. Exact proof
bytes succeed and one byte less rejects, including after a later append.
Publication's private proof input also has a finite allowance; its path is
bounded by the same 128-bit key space.

The actual Stream observer measures read, tail, write, record, idempotency,
commit-read and other provider calls; incoming record payloads, attempted write
payloads and received commit payloads; and unique committed certificate/node
records. At 1, 100 and 1,000 terminal effects with three fixed active effects,
the original effect takes 11 record reads and five tail reads. The three new
effect commands at the 1,000 sample take 39 reads, 31 tails and 27 writes,
reading 15,977 payload bytes and sending/receiving 13,809 payload bytes.
An admitted 129-key adversarial archive exercises all 128 branches: its original
three-transition read takes 42,880 payload bytes and the stated maximum reads.
Every sampled and adversarial three-command publication stays within 512 calls per observed method category
or records and 1 MiB of payload in each measured direction.

Retention remains proportional to history. The 1,000-terminal sample retains
24,284 immutable nodes (6,117,494 payload bytes) and 13,010 certificates
(6,436,108 payload bytes); the latter include 10,000 unrelated canonical events.
These are complete Stream-provider measurements for this fixture, excluding
wire control-envelope overhead, Filesystem backend IO and whole-process RSS.
The default now uses the finite terminal cache and bounded archive consumers.
These historical effect-IO measurements do not qualify the changed source,
Filesystem byte retention, process RSS or 10,000 model turns.

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

The native Summary fixture retires the complete resident suffix while retaining
logical head four, then prepares, publishes and imports the original Summary.
The Full fixture uses a compacted twelve-message parent, rejects missing,
forged or rebound archive witnesses, cold reopens publication and pages the
original inherited records. Witnesses are verified by the original issuer,
not just a public digest or a caller-created selection.

The continuation fixtures now both contain 10,000 small-input turns with an
eight-message bound and the original 262,144-byte payload limit. The memory
composition retains its original Objects capacity. The local fixture uses
`Fs::local` and `LocalStream` with their existing default options, drops all
owning journal/Filesystem handles, reconstructs the binding near the final
turns and replays the first original identity without another model request.
Both use the same canonical `FilesystemExecutionJournal::run_conversation`;
the former memory-only publication loop and helpers have been removed.
Adding these fixtures is not a passing receipt: integrated exact-source
qualification remains required.

The real Filesystem effect fixture keeps three planned/dispatched/indeterminate
effects active while terminal history grows through 1, 100 and 1,000 effects.
An explicit two-terminal cache survives cold reopen and preserves original
archived results, retries, attempt identities and lifetime ID fences. Five
publication-fault controls cover pre-commit failure, visible/hidden lost
acknowledgements and malformed receipts. A nine-event control exercises the
pure-planner guard before event eviction and admits cold retirement at the
exact combined snapshot, canonical and derived-proof byte allowance; one byte
less fails, and an insufficient event allowance fails before record IO.
These controls exercise retirement and publication uncertainty; final-source
default-consumer qualification must run after integration.

Fresh dispatch and resolution commands for retired terminal effects read the
original effect projection at the current committed cut under the configured
history allowance. The original planner borrows that one verified projection
for provider authorization and transition validation. It does not clone the
reducer, mutate the cache or duplicate terminal transition rules. The real
Filesystem fixture rejects fresh dispatch/resolution as conflicts, rejects a
missing provider grant, and rejects an allowance below the original three
transitions without changing the aggregate revision or resident cache.

Simplification audit for this boundary removed whole-reducer planning clones
and three repeated effect lookups. One private lookup serves authorization and
transition validation; canonical replay supplies no archival override. The
private admission entry checks the exact command target and settled status
before borrowing authenticated state. The store owns bounded IO and the core
retains scope, revision, attestation, causal and transition checks.

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

Normal fixed-active conversation work uses the resident suffix, incremental
identity indexes, unchanged checkpoint chunks and bounded effect-index paths.
Explicit archival fork capture may traverse its finite selected prefix.
Growing active graphs or provider-retained immutable history still consume
their declared capacities; no unrestricted memory or throughput claim follows.

Canonical journal payloads reuse one `.system/execution/payload.json` slot.
Canonical assistant content, metadata and attachment manifests reuse one
host-reserved, normally readable `turns/.canonical/payload` slot. Internal
upload receipts and metadata are keyed by the stable payload path, not by
the lifetime operation count. Original operation keys still resolve the
original immutable generation and exact receipt; previously returned
references are never repinned or rewritten. Arbitrary public upload paths
and public upload receipt identities are unchanged. The durable 10,000-turn
fixture checks the actual current payload, metadata and receipt directories
at turn 114 and after all turns, then retries its first original reference.

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

## Exact-source verification and unresolved diagnostics

Run the integrated source, not an earlier candidate:

```sh
cargo test --locked -p acyclic-harness --all-features --all-targets
cargo test --locked -p acyclic-fs --all-features --all-targets
```

The Harness gates include admitted summary uncertainty, original retry,
authenticated snapshot/checkpoint restoration, effect tampering/absence,
Full/Summary archive witnesses, both 10,000-turn continuations, existing
production transition proptests and seeded radix update/read-fault schedules.
The Filesystem gates include shared extent span tampering, original
publication-proof custody, stale/missing-object rejection and concurrent GC.
Mutation controls must reject changed cuts/issuers/roots, omitted terminal
transitions or fork grants, invalid shared lengths and one-byte-short reads.

The preserved original Windows failure is
`b-effect-root-windows-native-r189/stderr.log`, source
`7a9aef9997bf61d03e9b9a41996828f785891f4d`, tree
`592615da0d51de3802c67b871236aae8913f8a11`, MSVC rustc 1.98.1,
`cargo test --no-fail-fast --locked --offline -p acyclic-harness
--all-features --all-targets -- --test`: `0xc0000374
STATUS_HEAP_CORRUPTION`. The later original-binary runs r194, r201 and r212
did not reproduce it. No corruption dump or causal stack was captured.
Their successful exits are not cause resolution or a release waiver.

Source inspection covers owned Rust future/candidate/proof lifetimes and
the Windows native directory-query/overlapped-I/O boundaries. The new closure
memo accepts only a complete proof at the same declared span; publication
custody is private, exact-generation and single-use, invalidated by collection.
These changes remove redundant proof work, not an identified heap overwrite.
The original corruption cause remains unresolved.

The independent retained-turn failure is recorded in producer
`b-default-continuation-r217/0.stdout`: turn 114 exhausted Objects capacity.
The memory composition's byte capacity is 64 MiB, with no cardinality ceiling.
An eventual passing continuation must preserve this capacity, history,
durability and full workload; reducing proof reads alone is not evidence
that retained provider bytes were repaired.
The fixed-slot internal publication cutover addresses the growing active
internal namespace rather than suppressing the capacity error or increasing
the allowance. The original memory control still stages its explicit
user-chosen input paths; its unchanged full workload must also qualify.
No unrun control establishes that all pinned historical bytes fit 64 MiB.

The full composition goal remains open until all required owned work is qualified
and merged into `main`; a first focused landing is not full goal completion.
