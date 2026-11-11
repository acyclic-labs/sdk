# Unbounded atomic Stream commits

Status: content-page foundation implemented; atomic publication and its proofs remain outstanding.
Baseline: 506843a3822. Lane: stream-unbounded. Objective includes SDK main landing
through Foreman and a separate Cloud adoption PR; neither is complete.

## Required public atomicity contract (user requirement, 2026-11-11)

This section is authoritative over earlier planning. A large commit is ONE
atomic unit under failure, concurrency and recovery. Pages are physical work
units only; they are never independently applied Stream commits.

1. **Invisibility.** No read, follow, fork, children_page, heads query or
   conditional write can observe a strict subset of the staged effects as
   committed. Before publication all effects are absent; after publication all
   effects belong to one immutable published root. This holds across a crash or
   restart at every byte of staging and apply. An individual read page remains
   a selection from that complete root, never a partially installed namespace.
2. **One linearisation point.** Conditions are checked and the successful
   commit's sequence, revision and head are assigned exactly once by final
   apply. Staging assigns no sequence, revision, head, fork cut, condition
   outcome or Stream position, and creates no Stream conflict. The one durable
   publication decision is the linearisation point for all these outcomes.
3. **Idempotency and stage ownership.** A stage is permanently bound to one
   commit id and retry identity before accepting its first page. No operation
   can rebind it or use it for a second commit. Concurrent or recovered final
   applies of that identity yield the same retained receipt; they cannot apply
   twice. A conflicting logical digest is refused without touching visibility.
   Immutable content pages may be shared; authority to consume a stage may not.
4. **Forks and exclusions.** A staged commit preserves existing fork source-cut,
   pre-commit-state, exclusion segment-prefix, inheritance and retry semantics.
   Upload boundaries change none of these. Forks never capture a staged subset.
5. **Ordering.** Records preserve caller order, including across leaf, effect,
   manifest and transport-page boundaries. Canonical path ordering cannot sort
   the records within an append. Retry and recovery preserve that same order.
6. **Followers.** A follower observes one atomic batch event per commit. The
   event may be delivered in pages, but a concurrent commit cannot interleave
   between its pages. Every page binds the same commit id and published root;
   the batch cursor finishes that event before advancing to the next commit.
   Disconnect/reconnect resumes the retained batch and page cursor, never treats
   a received prefix as a separate commit. Filtered batches retain exactly the
   matching selection from the one complete publication.

Receiving a page is not a separate visibility decision. A consumer receives a
handle to a complete immutable published batch and pages its contents; an
incomplete transfer cannot certify a truncated batch as committed. The shared
core supplies this boundary even when the underlying backend only supports
atomic replacement of one small value.

### Final-apply protocol required by that contract

Staging is only immutable byte/content preparation. Begin permanently binds
stage id, commit id, account and retry key; Seal binds the canonical logical
request digest without observing mutable Stream state. There are no staged
conditions, destination reservations, sequence allocations or fork positions.

FinalApply obtains an event-driven exclusive authority decision capability,
shared with EVERY write family, pins the current published root, and settles
conditions/fork cuts exactly once for this logical decision. Its incremental
bounded work builds an unpublished successor and receipt pages. Other writers
wait on capability transfer; readers retain the old root without waiting.
The capability is a logical writer ownership token, never an OS lock held
through all page IO. Admission uses backpressure, without a queue-depth cap.

The final small durable publication atomically installs the successor root,
sequence/revision/head assignments, complete retry receipt and consumed-stage
binding. No other root can publish between condition settlement and this one
publication decision. Abort/failure before that decision leaves the old root
whole. Recovery either resumes the same unpublished decision at its pinned cut
or installs the one durable successor; it cannot create another logical
condition evaluation or assignment. Replaying pure calculations against that
same immutable cut is recovery of the same decision, not a second condition
outcome or a second linearisation point. Intermediate durable planning pages
have no authority to publish themselves.

A backend without a multi-key atomic transaction stores immutable pages first,
then durably replaces only the small root/decision reference. Its recovery
codec rejects truncated or unchecked root bytes and retains the previous whole
root. The shared core publishes the namespace and retry observation together;
a backend-local visibility path cannot bypass it. Durability acknowledgment
covers referenced pages before it covers the new root. Follow batch descriptors
are derived only from that root decision, never from individual page uploads.

### Required proofs and evidence (all outstanding)

* Deterministic simulation executes the SAME shared staged state transitions
  and recovery codec as production. Crash and reopen at EVERY chunk boundary
  of staging, final-apply preparation and publication; inject torn writes at
  every byte offset in durable page/root frames. For every cut, probe reads,
  follows, heads, forks, children_page and conditions for either the complete
  predecessor or complete successor. Check identity, order and one receipt.
* The simulation has a mutation negative control that exposes a staged page or
  publishes an intermediate root. It MUST fail the corresponding invisibility
  invariant. A positive-only run does not prove the harness detects this bug.
* A loom model drives concurrent final applies through the shared core's actual
  decision state machine, including identical keys, two keys against one
  condition, consumed-stage reuse, followers and authority-owner cancellation.
  Exhaust schedules of stage seal, final-apply ownership, publication, receipt
  retention and event delivery. Assert one root transition/receipt per key,
  indivisible publication, stable ordering and non-interleaved batch cursors.
  A model of a separate toy implementation does not satisfy this obligation.
* State-machine invariants: (I1) only a published root is reachable by queries;
  (I2) one stage has one permanent commit binding; (I3) root and terminal retry
  observation advance together; (I4) one decision capability owns final apply;
  (I5) a pinned batch cursor references one root until complete; (I6) content
  and effect iterators preserve logical ordinal across every page boundary.
  Each production transition must preserve these, including recovery and GC.
* The shared conformance suite proves the six contract clauses on memory,
  local/native persistence, PostgreSQL, gRPC, HTTP, wasm/browser and Cloud.
  Include 100k effects, 64 MiB records, forks/exclusions across pages, conflicting
  final applies, retry after acknowledgment loss, page-order followers and
  event-driven abandoned-stage collection with pinned readers still alive.

Current evidence: the content-page foundation exists, with validation running;
none of these publication proofs or backend conformance obligations is complete.
The goal cannot be marked READY or complete based on the foundation tests.

## Contract

A legitimate write has no configured count, record-byte or command-byte ceiling.
Atomicity includes hard conditions, soft-effect decisions, fork source cuts,
records, child indexes, retry identity, immutable commit facts and follow events.
Every observation uses a single published root: no staged material is visible.
Frame/page sizes bound individual steps, never the logical request. Allocation
refusal is caller-owned capacity/backpressure, not a hidden MAX_ITEMS fallback.
Existing owned Vec/OwnedBytes APIs necessarily retain caller-owned input/output;
the bounded-space guarantee applies to additional working storage per step.
A streaming API also permits inputs and outputs larger than available RAM.

## API and shared mechanism

Introduce a shared staged module, used by small and large writes alike. There
must be no second direct-write implementation. The backend interface supplies
immutable page put/get, a pinned root read, one conditional root publication,
and an ordered root/owner lifecycle event feed. Auth and original-work admission
remain at the primitive boundary, before staging and publication respectively.

Logical operations are Begin(retry key, family), Put(stage, page), Seal(stage,
manifest digest), Publish(stage, expected root), and Abort(stage). They are
internal provider/transport operations; public append/fork/commit transparently
use them. A stage is scoped to account, authorization binding and owner session.
A sealed manifest binds the canonical request digest, ordered effect pages and
content roots. A page digest binds a versioned domain, type and bytes. Page
uploads are idempotent; malformed, incomplete or cross-account references fail
before publication. Retry identity binds logical content, independent of page
boundaries, upload order and interruption. No secret is persisted in a stage.

Effect/condition/path/header bytes and record bodies are incremental codecs over
bounded pages. An individual effect, path or header may itself span pages; a
page must not imply a new logical maximum. Canonicalization uses paged sorted
runs and merge, detecting duplicate destinations across all pages. Offsets and
counts use checked wide integers, not the existing u32 effect-index ceiling.

Record content is a content-addressed chunk tree, including a versioned length
and root digest. Immutable record facts hold its reference and headers, never a
large inline value. Shared readers resolve chunks lazily; existing owned read
adapters assemble only at the caller boundary and streaming reads yield chunks.
Use the existing Objects content primitives where dependency direction permits;
otherwise extract their shared content core rather than implement another CAS.

Final-apply preparation and publication follow the exclusive authority decision
protocol above. Staging performs no condition settlement or source-cut capture.
The authority root and terminal retry observation publish together at the one
linearisation point; no root-CAS replan may create another logical decision.
Receipts, conflict lists, skipped effects, read_commit and inspection become
paged streams rooted in the retained terminal decision. Owned convenience APIs
collect these pages outside transport and publication locks. This is necessary:
a 100k-effect receipt cannot fit the current one-frame response limit either.
Follow delivery derives from the published decision and retains existing gapless
registration semantics. No per-record event is sent before root publication.

## Backend mapping

* Memory: immutable shared pages and a short root swap under the existing
  authority synchronization; read snapshots retain roots, not the mutable tree.
* Local/native: immutable content pages durable before a versioned root journal
  decision. Recovery installs only committed roots; it does not reconstruct a
  huge WriteRequest. Native work credits cover incremental page work and claims.
* PostgreSQL: immutable pages plus a root-row conditional transaction; no
  transaction or row-lock lifetime proportional to total effects. Existing
  publication/retry authority is preserved in that transaction.
* gRPC and HTTP: bounded stage/page RPCs and paged results; shared planner and
  reader run above transport, with server authorization on every stage handle.
  Decoder frame checks stay in place. No backend-specific chunking policy.
* wasm/browser/injected providers: the same staged codec/planner with local
  futures; IndexedDB persists pages and commits root in its transaction.
* Cloud: thin adapter supplies durable page IO, root compare/publication and
  owner events to this core. Adoption must use the released SDK interface and
  run the same conformance cases, not reproduce SDK staging in the service.

Persisted page, manifest and root formats start at v1 with explicit domains;
change existing journal/session versions where their representation changes.
Unknown/old versions return Unsupported; there is no migration or dual decoder.

## Collection and crash safety

Before adding collection, declare large-field reachability in the shared content
graph. `content::tree::Writer::reference(root)` records a dependency without
appending its bytes. Sealing a writer with dependencies wraps its byte root in
one manifest whose parts carry the original bytes and whose edges point to a
bounded dependency tree. Dependency nodes use the existing Objects manifest
edge encoding, and the same carry builder as byte parts; there is no second
Merkle-tree implementation. Arbitrarily many references spill into edge pages.
Writers without references retain their existing content identities.

The byte reader follows only manifest parts, so dependency edges do not change
logical bytes, byte ranges or record order. Graph retention/retirement follows
both parts and edges through that one canonical codec. The staged encoder calls
`reference` whenever it writes a content-root field, including a supplied root.
Interrupted reference writes retire the writer exactly like interrupted byte
writes. Supplied roots still require owner authorization and validation before
final apply; declaring an edge is not authority to publish or consume a stage.
This is a collection prerequisite, not completion of stage-owner/event GC.

An unpublished stage owns page references. Session close, explicit abort,
publication completion and recovery's owner-death event enqueue retirement.
Collection processes a bounded reference-edge work queue; shared content is
released only after no published root, active stage or pinned reader owns it.
Retirement of a huge tree is paged too. Stage admission and root publication
serialize against owner retirement so GC cannot delete a concurrently published
page. Durable queue progress survives crashes. Recovery completes retirement of
unowned stages before admitting their handles. No timers, scans of all agents,
sleep loops or periodic probes. Tests wait on queue-drained/owner events.

## Inventory classification

The companion limit inventory records every current Stream source error site.
Request-size restrictions must move behind staging, including follow filters,
path depth, headers, fork exclusions, read selectors and response memberships;
protective frame/codec bounds remain only on individual physical frames/pages.
MAX_PATH_BYTES and InvalidPath checks must also be audited: not every size
restriction is spelled LimitExceeded. Empty requests and malformed canonical
syntax are InvalidArgument/InvalidPath rather than size errors. Checked integer
arithmetic and platform allocation failures are representability/capacity errors;
they cannot justify a low fixed logical limit.

Request/response paths: request.rs; lib/path.rs, domain.rs, receipt.rs, stream.rs;
record_headers/validation.rs and prepared/controls.rs; follow_filter.rs;
changes.rs; memory/{commit,prepared,provider,capacity}.rs; native/prepared.rs;
preparation.rs; journal.rs; persistence.rs; postgres/capability.rs;
http_validation.rs; wire_codec/{requests,records,head,provider_cursor}.rs.
Transport protections: grpc/server.rs; http_codec.rs; session/frame.rs;
session/{calls,client,sockets}.rs and client/{input,link}.rs;
wire_codec/session/{framing,resource}.rs; browser/port.rs;
wasm/{validation,provider_rpc,remote,socket,session_io,session_server}.rs.
Several files contain both classes: classify the actual operand, not the file.
Generated protocol limits, generated SDK projections and downstream consumers
must be updated from their sources, never by hand editing generated outputs.

## Required evidence before completion

1. Shared conformance commits 100,000 effects, checks all destinations, one
   decision identity, paged receipt and no partial observer-visible state.
2. A 64 MiB record crosses page/frame boundaries, reads byte-identically through
   owned and streaming readers, and retains exact fork/retry behavior.
3. Crash after stage upload and before publish, then reopen: heads, children,
   retry inspection and follows show no write. Crash after publication retains
   the whole write and one retry decision.
4. Explicit owner-death/abort event retires uncommitted pages; shared published
   content and pinned readers survive. Drain acknowledgment proves collection.
5. Concurrent root changes, duplicate retry keys, hard conflicts and skipped
   soft effects preserve original semantics across page boundaries. Corrupt,
   missing, reordered and cross-account pages fail without partial publication.
6. Run those same cases through memory, local/native, PostgreSQL, gRPC, HTTP,
   browser/wasm and the Cloud adapter. Bound per-step allocations/lock work using
   existing native accounting; no whole-request reconstruction inside adapters.
7. Remove consumer chunking for atomic capture (including GraphCoder GC-1),
   retain genuine pagination, regenerate contracts and update public guarantees.
8. Signed SDK PR validated, threads resolved, landed on main by Foreman; Cloud
   adoption PR opened and linked. Green narrow tests alone prove neither item.

## Execution order

Implement versioned content/page codec and incremental canonical request input;
then shared staged state and persistent authority-root preparation/publication;
then adapt all provider/transport/read surfaces; finally replace old limit tests
with the conformance obligations and remove caller workarounds. Each commit
must leave its implemented mechanism reviewable, with focused validation.

## Implementation checkpoint (2026-11-11)

The first foundation change extracts the Objects manifest v1 codec into
Stream's content module. Objects delegates both directions to it and preserves
its existing decoded-vector admission. Manifest::encode now returns a Result:
physical-count overflow and allocation failure cannot silently truncate a
persisted count. Call sites and the retained fuzz oracles propagate this result.

The shared content tree stages deterministic 64 KiB physical pages, independent
of input fragmentation, and returns a small digest/length/height root. Leaves
have a v1 domain prefix disjoint from manifest bytes: a legitimate value equal
to encoded manifest bytes cannot collide with internal node interpretation.
Readers verify every digest and length, retain original leaf custody, and load
one bounded page at a time. IO is a two-method adapter; publication and stage
retirement remain outside this foundation and are NOT implemented yet.

Focused controls use actual immutable files: 64 MiB streamed upload and reopened
lazy read, fragment-independent identity, embedded manifest bytes, empty values,
fanout carry into a second manifest level, and corruption refusal. These prove
content-tree behavior only, NOT atomic Stream writes, crash staging visibility,
event-driven collection, all-backend conformance or removal of public limits.

## Reuse of existing publication and proof machinery

The final-apply capability extends MemoryStream::publication in
src/memory/memory_stream.rs. Append, fork, commit and durable apply already
acquire it in memory/provider.rs and memory/decision.rs. Keep that one mechanism;
do not add a separate staged authority lock or an independent writer queue.
The immutable namespace trie already supports retaining a predecessor while
building a successor. Preparation must become paged without publishing any
intermediate Namespace root, fact list or retry receipt.

The production-linked loom driver is memory/recovery_model.rs, exported through
verification with cfg(loom, feature=local). Its ReleasePendingPublication fault
already demonstrates why final-apply ownership must span durable IO. Extend this
instrumentation for staged applies and batch cursors, with negative controls;
a disconnected generic Stage model would not prove this provider's behavior.
Existing models are evidence for their current bounded paths only and do not
certify the new staged protocol until it is wired through those same paths.
The publication audit also identified an existing fallible tail:
`memory/commit.rs::install_namespace` replaces `state.paths` before fallible
`state.signals.get` / `keys_in` calls, and `CommitPublication::publish` retains
the retry observation only after that function returns. The staged replacement
must remove this failure window structurally: prepare fallible notification
lookups before publication or derive notifications from a retained committed
batch after the infallible root+receipt transition. Do not preserve a fallible
partial installation inside the final-apply callback. Add this cut to the
failure simulation; it is not fixed by the content-tree foundation.

## Foundation validation (source 8222ad7d505)

Linux `cargo check --locked --all-targets` passed for Stream and Objects with
local/http plus Objects binary/test-support. Three shared content-tree controls
passed in 0.446 s, including the real 64 MiB reopened-file round trip. Objects'
three existing controls (persisted version refusal, retained codec fuzz oracle,
and memory shared conformance) passed in 0.236 s. Remote job
1794355868856-784080-6f5af786 completed rc=0; retained log:
Q:/scratch/stream-unbounded/content-8222ad.log.

The submission client's optional result-file export failed after that successful
job because custom commands are not scoped exports. This did not rerun a check
or change its result; use the actual job terminal and retained log as evidence.
Subsequent commits change only this design. No Windows-specific or macOS-specific
code changed. The final-apply, crash simulation, negative-control and loom
obligations above remain unimplemented and unproved; none is inferred from
these content-only passes.

## Publication failure prerequisite

The namespace installer is now infallible. Decision::install_at retains the
predecessor root before entering publication. Append, fork, commit and accepted commit install the whole namespace and
retain their terminal retry result under the same state write guard before
calling the fallible notification function. Notification reads the just-retained
envelope from the retained tail in O(1), without adding a quorum or history read.
An accepted decision always publishes its actual applied index, including when
notification subsequently fails. A transport error after this boundary remains
an unknown outcome whose original key resolves the one retained receipt.

The focused real-provider regression poisons the actual notification registry
through its original create callback, then checks whole coordinated visibility,
retained observation, exact family-specific retry and one revision assignment
for all four write projections. Its negative control reverses the production
fix while keeping the same test, requiring the lost-observation assertion.
This fixes the named root/receipt failure window; it does not certify recovery
of failed follower delivery or fulfill the staged crash/loom proofs above.
Validation evidence follows below.

The notification boundary is centralised in Decision::install_at, including
prepared native commits. Native write accounting completes before notification
attempts: a notification error must not erase byte transfer for an installed
write. The predecessor namespace is retained by one O(1) root clone across that
boundary; no subscriber scan or notification-copy queue is added. The regression
also covers native append, commit and accepted commit with original Meter byte
and operation accounting (native fork is an explicitly unsupported capability).

At 997f0a962b2, Linux all-target check and the four focused positive controls
passed (0.168 s). Reversing production code failed exactly the lost-terminal-
observation assertion. The custom command then failed because rg is absent on
the build host; its EXIT trap restored the patch. Use grep for the next control
witness. The accounting-boundary refinement requires its focused run next.

## Publication prerequisite validation (source b35862f1e7c)

Linux Stream local/http all-target check passed (25.40 s). Five focused controls
passed in 0.132 s: all-family notification failure and exact retries, original
native operation/byte accounting, real append/fork/commit snapshot replay,
fork prefix cuts, follow ordering, and prepared native replay across growth.
(The all-family and native-accounting obligations share one test.)
The reverse-production-patch control failed the required lost-terminal-
observation assertion; its witness was checked, the positive patch restored,
and git diff proved the source clean. Job1794356986407-808796-5c1529b0 exited0.
Retained evidence: Q:/scratch/stream-unbounded/notification-b35862.log.
A subsequent source cleanup only removes the import the compiler reported
unused; production code is identical to this validated source. The new staged
protocol and six complete atomicity proofs remain outstanding.

## Staged ingestion wire layout (before implementation)

Expose the content tree's existing uploader as an incremental Writer, and keep
upload as its stream adapter. An interrupted/failed write permanently poisons
that writer; it cannot later seal a truncated prefix. Both use the same builder,
page codec, storage IO and lazy verified reader.

The staged request v1 byte stream begins with a domain/version, commit identity,
request family, retry key and optional deadline, then effect count. Its tokens
are Effect, Condition, Append, Fork, Exclusion, Record, Key and Attribute, with
wide counts and explicit tags. Caller record order is retained. Each byte field
has either an inline chunk representation or a content-tree root; large record
values therefore live in their own content tree and record tokens retain only
the root. The inline threshold splits physical work and never rejects a value.
Paths, exclusion lists and header lists use that same field/token representation
rather than imposing a request-sized frame. The decoder retains only one field,
one content page and fixed counters for its current effect/mutation/record.

A sealed StagedWrite has private immutable commit id, original request family,
retry key and content root. Its header is bound inside the hashed bytes. It
assigns NO stream position or condition outcome. Its decoder verifies the
binding, token order, declared counts and end of stream. The content root is a
transport/staging identity, NOT a substitute for the final canonical retry
digest: deadline and accepted-output selection must preserve existing retry
semantics. Final apply consumes this cursor rather than rebuilding a huge
WriteRequest. Backend entrypoints, publication, owner-event collection and the
six complete atomicity proofs remain separate integration work; ingestion-only
controls cannot certify them.

The ingestion checkpoint implements the versioned token stream, fixed-counter
shape decoder, private commit/key/family binding and large-field references.
The content uploader now delegates to the same incremental Writer. A real-file
cancellation control waits on the stored-page channel event and checks the
writer cannot resume or seal after its write future is dropped. A production
patch removing that guard is retained as its negative control. Additional
controls ingest 100k effects with two ordered records each, a 64 MiB addressed
record between ordinary records, fork/exclusion/header tokens, and every byte
truncation of a complete staged frame. They do not apply a Stream commit.

Current StreamProvider write entrypoints still use their bounded request path.
This checkpoint is not completion of public-limit removal or any backend's
atomic staged publication. The native original-work claims, stage lifecycle
retirement, final-apply cursor planner, transport methods and batch-follow frames
must be integrated before the conformance/proof completion audit can pass.

## Streaming ingress refinement (before implementation)

The shared content reader also needs `range(storage, root, offset, length)`.
It validates the requested interval against the root's logical length, skips
unselected manifest subtrees without fetching them, verifies selected pages,
and returns only the requested bytes in original order. Its working set remains
bounded by physical pages and tree height. An empty interval performs no IO;
the existing whole-content reader retains its empty-content behavior. The same
interruption guard retires a canceled range cursor. This supports reopening
staged sections and seeking large record content without prefix materialization;
it does not itself publish or authorize a stage.

An opaque `staged::Bookmark` records a successfully decoded token boundary,
the remaining shape counters, and the verified submission binding. `bookmark`
refuses an interrupted decoder. `Reader::resume(storage, staged, bookmark)`
accepts only the same immutable body, commit id, retry key and request family;
accepted-output projection is not part of that identity. It seeks through the
same content range reader and continues the same structural validation. A
bookmark at end resumes as end. Bookmarks retain constant metadata, not decoded
prefix tokens or record bytes. They are process-local decoder state, not a wire
authorization credential or a substitute for a stage owner's retention pin.

Add stage_tokens(Submission, Stream<Token>) as the bounded-memory entrypoint.
Submission binds commit id, retry key, original family, deadline, effect count
and output projection. Owned stage(WriteRequest) remains a convenience adapter;
both MUST use one token encoder and one shape validator. Generic token fields
borrow owned-request bytes without copying, while a streamed token may carry
an already staged content root. Small addressed fields are valid too: the
staging root is a transport identity, not the canonical logical retry digest.

The content writer cooperates with the runtime after each completed physical
page. This is scheduler credit, not a readiness probe, timer or sleep. Native
original-work admission remains an integration obligation. The 100k-effect
control will generate its input lazily, and the 64 MiB control will build its
value root from bounded fragments instead of allocating one whole record.
The old validation client was asked to cancel after its target was proved
terminated; do not create a second live validation submission while it remains.

The streamed entrypoint and owned adapter now share the same generic token
encoder and fixed-counter shape validator. Borrowed fields avoid copying owned
request paths/values; streamed fields may reference existing content of any
length. Request-family structural mismatches, missing tokens and extra tokens
are refused before seal. The 100k-effect producer retains one effect's tokens,
and the 64 MiB value is built from 64 KiB fragments, without a whole-record
allocation in either ingestion control. Page cooperation reuses runtime::yield_now,
including its existing browser task-event adapter; no second scheduler is added.
These source changes are pending remote validation. Neither earlier unrun
ingestion controls nor these new controls are claimed as passed.

Content-reader cancellation must also retire the cursor: a dropped get future
has already removed its pending root and cannot be resumed by skipping that
page. Add the same explicit interrupted-state guard at the shared content reader
and cooperate at leaf delivery through the existing runtime scheduler. A real
file read gated by a channel event must demonstrate that cancellation refuses
reuse. This is a content-transfer guarantee, not the still-outstanding atomic
publication/crash proof.

Linux ingestion validation did not execute Rust: the original host terminated,
and the replacement remained held by source-sync/owner-proof qualification
failures. The canceled original client and the one replacement attempt are
terminal; no duplicate Linux job is live. No test failure or pass is inferred
from these pre-compilation infrastructure exits.

Native macOS `cargo +1.98.1 check --locked --all-targets -p acyclic-stream
--features local,http` passed at exact runtime source
`51c07fd8384e7550364b412ed269ac808467c115`. It used the already installed
toolchain, `CARGO_INCREMENTAL=0` and the shared target
`/Users/var/foreman/target/lanes`. Log:
`Q:/scratch/stream-unbounded/ivar-51c-check.log`. The design/inventory commits
through `e948030667c` do not change that checked Rust source. The subsequent
range-reader and bookmark extensions are checked at their own source below;
results at `51c07fd8384` cannot validate those extensions.

The queued focused test run at `51c07fd8384` was stopped before any test executed
because the range/bookmark source superseded it. Its process and script exited,
and its checkout was confirmed clean before the replacement started. That stop
is neither a test failure nor a test pass.

The same native all-target check passed at exact source
`e7ec6d0a1b33acd2cae17932b84ad77dafffd355`, including the shared range reader,
staged bookmarks and their test targets. It used the same installed toolchain,
shared target and incremental setting above. Log:
`Q:/scratch/stream-unbounded/ivar-e7ec-tests.log`; command:
`Q:/scratch/stream-unbounded/ivar-e7ec-tests.sh`.

That source's focused run completed successfully: seven `staged::tests::`
cases passed in 0.23 seconds and five `content::tree::tests::` cases passed in
0.45 seconds. This includes lazy 100k-effect ingestion with record order, a
64 MiB addressed value, truncation/trailing rejection, request-family shape,
fork/header token order, bound bookmarks, range seeks without unselected IO,
reopen, corruption detection and writer/reader cancellation refusal.

Both guard-removal negative controls failed at their intended assertions:
`cancelled staging writer accepted replacement bytes` and
`cancelled content reader skipped the consumed page`. The script checked both
failure markers, reversed both mutations and verified `git diff --exit-code`.
It emitted `POSITIVE_CONTROLS_PASS_NEGATIVES_REJECTED_SOURCE_RESTORED` with the
exact `e7ec6d0a1b33acd2cae17932b84ad77dafffd355` source SHA. An independent
native HEAD/diff/status check confirmed that same source and a clean checkout.

These results validate ingestion/content behavior. They are not successful
100k-effect or 64 MiB atomic write conformance, crash-at-publication simulation,
the final-apply loom proof, owner-event collection or any backend's staged
publication integration. Those obligations and Linux qualification remain due.

The subsequent reference-edge extension requires its own compile and focused
run. Its controls add dependency-page spill and reference-write cancellation,
and require the large-record stage to declare its value edge. A third mutation
removes that declaration and must fail with `staged body omitted content
dependencies`. Earlier `e7ec6d0a1b3` evidence does not validate this extension.

The reference-edge reader also accepts an empty logical body: its wrapper has
one zero-length byte part and retained edges. Zero-length parts in a nonempty
body remain invalid. The existing dependency-spill control covers whole empty
reads and empty ranges without IO; a mutation restoring the old blanket
zero-part rejection must fail with `empty referenced content was rejected`.
The earlier iterator fix at `88b5847a992e59edca4934c1ade93a571dd646c5` passed
native all-target Stream local/http compilation. Its tests were still waiting
for the shared target lock when this empty-body fix superseded that source;
the exact queued Cargo process was stopped before any test or mutation ran.
The revised source requires its own focused check and all four controls.

Native macOS all-target Stream local/http compilation passed at exact source
`21f5143212d4eb9ce55ca285579404ebef74a6e5`, including the empty-reference fix.
It reused the installed 1.98.1 toolchain, incremental disabled and the one
shared target. Log: `Q:/scratch/stream-unbounded/ivar-21f5-tests.log`;
script: `Q:/scratch/stream-unbounded/ivar-21f5-tests.sh`.
The same live script proceeds to 13 positive cases and four exact mutations.
At this checkpoint its test command is waiting for the shared target lock;
no positive or mutation result at this source has yet been observed. Continue
observing that existing process rather than launching a duplicate validation.

### Unbounded staged path grammar

Before canonical planning, every staged condition, append destination, fork
source/destination and exclusion must pass the existing ASCII segment grammar.
Use one incremental grammar state shared with StreamPath::validate; it retains
only the current segment class (empty, dot, dot-dot, ordinary), with no byte or
segment-count ceiling. Both inline input and content roots are checked in
physical-page steps; the latter is read through the shared verified reader.
Reject malformed paths before sealing, including a dot segment or separator
split across pages. Opaque values and header bytes are not path grammar input.

The owned StreamPath adapter keeps its existing request bounds until namespace,
transport and publication cutover is complete; extracting grammar alone is not
permission to expose deep paths to the current recursive namespace. Staged
path validation does not observe Stream state, reserve destinations, settle
conditions, assign positions or provide any of the outstanding publication
proofs. Its conformance control must include paths larger than the old byte and
segment ceilings and malformed segments crossing physical page boundaries.

### Paged ordered index for the shared planner

The planner needs immutable ordered maps whose keys and values may themselves
be arbitrarily large content roots. Each index node contains bounded references
and a bounded key prefix; comparisons past that prefix stream verified content
pages. Node payloads use the existing content writer and declare every key,
value and child as retained manifest dependencies. A copy-on-write update changes
only the bounded search spine and returns a private successor root. It does not
install an authority root or settle Stream conditions.

Reuse collections page shapes and the existing ordered binary-search decision
tree. Extract its comparison continuation so a caller can await a content-key
comparison without adding a second binary-search implementation. Page-format
keys need equality and ownership, but ordering belongs to the admitted comparator;
algorithms that directly call Ord retain that explicit bound. The eventual shared
planner uses these maps for canonical descriptors, namespace indexes and terminal
receipts, rather than materializing complete vectors. Physical fanout and u64
coordinates bound each step; they are not accepted logical request ceilings.

The index must demonstrate immutable predecessor visibility, keys sharing an
arbitrarily long prefix, splits across multiple levels, ordered seek, overwrite,
missing keys and retained content dependencies using the real-file content store.
These index controls do not replace the required production publication, crash,
loom and all-backend proofs. Root publication still requires the one shared
authority decision described above.

The first index prerequisite extracts Search/Probe continuations from the
existing collections search; synchronous callers immediately resolve the same
continuations. A generator checks unique sorted positions, admitted probe
subsequences and refusal boundaries against the standard ordered result, and
a full-width coordinate case allocates no slice. The content-key map, async
comparison adapter and page-format bound change remain to be implemented; this
continuation extraction alone is not an index or atomic publication.

The exact `21f5143212d4eb9ce55ca285579404ebef74a6e5` native run completed:
all-target Stream local/http compilation passed, all 13 content/staged cases
passed in 0.98 s, and all four mutations failed at their intended assertions
in 0.19 s. The markers covered writer cancellation, reader cancellation, missing
content dependencies and empty referenced-body rejection. The script reversed
all four mutations, checked a clean diff, and emitted
`DEPENDENCY_CONTROLS_PASS_FOUR_MUTANTS_REJECTED_SOURCE_RESTORED` with that SHA.
Logs/script: `Q:/scratch/stream-unbounded/ivar-21f5-tests.log` and
`Q:/scratch/stream-unbounded/ivar-21f5-tests.sh`. This is content/ingestion evidence;
no final-apply, crash, loom, GC or atomic-write conformance claim follows.

The index foundation now implements a bulk ordered Builder and point lookup.
It reuses collections Page/Child shapes, content-defined packing and Search
continuations, and the existing verified content trees and retained manifest
edges. Byte fields and their inline policy now live once in the general content
module; staged tokens expose that same type. Keys retain only a bounded prefix
alongside an inline field or root; comparisons stream beyond the prefix. Builder
failure/cancellation cannot return a prefix root. General page shapes no longer
require Ord; algorithms that directly compare synchronously, including the
filesystem format facade, state their existing Ord requirement explicitly.

Three new real-file controls cover multiple internal levels with a pinned
predecessor and reopened storage, arbitrarily long common key prefixes and
representation-independent duplicate refusal, reachable key/value/child pages,
and interrupted input. Three corresponding mutations remove the input guard,
field references and child references. They must trigger their distinct assertion
markers. This new index source is pending remote compilation and tests.

This is not yet the full index API: paged ordered iteration, efficient immutable
updates and authenticated root-descriptor persistence remain. Reopening storage
with an existing pinned Index handle is not a crash/restart proof. The staged
canonical planner, original-work admission integration, authority publication,
terminal receipts, event GC, all six production proofs and backend/caller cutover
remain outstanding. No backend write is routed through the new index yet.
