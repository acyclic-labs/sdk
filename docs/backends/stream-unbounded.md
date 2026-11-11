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

### Ordered index cursor

Add scan(lower-bound) over one immutable index root. The cursor descends through
the same Search continuation as point lookup and retains at most one bounded
page per fixed-width level. Once positioned it advances through that root's
children, without re-seeking from the root or reading skipped subtrees. Returned
keys and values remain bounded inline fields or content references. Point get
uses this same descent/cursor mechanism, avoiding a second tree reader.

A cancelled next operation permanently refuses cursor reuse; it cannot skip a
child already removed from its frame before IO. A fresh cursor may reopen the
same retained root. The caller's enclosing stage/published pin owns page lifetime;
this index cursor neither creates a Stream position nor substitutes for the
required authority/batch retention capability. Extend the multilevel real-file
control with complete ordered traversal and lower-bound seeks, and gate a real
child read to prove interruption refuses reuse. A guard-removal mutation must
fail that cursor assertion. This is index traversal, not the final follower or
crash-at-publication proof.

The index cursor implementation now shares descent with point lookup. A scan
retains its bounded spine; point get discards parent frames and carries the
search's exact-match result, avoiding extra leaf reads and repeated large-key
comparisons. The multilevel control traverses every entry and seeks across page
boundaries. A second read gate on the same real-file fixture cancels a child
fetch and requires refusal rather than skipping it; a ninth mutation removes
that guard. These cursor changes are not remotely compiled yet.

The `6268291c8ffb6a373c9757e6c416174a5f9f4e6a` check did execute and stopped
before tests with E0308 in filesystem allocation/writer.rs:46. The compiler
reported an Option<&str> base in filesystem-base admitted::merge at line 780,
but the exact checked-out file AND git show HEAD both define &str at line 765.
The unchanged caller correctly supplies &str. No Some(base) SDK change was made.
This is evidence of incompatible reused dependency metadata in the shared
native target, not evidence that the checked-in API needs adapting.

The next focused filesystem check must refresh only that dependency file's
mtime in this checkout, verify its bytes still match its committed object,
and compile the failing package first. It must not delete or alter the shared
target or another checkout. Then check Stream/collections at the new source
before targeted controls. Record the source SHA and any freshness evidence;
if the same source/metadata mismatch remains, do not repeat it unchanged.
All original publication, crash, loom, GC and backend obligations remain due.

### Index checkpoint reference (before implementation)

checkpoint(storage) returns a content root for a versioned, fixed-size descriptor
of one immutable Index. The v1 descriptor binds its optional node root, logical
index height and entry count. It declares the node as a retained content edge;
retaining the checkpoint therefore retains its complete transitive page graph.
The empty descriptor has no node. This creates no authority publication, Stream
position or condition outcome.

open(storage, checkpoint) verifies and parses the bounded descriptor, checks its
shape and the top node, and reconstructs the handle without scanning the whole
index. Its checkpoint must come from an authenticated published/staged owner;
opening bytes is not authorization or a certificate of an arbitrary supplied
graph. The enclosing published root is responsible for authenticating the
producer's entry count, and durability covers all referenced pages before that
root can publish. Unknown versions refuse; there is no migration decoder.

Reuse the existing node reference codec and verified content reader. Extend the
multilevel control to discard the old handle and open its checkpoint on reopened
storage. Check reachable node ownership, empty round-trip, every descriptor
truncation, trailing bytes, unsupported version and invalid leaf count. A mutation
that accepts trailing descriptor bytes must fail the malformed-frame control.
These checks are recovery building blocks, not the required full shared-core
crash-at-every-byte publication simulation or its negative control.

Checkpoint persistence is now implemented using the existing reference codec
and bounded content reader. The multilevel case stores a checkpoint, proves its
node is reachable, drops the original handle and reopens it on reopened storage.
A new real-file malformed-descriptor case covers empty checkpoints, every byte
truncation, trailing bytes, unsupported versions, wrong leaf count and height;
its trailing-byte mutation is the tenth negative control. These changes await
remote validation and are not a full crash/restart authority proof.

At exact source `c2b09139f8581e25f93c64591bc6345dbc408d71`, the focused native
filesystem all-target check passed (21m24s including queue), followed by the
Stream/collections all-target check (7m06s including queue). No SDK writer
adaptation was needed after refreshing its dependency metadata. Test compilation
then failed before executing tests: imported collections metadata lacked Search
and required Ord, while both checked-out files define Search and omit that bound.
The remote checkout remained clean at c2b. This repeats the shared artifact
identity/freshness problem in test-build mode; adding Ord or a duplicate Search
would change correct source to accommodate a foreign artifact and is not a fix.

The next task-local validation uses a pass-through RUSTC_WORKSPACE_WRAPPER at
one stable path owned by this task. Cargo documents that this separates workspace
artifact filename hashes, while registry dependencies remain shared. It keeps
CARGO_TARGET_DIR at /Users/var/foreman/target/lanes, incremental disabled and the
installed toolchain; no other target, shared artifact deletion, new toolchain or
shared machinery edit is involved. The wrapper forwards the compiler arguments
unchanged. Check its exact bytes and source SHA in the log before compile and
focused controls. Source: [Cargo environment variables](https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-reads).
The native volume had 16 GiB available before this change.

### Bounded external ordering for canonical descriptors (before implementation)

Add an unordered Sorter over the same index writer. It normalizes fields, keeps
one physical-page-sized sorted buffer, and uses Search for insertion. A full
buffer becomes an immutable run. Binary carry merges runs through the existing
index cursor and writer, retaining at most u64::BITS run roots plus bounded
cursor pages. No request-sized descriptor vector or caller chunking is needed.
Equal logical keys refuse both within a buffer and across runs; interruption
cannot seal a successful prefix. Returned runs remain private stage data.

The index writer exposes one internal append for already ordered, normalized
entries; public ordered input keeps its validation. Merging uses that same append
and codec, not a second index format. Canonicalization will sort descriptor keys;
record payload tokens and their caller ordinals remain in the original sealed
body and must not be reordered by this utility.

Inline fields must also have bounded retained backing, not just bounded visible
length. Reuse OwnedBytes::into_backed when normalizing small fields so a tiny
slice cannot pin an arbitrary allocation in a pending page. Large visible fields
still spill rather than reject. Original-work admission integration remains due.
Controls compare multi-run output with an independent ordered map, refuse a
cross-run duplicate, and prove a large original slice owner retires while a small
normalized field is buffered. A merge duplicate-guard mutation must fail.
This is canonicalization groundwork; no Stream publication is added here.

At exact c278ae9d6f6e7a81e36077e33b96e397bacafc2b, native Stream/collections
all-target checking and test-binary compilation passed with the verified workspace
wrapper. The binaries use separate workspace artifact hashes. Both collections
controls passed in 0.01s and all 20 Stream controls passed in 0.90s, including
index traversal/checkpoint recovery and staged path validation. Its ten mutations
are still pending at this checkpoint; no negative result is inferred early.

Sorter is now implemented over the same internal index append, page codec and
cursor. The sorted-input fast path appends to the bounded buffer; arbitrary input
uses Search. A deterministic permutation of 4097 descriptors is compared with an
independent BTreeMap, with duplicate checks inside a buffer and across completed
runs. A separate real-file gate covers cancelled unordered input. Two mutations
remove the cross-run duplicate refusal and the sorter interruption guard.

Working-space argument: normalized inline keys/values retain at most 64/4096
bytes using the existing bounded-backing helper; content fields retain fixed
references. Each encoded entry is at least 50 and at most 4210 bytes. Successful
push either keeps a buffer below half a physical leaf or flushes it; an in-flight
push adds at most one bounded entry. Run carry stores at most 64 index handles.
Merging retains two bounded cursor spines and one bounded output writer. No bound
scales with total input count. Original-work admission remains an integration
obligation, and this is not the production atomicity proof. The large-key control
also witnesses release of a 1MiB original owner while its 8-byte slice is buffered.
The new sorting/retention source needs its own remote check and controls.

### Staged destination directory contract (before implementation)

The next integration is a private MutationDirectory built from one sealed
StagedWrite. It is not a prepared authority decision. Its byte-ordered unique
keys are mutation destinations; values carry original effect and mutation
ordinals plus a verified decoder bookmark immediately before that mutation.
Building it reads the original token body once, retaining only the current token,
constant counters and the shared external Sorter's bounded working pages.
Duplicate destinations anywhere in the request are InvalidArgument, matching the
existing cross-effect mutation shape rule. Sorting descriptors never rewrites the
body: records, keys and attributes retain caller order across physical boundaries.

Bookmarks stored in directory values have a fixed versioned encoding of the
logical byte position, optional deadline and decoder counters. The body root,
commit id, family and retry key come exclusively from the enclosing StagedWrite;
they cannot be rebound by a value. Only authenticated directory roots may be
opened: this is not an untrusted seek credential or proof that arbitrary supplied
bytes are a token boundary. The private directory root retains both the original
body and the ordered-index checkpoint, binds the exact body descriptor in its
versioned bytes, and is never a namespace, retry, sequence or follower root.
Lookup resumes the original body at the saved boundary without scanning its
prefix. Reopening requires the same original staged binding; accepted-envelope
projection may change as for existing Reader bookmarks. Failure or cancellation
returns no directory; its successful page writes remain owned by the enclosing
stage and require event-driven retirement at the eventual registry integration.

A real-file conformance control must compare directory iteration with an
independent path ordering while lookup returns original ordinals and records;
reopen must preserve that result, a different stage must be refused, and duplicate
destinations across effects must be refused. This control proves descriptor
integration only. Canonical condition/header/exclusion ordering, namespace COW,
final publication, original-work admission, event GC, crash/loom proofs and every
backend's production conformance remain separate outstanding obligations.

The destination directory is now wired to Reader bookmarks and the shared
Sorter. Its real-file control covers 514 shuffled destinations over 257 effects,
a close/reopen, full independent ordered-map comparison, point lookups, original
record order, stage/body rebinding refusal, accepted-output projection and a
cross-effect duplicate. A mutation increments every stored effect ordinal; the
control must fail with the original-ordinals assertion. This new source is not
yet remotely compiled or executed. The earlier c278 ten-negative run remains
live, waiting for the shared native target lock; its restoration must finish
before a newer source is checked out there. Production writes still enforce the
original public limits; no atomic-apply or backend conformance result is claimed.

At exact 4ac6bd0196c5f1938cd01c1d472f18d1207b4814, Linux scoped
all-target Stream/collections checking passed in 25.78s. Both collections search
controls passed in 0.01s. The four targeted Stream controls (unordered sorting,
its cancellation, large-key/backing retention, and destination-directory
reopen/order/rebinding/duplicates) passed in 0.03s. All three selected mutations
failed with their intended assertions (0 passed, 3 failed, 0.03s), reversed, and
git diff verified clean. Terminal marker:
DIRECTORY_CONTROLS_PASS_THREE_MUTANTS_REJECTED_SOURCE_RESTORED.
Evidence: Q:/scratch/stream-unbounded/directory-4ac6-linux.log. The command ran
through the shared runner's newer installed generation; no infrastructure files
were modified. No public-write atomicity or all-backend result is inferred.

The superseded c278 native negative Cargo21412 was cancelled after the current
source's Linux controls passed. Its log establishes it was still waiting on the
target lock and had executed no negative tests. Its earlier 20 Stream and two
collections positive results remain valid for that exact source. Cleanup and a
new exact-source native run are tracked in the operational handoff; cancellation
is not counted as a negative-control success.

### Effect/condition directory contract (before implementation)

Extend the private destination directory into one Directory built by the same
single pass over the sealed token body. Keep the existing global unique mutation
index; add a caller-ordered effect index keyed by the fixed-width big-endian
original effect ordinal. Each effect value retains a versioned metadata page
containing its original boundary, soft flag and a checkpoint of its own unique
byte-ordered condition index. A condition index uses the same Sorter, Index,
location codec and bounded cursor as mutation destinations. The current effect's
sorter is sealed before the next effect starts; no vector grows with the number
of effects or conditions. Equal condition paths in different effects are allowed;
equal paths inside one effect are refused. Effects with no conditions keep an
empty checkpoint and remain present in caller order.

Locations expose effect and original within-effect ordinal, with one bookmark
codec shared by conditions and mutations. The Directory root retains the body,
mutation checkpoint and effect checkpoint; each effect metadata page declares
its condition-checkpoint reference. Reopening checks the immutable body binding
and lazily opens effect/condition metadata. No condition is evaluated and no
sequence, revision, head, fork cut, namespace or retry decision is assigned.
The original token body and all record order remain unchanged. This replaces the
unpublished mutation-only directory API/codec rather than adding a second path;
the new directory/effect formats each start at v1, without migration code.

Controls must cover shuffled condition paths, original effect/condition order,
soft flags, empty conditions, duplicate scope across/within effects, reopen and
complete dependency retention. A negative control removes the metadata page's
condition-checkpoint edge; the independent retained-graph control must fail.
Canonical headers/exclusions, authority publication and all six final-apply
proof obligations remain outstanding.

The effect/condition directory is now implemented over the same location codec,
Sorter, Index and bounded path cursor. The original effect boundary and soft flag
are retained. Controls include 4097 shuffled conditions, duplicate paths allowed
across effects/refused inside one, empty condition sets, original ordinal/token
recovery and an independent retained-graph check after reopen. A real-file gate
checks that cancelling a consumed effect metadata read cannot skip that effect.
The existing file/gate/graph fixture is shared by content-index and staged
controls rather than copied. New mutations remove the checkpoint dependency and
effect-cursor interruption guard; the ordinal mutation follows the shared codec.
This source needs remote compile and controls. Metadata object packing and native
original-work admission remain required before production cutover; this is not
the 100k-effect atomic-write conformance or any final-publication proof.

At exact2d7c024fdc6391f8a530676e26850016125e3ee7, Linux Stream all-target
checking passed in17.41s; all ten selected index/directory controls passed in
0.12s. Three simultaneous mutations each failed at the intended retention,
cancellation or ordinal assertion (0 passed,3 failed,0.03s), reversed and git diff
verified clean. Terminal EFFECT_CONTROLS_PASS_THREE_MUTANTS_REJECTED_SOURCE_RESTORED;
evidence Q:/scratch/stream-unbounded/effects-2d7c-linux.log. No public-write or
atomicity claim follows from these descriptor-preparation results.

Native exact65b98b5d0f982c5ab664129f266287b27bcb54bb also completed: scoped
Stream/collections all-target check and test-binary build passed; 23 Stream
controls passed in0.55s and two collections controls in0.01s. All13 mutations
failed at their intended assertions in0.28s and reversed. Terminal
INDEX_CONTROLS_PASS_THIRTEEN_MUTANTS_REJECTED_SOURCE_RESTORED is recorded in
Q:/scratch/stream-unbounded/ivar-65b9-tests.log. Independent SSH subsequently
verified both staged and unstaged diffs clean at that exact source. No native job
remains. These results cover the earlier mutation-only directory, not the newer
effect/condition implementation.

### Packed effect metadata contract (before implementation)

The effect directory must not create a private metadata object for every small
effect. Replace its ordered effect Index and per-effect metadata objects with
one fixed-width metadata stream built by the existing content-tree Writer.
A row stores soft flag, condition count, original Bookmark and condition
checkpoint; original effect ordinal is its stream position. Root length and the
fixed row width bound/derive the count without allocating an effect vector.
The stream header starts at effects-v1. The enclosing directory changes to v2;
remove its v1 decoder and the per-effect object codec, without migration code.
The enclosing root still retains exactly the body, mutation checkpoint and
new effect-stream root. The effect-stream writer explicitly retains condition
checkpoints. Cache the canonical empty condition checkpoint once per build and
declare its dependency once; empty effects must not issue one repeated upload
or read per effect. Nonempty condition indexes still use the single shared Index.

The effect cursor reuses staged decode::Cursor for fixed-width bounded reading
and its current interruption latch. Sequential effects share physical metadata
pages. Empty conditions use the empty Index without IO; nonempty conditions open
their checkpoint lazily and verify the stored count. No Stream authority state is
sampled or changed. A cancellation control can use a nonempty condition index to
suspend after a row has been consumed. The existing 100k-effect ingestion control
should also build/reopen this Directory and walk effects without request-sized
allocation. It remains preparation coverage, not the required atomic-write
conformance. The condition dependency mutation must follow the new stream writer
and retain the same graph-control failure. This packing change is not coded yet.
