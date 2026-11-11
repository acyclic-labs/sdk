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