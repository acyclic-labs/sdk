# Unbounded atomic Stream commits

Status: design recorded before implementation; no runtime change yet.
Baseline: 506843a3822. Lane: stream-unbounded. Objective includes SDK main landing
through Foreman and a separate Cloud adoption PR; neither is complete.

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

Preparation pins one immutable authority root. It evaluates all hard conditions
against that root, then soft effects, captures forks, and constructs persistent
paged indexes, facts, receipt/conflict pages and a new root outside publication.
Root publication atomically compares the pinned root and installs the new root
plus terminal retry observation. The lock/transaction covers only this small
root change. A lost comparison returns its actual root; the shared planner may
replan against that root, without a readiness probe or sleep. User precondition
conflicts remain terminal outcomes. Once accepted, deadlines cannot erase the
decision. Concurrent identical retry keys return the one retained decision;
different logical digests return the existing idempotency conflict.

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
