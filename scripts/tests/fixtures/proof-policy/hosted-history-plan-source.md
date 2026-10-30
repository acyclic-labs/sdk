# Hosted Streams qualification response

rx-qualify -> rx-hostability, 2026-10-26. **Plan only; no execution receipt.**
Reviewed cloud-review/streams.md and cloud-review/verify.md, current api/verify.md,
and  Cloud source inspected at main
`da1c9ca3e27d8f6ed9dc2b93c78696ec94a99e1a`; review historical source pin is
`a80dd0b8d96a5af569a14fd05df01277ffdfee5a`. These pins are not interchangeable.
#6599 merged; #6648 is ready/frozen and is unrelated existing fault evidence,
not proof of the new subtree contract.

## Decisions and shared proof boundary

Use the **same public SDK generic `verify<P: ProviderTrait + ?Sized>` suite**,
its production request/error/receipt types, `run_history`, `check_history`, and
`require_mutant_failure` described by api/verify.md. Memory, local, transport,
native and browser carriers select adapters/cases, never different oracles.
Pin SDK commit plus Cloud commit/image digests in every hosted receipt. The
invariant IDs below are stable proposed qualification IDs for SDK integration,
not a claim that those IDs/cases or a new SDK API already exist. Map them into
the canonical package once; do not create a Cloud copy of its schema/checker.

Accept the settled contract: one namespace consistency domain within one cell;
logical authority-lifetime successful key/digest/receipt binding survives physical
restart, restore and move; Unknown/None requires exact retry; no changed digest
on uncertain completion; definitively rejected attempts may release the key only
with generation fencing. Old messages cannot mutate a newer attempt. One serializable
namespace cut includes membership and heads. Live holds retain the full sealed
and unsealed closure. Missing-path follow is **NotFound, no subscription or path
creation**; Tail(0) on absence is independent. Inherited existing follows still
need root-only update wakes. Replay permits duplicate unacknowledged frames,
not duplicate applied effects or gaps; channels do not promise simultaneous delivery.

The final paragraph of api/verify.md also requires old keys never silently
republish after namespace/history retirement. Exercise typed rejection for a
retired logical authority, and no resurrection by restoring it as a fresh
identity. Physical source-path retirement must preserve live destination bindings.
The absent-follow contradiction remains the coordinator's existing cross-org relay
in cloud-coordinator-request.md; this response adopts NotFound and does not fork
that oracle locally.

## Invariant-to-history and negative-control matrix

S# and V# refer to stable numbered objections in the two Cloud reviews.
Each row runs through the public generic suite. The private driver only schedules
transitions, fault acknowledgments and observations. Every negative control must
return **the exact ID listed**, after successful setup and selected case count >0;
a build error, fixture timeout or unrelated invariant failure is not proof.

| ID / priority / review | Public suite history and required observation | Private production boundary and seeded control (expected same ID) |
|---|---|---|
| HST-01 P0 S1/V3 | Race fork with descendant creation, source/child appends and root publication; enumerate through children_page and read all selected records. Each observation must fit one serializable membership+heads cut, not a mix. Include composed Commit over >=2 domains in one cell and the spawn-shaped envelope (subtree, volume authorities, ForkAdopted), all-or-none. | Pause after capture/before publication; real Command::Commit or coordinated participant publication. Observe domain/generation and applied commit identity. Control omits namespace version validation or splits one selected volume into a helper commit: require HST-01. No unsupported arbitrary sharding claim. |
| HST-02 P0 S2/V1 | Pause an accepted exact key K before terminal publication; inspect Unknown/None, cancel client, retry same digest; release original and lose its ack. Require one effect and original receipt. Changed digest cannot publish while unresolved; successful same digest replays despite conditions becoming false. | Pause after durable Accept/Begin/AcceptFacts/ReserveRoute, before external retention or PublishReceipt; crash owner and replay retained intent. Control treats None as free/reserves a second identity: HST-02. |
| HST-03 P0 S3/V1 | Lost-ack success, drive replicated retention clock >120s (121s and >2 sweep intervals), reclaim, restart/restore/move owner, exact retry; require original digest/commit/receipt and one effect. Changed successful payload is IdempotencyMismatch. Retired logical namespace cannot reopen key as fresh. | Committed fenced Command::Reclaim elapsed_micros, observed clock on all replicas; not OS sleep or client wall clock. Control restores current window-based successful-binding deletion: HST-03. Test late success at old owner after move as well. |
| HST-04 P0 S2/V1; P1 S5/V4 | Definitive conflict leaves no successful binding/effect; revise same still-unspent key, succeed once, deliver old prepare/commit/abort/pin-release messages afterward. Inspect/retry still resolves the newer success; no unlocked stale write or released new hold. | Actual Begin/Stage/Decide/Resolve, PublishReceipt, Coordinate actions and Objects release acceptance; carry attempt generation, authority incarnation/fence and term. Control removes old-attempt generation check: HST-04. |
| HST-05 P1 S5/V4 | Crash before/after each durable prepare, decision, root and receipt publication; restart retained stores. All participants converge on one decision, and read_commit/inspect/root effects agree. Private fenced abort records are permitted; terminal conflict receipt is not. | Command::Coordinate + coordinator::Action and participant Begin/Stage/Decide/Resolve/PublishReceipt; real retained Objects receipt. Control publishes receipt before root decision or drops durable coordinator decision: HST-05. |
| HST-06 P1 S4/V3 | Fork sealed+unsealed source, fork-of-fork, append independent suffixes, retire source, compact/GC; crash and restore snapshots onto another host. Read exact bytes, original record/commit identities and replay bindings from every remaining fork. Keep read/recovery holds while deleting other roots. | BeginSeal/FinishSeal, retained history/frames, Objects accepted retain/release and real collector; stop at durable hold publication/release/delete. Control drops transitive unsealed/shared hold or releases source's last hold unconditionally: HST-06. No local Arc/reference-count proxy for disk retention. |
| HST-07 P1 S6 | Fork p -> p/children/c from pre-commit root with a companion append, existing grandchildren, exclusions children and normalized overlaps; children-suffix remains included. Child is finite, source/child diverge, no inherited destination contents. Invalid escape/empty exclusion and destination collisions yield canonical typed refusal. Retry canonical exclusions yields same identity. | Same Command::Commit cut; SDK canonical path/exclusion/digest preparation. Control ignores exclusions: HST-07; a second control uses post-commit source root: HST-07. |
| HST-08 P0 S9/V2 | Exact-root-only source grant + readable destination must be denied before descendant visibility. Covering source/destination grants succeed; exclude protected branches correctly; textual sibling prefixes do not grant authority. Inheritance never transfers capabilities. | Trust capability/streams::authorize_request and production Edge authorizer/lease with canonical SDK grants; revision/expiry driven through real auth transitions. Control accepts today's exact-path fork check for subtree effects: HST-08. |
| HST-09 P1 S7/V5,V6 | Absent follow at cursor0 gives typed NotFound and no side effect. Create/fork then follow existing inherited path. Race first publication with subscribe: NotFound if first, otherwise valid subscription/replay. Race append/root-only update before/after register/read/park; require wake and gapless applied cursor without a materialized descendant row or timer. | native::Reading::follow subscribe-before-bounds/replay, store row watches plus new namespace/prefix version watch owned by authority lane. Control keeps only exact roots-row wake or registers after read without replay fence: HST-09. |
| HST-10 P1 V5 (S8 P2 delivery detail) | Two existing-path channels on one native or WebSocket session; exhaust A credit while B progresses under sufficient session credit; exhaust aggregate credit separately. Cancel A, revoke/revise its auth lease, attempt stale channel generation; no later unauthorized delivery or leaked interest. | Follows::apply/replenish/acknowledge/pause/handoff, Session::install/revalidate/install_renewal, Access::Lease invalidation, real Edge WebSocket carrier. Control consumes wrong channel credit or ignores revocation: HST-10 (separate runs). |
| HST-11 P1 S7/V5 | Move leader with buffered and lost-ack frames; resume using prior applied cursor on another member, duplicate unacked delivery allowed. Apply each sequence once, preserve commit IDs, no missing record, no post-cancel stale generation delivery. | Actual Raft leader loss/heal plus session placement/handoff and channel preparation; retain subscriber/replay holds across reconnect. Control advances applied cursor before acknowledgment or resumes after next unseen record: HST-11. |
| HST-12 P1 S1,S4/V3 | Vary descendant count with fixed depth/payload shape; fork adds zero payload copy/seal and no descendant walk, holds shared bytes once. Separately record path/exclusion bytes, index spine work and composed participants O(S). Include sparse paged traversal and prefix/glob snapshot queries, selected/unselected concurrent writes. | Production storage/SDK cost counters, not wall-clock scaling alone. Control enumerates/copies descendants or charges shared payload per fork: HST-12. Lazy pagination is not proof that lookup avoided an all-tree scan. |

Tail(0) absence, definitive conflict reuse, changed successful payload and nested
fork exclusions remain ordinary cases in that same suite. SDK-reported six intended
memory/local failures at old b733 are **reported negative evidence only**; this
lane has not replayed them and they are not hosted positive receipts.

## Real hooks and transitions to coordinate, not implement in this review

Reuse shared `cloud_authority::simulation::{rng,network}` and existing service
DST adapters: `svc/streams/src/runtime/dst.rs` uses three real Raft/SQLite replicas;
`native/provider/sdk_conformance.rs` already binds canonical provider conformance.
Reuse the one shared SQLite writer/durable simulation and transport scheduling;
no service-local clocks, network simulators, retry loop, ledger checker or cache.
These storage fault seams are currently crate-private and cfg(test): an Objects integration test cannot call them as a dependency. Extend the existing shared authority-owned private harness route when assigned, never a public fault API or service-local simulator. Current root state is flat heads/chunks, the history tree is per stream, and
current successful replay reclamation starts at >=120s. These are gaps, not an
implemented persistent namespace mechanism. Existing session simulation/lease tests
can contribute deterministic scheduling, not hosted browser/GC receipts.

| Owner | Precise private qualification hook/observation need |
|---|---|
| rx-hostability (Streams); rb-actors (shared Raft) | Return supported transition handles at **durably applied**, not merely submitted, Accept/Begin, AcceptFacts, ReserveRoute, Coordinate prepare/decision, Commit/PublishReceipt and Resolve. Pause/release the real continuation; hold/reorder/duplicate named old-attempt messages through existing transport, then restart retained Raft/store or transfer leader. Report logical authority ID, physical incarnation, attempt generation, domain/root version, term, applied index, publication/receipt identity. Inject production Reclaim elapsed via private test driver and prove the advance is replicated before assertions. For membership use real learner add/catch-up/promotion and removal through existing admission/catalog, not editing membership tables. Restore through actual snapshot install into retained/new member, including interrupted install and fresh replay. Split/move only if production supports that transition; otherwise typed unqualified. Add namespace-version wake registration boundary once the root implementation exists. |
| rb-fsio | Gate actual accepted Objects retention/publication and release/collector deletion, shared unsealed frame creation, BeginSeal/FinishSeal completion, source retirement, durable namespace/root holds and snapshot restore. Record reachable content IDs, durable holds, byte/hash verification, shared-byte accounting and deletion decisions. Drive real GC at an explicit committed retention cut/time: applied removals, no eligible unreferenced generations or due locations, empty reclaiming, physical absence and retained-root readability. Park on applied FinishReclaim/store changes between bounded passes; no graph-wide done event exists. Reuse shared SQLite writer/durability fault hooks for fsync-loss/torn-write/ENOSPC/crash. A fork read after source retirement must load retained bytes on a different host with empty caches. |
| rb-identity | Supply actual covering subtree/deny/protected-path credentials and expiry/revision/lease invalidation using production Trust/Edge. Observe auth decision before effect publication and count forbidden descendant enumeration. Drive native session and canonical Edge WebSocket carrier, independent channel/session byte+frame credits, cancellation/drop and lease revocation/renewal. Existing Follows and session transport own bounds; no second follow protocol or public fault route. |

Fault controls are private in-process test seams or a private isolated Linux
supervisor controlling processes/network/storage. No deployed fault endpoint,
env-mode switch or artificial public API. A pause is not a crash: after a crash,
restore from copied durable bytes and kill outstanding work/old listeners; after
resume, allow only the production recovery logic to decide. Test both cancellation
with durable work completing and actual process death with retained disk.

Reuse test/jepsen/check.mjs and ops/resilience/src/linearizable.rs (existing Stateright search) where the canonical SDK adapter binds it. Its current per-path/per-Actor specs are substrate evidence, not a namespace transaction oracle. HST-01 must group membership, heads and all composed participant effects into one namespace/envelope sequential specification supplied once by the canonical suite; independent per-path linearizability cannot prove an atomic multi-path cut. Extend the shared boundary rather than copying its checker/model into a hosted-only suite. Search timeout/unknown remains unqualified/failing, never valid.
Capture invocation, accepted boundary, completion/unknown, read result, root cut,
key/digest/receipt, fault start/end and applied cursor for each public operation.
Pending operations may be completed or omitted only as the canonical history
checker permits; acknowledged writes must appear. Correlate private facts with
public results; do not substitute private state for public visibility assertions.

## Tiers, bounded cost and receipts

1. **Fast deterministic:** fixed register/read/notify reorder schedules, conflict/
   pending/exact retry/old generation, root cut/exclusions, credit/cancel and
   replicated-clock advance in real production transition adapters. Enumerate
   schedule choices from one seed; record seed and exact replay command. Each
   default case <=2s runtime, aggregate new seed sweep <=60s; suite well under
   SDK's 10s target where possible, compilation reported separately. Measure
   wall and waited CPU on Linux; route cases that exceed 2s to qualification rather
   than enlarging waits or skipping assertions. Only existing events/scheduler
   steps order progress; watchdog timeout detects a stuck schedule, not readiness.
2. **Bounded isolated qualification:** >=3 member local cell on Linux, retained
   stores across hosts, actual SDK transport + native/WebSocket sessions, held
   decisions/lost acknowledgments, kill/restart/restore, membership and leadership
   changes, GC/cold-cache reads, partitions/delay/reorder/duplicate and SQLite
   faults. Initial bound 120s per named recovery history and <=15min campaign;
   abort with failure plus trace if exceeded. Replicated time >120s is injected,
   not a 121s sleep. CPU admission low priority, never compete with merge gate
   capacity. Existing provisioning/gate machinery owners control isolated cells.
   Longer cases `#[ignore = "stress"]` or `*.stress.test.mjs` are explicitly selected
   by qualification; their omission from default is **unqualified, never pass**.
3. **Controls:** isolated scratch source revisions reintroduce each matrix mutant
   into the same production transition, run the same SDK case/checker, require its
   listed ID, restore exact good source, rerun the case. Keep no production bug
   switch/compatibility path. Store good/mutant hashes, build/setup status,
   nonzero selected counts, invariant violation, seed and trace. Mutating only
   an oracle/mock, or showing unrelated timeouts, does not count.

Every receipt names all required IDs, backend/carrier, topology, service/SDK
versions and image digests, seed/schedule, invocation history, fault/time/GC
acknowledgments, cut/domain/generation, expected/observed failure ID, replay
command, wall/CPU cost and public post-recovery observations. Retain raw logs.
A typed capability result in the canonical SDK receipt must distinguish
**qualified / unqualified(reason, missing invariant IDs) / failed(invariant,
trace)**; exact enum spelling belongs to SDK, not a Cloud duplicate. Unsupported
namespace root, lifetime binding, restore/move, auth hook or browser carrier must
be unqualified. A run with missing required IDs, zero cases or skipped cases
cannot emit the complete hosted capability receipt. A real invariant violation
is failed, not unsupported. No memory fallback.

## Deletions and next handoffs

Delete superseded oracles in the same implementation change: None=>free-key;
missing-follow waits; universal no-private-journal-on-conflict; exact-root-only
fork authorization applied to subtree effects; descendant-copy correctness as
proof of O(1); root-pointer/local-Arc retention as durable closure evidence;
exactly-once-socket and simultaneous cross-channel delivery assertions. Keep
private transaction recovery checks and N-1 rolling decoders/fixtures; those are
not obsolete SDK-agent coordinator code. Historical receipts retain old pins.
Delete duplicate service-local fault/clock/checker helpers when replacing them.

This response assigns no new service implementation. Direct owner steers ask
for hook availability/source-backed plans and missing boundaries; rx-qualify
will implement shared schedules when the assigned production changes exist.
SDK suite/oracle changes remain the existing coordinator cross-org relay with
rb-sdk integration; no direct cross-org edits. Hosted capabilities above all
remain **unqualified** until positive and intended negative receipts execute.

## Owner responses incorporated (01:29Z)

The topology change in rx-hostability/steer.md now makes rx-hostability the
Streams A1/A2/A3 owner across repos and rb-fs the A5 owner; rb-actors remains
shared Raft/Actors. This supersedes the earlier review's namespace assignment
and the broad part of my first rb-actors steer. No second namespace implementation
is requested. Shared authority scheduling/ingress lifecycle coordination stays
with rb-actors; namespace, attempt-generation and replicated lifetime replay
hook requests go to rx-hostability, fs composition to rb-fs. This response itself
remains plan only and does not authorize implementation by rx-qualify elsewhere.

rb-fsio's source-backed hostability-hooks.md provides the storage plan:

- Existing catalog consumer claims/child edges (`catalog/consumer.rs:190,304`,
  `catalog.rs:513`), fenced consumer execution (`serving/consumer.rs:51`), Retire
  (`catalog.rs:1413,1455`), BeginReclaim -> actual physical delete/absence check ->
  FinishReclaim (`serving/maintenance.rs:556`) are the real HST-01/06 boundaries.
  Named private combined pause handles do not exist yet; do not claim availability.
- Real collector completion requires applied removal at the explicit retention cut/time, no eligible unreferenced generations/new due locations, empty reclaiming/backlog plus physical
  absence assertions and retained-root readability, with bounded pass count;
  neither a timer nor one zero-retired tick proves completion. Include eventual
  reclamation of eligible closures after the final owning hold is released.
- Existing cold backend, pinned-reader/restart and restore tests are substrate
  evidence (`authority_quorum/maintenance.rs:255,546,606` and restore modules),
  not another-host namespace proof. Shared authority Fault/arm/durable images
  remain private to the authority crate. New namespace/recent closure hooks,
  empty-cache other-owner restore and combined per-intent counters remain unqualified.
- HST-06 control variants: omit real fork/read/recovery/unsealed holds before actual
  GC, omit a recent object from restore (admission must refuse), deliver old release
  after a new exact claim. HST-12 counts unique content IDs/bytes, new pages,
  source sealing/re-encoding, nodes visited and publish/retain calls. Count work,
  not merely elapsed latency. Reuse the shared production accounting harness.

Source for this availability update:

rb-identity/hostability-response.md is now incorporated for HST-08/10/11:
require covering source Read plus destination Fork/Commit at every conjunctive
capability layer, bind canonical exclusions and pre-commit source cut into the
intent, and recheck locally committed policy/revocation generation and ownership
lease at publication. Check cancellation/revocation between preparation and
publication, plus invalid direct-ingress credentials. During partitions require
last-applied valid policy/revision semantics, not instantaneous global revocation;
unknown revisions deny. Bound total chain bytes/links/grants/path work under the
existing canonical limits; use native and existing Edge SessionFrame WebSocket,
never a second follow carrier. Typed private hook handles still need the owner;
the response is a source-backed plan, not executed endpoint evidence.
For HST-03 first establish a committed settle/sweep boundary, then advance the
actual replicated elapsed clock past 121s and another sweep; merely changing a
client timestamp or a single first sweep cannot demonstrate reclamation resistance.


rb-actors/hostability-response.md now confirms the shared boundary inventory:
public simulation Network/Schedule/exchange/rng; production SqliteStore
changes/watch_row/checkpoint/restore_checkpoint; private real-cell World
start/crash/wipe/settle_membership/transfer; private storage track/arm/power_loss/
Image::restore. Observe applied index and explicit fsync/durable image separately;
a change notification alone proves neither a selected command nor its fsync.
There is still no callable dependent-crate application Accept/Commit pause or
combined namespace/storage-fault route. rb-actors owns that shared route when
allocated; rx-hostability owns application phases. Combined HST-01/06/12 remain
unqualified. No competing ingress-start event was found in their inspected WIP
9784e29971, confirming one shared implementation for the separate readiness fix.

HST-12 cost collection must plug into rx-perf's existing benchmark/accounting
harness and shared resource budgets. The generic suite checks counted bounds;
it does not add a second benchmark runner or service-local metrics ledger.

