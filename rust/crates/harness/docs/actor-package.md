# Harness Actors package contract

`package` is the hosting boundary for one versioned harness code package. An agent
is an actor running a pinned `ResumableMachine`. Installing the package is one
`ActorRuntime::install` call. Local and deployed hosts install the same package;
neither a `HarnessService` nor a gateway is part of this contract.

The manifest declares its agent implementation, stream namespace, volume,
inbox subscription, checkpoint/outbox location, and model HTTPS, compute and
secret bindings. Paths are relative to the declared stream namespace. Each actor
owns `agents/<id>/inbox` and `agents/<id>/workflow`; the latter contains the
existing workflow admission and atomic checkpoint/outbox records. Only inbox
append/follow wakes the handler; workflow follows observe ownership and cancellation
changes. Clients append input and follow streams or volume
publications. Agent communication uses stream transactions, never actor sockets.

## Admission and transactions

The runtime authenticates every event using the runtime-provided workload
capability, checks audience, expiry, revocation and bindings, then admits it to
the inbox. The runtime owns the root signer; the package receives only attenuated
authority. Capability grants intersect volume policy, with deny winning. These
are host requirements, not authority implied by constructing a Rust value.

An event has an immutable operation ID and idempotency key. An identical retry
returns its original result; reuse with different input fails. One bounded,
deterministic handler step consumes one input. Its input cursor, next checkpoint,
status and complete command outbox commit atomically under the placement fence
and expected revision, using the Streams provider transaction. A rejected commit
publishes none of them. Immutable volume payloads may be staged first; their
publication reference joins the transaction. No handler performs external I/O.
Terminal transitions emit no new effects and settle all earlier outbox commands.

After commit, the runtime dispatches outbox commands through explicit bindings.
Model HTTPS and compute are long-lived effects, outside the handler. Model chunks
are appended individually as accepted, without application batching. Results are
inbox events using stable command identities. A retry reconciles an uncertain
external effect before dispatch; it must not claim exactly-once external I/O.
Accepted visibility and confirmed durable watermarks remain distinct: effects
require a durably committed outbox. A memory provider's durability lasts only for
that provider's lifetime and does not promise survival of process death.

## Cancellation, placement and recovery

Cancellation is a durable inbox event. The runtime fences effect dispatch, stops
and drains the actor's live effect tree, then records cancellation settlement.
An old owner or late result cannot publish after settlement. Dropping a client
does not cancel actor work; explicit cancellation and runtime shutdown do.

Placement has a monotonically increasing epoch. Every event commit and effect
publication checks it. A portable volume can move; a node-local volume pins the
actor and compute to its owning node. Loss of that node yields unavailable until
it recovers, never a silently empty replacement volume. Ownership transfer fences
the old owner before the new owner resumes effects.
A durable move intent precedes that irreversible fence, so a failed or interrupted
transfer can resume without reviving the old epoch.

Recovery restores the pinned machine, checkpoint, consumed inbox cursor and
pending outbox from provider history. It replays deterministic transitions and
reconciles pending effects by command ID. A handler crash before commit retries
the input; a crash after commit resumes the outbox without rerunning the handler.
No harness-private journal, timer polling or socket transport substitutes for
Streams/fs. Subscriptions use push follow, with accepted/durable cursors.

## Bindings and qualification

The model binding names an HTTPS origin and a secret binding, never a key value.
The runtime resolves secrets at dispatch; neither checkpoints nor diagnostics
contain secret bytes. Compute names a runtime-managed executor, not an implicit
host shell. Both dispatch and recovery use the same bindings and placement fence.

`package::conformance` is the shared driver boundary: every host must run the same
event history and compare checkpoint/outbox observations, including retry,
conflicting retry, cancellation, stale placement and recovery cuts. Constructing
a manifest or implementing a trait alone is not runtime conformance.

`package::in_process::InProcessRuntime` installs one package over a supplied
Streams provider and reuses `DurableWorkflowHost` for deterministic execution.
The embedding runtime polls `run` independently of attached clients. Its host
must supply `AdmissionPolicy` and `EffectHost`; these hooks do not themselves
implement capability verification, secret resolution or native effect custody.
The memory provider reports a zero durable watermark, so this host rejects
effect dispatch, cancellation fencing and ownership transfer there. Disk-backed
qualification uses `LocalStream` and its confirmed durable watermark.

The runtime's package tests exercise a shared history on memory and local-disk
providers, plus interrupted inbox, outbox, cancellation and placement recovery.
GraphCoder and `LocalHarness` still use their existing native composition;
migrating those entry points through the package remains required.

SDK inventory (2026-10-22): `rust/crates/actors` contains the client and guest ABI,
but no server binary; no local-cloud server binary exists under `rust/crates`.
The counter tests in `harness/tests/local_cloud_actors` require an external full
local cloud. Deployed harness qualification therefore remains a gap; this SDK
lane does not build cloud code.
