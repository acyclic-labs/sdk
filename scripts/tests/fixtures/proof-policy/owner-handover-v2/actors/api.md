# A4 Actors: canonical runtime contract

Codex, 2026-10-26. Base: origin/main b7333953cbd5d3b31adc05cf2f0851f049f6c16a. Implementation branch: codex/b-runtime-actors-wave1 (unpublished). No redesign PR or runtime proof exists yet. PR1512 is the separate, frozen urgent wire/secrets predecessor; legacy localhost/fork work is preserved unpublished at169c02ee53 and is not an A4 implementation.

## Public API sketch

Extend acyclic_actors::guest, reusing its Checkpoint, CodecIdentity, GuestEvent and code identity. Do not introduce another harness guest ABI.

```rust
pub trait Handler: Send + Sync {
    fn manifest(&self) -> &Manifest;
    fn on_record<'a>(&'a self, delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>) -> GuestFuture<'a, Transition>;
}
pub struct Manifest { /* artifact identity, subscriptions, checkpoint codec, explicit bindings */ }
pub struct Transition { pub checkpoint: Checkpoint, pub alarm: AlarmChange, /* outbox appends */ }
pub enum AlarmChange { Keep, Set { deadline_unix_ms: u64 }, Clear }
// Durable state also contains the consumed cursor, epoch, alarm generation and pause state.
pub struct local::Runtime<P, A, O> { /* StreamProvider + Fs<A,O>; shared ready/due index */ }
// install(package), place(address, epoch), append(address, intent), pause/resume,
// step_ready(now, work_budget), hibernate(address), inspect(address), serve(bind).
// A native package and a WASM execution adapter supply the same Handler contract.
pub async fn conformance<P: ActorProvider>(provider: &P) -> Result<Report>;
// actors-conformance reads ACYCLIC_CONFORMANCE_ENDPOINT and optional credential env.
```

The exact constructor/signatures will be pinned as code lands. The existing domain namespace/subscription and addressing types remain canonical; no private invocation gateway. The local service implements canonical Actors RPCs and binds canonical Streams/fs services, with explicit account/namespace/id routing. Native invocation and hosted clients implement one ActorProvider contract. WASM uses the same checkpoint/event encoding; no embedded second scheduler.

## Invariants and cost bounds

- One StreamProvider commit CAS fences the actor epoch and publishes checkpoint, consumed cursor, alarm set/clear generation, and outbox appends. No success receipt before commit acknowledgement. Crash/retry may rerun a physical handler, never duplicate a committed effect.
- Every alarm replacement/clear advances a durable generation; old queued generations cannot publish or consume a replacement. Pause persists, preserves queued inputs/alarms, and forbids execution until resume.
- The production ready primitive registers before replay and rechecks the durable tail before hibernating. Append racing registration, empty replay, pause/resume or cancellation cannot lose wake. Loom uses this actual primitive, not a model copy.
- Idle actor metadata is O(1); no per-actor timer, task, socket or pending future. One bounded ready queue and one shared due index drive ready work. Hibernation releases active heaps/subscriptions; durable input remains the source of truth. Due/ready work scales with due/ready actors, not population scans.
- Subscription cursor advances only in the checkpoint commit. Allocation, epoch and original cancellation custody remain at shared provider boundaries. No new retry loop, polling coordinator, lease clock, or OS-specific state machine.

## Proof plan and current delta

Public provider-generic histories run against memory, durable-local and arbitrary endpoints. Crash-cut histories assert the checkpoint/cursor/alarm tuple; takeover and generation histories reject old owners/alarms. Actual negative controls split the atomic transition, omit epoch/generation fencing, and remove register/recheck. Runtime resource counters cover dormant populations through100000 actors. All selected suites must finish under10s; every new Linux job is affected-crate-only and under600s. No current positive/negative receipt or signed A4 source pin exists yet; the next measurable checkpoint is the public types plus transactional storage oracle on memory/local providers. A proof-ready/merge ETA is not supportable before that checkpoint and the hosted/Streams dependency review.

## Deletes and ownership

A4 is the only scheduling/checkpoint/alarm implementation. Wave2 migrates the harness and deletes coordinator, worker_tick, legacy timer schedulers, bespoke harness gateway and the predecessor harness package runtime; this PR does not port the harness. Existing guest authoring and alarm types must be extended/adapted rather than copied. Native/OS adapters remain thin.

Streams A1-A3 is owned by the Streams lane; consume its existing commit/follow contract now and its merged fixes when available. Filesystem A5 is fs-concurrency-owned. Auth A6 is b-authority-owned. Main-green owns retained parent/content authority and legacy publication cleanup; do not duplicate those changes in A4. Missing exact dependency PR/source pins are being requested directly. Hosted execution is Cloud-owned; its review belongs in redesign/cloud-review/b-runtime.md. No Cloud code or credentials are read.
