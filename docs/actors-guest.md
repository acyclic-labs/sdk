# Authoring an Actor guest

`acyclic_actors::guest` owns ABI v1. TypeScript declarations in
`@acyclic-labs/actors` are generated from those Rust declarations. This surface
defines authoring hooks only; the actor runtime supplies execution and admission.
The existing `scripts/generate-actors.mjs` generation and drift check includes
this surface alongside the RPC declarations.

Rust authors use `define_actor(ActorDefinition { restore, event, checkpoint,
fetch })`; TypeScript authors use `defineActor<State, Bindings>({...})`.
Export the lifecycle hooks as `actor.restore`, `actor.event`, and
`actor.checkpoint`, and expose the HTTP hook as `default.fetch`. The typed hooks
take explicit state; a host adapter retains that state between calls.

`restore(checkpoint, context)` produces state from either an absent checkpoint
or artifact-owned bytes. `event(state, event, context)` handles the single
`GuestEvent` union: stream subscription record, Invoke, fetch, timer, or app-only
socket event. `checkpoint(state, context)` captures logical state after admitted
handlers quiesce. `fetch(state, request, context)` is the HTTP adapter for the
fetch event. Portable requests/responses preserve ordered header pairs and byte
arrays; the host converts its native HTTP objects at that boundary.

Every hook receives the same `GuestContext<Bindings, State>`: scoped `env`
handles, an injectable Unix millisecond `clock`, a stable `deliveryId`, artifact
`codec` hooks, and `retryKey(effect)` (`retry_key` in Rust). Clock results are
`u64` in Rust and `bigint` in TypeScript. Restore and checkpoint have no delivery
identity and cannot derive effect keys. Hosts supply the same delivery identity
on every retry. The retry-key format length-prefixes the UTF-8 delivery identity
before the effect name. Arbitrary external services still need their own
idempotency support; there is no cross-service transaction guarantee.

Stream fields preserve the Cloud bridge projection: decimal u64 sequence and
commit timestamp, base64 value, and hexadecimal commit identity. Timers arrive
through idempotent appends to the target inbox, keyed by timer ID. The harness
uses StreamProvider and fs/volume only; socket events are for apps.

Each checkpoint carries its artifact codec name/version and opaque bytes. The
candidate codec must explicitly list its own identity and every older format it
can decode. `GuestInstallation::update` validates compatibility and decodes the
retained checkpoint before changing configuration or subscriptions. Rejected
compatibility or decoding preserves all three installed values exactly. Accepted
updates retain the original checkpoint until a new capture commits. The host
must fence and publish the resulting boundary atomically under its authority.
Public `CheckpointActor` requests a host capture; it does not select a codec or
permit callers to replace artifact state. Public Invoke enters the invoke event;
HTTP requests enter fetch. This contract adds no service RPCs.

The `guest_contract` property suite checks serialization round-trip, retry-key
injectivity, successful migration and preservation on incompatible or failed
decode. The parser fuzz target is `fuzz/guest_json`, with a checked seed corpus;
run `cargo fuzz run guest_json -- -max_total_time=5` from `fuzz/`.
