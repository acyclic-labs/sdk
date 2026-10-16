# Authoring an Actor guest

`acyclic_actors::guest` owns ABI v1; `scripts/generate-actors.mjs` emits and checks
the TypeScript projection. This package declares guest hooks and shared-state
mutation; it does not implement actor scheduling, storage, sockets or HTTP serving.

Export `actor.abiVersion = GUEST_ABI_VERSION`, `actor.codec`, `actor.restore`,
`actor.event`, `actor.checkpoint`, and `default.fetch`. Rust authors use
`define_actor(ActorDefinition { abi_version, codec, restore, event, checkpoint,
fetch })`; TypeScript authors use `defineActor<State, Bindings>({...})`.
Hosts negotiate the artifact version before restore or update admission, reject
unsupported versions without changing the installed boundary, and project only
events defined by the negotiated version. Inbound structures ignore added fields;
unknown event discriminants decode to `Unknown` and may be ignored by old guests.
The event/socket enums and context are non-exhaustive Rust APIs.
SocketCloseInitiator and SocketTransport have closed v1 value sets; new values
require a new negotiated ABI. Hosts must project supported values for v1 guests.

The artifact exports its codec identity (name/version), readable identities and
encode/decode hooks as `actor.codec`. It is absent from context. Checkpoints carry
that identity and opaque `Bytes`/`Uint8Array`. An update checks codec compatibility
and decodes prior bytes before replacing configuration or subscriptions.
`GuestInstallation::update` preserves configuration, checkpoint and subscriptions
on incompatible identity or decode failure. Successful updates retain original
checkpoint bytes until a new capture commits. Hosts must fence and publish the
boundary atomically. Public CheckpointActor requests a capture, not a codec change.

Every hook uses `GuestContext<Bindings>`: manifest-owned `env`, an injected clock
(Rust u64/TS bigint Unix milliseconds), stable nullable deliveryId, retryKey(effect),
host-supplied admittedIdentity, and a separate runtime-owned `actor` namespace.
Construct Rust contexts through `GuestContext::builder`. Host metadata
uses `AdmittedCaller::new`, `CallerCapability::new` and `RuntimeHandle::new` for
validated construction without serde. These check representation, not authority.
The identity projection contains accountId, orgId, principal, credentialId, actorId, expiry and an optional
versioned opaque capability handle for S1 forwarding/attenuation. It is neither
a bearer token nor a second auth chain. The host authenticates it; public request
headers cannot install it. It is null for Stream, timer, restore and checkpoint.
The runtime namespace reserves actor id/class and optional versioned handles for
sockets, auto-response, alarms (setAlarm/schedule), SQL over the fs volume, and
waitUntil. Their implementations belong to their feature/runtime owners, not env.

Restore produces logical state. The host wraps it in `GuestState`; event, fetch
and checkpoint receive shared handles. `state.with` permits only a synchronous
mutation and releases its mutex before returning. Rust cannot borrow that state
across an await; TS callbacks must return undefined, not a Promise. Hosts implement
the same synchronous operation and reject nested operations. Lock order: no nested
state mutation locks, even for distinct states; a checker rejects nesting before
locking. The actual Rust mutation path is exercised by loom with two handlers and
an intervening yield, plus same-state and distinct-state lock-order violations.
Cloud orders handlers per subscription only. Other subscriptions and Invokes can
interleave across awaits; a read before an await does not isolate a later mutation.
Capture happens only after admitted handlers quiesce. Handler failure discards the
heap and restores the committed boundary.

One event union covers Stream, Invoke, fetch, timer and app-only sockets. Stream
fields retain the Cloud projection: decimal u64 sequence/time, base64 record value
and hex commit ID. Retry keys length-prefix the UTF-8 stable delivery identity;
lifecycle hooks without a delivery cannot derive keys. Rust returns errors; TS
retryKey throws for a missing delivery (restore/checkpoint)
or an empty effect name. Arbitrary external effects
still require service-specific idempotency. Timer fires are inbox appends keyed by
timer ID, never Invoke. The harness uses StreamProvider and fs/volume only.

Socket Open requests include offered subprotocols, allowlisted headers, application
channel, resume position and reconnect diagnostics. The enclosing event supplies
connection identity. Return SocketAccept(subprotocol, attachment, tags) or
SocketReject(status, reason). Message uses text or binary payload; Close includes
offset and peer/actor/runtime initiator; Error includes offset, code and message.
Close/error advance application delivery offsets. Ping/pong, channel-0 keepalive,
keepalive templates and auto-response matches never wake guests, produce events or
advance those offsets. Only events delivered to event count as messages_in/wakes.

TS fetch uses native WHATWG Request/Response, including ReadableStream bodies and
SSE. Rust fetch uses `http::Request<GuestBody>` and `http::Response<GuestBody>` with
buffered Bytes or a lazy fallible chunk stream. Portable buffered GuestRequest and
GuestResponse remain available with into_streaming adapters preserving duplicate
headers and byte allocations. Hosts own backpressure and cancellation on drop.

Focused verification: guest_contract properties/corpus/drift; loom state models;
Actors check:types (included in root check:types); and debug cargo-fuzz guest_json
with checked seeds (`cargo +nightly fuzz run --dev guest_json -- -max_total_time=5`).
