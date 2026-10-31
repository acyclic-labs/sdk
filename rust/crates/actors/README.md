# acyclic-actors

Rust-owned Actors v1 public contract. An Actor has an identity separate from
Streams, an immutable code version, explicit bindings, and independently
recoverable subscriptions. Adding a subscription selects its start once;
delivery and safe cursor advancement are service-owned. Pausing follows a
bounded handler failure, and explicit resumption can replay external effects.
Code and bindings are updated by a full configuration CAS. Checkpoint schema
compatibility or migration is checked before activation; a failed update keeps
the previous version active, and paused subscriptions remain paused.

Deployment and update requests can carry `EntrySource`: an exact selected
immutable Objects digest, descriptor length, environment and entry. Its entry
must match the deployment namespace or Worker name. It identifies the complete
selected module closure, while the original account and resource grants remain
the only authority. Services persist it in the same accepted configuration as
the code digest and preserve it in deployment and actor observations, including
restore and idempotent reconciliation. An update without a source explicitly
removes that selection. The shared Workers entry loader verifies observations
through the original Objects provider; this crate introduces no execution or
storage path for attachments.

The service owns execution, fencing, checkpoint storage, and hibernation. This
crate validates customer-authored requests through its typed semantic domain
(`ActorId`, `CodeSha256`, `PositiveU64`, finite states, nested records, and
presence-checked requests) before a request reaches either transport. Native
N-API and browser WASM adapters carry only bounded protobuf bytes; they decode
into that same domain and encode responses only after fallible semantic
conversion. The package descriptor is generated from the maintained Rust
contract declarations and is used to generate TypeScript bindings.

`grpc::connect(endpoint, token)` exposes every generated Actors service RPC.
It trusts the platform's roots (on Linux, those `SSL_CERT_FILE` and
`SSL_CERT_DIR` select). Use `grpc::connect_with_ca_certificate` for a
caller-supplied private CA; that CA is then the whole trust.
`http::Client::new(endpoint, token, maximum_response_bytes)` exposes the same
nine operations using canonical Protobuf JSON. Invocation carries request and
response headers. HTTP mutations are not automatically retried.

Local runtime pause/resume and dispatcher checkpointing each submit at most one
conditional publication. A competing owner returns `local::Error::Conflict`;
the call does not reload state and propose again. Dispatcher startup also
returns that original result. Restoration advances through one hierarchy
version in pages; `HierarchyChanged` ends that traversal. The caller can
explicitly restart after observing either refusal. The running dispatcher
returns these failures to its owner, and unknown acknowledgements retain the
original publication and participant custody.

## Guest streaming fetch

`GuestStreamingResponse` carries `GuestBody`, an HTTP body that preserves data
frames, terminal trailers and cancellation when dropped. Existing buffered
bytes and lazy chunk streams remain supported without collecting the body.

JavaScript `ActorDefinition.fetch` accepts a WHATWG `Response` or
`GuestResponseStream`: status, initial headers, a lazy body, a terminal
`Promise<Headers>` and the original `close` callback. `GuestActivation` retains
that response unchanged. `openGuestResponse` exposes one lazy frame consumer
for local and hosted transports. The transport must publish trailer frames
after data and close the original response on disconnect or failure. A failed
body or trailer promise is an error, never a successful empty trailer result.
Ordinary responses terminate with empty trailers. This extends the existing
fetch boundary; it adds no manifest grant or authority.

## Deployment secret selections

Actor and Worker deployments carry an ordered `secrets` list of `{name,
reference}` declarations. Namespace configuration updates carry the same list.
The list is a full replacement: an empty list selects nothing. Bindings and
secrets share one environment-name namespace; duplicates and collisions are
rejected by the canonical Actors admission function.

Manifest lowering resolves only names explicitly listed in `entry.secrets`
against `environment.secrets`. Declaring a secret in an environment grants no
entry access to it. Both runtime kinds use this same lowering function.

References use the bounded `secrets:/scope/name` grammar. They are addresses,
never plaintext, versions, bearer tokens or unwrap credentials. Resolution and
authorization belong to the deployment host; SDK lowering never reads a value.

### Mixed rollout

`SecretSelection` uses new message fields 1 (`name`) and 2 (`reference`).
DeploymentRequest appends field 12; UpdateNamespaceRequest appends field 7.
Existing field numbers, wire kinds and meanings are unchanged. Writers never
encode selections as bindings, reuse predecessor fields or send credential
values. Empty lists emit no new fields.

The retained predecessor descriptor was captured from SDK main `b7333953cb`.
Executable tests decode new messages with that actual reader and verify that
every known predecessor field is unchanged. They also decode predecessor
messages with the new reader and require empty selections.

Unknown-field compatibility does not make an old deployment host resolve
secrets: it ignores the new fields. Deploy hosts that support selections before
enabling nonempty-selection writes. A host must authorize and resolve every
selected reference before accepting a deployment; it must reject an unsupported
selection rather than acknowledge a deployment with missing environment values.
There is no fallback to environment inheritance or plaintext bindings.

With the `conformance` feature, downstream hosts implement
`acyclic_actors::conformance::Backend` and register their async setup function
with `conformance::suite!(MyBackend::fresh)`. The backend retains disposable
storage, a running host loaded with the shared `Package`, its clock, pause/restart
controls, and a `RequestLedger` instrumenting the actual provider dispatch.
`verify(&backend)` drives the same ordered delivery, alarm, pause and restart
vectors for every implementation. Existing `Target` implementations use the
same trait through its compatibility alias.

`conformance::bench(&backend)` is an on-demand workload over a fresh backend.
It excludes setup and warmup, verifies all 256 deliveries, and returns elapsed
time plus data/control request counts. A downstream service can instrument
physical dispatch with the same ledger; counts at an API wrapper boundary alone
do not establish physical network traffic. No benchmark is scheduled by CI.

### Reservation transitions

`reservation::Book::step` validates the request, accounts for due grants and
encodes its decision before changing the book. An error leaves every grant,
counter and generation unchanged. An exact live grant can still be released
when the generation counter cannot admit another grant. The transition visits
due grants and the addressed grant; it does not copy unrelated grants. The
existing tree lookups and whole-checkpoint package encoding remain.

### Finite delivery

`local::Runtime::drain` drives ready actors concurrently through the same
delivery and active-admission mechanism as continuous and event hosting. It
retains at most `RuntimeLimits::active` executions; dormant actors own no task.
Dropping the drain cancels its owned executions and releases their admission.
Unconsumed inputs remain durable for the caller's next explicit wake.

### Invocation failures

`local::Store::invoke` and `wait_invocation` return `Error::InvocationFailed`
when the guest rejects a committed, explicitly addressed invocation. The error
retains its original typed `GuestError` and an `InvocationReceipt` proving that
the input cursor and failure reply committed together. The failure keeps the
previous checkpoint, metadata, journal and alarm; failed-attempt effects are
discarded. Retrying the same invocation, including after reopening storage,
returns that original cause and receipt without running the guest again.

Failure uses the same conditional publication as a successful response. It adds
no commit or durability barrier. Cancelled attempts and ordinary unmarked input
failures remain queued and retain the runtime's existing error behavior. Provider
errors keep their storage or unknown-outcome classification; an absent reply
after proven consumption is an invalid publication, not a successful response.

### Guest durable state binding

`domain::Binding::actor_state(name)` selects the executing actor's own state.
Its public declaration is `stateTransaction: true`, with no capability,
resource, target account, namespace or Worker target. Workers cannot select
this kind. Every selected name for this kind must share one admitted owner,
epoch, state cut and pending transaction group.

JavaScript hosts use `materializeActorState` with the public `ActorStateHost`.
The host loads `seed` from `local::ActorStateBinding::seed`, applies each
`ActorStateCommit` through that same native binding's `apply`, and resolves
`commit` only after its original `flush` confirms durability. The host supplies
and retains the original invocation/flush identity; guest packets carry no
provider, actor, placement epoch or publication key. A hosted runtime must use
this same materializer and native producer, not a private guest storage API.

`transaction` runs synchronously. Its borrowed `get`, `put`, `entries`, journal
append and alarm operations expire at callback return. Failure rolls back the
callback, preserving any earlier pending transaction group. Values are copied
at the JavaScript boundary so later caller mutation cannot alter admitted work.
Native read-only transactions borrow the admitted in-memory cut, allocate
nothing and leave the binding clean; flushing a clean binding performs no I/O.
`ActorStateBinding::get` borrows a value directly from the same cut, including
staged writes, without creating a mutation transaction or cloning a byte handle.
Both read forms preserve the binding's existing fence and pause checks.
Both projections roll back only touched keys and the journal's original length;
native rollback also handles callback unwind. Neither copies unrelated metadata,
pending writes or staged journal frames on the first write. JavaScript still
copies byte values at its guest boundary; explicit enumeration visits its output.
`flush` publishes all pending metadata, journal frames and alarm in **one**
conditional Stream commit against the original actor head and journal tail.
Only its confirmed result acknowledges durability. Failed/unknown publication
fences the binding; an unknown result retains its original PreparedPublish and
never dispatches the guest callback again. The delivery/checkpoint owner takes
the updated Snapshot through `finish` before its next normal publication.

When state and the delivery's checkpoint have the same durability boundary,
use `ActorStateBinding::prepare_publish` instead of flushing first. It joins
metadata, journal, alarm, consumed input, checkpoint, response and effects in
the original conditional commit. `Store::submit_retained` returns the confirmed
authored Snapshot without a reread or decode; `Store::claim` likewise returns
its confirmed placement cut. The original owner and exact-head fences still
apply. A retained cut is a snapshot, not a fresh observation of another owner.
`Store::observe` performs the required fresh head observation before handler
admission. An unchanged tail and commit anchor retain the original typed state
without decoding it again; a changed cut is decoded before delivery continues.

Metadata is retained with the actor checkpoint/epoch/alarm state. Journal frames
stay in the canonical `<state-path>/journal` stream and are read in requested
pages bounded by the journal tail in that same committed state cut. Later owner
epochs cannot extend an older cut's replay. Missing frames are errors, not EOF.
The journal is opaque persistence: the existing Stream journal engine retains
all command interpretation. Actor alarms use the existing generation fence and
are delivered as `GuestEvent.alarm`, without a guest timer.

The local state record is explicitly `AST` version 1, with an invalid-protobuf
prefix that makes older unversioned decoders refuse it. Its reader rejects
unversioned records; there is no migration or old-writer fallback. Adoption
requires a fresh state root/data generation. Metadata and the journal cut cannot
be silently discarded by an older decoder. Older deployment validators also
reject the new binding kind because its external capability/resource are empty.
Hosted adoption must deploy the new reader/materializer and make its explicit
preproduction generation reset before admitting the new kind or promotion.

Harness EventStorage/streamJournal adapters compose this primitive; no harness
logic, signing key or second StreamLedger belongs in Actors. Objects grants keep
their original immutable Objects authority. Platform authority admission and
trailer-preserving fetch are separate public contracts, not implicit effects of
this state binding.
