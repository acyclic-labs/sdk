# Exact revocation and credential custody

This document records implemented guarantees, their measurements and the
integrations still required. It does not qualify a deployed service.

## Exact revocation

`RevocationDelta` carries only changed key/block identities and an optional,
explicit `revoke_all_before_epoch`. Neither index size nor publication size
can change a targeted revoke into an epoch change. Keys are account-scoped
and must never be reused. Revoking a certificate key or parent block covers
its descendants through the existing verifier's complete ancestry check.

Publications have a versioned signing domain and use an independently trusted
account issuer key. A delta must be consecutive and name the previous signed
publication digest. Validation completes before mutation. A checkpoint must
advance the sequence, retain every still-live revocation at least as long as
before, preserve the epoch floor, and advance its retirement time monotonically.
The retirement bound comes from the issuer's authority lifetimes, never the
requesting holder. A forged, gapped, stale or malformed publication cannot
partially change state.

Account trust now carries the earliest supported verification time. The
verifier applies that floor to both fresh authentication and retained evidence.
After retirement, asking a current trust view to authenticate at an earlier
time refuses rather than resurrecting a retired revocation.
Delta publication time is distinct from the retirement floor. A targeted delta
preserves unrelated historical admissions while applying current revocations
to the exact affected key or block at every supported verification time.
A complete checkpoint can retain the same retirement floor after a newer
delta. It preserves publication time independently, so compaction neither
retires unrelated history nor permits a backdated next delta.

The owner must atomically persist the accepted frontier and state before
acknowledgement, retain them across restart, and authenticate the current
bootstrap checkpoint independently. A signature alone does not prove that a
checkpoint is current. The SDK core provides an in-memory exact index; it does
not yet provide that durable producer or a paged checkpoint transport. Its
guarantee follows the owner's accepted local publication order, not immediate
global revocation during a partition. `RevocationEffect` distinguishes exact
delta invalidation, explicit epoch invalidation and checkpoint replacement.

## Credential custody

Rust `CredentialProvider::resolve` returns `ResolvedCredential`, preserving
the exact cached snapshot rather than flattening it into an unrelated string.
The request retains the lease through its response. A late rejection can only
invalidate the snapshot that request used. Source errors, policy denials,
transport failures and cancellation do not become remote rejection verdicts.
Actors exposes an explicit remote-rejection diagnostic for its JS bridge.
Both the N-API projection and WASM error normalization carry that diagnostic
to the default JS Actors client, where it invalidates the retained request
lease. The installed native and Chrome conformance cases exercise those
actual bindings; their new rejection controls still await qualification.

The JS Auth credential module shares the existing Rust cache's validity model:
one source partition per account/audience/auth context, a trusted nonblocking
revision/clock observation, absolute half-open expiry, one refresh per cohort,
no fallback, and exact rejection. A caller cancellation releases one interest;
the last cancellation aborts the source and fences late completion even when
the source ignores abort. A watcher must publish a changed or unavailable
revision before changed credentials can be reused. Raw callbacks remain fresh
per request when no trusted validity is available.

`ResolvedCredential::binding()` and JS `CredentialRequest.binding` project the
custodian-supplied revision and absolute expiry without exposing the bearer.
Raw callbacks return no binding. Converting a cache into a provider and wrapping
it with `credentialProvider` preserve this metadata and exact rejection custody.
Providers backed by a trusted cache expose `watch`: register before consulting
source-bound reader state, compare the admitted request's binding, and retain
its validity check through dispatch and result admission. A notification cancels
outstanding work and retires cached data before subscribing again. Custodians
must publish changed or unavailable source validity before `invalidate`; exact
server rejection also notifies, while rejection of an older lease does not.
Notifications use active registrations with no timer, detached task or queued
event history. Expiry still uses the absolute request deadline and validity
check. A revision is freshness metadata, not proof of permission for a target.

`StreamProvider::credential_provider` forwards the configured Auth source through
gRPC, HTTP and the counted/attested adapters. Higher-level native and Rust WASM
readers can use that same source through their provider, register its watch,
resolve a lease and retain its binding/check around their work. An absent source
or a raw callback supplies no trusted cache identity.

`CredentialProvider::from_custodian`, `ResolvedCredential::from_custodian` and
`CredentialWatch::from_custodian` retain an existing owner's source, exact
request and drop-owned registration. They introduce no second cache or refresh
path. The optional JavaScript adapter forwards `resolveRequest`, `check`,
`reject` and `watch` to the original objects, aborting its resolution interest
when the Rust future drops. Revisions preserve native bytes or exact UTF-16
code units, including unpaired surrogates, without hashing or normalization.
The browser Stream remote uses this source for unary calls and WebSocket
sessions, retaining checks through replies, frame admission and sends. Opaque
WebSocket handshake failures still cannot reject a credential. This does not
qualify the separate JavaScript-to-native Stream ABI.

Rust Actors, Streams and Objects retain leases across their HTTP/gRPC attempts.
JS gRPC also retains custody through streaming trailers. Browser WebSocket
sessions check their exact lease before connection, after open, before frame
decoding and send, and when returning queued records or completion. A source
notification closes the socket and ends pending reads; every terminal path
unregisters that watch. Browser handshake transport
failure does not reveal an authenticated HTTP rejection and cannot invalidate
it. The shared native tonic service rechecks leases after readiness, before
submitting to the transport. This does not make an atomic promise about a
later remote receive or replace the server's admission check.

Successful Actors replies, shared Auth channel calls, Stream unary replies and
native session records recheck their retained lease before admitting the result.
HTTP Actors, Objects and Stream retain that same lease through the complete
body read and check before decoding and before successful return. JavaScript
Objects checks each successful body chunk and final completion. Local freshness
refusals remain separate from malformed responses and remote rejection.
JavaScript Actors checks before decoding its returned bytes; shared JavaScript
credential streams check each record and completion and retire their iterator
on refusal. Tonic and Connect may decode wire messages before this check;
predecode work admission remains a separate integration below.

Auth owns the native TLS channel setup for platform roots, an exclusive private
CA, or a custodian-supplied member/SVID verifier, with optional mutual TLS.
Actors, Streams and Objects use it. Clones reuse tonic's maintained connection.
Auth's call helper retains the operation's original monotonic deadline across
refresh, readiness and dispatch; Objects uses that helper. A source failure at
the local dispatch guard retains a typed local cause and cannot masquerade as
an authenticated remote rejection.

## Measurement receipt and controls

On Linux x86_64, 2026-10-28, `cargo bench --locked -p acyclic-auth --bench revocation`
applied 128 synthetic single-key deltas at each starting population. Signing
was outside the application timer. Bytes are canonical protobuf publication
bytes, not a network trace; the core includes signature verification and index
updates. No bearer or signing material is emitted.

| Starting targets | Mean delta bytes | Initial checkpoint bytes | Full checkpoint after 128 changes | Retired checkpoint bytes | Mean delta apply (us) | Full checkpoint apply (us) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 133.15625 | 102 | 2,170 | 86 | 36.30 | 72.83 |
| 4,096 | 133.15625 | 80,897 | 82,964 | 86 | 36.29 | 1,397.06 |
| 16,384 | 133.15625 | 333,041 | 335,108 | 86 | 36.45 | 5,559.59 |

Each delta affects exactly one identity. The full checkpoint is the population
control: its bytes and work grow with retained entries. No deployed latency or
disk/network improvement is inferred from these synthetic measurements.
The current ordered index has O(log R) lookup/insertion in R retained entries;
these measurements do not prove population-independent operation cost. Full
checkpoint validation and replacement also process the complete retained input.

Rust tests cross the previous 4,096-entry threshold while leaving an unrelated
key valid, then explicitly revoke all older epochs while preserving the new
epoch. Controls reject stale checkpoints, shortened retention, epoch rollback,
backdated verification after retirement, forged signatures, wrong accounts,
gaps and malformed entries without changing the frontier. JS tests exercise
100 concurrent cold calls, 100 warm calls and 100 expiry calls: two refreshes
total. They also cover old rejection/new lease, account-context revision
changes, failed refresh, unavailable source, reused rejected bearer, independent
and last-waiter cancellation, abort-ignoring completion, and streaming rejection
versus policy denial/cancellation.

Checkpoints sign publication time independently of the retirement floor. A
live verifier and one reloaded from the same accepted checkpoint reject the
same backdated next delta and accept the same monotone successor. Compaction
can retain older history without weakening publication order on restart.
Real loopback HTTP controls hold successful bodies while the source changes
account, becomes unavailable, expires or invalidates. The unchanged source
succeeds; local refusal and cancellation preserve its cached snapshot.
Objects blob controls additionally hold an authenticated rejection until a
replacement lease is published. That late rejection cannot invalidate the
replacement, while rejection of the current lease still forces renewal.
Real loopback WebSocket controls exercise queued records, new wire frames and
writes under the same changes and caller cancellation. Invalid protobuf bytes
after source change must fail with a credential refusal before decoding. An
unchanged lease can send and receive; source invalidation and peer closure
complete an empty pending read without another wire record. Local refusal
preserves the source snapshot, and only explicit invalidation refreshes it.

The native Stream gRPC test also sends real tonic requests through a duplex
transport and the production service adapter. Remote credential rejection
invalidates the used lease; permission denial preserves it; a replacement
published before an old request's rejection remains valid. All four focused
Stream credential tests passed on Linux x86_64 in 0.01 s.

On Linux x86_64, 2026-10-28, `cargo bench --locked -p acyclic-auth --bench credential`
held each synthetic cold/expiry source on a one-shot event until every caller
joined. Warm resolution and check timings each average 16,384 sequential calls.
The raw callback is the negative control for reuse, not a latency comparison.

| Cohort callers | Cold cohort (us) | Warm resolve (ns/call) | Check (ns/call) | Expiry cohort (us) | Source calls total | Raw callback calls |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 23.02 | 58.30 | 24.93 | 2.77 | 2 | 1 |
| 100 | 45.74 | 57.05 | 22.45 | 36.20 | 2 | 100 |
| 1,000 | 386.22 | 57.06 | 23.33 | 353.85 | 2 | 1,000 |

Every cold or expiry cohort shares one exact snapshot. Warm operations add no
source calls; expiry invalidates the old lease. Cohort timing includes caller
registration and delivery, so total work grows with live callers. This is an
in-process synthetic source: no disk opens, JSON parsing, minting, remote token
calls or deployed latency improvement is measured here. Those integrations
still require their separate counters below.

## Remaining integrations

- Integrate durable revocation publication, restart frontier retention, paged
  checkpoint transfer and dependent-cache invalidation with the actual owner.
  The old downstream overflow behavior is not removed by adding this SDK core.
  Replace the ordered index's population-dependent lookup/insertion before
  claiming constant per-identity operation cost.
- Bind member/SVID identity, exact scope, original deadline/cancellation and
  predecode work admission in the shared public transport context; qualify
  actual native/browser requests rather than only the cache primitive.
  The JS-to-native Stream callback ABI still flattens a JS source lease into
  a string. It needs exact request custody across that boundary, including
  native dispatch freshness and rejection of the original JS snapshot.
- Adopt snapshots in `harness-providers/src/codex.rs` using the shared filesystem
  source notification primitive. Count warm disk opens, JSON parses, refresh
  lock waits and token requests at concurrency 1/100/1,000. External rotation,
  account change, expiry and 401 must refresh only the affected source cohort.
- Add remote same-key issuance and typed indeterminate delegation outcomes,
  preserving request identity and exact parent ceilings without retry masks.
- Replace filesystem signed-label authority with verified Auth ceilings
  intersected with current filesystem owner policy. Cache only exact
  principal/resource/command/policy bindings. Preserve deny precedence,
  symlink traversal checks, historical generation checks and both rename ends.
  Count mint, parse, signature and policy evaluations separately.
- Implement the actual device approval-event producer before migrating
  `plugin/identity/src/lib.rs`. The current Cloud control producer updates
  `global_authority.device_codes` transactionally but exposes only polling
  redemption. Completion must authenticate device possession and PKCE, wait
  without periodic queries, handle approval before subscription, expiry,
  denial, cancellation and producer failure, and preserve single redemption.
  Count token calls before approval: the required result is zero. An SDK sleep
  wrapper or an assumed endpoint would not satisfy this contract.

## Admission ownership and required work

`CredentialAction` is the shared check, exact rejection and source-registration
operation. An in-process owner returns an immediate result with no future
allocation or scheduler hop. A thread-confined owner supplies its completion
future; transport workers await that event under the caller's original
deadline/cancellation. Adapters must return a typed source failure when that
producer disappears. A future that owns its completion producer violates this
contract.

The tonic guard retains the handle that acquired readiness while waiting for
admission. Its inline state machine either dispatches that reserved handle once
or drops it and the request on refusal/cancellation. It adds no boxed dispatch
future. The next service call acquires readiness on the replacement handle.
No retry, readiness probe, new timeout or credential re-resolution is added.

The counts below compare the immediately preceding implementation with this
change. They are static owner-operation counts, not measurements of allocator
internals, TLS, HTTP/2 or operating-system scheduling. The shared core changes
issue zero syscalls, durability barriers and remote round trips. Foreign-owner
callback costs need their own qualification; transport's existing RPC and
buffering costs remain separate.

| Operation | Before | After | Required guarantee |
| --- | --- | --- | --- |
| Construct and resolve a custodian request | 2 bearer validation scans, 2 owner checks | 1 scan, 1 owner check | Validate opaque bytes and admit original custody once |
| Shared native request metadata construction | 1 owner check | 0 owner checks | Carry secret metadata; the guard checks after readiness |
| Bearer encoding in Auth, Actors, Streams and Objects | 2 owned payload buffers and 2 token copies, with possible formatter growth | 1 exactly sized payload buffer and 1 token copy | Prefix the opaque token once and transfer that allocation into transport metadata |
| Immediate native dispatch | 1 owner check, 0 added allocations, 0 channel clones | Same | Admit immediately before the maintained channel call |
| Deferred native dispatch | Unsupported | 1 owner check/event, 0 added dispatch allocations, 1 channel handle clone, 0 bearer copies | Preserve the readiness reservation while the original owner admits dispatch |
| Successful native reply | 1 owner check | Same, awaited if required | Reject a changed/expired lease before accepting its result |
| Native session command | 0 owner checks | 1 owner check, 0 added frame copies/allocations for immediate owners | Refuse an old lease before the gRPC body consumes a command |
| Credentialled native physical session | No local admission-failure event | 1 one-shot event and 1 retained lease clone, shared across its commands | End a pending response on outgoing refusal without waiting for a server reply |
| Browser session frame | 2 owner checks, 1 buffer copy, 1 protobuf decode | 1 owner check, 1 copy, 1 decode for an admitted frame; 0 copies/decodes for a refused queued frame | Check at consumption before copying or decoding; retain only size-checked buffers |
| Native session completion | 0 owner checks | 1 owner check | Do not admit successful completion under a changed/expired lease |
| Physical native/browser source notification | No source registration in Rust sessions | 1 registration per trusted source/session, retained across resolve, connect and reads | Retire an idle session on invalidation without another frame; raw callbacks register nothing |
| Accepted revocation publication transcript | 2 publication clones/conversions and encodings; 2 domain buffers plus 2 temporary encoded buffers, with possible buffer growth | 1 clone/conversion and encoding into 1 exactly sized transcript buffer, reused for frontier hashing | Verify and identify the exact same canonical bytes; publish the digest only after full validation |

The deferred owner's type-erased completion future is one owner allocation,
not a dispatch allocation; owners with immediate checks allocate neither.
The channel handle clone shares the existing connection and is required only
for deferred admission. No caller/source registry or history traversal is added.
Each state transition performs fixed work; notifying multiple live watches
still costs one notification per observer as described below.
Validation and encoding necessarily process opaque input bytes once; their
cost is proportional to that input, independently of retained history.

`credential_custodian` holds real cache-backed custody on a one-shot admission
event. Its controls retain the producer (still pending), drop the producer
(typed unavailable immediately), cancel the admission (receiver released),
and delay an old rejection until after a replacement lease (replacement stays
valid). `channel` exercises the deferred operation through the maintained tonic
client after readiness, including expiry, producer loss and cancellation.
Local failure never invokes exact remote rejection. The existing immediate
owner and successful reply controls exercise the other branch.

The native session input holds at most its current frame and that frame's
admission action. Outgoing refusal ends the input and sends a typed failure to
the response wait. If normal gRPC body teardown drops that sender, the response
continues waiting on its existing maintained transport operation, preserving
real statuses and trailers. Neither path starts a task or timer. A loopback
session control first observes a healthy command acknowledgement, then changes
account, expiry, availability or explicit invalidation before another command.
The old command must contribute zero received bytes on the server and must end
the pending response locally; the unchanged control must reach the server.
Native and Rust browser sessions share one event selection helper. They
subscribe before resolution and retain that source event through connection
and response waits. A ready invalidation or producer refusal wins over ready
transport data. It ends the operation and drops its registration/transport;
there is no monitor task, extra timer or event history. The unchanged loopback
control first proves an idle read remains pending, then publishes invalidation
and observes an immediate typed refusal without sending or receiving a frame.

Revocation authentication retains its canonical transcript for frontier hashing
after validation. It does not clone or encode the publication again. Existing
forged-publication, malformed-entry and stale-checkpoint controls still require
the accepted frontier to remain unchanged on refusal. Signature verification
and hashing each occur once; no persistence or remote operation is added.

JavaScript source watches also register before observing, matching the Rust
contract. Reentrant invalidation completes the returned signal immediately;
an observation failure removes its registration before propagating. The
unchanged and closed-watch controls remain unnotified.

## Operation context and injected native channels

`actors::client::Client::from_channel`, `objects::grpc::GrpcObjects::from_channel`
and `stream::grpc::Client::from_authenticated_channel` adopt the caller's existing
authenticated channel. The channel owner selects Auth's platform, private-CA or member/SVID
verifier and optional mutual-TLS identity once. The product constructor starts
no connection, parses no certificate and resolves no bearer. It installs the
same dispatch guard, interceptor, tracing and message ceilings as the endpoint
constructor. This is a transport injection boundary, not evidence that a caller
supplied channel authenticated its peer.

Streams additionally requires an explicit `ChannelConnector` for every routed
session. It must validate the advertised member and endpoint and preserve the
home connection's peer and client-identity policy. A route URL is not evidence
of authority. Its refusal is propagated through the existing session pool;
there is no fallback to platform roots or a separate connection pool. Credential
changes replace the session cache while preserving that same connector.
The real two-node move control verifies one home connection and one accepted
route connection while an unrelated follow remains live. A foreign route is
refused with `AccessDenied` and does not increment the accepted-dial count.

Use the original `auth::operation::Context` for connection as well:

```rust,ignore
let channel = context.run(acyclic_auth::channel::connect(endpoint, peer, identity)).await??;
let client = acyclic_actors::client::Client::from_channel(channel, credential)
    .with_context(context);
let response = client.invoke_actor(&request).await?;
```

The context is never reset by a stage. A client bound to that context retains
its original deadline across subsequent calls. An ended context wins before
binary request decoding, typed request cloning or credential resolution.
Transport dispatch and semantic reply admission each check the same stop again.
Credential resolution, remote-rejection handling and deferred owner checks are
inside that wait. The exact lease survives semantic conversion and is checked
before and after it. Tonic still owns protobuf decoding; this change does not
provide native preallocation admission or a wire-level absolute deadline.

Safety follows from checking the original context before starting work and
checking the original lease through final admission. Cancellation does not
establish whether a remote mutation committed. Progress waits on the context's
original expiry/cancellation event or the maintained transport/credential
operation. The host maps cancellation-producer loss to cancellation; the real
HTTP/2 request-path control exercises both explicit signal and producer drop
during credential resolution. Both end with typed cancellation, with no service
call. Its unchanged control completes six real exchanges. An expired context
refuses even malformed input without another credential callback or service
call. Removing context enforcement must fail that control before any pending
credential wait starts.

The added stop checks have fixed state and no task, timer, registry or scan.
One context owns one stop future; stages share it. Protobuf encoding/decoding
remains proportional to payload size, and foreign credential-owner costs are
separate. Static counts compared with the preceding Actors implementation:

| Operation | Before | After |
| --- | --- | --- |
| Adopt an already authenticated channel | No public constructor | 0 connections, certificate parses or credential callbacks; 1 existing channel moved into the shared guard |
| Objects client bookkeeping | 1 revision-tracker owner allocation | Same; adopting a channel adds no owner, RPC or payload copy |
| Streams client bookkeeping | 3 top-level owner allocations for claims, session cache and revisions, plus claim watch/identity initialization | Same; 1 caller-owned route connector moved into the client, no extra pool or connection |

The constructor counts identify their explicit top-level owners. Existing
claim initialization additionally creates its notification owner and obtains
one random identity; entropy-provider/syscall and allocator internals are not
measured here and are unchanged by channel adoption. No total allocator or
system-call count is inferred from these source counts.
| Caller budget across one operation | No gRPC context | 1 original stop event shared at 3 boundaries; 0 additional timers, tasks, syscalls, barriers, RPCs, payload allocations or copies |
| Successful reply admission | 1 lease check before semantic conversion | 2 lease checks, bracketing semantic conversion; 0 added response copies |
| Ended budget | Could decode input and call the credential source | 0 decodes, credential callbacks or RPCs |

## Source event progress and operation cost

Native source watches share one one-shot event per current source epoch. A
watch owns only the receiving side, so dropping the last source producer
completes every pending watch with `CredentialCacheError::Unavailable`.
Invalidation publishes the next epoch before delivering the previous event;
a registration created by a waking observer cannot consume that old event.
Registration captures the event before observing validity, releasing its lock
before calling the source. A source observation may reenter invalidation; the
already-registered event then completes rather than losing that change or
deadlocking. A concurrent change can conservatively retire a fresh observation.
Foreign custodians forward the same typed result and release their original
registration on drop.

Registration, cancellation and epoch replacement update a fixed amount of
owner state. There is no per-source observer map or history scan. The shared
future primitive owns current task registrations and removes a dropped task's
waker. Delivering an event to N live observers necessarily performs N wake
notifications; this is O(1) per notified observer, not O(1) total broadcast.
The controls exercise a reentrant registration, dropped task cleanup, and a
retained producer as the negative control for last-producer loss. All results
are checked by polling the event directly, without a timeout or sleep.

The native JavaScript adapter holds one cancellation cohort rather than a
collection of pending callbacks. Retain/release changes a counter and the last
release detaches that cohort before fencing transport and aborting its event.
A reentrant request belongs to a new cohort. Credential resolution uses Auth's
shared abort-aware primitive, so losing the producer cannot keep an abandoned
request pending. The callback's eventual settlement remains observed. A request
bound to a caller signal rechecks that signal without rejecting a shared lease.
Mutation controls remove cancellation custody, fence ordering or cohort
separation and must fail the same deterministic schedules. As with native
watch delivery, abort notification work is proportional to live listeners;
there is no scan over completed requests or historical cohorts.

## Identity MCP completion ownership

An identity operation's task owns the sole completion-watch sender; the parked
request holds only a receiver. The task publishes its result before completing
that event. Aborting or unwinding the task drops its producer and wakes the same
wait. An absent result then becomes typed `operation_unavailable`, with a fixed
recovery command and no task diagnostic or credential contents.

Result removal and the closed-producer observation share the existing result
mutex. A producer therefore cannot publish between an empty-slot observation
and a terminal-loss verdict. The existing task, result slot and MCP wait remain
the only owners; no monitor task, timer, registry, history or readiness probe is
introduced. Each progress invocation does fixed work after a primitive event.
The real Tokio task control holds server admission before credential or network
access: a live producer keeps the wake pending, task abort completes it, and the
parked request settles once with the typed error. Removing producer-loss
detection fails that same control immediately. The stdio tool-list/argument
control still exercises the maintained MCP server.

| Operation | Before | After |
| --- | --- | --- |
| Completion storage | 1 shared result slot and 1 success-only notification owner | Same result slot and 1 producer-owned watch channel |
| Pending progress | 1 result mutex acquisition and notification wait | Same result lock, 1 closed-producer observation under it, and the watch wait |
| Completion publication | 1 result publication and 1 notification | Same; the watch additionally carries producer lifetime |
| Lost operation task | No result or completion event; wait could remain pending | Producer drop completes the existing event; 1 typed terminal result |

This change adds zero tasks, timers, RPCs, durability barriers, credential reads
or payload copies. Watch synchronization replaces notification synchronization;
no latency reduction or total allocator-internal count is claimed. Blocking
identity persistence still requires the native bounded-I/O adapter separately.

## Explicit native custody and synchronous retained access

Production signing custody selects exactly one backend. Native is the default;
private files require the owner's explicit choice. A backend read/write refusal
terminates admission and never becomes an absence or triggers another store.
The process-local fixture backend remains restricted to test-support builds;
standalone production-binary fixtures explicitly own a private-file directory.
The production child-process control crosses real process exits and reopens
that same directory. Unix mode changes and Windows Everyone ACL grants must
refuse the original reference without creating another entry. Invalid policy
combinations must fail before opening a credential service. Diagnostics retain
operation and native status, never stored seed bytes.

| Operation | Before | After |
| --- | --- | --- |
| Backend choice | Up to the fixed platform backend count, possibly after a failed write | Exactly one selected backend; first refusal terminates |
| New-entry candidates | One growable candidate vector | Zero candidate vectors |
| Existing-entry bootstrap | One selected successful read | Same one read; no write, lock, barrier or listing |
| New-entry admission | Absence read, write and verification per attempted backend | One absence read, one write and one verification read |
| File durability | One publication commit | Same shared commit; no per-read barrier |
| Synchronous retained getter | No explicit capability; an awaited check could dispatch work | One original-owner synchronous observation, or explicit unsupported |

`ResolvedCredential::retained_access` never calls asynchronous admission or
resolution. It returns `None` for raw and async-only owners. Cached and browser
JS owners check their original exact request, observing source invalidation,
exact rejection and half-open expiry before exposing retained data. A successful
observation is valid only at that instant; consumers cannot cache it as a grant.
This adds zero events, tasks, timers, allocations, RPCs or bearer copies. It
retains the original owner's fixed freshness checks; variable-length foreign
revision comparison still depends on the supplied revision length. Existing
custodian controls check old rejection against a new lease, immediate expiry,
raw refusal and async-only refusal. Awaited checks remain required at transport
boundaries. No new wait or completion producer is introduced.
