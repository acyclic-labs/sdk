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

Rust Actors, Streams and Objects retain leases across their HTTP/gRPC attempts.
JS gRPC also retains custody through streaming trailers. Browser WebSocket
setup checks its exact lease before connection; browser handshake transport
failure does not reveal an authenticated HTTP rejection and cannot invalidate
it. The shared native tonic service rechecks leases after readiness, before
submitting to the transport. This does not make an atomic promise about a
later remote receive or replace the server's admission check.

Successful Actors replies, shared Auth channel calls, Stream unary replies and
native session records recheck their retained lease before admitting the result.
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
| 1 | 133.15625 | 100 | 2,168 | 84 | 37.81 | 79.58 |
| 4,096 | 133.15625 | 80,895 | 82,962 | 84 | 37.86 | 1,477.58 |
| 16,384 | 133.15625 | 333,039 | 335,106 | 84 | 37.64 | 6,036.51 |

Each delta affects exactly one identity. The full checkpoint is the population
control: its bytes and work grow with retained entries. No deployed latency or
disk/network improvement is inferred from these synthetic measurements.

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

The native Stream gRPC test also sends real tonic requests through a duplex
transport and the production service adapter. Remote credential rejection
invalidates the used lease; permission denial preserves it; a replacement
published before an old request's rejection remains valid. All four focused
Stream credential tests passed on Linux x86_64 in 0.01 s.

## Remaining integrations

- Integrate durable revocation publication, restart frontier retention, paged
  checkpoint transfer and dependent-cache invalidation with the actual owner.
  The old downstream overflow behavior is not removed by adding this SDK core.
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
