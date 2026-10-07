# Diplomat semantic surface audit

Captured 2026-10-07 from the rebuilt installed package. The bridge stores canonical `acyclic_actors::domain` values in every opaque object; it does not define C++ request/result/error field mirrors. The producer archive and its 30-file inventory are recorded in `Q:\cpp-actors-diplomat-package-final\source-closure.txt`.

## Current producer identity

- Git source revision: `371bb4170e16aca973176b6756a261ee5add7297`; the producer checkout is dirty, so the archive hash and inventory are the source identity.
- Current producer archive: `acyclic-actors-0.2.0.crate`; SHA-256 `e88dd52904cb0d976ab018061b7493230104408e64c53ea856787fe3ae269a2b`.
- Package command: `cargo package --manifest-path rust/crates/actors/Cargo.toml --allow-dirty --no-verify --offline`; Cargo reported 30 packaged files.
- The static library was rebuilt after the current bridge and producer sources were present; the package contains the matching archive, lockfile, bridge source, generated headers, and library.

## Nineteen semantic roots

| # | Canonical root | C++ surface and audit result |
|---:|---|---|
| 1 | `ActorId` | Nominal opaque constructor; empty identity is a typed `ActorsError`. Stored immutably in every request. |
| 2 | `CodeSha256` | Nominal opaque exact 32-byte constructor; zero/incorrect length rejected. Exposed only through byte accessors. |
| 3 | `PositiveU64` | Canonical Rust admission for positive limits/cursors; generated C++ uses `uint64_t`. |
| 4 | `Binding` | Canonical nested name/capability/resource builder and accessors; wrong nominal type negative compiles fail. |
| 5 | `ActorLimits` | Three canonical positive `u64` values; C++ builder preserves handler timeout, memory, and checkpoint bytes. |
| 6 | `SubscriptionStart` | Canonical cursor/current-head variants with presence and exact `u64` cursor access. |
| 7 | `SubscriptionSpec` | Canonical subscription id, stream path, start, and placement-anchor fields. |
| 8 | `SubscriptionState` | Rust enum remains an integer-backed canonical state; response accessor preserves raw published value. |
| 9 | `ActorState` | Rust enum remains an integer-backed canonical state; unknown values become typed semantic errors. |
| 10 | `SubscriptionObservation` | Response accessors preserve ordered id, stream path, state, and placement-anchor entries. |
| 11 | `ActorObservation` | Optional actor presence plus identity, digest, region, state, subscriptions, checkpoint, epoch, and revision. |
| 12 | `CreateActorRequest` | Caller supplies digest, region, nested bindings/limits/subscriptions, and idempotency key. |
| 13 | `UpdateActorRequest` | Caller supplies actor identity, replacement digest/configuration, expected `u64` revision, and idempotency key. |
| 14 | `InspectActorRequest` | Caller supplies nominal actor identity; no JSON/protobuf blob substitute. |
| 15 | `AddSubscriptionRequest` | Caller supplies identity, full nested subscription, and idempotency key. |
| 16 | `RemoveSubscriptionRequest` | Caller supplies identity, subscription identity, and idempotency key. |
| 17 | `ResumeSubscriptionRequest` | Caller supplies identity, subscription identity, and idempotency key. |
| 18 | `CheckpointActorRequest` | Caller supplies identity and idempotency key. |
| 19 | `InvokeActorRequest/Response` | Caller supplies identity, method, URL, arbitrary body bytes, and ordered headers; response preserves absence, status, body, and ordered headers. |

The eight-operation consumer sends every caller-supplied field to an authenticated TLS fixture. The fixture asserts nested binding fields, all three limits, the large cursor `9007199254740993`, every idempotency key, expected revision, actor identity, method, URL, body bytes, and header name/value. It receives all seven actor responses and checks actor presence, identity, region, state, subscription count, checkpoint epoch, revision, and digest.

## Presence, immutability, and errors

- Optional actor observations remain `has_actor()` plus accessors; absent invoke output remains a null/absent typed result; optional checkpoint remains `checkpoint_present()` plus value.
- C++ response objects expose const accessors. Builders are mutable only while constructing canonical Rust values (`BindingList`, `SubscriptionList`, and `Headers`); operation requests and responses are immutable opaque values thereafter.
- `ActorsError` preserves canonical construction categories, display text, and numeric contract/unknown-enum detail. `AsyncError` preserves configuration, transport, service gRPC code/detail presence, contract, semantic, and cancellation categories plus message.
- Nominal negative compile evidence rejects `ActorLimits` where `Binding` is required. The package also compiles all operation-specific response accessors from the clean external install.

## Callback and cancellation contract

Diplomat 0.16.1 generates maintained `std::function<void()>` callbacks. The Rust runtime owns the Tokio task and `CancellationToken`; C++ calls `poll()` to dispatch completion. Diplomat rejects a callback parameter constrained by both `Fn` and `Send`, so automatic cross-thread callback delivery is not claimed. A live cancellation run returned typed `Cancelled` and the fixture observed `{ "inspectStarted": true, "inspectAborted": true }`.

## Scope boundary

The result is an installed, strongly typed prototype over the current producer archive. It is not a production annotation cutover. Linux/macOS/other platform claims require their own maintained toolchain and live package run; this receipt claims the Windows clang/MSVC-compatible package run only.