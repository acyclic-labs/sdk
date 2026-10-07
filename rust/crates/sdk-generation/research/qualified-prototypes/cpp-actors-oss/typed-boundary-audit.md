# C++ typed-boundary audit

Captured 2026-10-07 against the final opaque-result CXX prototype and the
authoritative `actors::domain` and `actors::client` sources.

## Qualification boundary

The current package is a real installed CXX artifact with eight authenticated
runtime operations, typed cancellation, typed `u64` admission, optional-value
presence, and expected-negative C++ checks. It is a canonical conformance
consumer, not a complete idiomatic C++ Actors SDK. The public CXX functions
take no request objects: each function constructs one canonical Rust request
internally. They do not accept JSON or protobuf byte blobs, but that is also
why they cannot be used to issue arbitrary typed Actors requests.

`ActorOperationResult` is opaque across CXX. Its private Rust projection is
queried through accessors, so the C++ header does not publish a hand-authored
response record. The three remaining shared CXX records are qualification
reports (`PositiveU64Result`, `ClientConnectResult`, and
`RemoteConformanceResult`), not the Actors request/response domain.

## Public request coverage

The Rust client exposes these semantic request fields. The current CXX
operation receives none of them from its caller; the values shown are fixed in
Rust and therefore are not a C++ API surface.

| Rust request | Rust fields | Current CXX behavior | Information available to a C++ caller |
|---|---|---|---|
| `CreateActorRequest` | `code_sha256`, `home_region`, `bindings`, `limits`, `subscriptions`, `idempotency_key` | `actors_create_actor` builds canonical constants | none; no arbitrary create request |
| `UpdateActorRequest` | `actor_id`, `code_sha256`, `bindings`, `limits`, `expected_configuration_revision`, `idempotency_key` | `actors_update_actor` builds canonical constants | none; no compare-and-replace input |
| `InspectActorRequest` | `actor_id` | `actors_inspect_actor` uses `actor-a` | none; no arbitrary Actor identity |
| `AddSubscriptionRequest` | `actor_id`, `subscription`, `idempotency_key` | `actors_add_subscription` builds one canonical subscription | none; no subscription builder |
| `RemoveSubscriptionRequest` | `actor_id`, `subscription_id`, `idempotency_key` | `actors_remove_subscription` uses fixed strings | none |
| `ResumeSubscriptionRequest` | `actor_id`, `subscription_id`, `idempotency_key` | `actors_resume_subscription` uses fixed strings | none |
| `CheckpointActorRequest` | `actor_id`, `idempotency_key` | `actors_checkpoint_actor` uses fixed strings | none |
| `InvokeActorRequest` | `actor_id`, `method`, `url`, `body: Vec<u8>`, ordered `headers` | `actors_invoke_actor` uses `POST /`, `{}`, and one header | none; body bytes are Rust-internal and cannot be supplied by C++ |

The body on `InvokeActorRequest` is an intentional byte-preserving HTTP
payload in the Rust semantic model. It is not a protobuf escape hatch, and the
current CXX API does not expose it. A future full SDK must expose an explicit
Rust-owned byte-slice/builder type with ordered header accessors, or document
the operation as unavailable; passing an untyped JSON/protobuf blob as the
general SDK contract would not qualify.

## Result coverage

The seven Actor response types carry `Option<ActorObservation>`. The Rust
observation fields are:

`actor_id`, `code_sha256`, `home_region`, typed `state` (`Unspecified`,
`Active`, `Hibernated`, `Paused`), ordered `subscriptions`, optional
`checkpoint_unix_millis`, `checkpoint_epoch`, and
`configuration_revision`.

The opaque CXX result currently preserves only `actor_id`, `home_region`, an
`active: bool`, `subscriptions_empty: bool`, `configuration_revision`,
`checkpoint_epoch`, and `has_checkpoint + checkpoint`. It therefore loses the
code digest, the distinction between non-active states, every subscription
field and ordering, and the distinction between an absent Actor response and
an empty/default projection. The checkpoint presence bit is preserved, but the
public `checkpoint` accessor is only meaningful when that bit is true.

`InvokeActorResponse` carries `status`, the complete body byte sequence, and
all ordered headers. The current projection retains only status and whether
the first header is named `location`; it loses the body, all header values,
all other headers, and header order. This is sufficient for the conformance
fixture's assertion and insufficient for a general C++ client.

## Error coverage

Rust `client::Error` has `Configuration(String)`, `Transport(String)`,
`Service { grpc_code: i32, detail: Option<wire::Error> }`, typed
`Contract(ContractError)`, typed `Semantic(DomainError)`, and `Cancelled`.
The current CXX bridge maps these to `ErrorKind` plus a display string. It
preserves a coarse category and human-readable message, but loses:

* the numeric gRPC status;
* the structured service detail code and message;
* the individual contract variant as a nominal type;
* unknown semantic enum numbers, which become a generic `Semantic` message;
* structured domain error data beyond its formatted text.

The Diplomat `ActorsError` opaque type keeps the Rust-owned message and
category and exposes the numeric detail for contract and unknown-enum domain
errors. This is typed construction-error coverage; transport error parity is
still a separate gap.

## Maintained generator options

* **CXX 1.0.202** generates the current C++ declarations and supports opaque
  Rust ownership. It does not derive a C++ interface from the existing Rust
  `domain::*`/`client::*` metadata. Making every request, response, list, byte
  payload, service detail, and enum available requires explicit bridge methods
  for each maintained Rust type. Hand-authoring that list in this prototype
  would recreate the mirror surface this audit is intended to detect.
* **Diplomat 0.16.1** has a maintained C++ backend and supports opaque types,
  constructors, accessors, slices, and fallible methods. The isolated
  `diplomat/` prototype now generates and links C++ against the real Actors
  semantic dependency without adding production annotations. Adding Diplomat
  feature annotations to the production Actors domain still requires
  foundation-port coordination so the same annotated Rust source can remain
  authoritative for every SDK generator.

The practical next step remains a foundation-port-owned metadata pipeline that derives C++/Diplomat declarations from the existing semantic Rust types and their accessors. The isolated Diplomat prototype now covers all eight caller request families with canonical Rust domain storage, seven full actor-observation result wrappers, and typed construction/transport error accessors. Its installed package also proves authenticated TLS invoke, maintained callback completion, and Rust-owned in-flight cancellation with server abort observation.

The prototype still does not claim a complete eight-operation transport SDK. The six non-invoke operations have generated typed request/result surfaces but no `ActorsClient` transport methods yet. Diplomat 0.16.1 rejects a callback parameter constrained by both `Fn` and `Send`, so completion is caller-polled; automatic cross-thread callback dispatch remains unclaimed. These are explicit maintained-tool gaps. No production Actors annotations or domain cutover were made by this audit.