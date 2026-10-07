# C++ Actors binding qualification (CXX, maintained route)

This is a source-only qualification fixture against the authoritative checkout:

`C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`

The recorded source snapshot was Rust git `371bb4170e16aca973176b6756a261ee5add7297`.
The fixture is deliberately outside the public Actors crate and does not change the
wire schema, generated protobuf, or public package API.

## Decision

Use **CXX 1.0.202** (`cxx` and `cxx-build`, pinned in `Cargo.toml`) for the
small native C++ bridge. CXX provides generated C++ declarations, static checks,
opaque Rust ownership through `rust::Box`, and shared C-like enums/structs. The
Rust bridge is the contract source; C++ never receives Rust layout or a raw
pointer registry.

The smallest production boundary is a Rust-owned opaque client and operation:

* `ActorsClient` owns `acyclic_actors::client::Client` and is only passed through
  `rust::Box`/references.
* `ActorsOperation` owns `tokio_util::sync::CancellationToken`; C++ can request
  cancellation without touching a Rust future.
* `ActorObservationView` wraps the existing `domain::ActorObservation`; optional
  checkpoint data is represented by `has_checkpoint` plus `checkpoint`, preserving
  absence instead of using a sentinel.
* `PositiveU64Result` preserves the existing `PositiveU64` admission rule and a
  full `uint64_t` range, including `UINT64_MAX`.
* `ActorsError` maps the existing Rust client error categories to `ErrorKind` and
  retains the Rust error string. Production should add structured service-detail
  accessors before claiming complete typed-error parity.

This is a bridge to maintained Rust behavior, not a C++ protobuf SDK. The Rust
client, domain conversion, limits, enum validation, transport, and response
semantics remain authoritative.

## Current Rust evidence

The following files were read from the authoritative checkout and hashed when the
fixture was captured:

| Rust source | Relevant contract | SHA-256 |
|---|---|---|
| `rust/crates/actors/src/client.rs` | async connect/operations, `run_with_cancellation`, typed `Error` | `97A4B0318D4CE10BD45D0EA29C616EE13DFB7D0C83B6FFFE54303EC7C11E2342` |
| `rust/crates/actors/src/domain.rs` | `PositiveU64`, `ActorObservation`, optional/u64 getters, `DomainError` | `79092881EC6B9434A3AC3AE6B810DCA46BB85A8E4C8C1E98191982AC5D82B4B2` |
| `rust/crates/actors/src/contract_definitions.rs` | immutable wire field tags, `u64` and optional fields | `0B6039FBD35201CAF4C5AC5227C414EEF7E87C3A6037E576C28F76380E4428FA` |

The fixture uses the real `acyclic-actors` crate as a path dependency. The sample
observation is converted by `domain::ActorObservation::try_from`, rather than by
reimplementing protobuf semantics.

## Consumer compile qualification

`generated/lib.rs.h` is the CXX-generated header from this exact bridge source;
its SHA-256 is `F302DDDE6C4D2FE594B3C41EA45505C2EDCEA85DECD591EBD0AF45935BC5A713`.
The consumer source is intentionally tiny and checks only bridge contracts:

* `consumer/positive.cc` links and runs successfully with WSL Ubuntu `g++`.
* `consumer/negative.cc` is expected to fail: passing `std::string` to the
  generated `uint64_t` parameter fails at the C++ type checker (exit code 1).
* `consumer/negative-nominal.cc` is expected to fail: an `ActorsClient` cannot
  be passed where the nominal `ActorsOperation&` is required.
* The positive consumer checks `UINT64_MAX`, optional presence, typed cancellation,
  the real Rust `client::connect` configuration error path, and opaque
  client/operation ownership.

The exact independent consumer command was:

```text
g++ -std=c++11 -I generated -I <cargo-registry>/cxx-1.0.202/include -fsyntax-only consumer/positive.cc
```

It returned 0. The same command on `consumer/negative.cc` returned 1 with the
expected `std::string` to `uint64_t` conversion diagnostic. The nominal negative
consumer also returned the expected nonzero compile result.

The canonical Cargo build and link succeeded in WSL Ubuntu using the real
`acyclic-actors` path dependency:

```text
RUSTC_WRAPPER= cargo build --locked --target-dir /tmp/cpp-actors-oss-target
g++ ... consumer/positive.cc libcpp_actors_oss_qualification.a -ldl -lpthread -lm
/tmp/cpp-actors-positive
```

The executable returned 0. Its connection probe invokes the actual Rust
`client::connect` with an invalid non-HTTPS endpoint and observes the typed
configuration error; no toy transport or independent DTO is involved. The
Windows MSVC driver remains unqualified because it previously failed while
compiling emitted `lib.rs.cc`; WSL provides the linked producer qualification.

## Live authenticated TLS conformance

The bridge also has `consumer/live-remote.cc`, where C++ calls one opaque-client
entry point per Actors operation. Each entry point delegates one typed request
to the real `acyclic_actors::client::Client` and returns a typed projection of
the Rust domain response. The calls run against the canonical repository gRPC
conformance fixture with a generated local CA and bearer token. The Rust facade
checks the real `ActorObservation` fields, invoke status/header semantics,
`u64` subscription cursor, and rejection of a wrong bearer token.

The WSL run on 2026-10-07 passed:

```text
create/update/inspect/add/remove/resume/checkpoint:ok
invoke:status=201 location=true
live_cxx_operations:8 authentication_rejected:true cancellation:cancelled
```

The delayed fixture variant holds the second inspect request until the C++
operation token is cancelled and records the HTTP/2 stream abort. This qualifies
the reduced opaque bridge against the canonical fixture. It does not qualify an
internet service, every SDK family, or a direct C++ future ABI.

## Async and error boundary

The current client methods are Rust `async fn` and use `run_with_cancellation`.
CXX's current documentation states that direct async FFI is not implemented and
recommends an opaque oneshot context. Therefore the production adapter must expose
an opaque operation/context and a callback or poll/join function. It must not
translate a Rust future into a hand-written C++ algorithm. The delayed fixture
observes cancellation of an in-flight request at the server boundary. It still
does not expose a Rust future directly to C++.

`client::Error` has configuration, transport, service (with optional structured
wire detail), contract, semantic, and cancelled variants. The fixture maps these
categories to C++ `ErrorKind`. A complete facade must expose service status/detail
as a Rust-owned opaque/detail accessor and preserve unknown enum numbers through
`Semantic`; this remains a bounded follow-up.

## Generator comparison

* **CXX 1.0.202:** qualified for this reduced bridge. Current release source and
  docs describe opaque Rust types, shared types, generated C++ code, and static
  assertions. Direct async FFI is explicitly not implemented; use the opaque
  oneshot/cancellation pattern.
* **`pcwalton/cxx-async` 0.1.4:** maintained and specifically aimed at bridging
  C++20 coroutines with Rust futures/streams. It integrates with cppcoro or Folly
  and requires a separate C++ executor; it does not bundle or select the Rust
  runtime, so a Tokio-backed future can still be adapted if the executor and
  wakeup contract are wired and tested. Those C++20 executor headers/libraries
  are not installed here, so no linked cxx-async prototype is claimed.
* **cbindgen 0.29.4:** maintained Mozilla generator and a viable fallback for a
  deliberately authored `extern "C"` ABI. It emits C/C++ headers but cannot infer
  Rust async, `Option`, typed error ownership, or opaque lifetime contracts. It
  would therefore require the same Rust-owned facade and would provide weaker
  compile-time checks than CXX.
* **autocxx:** excluded. The upstream Google repository is archived and its README
  directs new users to CXX or Crubit; its intended direction is existing C++ into
  Rust rather than this Rust-owned SDK bridge.
* **UniFFI C++:** excluded as a maintained first-party route. Official UniFFI
  support lists Kotlin, Swift, Python, and Ruby; C++ is not among the built-in
  generators. Third-party C++ generators would add an unowned compatibility
  surface and do not qualify this SDK.

Primary upstream references (retrieved 2026-10-07):

* https://cxx.rs/
* https://cxx.rs/async.html
* https://cxx.rs/extern-rust.html
* https://github.com/pcwalton/cxx-async/releases/tag/v0.1.4
* https://github.com/dtolnay/cxx/releases/tag/1.0.199
* https://github.com/mozilla/cbindgen/tree/0.29.4
* https://github.com/google/autocxx/blob/main/README.md
* https://github.com/mozilla/uniffi-rs/blob/main/README.md

## Minimal viable production path

1. Add one Rust-owned CXX bridge module beside the Actors native adapter; keep the
   eight operation methods delegated to `acyclic_actors::client::Client`.
2. Expose only opaque client/operation/response/error types plus primitive/shared
   value views that CXX can check. Keep optional values as presence plus value or
   an opaque optional wrapper; never use sentinels.
3. Implement the documented opaque oneshot/callback operation context around the
   existing async methods and route cancellation to the existing
   `CancellationToken`.
4. Add structured service detail accessors and response/body/header views before
   calling typed-error or complete response parity qualified.
5. Run the packaged C++ positive and expected-negative compile checks for each
   supported compiler/target. Link/runtime qualification is still required on the
   supported native matrix.

No archive, generated protobuf, public Actor package, or frozen Q ownership was
changed by this qualification.
