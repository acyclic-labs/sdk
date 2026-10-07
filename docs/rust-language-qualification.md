# Rust-owned language qualification matrix

This matrix records the current qualification state of the native language cohort
against the C foundation checkout. It is a source and evidence record, not a claim
that an external prototype is already an SDK release.

Snapshot: 2026-10-07  
Authority checkout: `C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`  
Reviewed source revision: `b9cb80bc735815166ac23ea43ab6e45cb4753e05`  
Binding project: [Mozilla UniFFI](https://github.com/mozilla/uniffi-rs), MPL-2.0.

## Status rules

* **Qualified** means an installable package was built from this C authority,
  its native library was loaded by a consumer on the target platform, and the
  consumer exercised the typed remote facade and applicable embedded behavior.
* **Prototype** means generated bindings or static/runtime checks exist, but the
  package is not built from the current C authority or does not call the current
  typed facade.
* **Candidate** means the maintained OSS path and required packaging toolchain
  are identified, but current-authority qualification evidence is absent.

Receipts from the older Q Actors/control-wire architecture do not qualify this
checkout. In particular, the Python receipt at
`Q:\sdk\work\uniffi-python-oss-prototype\actors-receipt.json` is explicitly
`INVALID_FOR_CURRENT_ACTORS_BACKEND`.

## Cohort matrix

| Language | Maintained OSS path and exact observed pin | Evidence in this snapshot | State | Required work for qualification |
| --- | --- | --- | --- | --- |
| Python | UniFFI 0.31.0 was used by the older external package prototype; the separate generator-only template proof is UniFFI 0.32.2. Maturin 1.15.0 was the external packaging pin. | The 0.31.0 wheel exercised the superseded control-wire backend. The 0.32.2 proof covers opaque constructors, rich errors, `u64`, bytes, enums, and mypy/Pyright checks, but has no Actors backend or package. | **Prototype; current Python SDK unqualified** | Pick one cohort UniFFI pin, generate from the current Rust facade, build an installable wheel for each supported target, and run typed remote operations, errors, cancellation, recovery, `u64`, bytes, and embedded tests. Maturin remains packaging tooling; it must not own behavior. |
| Kotlin/JVM | UniFFI's maintained Kotlin backend is the candidate path. The external source snapshot contains UniFFI 0.31.0 templates; no current-authority Kotlin package pin has been selected. | No `Q:\sdk\work\uniffi-jvm-oss-prototype` or current-C Kotlin artifact was found. The existing source contains no C-authority UniFFI package. | **Candidate; unqualified** | Reproduce the selected UniFFI pin against the current facade, then package the native library plus generated Kotlin in a Gradle/Maven-consumable artifact. Qualify a supported JDK/Kotlin/OS matrix, typed errors, cancellation, streaming, and lifecycle behavior. |
| Swift | UniFFI 0.31.0 is the exact pin in `Q:\sdk\work\uniffi-swift-oss-prototype\source`. The source is Mozilla UniFFI with MPL-2.0 package metadata. | Generated Swift bindings and static checks cover `UInt64`, oneof/unknown values, semantic types, and async signatures. They are fixture bindings from the external prototype and do not call the C-authority Actors client or ship a Swift package. | **Prototype; current Swift SDK unqualified** | Re-run binding generation from the current facade, build the Rust native artifact, and ship a SwiftPM/Xcode-consumable package. Test macOS/iOS targets, typed errors, cancellation, streams, recovery, `u64`, bytes, and ownership/lifecycle. |

UniFFI's core maintained language set includes Kotlin, Swift, Python, and Ruby;
this matrix covers the requested Python/JVM/Swift cohort. C# and Go adapters are
third-party paths and are not silently treated as equivalent qualification.

## Rust source surface to bind

The current C Actors crate already has one typed client surface in
`rust/crates/actors/src/client.rs`. Its public methods consume and return the
Rust domain types for the eight operations `create_actor`, `update_actor`,
`inspect_actor`, `add_subscription`, `remove_subscription`,
`resume_subscription`, `checkpoint_actor`, and `invoke_actor`. The native
adapter must expose this surface through UniFFI; it must not reproduce wire
messages, validation, transport selection, or error mapping in each language.

The adapter needs explicit Rust-owned representations for nominal IDs and
hashes, enums with an unknown-value path, byte fields, `u64` fields, rich error
payloads, and cancellation. The default remote transport and recovery policy
remain Rust client behavior. Generated language APIs may be idiomatic, but
their values and errors must retain the Rust contract's identity and presence
rules.

## Common qualification record

Every qualified package must record:

1. the Rust source revision and contract/descriptor hash;
2. the exact UniFFI bindgen, Rust, native target, and packaging tool versions;
3. the generated-source hash and package hash;
4. the target platform and install command from a clean environment; and
5. consumer evidence for serialization, transport, streaming, cancellation,
   recovery, typed errors, and embedded behavior where supported.

Until those records exist for the current C checkout, Python, Kotlin/JVM, and
Swift remain useful generator/package prototypes rather than qualified SDKs.

## Bounded proof obligations

The following are the concrete obligations for the Rust contract and native
adapter. They are requirements for future evidence; none is recorded here as
proved.

1. **Wire identity:** each public operation keeps its declared RPC identity,
   field number, enum value, JSON name, and descriptor membership. Regeneration
   must produce the same descriptor hash when the Rust contract is unchanged.
2. **Presence and value round trips:** for every generated-valid domain value,
   encode/decode preserves `Option` presence, bytes, `u64`, nominal IDs, and
   enum values. An unknown numeric enum value must remain observable through the
   Rust unknown-value representation rather than becoming a known value.
3. **Validation agreement:** a value accepted by the Rust domain validator is
   accepted by the generated boundary, and a rejected value cannot reach the
   transport. Error variants and their payloads must preserve the Rust error
   identity.
4. **Facade correspondence:** each generated binding operation calls exactly
   one corresponding method on `actors::client::Client`; no generated language
   layer may add a second validator, wire schema, transport policy, or recovery
   implementation.
5. **Constructor invariants:** every public native constructor establishes the
   same invariants as `ActorId`, `CodeSha256`, and other Rust domain constructors,
   including rejection of invalid lengths and values.

Only bounded predicates, enum conversions, and selected constructor invariants
have a small pure-Rust proof surface. Wire identity and presence/value
round-trips depend on generated descriptors and codecs, so they require
artifact comparisons and runtime property tests. Validation agreement and
facade correspondence require source inspection plus executable boundary and
consumer tests. Package installation, native loading, remote transport,
streaming, cancellation, recovery, and OS behavior are runtime integration
claims; none can be established by a Kani harness alone.

The first bounded harness should stay independent of package generation and
network code:

* `valid_code_sha256(&[u8])` should be checked with a symbolic `[u8; 32]`:
  it returns true exactly when at least one byte is non-zero. This is the pure
  predicate used by `CodeSha256::new`; `Vec` allocation and the constructor's
  conversion are tested separately with runtime property tests.
* `SubscriptionState::try_from` and `From<SubscriptionState> for i32` should
  be checked for the three known values `0..=2`, then with a symbolic `i32`
  constrained outside that set to prove that
  `DomainError::UnknownSubscriptionState(raw)` preserves the unknown number.
  The same finite-value harness can be instantiated for `ActorState` and
  `ErrorCode` after the subscription proof is stable.

These harnesses target exact functions already present in
`rust/crates/actors/src/domain.rs`; they do not introduce a second contract or
validation rule. The proposed proof is bounded to fixed arrays, finite enum
domains, and one symbolic `i32`; it does not claim an unbounded theorem.

[Kani](https://github.com/model-checking/kani) is the maintained OSS candidate
for bounded model checking of small, pure Rust conversion and validation
functions. It can provide proof within explicit finite bounds; it cannot prove
native package loading, a remote service, or an unbounded stream. 

An attempted Actors run did not produce a proof receipt: the Kani execution
used a Rust 1.93 nightly compiler path while the product checkout is pinned to
Rust 1.98.1, so the run failed at toolchain compatibility before proving a
harness. The bounded obligations below remain unproved until Kani is run in an
isolated environment matching the product toolchain and its output records the
exact compiler, solver, harness, and unwind settings.

[Proptest](https://github.com/proptest-rs/proptest) is the maintained OSS
candidate for executable property tests over larger generated value spaces. It
provides shrinking counterexamples and repeatable seeds, but passing runs are
testing evidence rather than a mathematical proof. Use it for codec, presence,
unknown-enum, error, cancellation, and generated-binding cases that are not
tractable as Kani harnesses. The Kani and Proptest obligations above remain
unfulfilled until receipts identify the exact Rust revision, bounds or seeds,
and observed results.
