# Rust-owned language qualification matrix

This matrix records the current qualification state of the native language cohort
against the C foundation checkout. It is a source and evidence record, not a claim
that an external prototype is already an SDK release.

Snapshot: 2026-10-07  
Authority checkout: `C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`  
Reviewed source revision: `371bb4170e16aca973176b6756a261ee5add7297`  
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
adapter. The pure domain subset has a tracked Kani receipt below; the remaining
wire, boundary, packaging, and runtime obligations still require their own
evidence.

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
6. **Opaque-handle ownership:** a native handle is usable only while its Rust
   owner is live; creation, move/clone, and release preserve one ownership
   accounting invariant, release is idempotence-safe, and every operation on a
   stale, null, forged, or foreign handle returns a typed error before touching
   the pointee. A proof may model handles as registry keys and generations; it
   must not assume that an arbitrary foreign pointer is valid.

Only bounded predicates, enum conversions, and selected constructor invariants
have a small pure-Rust proof surface. Wire identity and presence/value
round-trips depend on generated descriptors and codecs, so they require
artifact comparisons and runtime property tests. Validation agreement and
facade correspondence require source inspection plus executable boundary and
consumer tests. Package installation, native loading, remote transport,
streaming, cancellation, recovery, and OS behavior are runtime integration
claims; none can be established by a Kani harness alone.

The bounded harness stays independent of package generation and network code:

* `valid_code_sha256(&[u8])` is checked with a symbolic `[u8; 32]`: it returns
  true exactly when at least one byte is non-zero. The same run checks rejection
  for `[u8; 0]`, `[u8; 31]`, `[u8; 33]`, and `[u8; 64]`. A separate harness uses
  Kani's maintained `kani::vec::exact_vec::<u8, 32>()` model to provide a
  symbolic, bounded 32-byte allocation, calls the real `CodeSha256::new`, and
  proves that `as_bytes()` preserves every input byte for valid non-zero input.
  This is a fixed-length constructor proof; it makes no claim about arbitrary
  or unbounded `Vec` inputs.
* A symbolic `i32` is checked through `SubscriptionState`, `ActorState`, and
  `ErrorCode` conversions. Known values round-trip, while unknown values remain
  observable in the corresponding `DomainError` payload.

* SubscriptionStart is checked directly through the existing wire conversion: every symbolic u64 cursor is preserved exactly, the CurrentHead(true) payload and oneof presence are preserved, while CurrentHead(false) and a missing start are rejected.
* PositiveU64 is checked directly through its Rust-owned NonZeroU64 constructor: every symbolic u64 succeeds exactly when nonzero, preserves the exact value through get(), and maps zero to Contract(InvalidArgument).
* ActorLimits is checked through its Rust-owned three-value constructor: every symbolic timeout, memory, and checkpoint value is preserved exactly when all are nonzero, and any zero component maps to Contract(InvalidArgument).

These harnesses target exact functions already present in
`rust/crates/actors/src/domain.rs`; they do not introduce a second contract or
validation rule. The recorded proofs cover the bounded fixed arrays and exact
32-byte allocation above, finite enum domains, and symbolic `i32` and `u64`
values; they do not claim an unbounded theorem.

[Kani](https://github.com/model-checking/kani) is the maintained OSS candidate
for bounded model checking of small, pure Rust conversion and validation
functions. It can provide proof within explicit finite bounds; it cannot prove
native package loading, a remote service, or an unbounded stream.

### C ABI opaque-handle proof boundary

Before any C ABI is used as the native substrate for another language, the
Rust side should expose a small pure state machine for proof, independent of
raw pointer representation:

* A fresh `(slot, generation)` key enters `Live` exactly once. A live operation
  preserves the key and its owned state; release transitions it to `Closed` and
  cannot free it twice.
* Releasing, borrowing, or cancelling a closed generation is rejected without
  dereferencing a foreign value. Reusing a slot requires a different
  generation, so an old key cannot address a new object; generation exhaustion
  must return an error before wrapping rather than silently reusing a key.
* A move consumes the source owner without increasing the live-owner count. A
  clone is allowed only when the contract names shared ownership and increments
  that count exactly once; a borrowed call cannot outlive its owner.
* Repeated release has one documented result, such as a typed `AlreadyClosed`
  error, and never performs a second destruction or decrements ownership below
  zero.
* Every exported fallible operation has a no-unwind result path. On an error or
  cancellation, the handle state remains either the pre-call live state or the
  explicitly documented closed state, and no callback is invoked after its
  owner has been released.

These are finite transition properties suitable for a Kani harness over an
integer key, generation, owner count, and closed flag. They prove the Rust
ownership protocol, not the validity of an arbitrary C address, allocator
behavior, ABI calling convention, thread scheduler, or foreign-language
garbage collector. Those boundary claims require an implemented C ABI and
consumer tests that pass only handles returned by the Rust constructors. No
C ABI implementation is qualified by this proof section alone.

### Current bounded proof status

The current receipt `rust/crates/actors/proofs/kani-domain-invariants-current.json`
records Kani 0.68.0 with CBMC 6.11.0, Rust nightly 1.100.0, unwind 65, and one
verifier worker. All ten listed harnesses passed with exit code 0: the symbolic
digest predicate, wrong-length cases, exact 32-byte constructor preservation,
enum, subscription-presence, PositiveU64, and ActorLimits constructor
invariants. The receipt identifies the exact source revision and
hashes the complete Actors Rust source tree, build inputs, manifests, lockfile,
toolchain/config files, and portable proof runner. Its external evidence log is
`foundation-kani-068/runner-validation-10-final.log`.

The Objects request receipt `rust/crates/objects/proofs/kani-request-invariants-current.json`
is a separate four-harness proof of range resolution, upload-size admission,
and multipart-number bounds. It is source-bound to the Objects Rustc dependency
closure and does not extend the Actors receipt or qualify an Objects transport.

The older individual Kani receipts remain historical evidence. The current
combined receipt is the source of truth for the bounded proof snapshot.

The earlier Kani 0.67 attempt is retained as historical failed evidence: it
used a Rust 1.93 nightly compiler path and stopped before proving a harness.
The current isolated Kani 0.68 run uses its pinned nightly compiler and records
the exact compiler, solver, ten-harness command, source inventory, output
validation policy, source hashes, and unwind setting in
`rust/crates/actors/proofs/kani-domain-invariants-current.json`.

[Proptest](https://github.com/proptest-rs/proptest) is the maintained OSS
candidate for executable property tests over larger generated value spaces. It
provides shrinking counterexamples and repeatable seeds, but passing runs are
testing evidence rather than a mathematical proof. Use it for codec, presence,
unknown-enum, error, cancellation, and generated-binding cases that are not
tractable as Kani harnesses. The remaining Kani and Proptest obligations stay
open until receipts identify the exact Rust revision, bounds or seeds, and
observed results.
