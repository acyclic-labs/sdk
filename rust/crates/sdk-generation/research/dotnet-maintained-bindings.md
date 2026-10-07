# Maintained .NET C ABI generator evidence

Status: research and declaration-generation probe only. No production C ABI,
managed package, ownership policy, or SDK qualification is claimed here.

## Generator boundary

The maintained candidate for a Rust-authored C ABI is **csbindgen 1.9.8**.
Its upstream project is MIT-licensed and generates C# `DllImport` declarations
from Rust `extern "C"` functions, including Cdecl and callback declarations:
[csbindgen README](https://github.com/Cysharp/csbindgen) and
[release v1.9.8](https://github.com/Cysharp/csbindgen/releases/tag/1.9.8).
The exact `=1.9.8` pin was resolved in the external probe lockfile.

csbindgen is a declaration generator. It does not infer Rust domain meaning,
private-field invariants, byte-buffer ownership, status/error semantics, async
completion, cancellation, streams, or unknown-enum behavior. Those remain
Rust-owned design and implementation work. The current repository's future ABI
is explicitly unimplemented and unpromised; see [ffi/README.md](../../../../ffi/README.md)
and [ABI policy](../../../../ffi/abi-policy.md).

The current Actors UniFFI crate pins UniFFI `0.31.0` for its JVM slice. Mozilla's
first-party UniFFI backends are Kotlin, Swift, Python, and Ruby; C# is listed as
a third-party integration in the [official README](https://github.com/mozilla/uniffi-rs/blob/main/README.md?plain=1).
The official guide confirms Ruby is shipped first-party, but says the team keeps
existing Ruby support working and generally does not add new Ruby features
([supported languages](https://mozilla.github.io/uniffi-rs/latest/)). Ruby is
therefore first-party with a lower maintenance cadence, not a third-party C#
backend substitute.

## Type mapping evidence

The Rust Actors domain keeps `ActorId`, `CodeSha256`, and `PositiveU64` as
private-field nominal types with checked constructors ([domain](../../actors/src/domain.rs)).
A future C ABI can expose opaque pointers for these types and let managed
constructors call Rust validation. A C# `ulong` can carry a Rust `u64`, but it
does not enforce `PositiveU64`'s nonzero invariant; that check must remain in
Rust. `Option<u64>` also needs an explicit presence representation in any
future C ABI. Neither mapping is an SDK qualification receipt.

## External generator probe

The isolated probe is at
`Q:\sdk\work\dotnet-csbindgen-nominal-probe-20261007` and is not part of any
SDK checkout. It declares placeholder opaque pointer signatures for nominal
constructors/release functions and a `positive_u64_value(...)->u64` accessor,
then resolves csbindgen `1.9.8` and generates
`generated/NativeMethods.g.cs`.

Observed generated declarations include:

```csharp
internal static extern int positive_u64_new(ulong value, PositiveU64Opaque** @out);
internal static extern ulong positive_u64_value(PositiveU64Opaque* handle);
internal static extern void positive_u64_release(PositiveU64Opaque* handle);
```

This proves only that csbindgen emits the expected C# primitive and opaque
pointer declaration shape. It does not establish SDK ownership, constructor
semantics, error mapping, async/cancellation support, or generated `ulong`
handle checks; those require a separately authored and qualified Rust ABI.

## Managed façade probe

A second disposable probe at
`Q:\sdk\work\dotnet-csbindgen-managed-consumer-probe-20261007` consumed the
generated declarations with the bundled .NET SDK `8.0.425`. The managed build
passed with zero warnings and errors, and the toy native library exercised
`ulong.MaxValue`, Rust-side rejection of zero, explicit optional presence, and
a Cdecl callback.

The thin shape is an internal generated `NativeMethods` class plus public
nominal classes backed by one typed `SafeHandle` each. Constructors pass values
to Rust and map only the returned status; they do not duplicate domain
validation. `SafeHandle.ReleaseHandle` calls the generated Rust release symbol.
Callback state still requires handwritten `GCHandle` retention and a `finally`
release. Optional values require an explicit presence bit or equivalent Rust
result shape; a zero sentinel would collapse valid `u64` values.

These are proof inputs for the ABI policy owner: handle release must be
idempotent and exactly once, callback completion must have a defined terminal
and cancellation rule, optional values must preserve absent versus zero, and
`u64` must round-trip both zero and `UInt64.MaxValue`. The generated C# layer
must remain a forwarding/lifetime adapter; transport, retry, recovery,
validation, and error meaning stay in Rust. The probe does not define a
production ABI and does not qualify the Actors SDK.

A future deployment should pin `csbindgen = "=1.9.8"` in the dedicated
binding-generation crate, retain its lockfile checksum, record the generator
and Rust source revisions plus generated-source/native hashes, and regenerate
only after the complete eight-operation Actors surface and ownership design
have passed review.

## Current Rust-owned target inventory

The current Actors source has two generated JavaScript target families and one JVM prototype; none is a .NET package:

| Target | Current generator/resolution | Rust-owned exports and exact gap |
| --- | --- | --- |
| TypeScript Node | [N-API-RS](https://napi.rs/) declarations from the `napi` workspace family; the manifest requests `napi 3.6.1`, while the current lock resolves `napi 3.12.7`, `napi-derive 3.6.8`, and `napi-build 2.4.4` | All eight unary operations, CA connect, cancellation, `ActorId`, `CodeSha256`, `PositiveU64`, typed result/error envelopes. Requests/responses cross as protobuf `Buffer`, so domain records remain Rust-owned. `BigInt` preserves `u64`; Rust still enforces nonzero and digest predicates. The generator range is not an exact release pin. |
| TypeScript browser | [wasm-bindgen](https://github.com/rustwasm/wasm-bindgen) `0.2.117` exact, `wasm-bindgen-futures = 0.4.67`, with [ts-rs](https://github.com/Aleph-Alpha/ts-rs) `12.0.1` semantic declarations | All eight operations, cancellation, Rust constructors, validation helpers, and protobuf-byte responses. `bigint` and branded types preserve intended shape, but brands do not validate at runtime; Rust constructors do. Errors preserve semantic category/raw enum value in the generated JS error metadata. |
| Kotlin/JVM prototype | UniFFI cohort `0.31.0` exact | Only `connect_actors`, `connect_actors_with_ca`, and `inspect_actor`; nominal opaque objects, records, known enums, `Option<u64>`, bytes, structured `BindingError`, and explicit cancellation handle exist. Missing seven operation adapters and request/response records, generated package/native artifact, and an `Unknown(raw)` enum projection. Smallest remedy is more adapter metadata over existing domain/client types. |
| .NET | No SDK generator or production ABI. External csbindgen probe uses `1.9.8` exact | No current SDK exports. Future declarations can carry opaque handles and `ulong`, but SafeHandle wrappers, explicit option presence, callback/poll lifecycle, status/error payloads, and Rust validation remain custom. |

The domain currently owns private-field `ActorLimits`, `Binding`,
`SubscriptionSpec`, `ServiceError`, all eight request types, observations,
and the `Create`/`Update`/`Inspect`/subscription/checkpoint/invoke response
projections. They are not exported by the UniFFI prototype. Their smallest
JVM remedy is an adapter record/constructor layer that delegates to these
existing Rust constructors and accessors; changing the domain or adding a
second wire model is unnecessary.

Across targets, `ActorId`, `CodeSha256`, and `PositiveU64` already have the
right Rust nominal authority. The remaining target-specific gaps are
representation gaps: JavaScript uses branded primitives and BigInt, UniFFI
uses opaque objects and Rust `Option`, and future .NET C ABI declarations
would use opaque pointers, `ulong`, explicit presence, and status envelopes.
Unknown enums are currently rejected by Rust; N-API/WASM preserve the raw
unknown value in structured semantic error metadata, while the UniFFI known
enum projection currently returns a semantic error. A forward-compatible
`Unknown(raw)` case would require a deliberate Rust domain change before any
binding adapter change.



