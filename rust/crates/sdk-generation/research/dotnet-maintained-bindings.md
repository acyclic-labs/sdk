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
