# C# pending native cancellation qualification

This source-only probe uses the source-fixed pinned `uniffi-bindgen-cs`
output and the exclusive pending Actors fixture at `https://localhost:60389`.
The fixture holds `InspectActor` for `pending-csharp-*` IDs and exposes state
at its control endpoint. Three real pending calls reached `active=1`; the
managed `CancellationToken` cancelled the generated Rust operation, producing
`BindingException.Cancelled`, and the fixture observed `aborted=1..3` with
`active=0` after each call. The generated C# continuation map was checked by
test-only reflection: baseline 0, pending 1, and after 0 for all three calls.

The maintained C# backend's generated method accepts an optional generated
`CancellationHandle`, not a `CancellationToken`. Therefore the tiny
`WithCancellation` adapter in `Program.cs` owns the unavoidable mapping: it
creates one generated handle only when the caller token can be cancelled,
registers `handle.Cancel`, and disposes both in a `finally`-equivalent scope.
The caller never constructs or passes a Rust handle. Passing `null` directly
cannot abort an in-flight native operation because the pinned C# runtime has
no native `CancellationToken` overload; this is the backend's precise
maintained gap versus Kotlin's coroutine bridge.

`RawHandleNegative.cs` is compiled as a separate consumer assembly against
the generated declarations. The internal marker constructor is rejected by
the external compiler (`CS1729`), while public validated constructors remain
available. No generated file is hand-edited and no custom handle registry is
introduced.
