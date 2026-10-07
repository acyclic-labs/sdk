# Pinned UniFFI C# backend source fix

This source-only qualification records a minimal template fix against
NordSecurity/uniffi-bindgen-cs at tag `v0.11.0+v0.31.0`
(commit `e10ce410eb3a10cc19c7928b93ea8d84e038c034`, MPL-2.0).

The unmodified backend emits a public `Type(ulong pointer)` raw-handle
constructor. For a nominal validated `PositiveU64(ulong value)`, that is a
C# signature collision and the fresh Actors generated source does not compile.
The patch in `ObjectTemplate.cs.patch` changes the raw constructor to
`internal Type(ulong pointer, bool _uniffi_raw_handle)` and updates every
backend-owned lift/factory call to pass `true`. It changes only the backend
template; generated output is not hand edited.

The generated source in `generated/acyclic_actors_uniffi.cs` was produced
from the current all-eight Actors artifact (`source_revision`
`371bb4170e16aca973176b6756a261ee5add7297`). It retains public nominal
validated constructors, full-width `ulong`, optional fields, enums, typed
errors, and cancellation-handle parameters. `managed/Program.cs` is the
source-only managed consumer: its `WithCancellation` helper creates and
disposes the generated Rust cancellation handle only for cancellable tokens.

Qualification evidence is in `receipt.json`. The source-patched generator
built successfully with Cargo and generated source compiled with .NET 8.
The external runtime probe exercised all eight Actors operations, default
(no cancellation handle) calls, a pre-cancelled `CancellationToken`, typed
service error mapping, `PositiveU64(0)` rejection, and
`PositiveU64(ulong.MaxValue)` preservation. Its isolated fixture used the
same canonical fixture implementation with only the checkpoint assertion
adapted to the consumer's idempotency key.
