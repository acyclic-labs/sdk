# Pinned UniFFI C# backend source qualification

This source-only qualification uses NordSecurity `uniffi-bindgen-cs` tag
`v0.11.0+v0.31.0` (commit `e10ce410eb3a10cc19c7928b93ea8d84e038c034`, MPL-2.0),
targeting UniFFI `0.31.0`. The source patch is in
`backend-source-cancellation.patch`; snapshots of the changed templates are
kept beside this file. No production ABI or Rust facade file was changed.

The backend now emits an optional `CancellationToken` on generated async
methods and constructors. It registers that token against each existing UniFFI
Rust future, calls the generated `rust_future_cancel_*` symbol, maps the
standard future status 3 to `OperationCanceledException` carrying the request
token, and disposes the registration before freeing the future. This reuses
UniFFI's existing future continuation map and native cancel symbols. It does
not add an Actors operation wrapper, a managed cancellation-handle adapter, or
a second registry. Existing Rust `CancellationHandle?` arguments remain part
of the facade ABI and are passed as `null` by the no-handle consumer.

The raw object constructor fix remains source-level: generated raw handle
construction is `internal Type(ulong, bool)` while public nominal constructors
remain available. An external assembly forge of `new PositiveU64(1UL, true)`
produces compiler error CS1729.

`generated/acyclic_actors_uniffi.cs` was freshly generated from the Actors
cdylib with the patched source backend. It includes all eight Actors operations,
full-width `ulong`, optional values, nominal validated types, typed errors, and
`CancellationToken cancellationToken = default` on async methods. The source
compiles with the installed C# compiler. `managed/AllEightProgram.cs` compiles
and runs all eight ordinary operations using only `null` for the Rust
cancellation handle. `managed/Program.cs` runs three gated pending operations:
continuation-map peaks are `1,1,1`, each map returns to zero, the fixture
observes started=3/aborted=3/active=0, and each await throws
`OperationCanceledException` whose token is the request token. Earlier
consumer outputs from `consumer/installed-final` are retained as historical
evidence because that extraction had the stale `3b5ctbbe.re3` assembly identity.

The fresh corrected package qualification is recorded in the external
`package-receipt.json` under `fresh_corrected_extraction`. It targets
`consumer/installed-corrected` (archive SHA-256
`5A50242B7317018E550EA908487BE8C27D2E1A72A070B139DEC361798F1F2FAC`, managed
assembly SHA-256 `0CFC7C771B3A5C152FBB95D9B47C130D61DC987754036DDAADEE1CD4ED2B8269`,
assembly identity `Acyclic.Actors`, version `0.2.0.0`) and reruns all eight
operations plus three gated cancellation requests. The live fixtures used by
that run were `https://localhost:55169` for all-eight and
`https://localhost:56426` with control endpoint
`http://127.0.0.1:56428` for pending cancellation. Its terminal logs record
the corrected all-eight PASS, cancellation cleanup `baseline=0`,
`peaks=1,1,1`, `started=9/aborted=9/active=0`, and fresh CS1729/CS1503
negative probes against the corrected assembly. A clean external net8.0
MSBuild consumer restores from the local feed and builds with zero warnings and
errors; the package targets copy the native closure with SHA-256
`A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241`.

A separate cross-platform cohort is retained at the external package path
`consumer/installed-cross-platform` and
`feed-cross-platform/Acyclic.Actors.0.2.0.nupkg` (archive SHA-256
`CC94B745551B2236561A64DCCA3AC200AB9CCE6CB44AEB0B042CCA63197AECF6`). It
contains the same managed assembly and Windows native asset plus the Linux
x64 native asset `libacyclic_actors_uniffi.so` (SHA-256
`60B22ED3000A996DB97EAF66B8C293649F7C895DFC35AEEF2853F2BCE146AC258`). A
clean external Linux net8.0 MSBuild consumer restored and built with zero
warnings and errors, copied that `.so`, and ran all nine generated Actors
methods successfully against a WSL-local fixture; the terminal evidence is
`corrected-linux-msbuild.terminal.log` and `corrected-linux-runtime.terminal.log`.
The Linux native library was built from this source revision with Rust
1.98.1 and the consumer with task-local .NET SDK 8.0.425. The available
`ivar` macOS host has Rust 1.96.0 and no .NET SDK, so no macOS package claim is
made.

The generated source and consumers are retained as qualification artifacts;
no DLL, native binary, Cargo target, or managed build output is persisted in
this primary research directory.
