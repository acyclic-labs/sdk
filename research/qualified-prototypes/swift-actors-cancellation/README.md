# Swift actors cancellation qualification

This directory is a source-only maintained-UniFFI Swift qualification artifact. It records the smallest generator-side change needed for Swift structured-concurrency cancellation and a consumer that exercises the generated API. Generated Swift, FFI headers, module maps, native libraries, SwiftPM build products, and Cargo targets are deliberately runtime staging products and are excluded from this artifact.

The pinned generator snapshot was read from `Q:\sdk\work\uniffi-swift-oss-prototype\source`, whose checkout commit was `c3d82376d5b5e32a7886a4af7f8a65de8fe60795`. The UniFFI crates used by the runner report version `0.31.0`, repository `https://github.com/mozilla/uniffi-rs`, and license `MPL-2.0`. The exact patched source templates are copied under `templates/` and are identified by their SHA-256 values in `receipt.json`.

The patch has three generator-owned pieces:

- `Async.swift` passes the existing generated `rust_future_cancel_*` symbol to the async helper and wraps the poll/complete sequence in `withTaskCancellationHandler`, retaining `defer { freeFunc(rustFuture) }`.
- `macros.swift` emits the cancel symbol at every generated async call site.
- `Helpers.swift` maps Rust `CALL_CANCELLED` to Swift `CancellationError()`; the unpatched 0.31 helper traps with `fatalError("Cancellation not supported yet")`.

The source consumer in `consumer/` passes `cancellation: nil` to both `connectActorsWithCa` and `inspectActor`. It does not construct or manage a public/manual cancellation handle. `runner.ps1` verifies the three pinned template hashes, runs the maintained UniFFI runner against a Rust cdylib, stages the generated output into a temporary SwiftPM package, builds it with Swift 6.4, and runs the live pending-operation probe.

Use a patched pinned source checkout whose three template hashes match `receipt.json`, then run:

```powershell
.\runner.ps1 `
  -UniFFISource Q:\sdk\work\uniffi-swift-oss-prototype\source `
  -RustDll Q:\sdk\work\actors-uniffi-all8-check-current-20261007\debug\acyclic_actors_uniffi.dll `
  -FixtureOptions Q:\sdk\work\root-pending-actors-fixture-options.json
```

The live gate result recorded in `receipt.json` is baseline `started=7, aborted=6, active=0`, active before cancellation `started=8, aborted=6, active=1`, then final `started=8, aborted=7, active=0`, with Swift task outcome `CancellationError()`.

The next maintained package step is to upstream these template changes through the pinned UniFFI Swift generator path and regenerate the normal consumer package from Rust-owned metadata. Do not preserve or publish the staged generated output as source.
