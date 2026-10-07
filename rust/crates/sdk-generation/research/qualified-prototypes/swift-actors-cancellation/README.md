# Swift actors cancellation qualification

This directory is a source-only maintained-UniFFI Swift qualification artifact. It records the smallest generator-side change needed for Swift structured-concurrency cancellation and a consumer that exercises the generated API. Generated Swift, FFI headers, module maps, native libraries, SwiftPM build products, and Cargo targets are deliberately runtime staging products and are excluded from this artifact.

The pinned generator snapshot was read from `Q:\sdk\work\uniffi-swift-oss-prototype\source`, whose checkout commit was `c3d82376d5b5e32a7886a4af7f8a65de8fe60795`. The UniFFI crates used by the runner report version `0.31.0`, repository `https://github.com/mozilla/uniffi-rs`, and license `MPL-2.0`. The exact patched source templates are copied under `templates/` and are identified by their SHA-256 values in `receipt.json`. `producer-source.lock.json` pins the final Rust producer source cohort (`actors-uniffi` plus the canonical Actors crate) by path and SHA-256; the runner refuses a different producer or file set.

The patch has three generator-owned pieces:

- `Async.swift` passes the existing generated `rust_future_cancel_*` symbol to the async helper and wraps the poll/complete sequence in `withTaskCancellationHandler`, retaining `defer { freeFunc(rustFuture) }`.
- `macros.swift` emits the cancel symbol at every generated async call site.
- `Helpers.swift` maps Rust `CALL_CANCELLED` to Swift `CancellationError()`; the unpatched 0.31 helper traps with `fatalError("Cancellation not supported yet")`.

The source consumer in `consumer/` passes `cancellation: nil` to both `connectActorsWithCa` and `inspectActor`. It does not construct or manage a public/manual cancellation handle. `runner.ps1` verifies the three pinned template hashes, runs the maintained UniFFI runner against a Rust cdylib, stages the generated output into a temporary SwiftPM package, builds it with Swift 6.4, and runs the live pending-operation probe.

The final producer checkout was separately pinned in `producer-source-final-20261007.lock.json` at `Q:\\sdk\\work\\sdkgen-main-actual03bb`; this lock is a new source identity and does not relabel the historical `producer-source.lock.json`. The runner accepts `-ProducerWorkspace` and `-ProducerLockPath` for this external checkout. Its build and bindgen pass, then stop at the existing semantic-object assertion because UniFFI emits the final producer's `ActorId`, `CodeSha256`, and `PositiveU64` custom types as Swift aliases. See `swift-final-producer-20261007-receipt.json` and `swift-final-type-verification-20261007.md`; the strongest-type qualification remains blocked until that producer surface is corrected.

Use a patched pinned source checkout whose three template hashes match `receipt.json`, then run the lane from the repository root:

```powershell
.\runner.ps1 `
  -UniFFISource Q:\sdk\work\uniffi-swift-oss-prototype\source `
  -ProducerSource rust\crates\actors-uniffi `
  -FixtureOptions Q:\sdk\work\root-pending-actors-fixture-options.json
```

The runner builds `actors-uniffi` from the locked producer source before invoking the pinned Swift bindgen runner. It stages generated Swift, the C header, module map, and native library directly from that build. `-BuildOnly` performs the source, bindgen, and SwiftPM checks without contacting a fixture. A prebuilt native library is intentionally not accepted, because its producer identity cannot be proven from the file alone.

Pass `-Product ActorsAll8ConformanceConsumer -FixtureOptions Q:\\sdk\\work\\go-remote-primitive-current\\fixture-options.json` to run the source-tracked all-eight typed consumer. The runner stages both source consumers and the one C module-materialization anchor (`ffi_anchor.c`) but never tracks generated Swift, headers, module maps, native libraries, or SwiftPM build products. The source-locked build-only result is recorded in `source-lock-build-20261007-receipt.json`; the Swift-specific signature review is in `swift-specific-semantics.md`.

The live cancellation gate result recorded in `receipt.json` is baseline `started=7, aborted=6, active=0`, active before cancellation `started=8, aborted=6, active=1`, then final `started=8, aborted=7, active=0`, with Swift task outcome `CancellationError()`. The 60389 fixture is a general all-eight fixture with an optional pending-operation hook; it is not a pending-only service.

The all-eight typed consumer is staged externally at `Q:\\sdk\\work\\actors-uniffi-current-prototype\\swift-generated-all8-current-20261007i-consumer` and must be regenerated from the same Rust cdylib before each receipt. The standard all-eight fixture uses `Q:\\sdk\\work\\go-remote-primitive-current\\fixture-options.json` and `Q:\\sdk\\work\\go-remote-primitive-current\\fixture-ca.pem` at `https://localhost:55755`.

The next maintained package step is to upstream these template changes through the pinned UniFFI Swift generator path, regenerate the normal consumer package from Rust-owned metadata, then rerun the all-eight consumer against the shared fixture and retain the typed error/u64/ordinary-completion receipt beside the cancellation receipt. Do not preserve or publish the staged generated output as source.
