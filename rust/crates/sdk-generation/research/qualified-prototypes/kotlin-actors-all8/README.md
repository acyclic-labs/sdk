# Kotlin Actors all-eight qualification prototype

This directory preserves the source-only consumer reproduction for the maintained UniFFI 0.31.0 Kotlin qualification. It contains no generated Kotlin binding, native library, Maven target, fixture certificate, or package binary. Those remain in the external `Q:\sdk\work` artifact directories named by `QUALIFICATION-MANIFEST.txt`.

The Rust-owned generator configuration is:

```toml
[bindings.kotlin]
generate_immutable_records = true
```

Generate the Kotlin binding from the pinned Rust cdylib with the pinned 0.31.0 bindgen executable, then copy that generated file into the Maven consumer's `src/main/kotlin/uniffi/acyclic_actors_uniffi/` directory. The consumer source exercises:

- all eight Rust-owned request constructors;
- CA-authenticated connect and typed inspection;
- `createActor`, `updateActor`, `addSubscription`, `removeSubscription`, `resumeSubscription`, `checkpointActor`, and `invokeActor`;
- immutable Kotlin records and unsigned/byte-preserving values;
- pre-cancelled Rust operations;
- coroutine cancellation forwarding through a thin package adapter;
- structured service errors (`grpcCode`, optional `serviceCode`, and `detailMessage`).

`CANCELLATION-DESIGN.md` describes the adapter boundary. The Rust-owned generator emits no-handle overloads that pass `null`; maintained UniFFI 0.31.0 coroutine future cleanup cancels the pending Rust future. The adapter does not install duplicate hooks or implement transport, retries, validation, or response mapping.

The external receipt records a successful installed consumer run against the shared TLS fixture. The live fixture endpoint and certificate are intentionally not checked into this source-only directory.
The maintained-runtime cancellation reproduction consists of `consumer/ActorsCancellationAdapter.kt`, `consumer/NativePendingCancellationProbe.kt`, and the test-only `consumer/NativePendingContinuationMapProbe.kt`. The adapter is emitted from Rust-owned metadata and only passes `null` to generated methods. The pending probe observes a real native abort; the same-package map probe observes continuation-map size 0→1→0 across three cancellations. Both compile alongside regenerated Kotlin source; no generated file is edited.

`KOTLIN-PORTABLE-PACKAGE.md` records the installed-JAR, WSL constructor, and live eight-operation receipts for JNA's standard classpath-native resource layout and the remaining platform scope.

`KOTLIN-FINAL-PRODUCER-INTEGRATION.md` records the integration boundary for
the single-domain Rust cutover. `qualify-final-producer.ps1` is a local,
source-only audit: it extracts the producer-owned semantic roots, eight
operations, and routes from the active checkout, hashes the source closure,
and checks an exact task-local JAR's standard JNA resource roots. It writes
JSON and raw terminal receipts under `audit/` and performs no publication or
global installation.

