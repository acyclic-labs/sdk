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

`CANCELLATION-DESIGN.md` describes the adapter boundary. The adapter forwards coroutine cancellation to the Rust-owned `CancellationHandle`; it does not implement transport, retries, validation, or response mapping.

The external receipt records a successful installed consumer run against the shared TLS fixture. The live fixture endpoint and certificate are intentionally not checked into this source-only directory.
The automatic cancellation prototype consists of `consumer/ActorsCancellationAdapter.kt` and `consumer/AutomaticCancellationAdapterProbe.kt`. The adapter provides overloads that omit `CancellationHandle`, while the probe covers pending-operation normal completion, cancellation, and a 200-iteration completion/cancellation race before a live generated `inspectActor` call. It is compiled alongside the regenerated Kotlin source; no generated file is edited.
