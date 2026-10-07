# Kotlin task-cancellation adapter design

The Rust facade exposes an optional `CancellationHandle` on every asynchronous ActorsClient method. UniFFI 0.31.0's generated Kotlin `suspendCancellableCoroutine` does not register `invokeOnCancellation`, so generated code cannot automatically signal Rust cancellation.

A package-level adapter can be generated mechanically from Rust-owned binding metadata: for each async method with an optional `CancellationHandle`, allocate one handle, register `Job.invokeOnCompletion`, call `handle.cancel()` only when the completion cause is `CancellationException`, invoke the generated method with that handle, and dispose the registration after normal return or non-cancellation failure. The adapter contains no transport, retries, request construction, or response mapping.

The probe in `consumer/src/main/kotlin/probe/ActorsAll8Probe.kt` implements this forwarding shape only as qualification evidence. It proves coroutine task cancellation reaches the Rust-owned handle (`task_cancellation_forwarded=true`). It does not claim generated UniFFI automatic cancellation. Production wrapper generation should consume the same Rust-owned method metadata and preserve each generated method's exact signature.
## Generated package adapter prototype

`consumer/ActorsCancellationAdapter.kt` is the external package adapter generated from the Rust-owned async metadata: every async Actors method with an optional `CancellationHandle` gets a caller-facing overload without that parameter. The adapter allocates the handle, installs `Job.invokeOnCompletion`, and forwards only coroutine cancellation to Rust. A synchronized completion gate makes normal-completion versus cancellation ordering explicit: a completion that wins the gate disposes the registration before a later cancellation; cancellation that wins calls `CancellationHandle.cancel()`.

The adapter does not alter generated UniFFI Kotlin, expose a consumer cancellation flag, or duplicate transport, retry, request, validation, or response behavior. `consumer/AutomaticCancellationAdapterProbe.kt` exercises a pending operation's normal completion, cancellation, and 200 completion/cancellation races, then calls the generated `connectActorsWithCa` and `ActorsClient.inspectActor` through the adapter against the current fixture.

Observed with native DLL SHA-256 `A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241`, Kotlin 1.9.21, coroutines 1.8.0, JNA 5.15.0:

- `adapter_normal_completion_cancelled=false`
- `adapter_cancellation_forwarded=true`
- `adapter_race normal=100 cancelled=100`
- `adapter_live_inspect=actor-a,state=ACTIVE,epoch=9`
- `KOTLIN_AUTOMATIC_CANCELLATION_ADAPTER_PASS`
