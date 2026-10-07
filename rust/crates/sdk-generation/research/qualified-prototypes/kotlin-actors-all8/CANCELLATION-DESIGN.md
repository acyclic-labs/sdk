# Kotlin cancellation package adapter

The Rust facade owns the async export metadata: each exported operation carries an optional `CancellationHandle` for explicit cross-language cancellation. The maintained UniFFI 0.31.0 Kotlin runtime already connects coroutine cancellation to its generated future bridge. `uniffiRustCallAsync` uses `suspendCancellableCoroutine`; its `finally` invokes the generated future free function. The pinned runtime then cancels and drops the Rust async future.

The Rust-owned `actors-uniffi-cancellation-adapter` generator emits only thin package overloads. Each overload omits `CancellationHandle` from the caller-facing signature and calls the generated UniFFI method with `null`. It allocates no handle, installs no duplicate `Job` hook, and contains no transport, retry, request construction, validation, or response mapping.

This boundary is intentionally generated from `src/cancellation_metadata.rs`, outside the generated UniFFI Kotlin file. Regenerate with:

```text
cargo run --locked --features bindgen --bin actors-uniffi-cancellation-adapter -- --output <consumer>/src/main/kotlin/adapter/ActorsCancellationAdapter.kt
```

The installed Kotlin probe used the gated TLS fixture at `https://localhost:60389` with control endpoint `http://127.0.0.1:60391` and actor ID `pending-kotlin`. The probe waited for the real Rust client request to become active, cancelled the coroutine while the native operation was pending, and observed the fixture's request abort:

```text
OBSERVED native_pending request_started=true state={"started":2,"aborted":0,"active":1}
OBSERVED native_pending coroutine_cancelled=true
OBSERVED native_pending abort_observed=true state={"started":2,"aborted":1,"active":0}
KOTLIN_NATIVE_PENDING_CANCELLATION_PASS
```

This is a real pending native transport operation through the generated method with `null` cancellation handle; no fake handle-only test is used for the cancellation claim.
Continuation cleanup evidence (2026-10-07): a test-only probe in the generated package observed `uniffiContinuationHandleMap.size` at baseline 0, during 1, and after 0 for three distinct real pending Rust operations. The gated fixture counters advanced `started=4..6`, `aborted=3..5`, and `active` returned to 0 after each cancellation. The terminal marker was `KOTLIN_NATIVE_PENDING_CONTINUATION_MAP_PASS`. This confirms the maintained generated bridge drains its continuation entry after native abort; no custom `Job` hook or handle lifecycle is needed.
