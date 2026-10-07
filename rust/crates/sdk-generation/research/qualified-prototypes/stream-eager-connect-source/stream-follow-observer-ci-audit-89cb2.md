# Stream follow recovery and observer/CI audit — origin/main 89cb2

Date: 2026-10-07

## Source identity

- Production candidate inspected: `Q:/sdk/work/stream-main-port`
- Candidate HEAD at inspection: `159b3d98571697838757599f6efe6110bb84d465`
- Candidate working tree: one pre-existing dirty file, `rust/crates/stream-napi/src/lib.rs`, owned by the follow-recovery work; no edits made by this audit.
- Main baseline inspected: `89cb2ec2e1c3b7a48c6555c0c3f9c7dcd77bdf3f`
- Main Stream gRPC blob: `619ca19f2c404bed3d101ce8ebd1e9973ee7f2cc`
- Main qualification workflow blob: `0308908795586fa7202acbbffa804e2d11a6b157`
- Main release workflow blob: `d4174b7ab362108ab0fb17a8b8bdb1c756be5ab1`

## Follow recovery audit

The current native bridge keeps the Rust `RecordStream` cursor in `FollowState.records`. `NativeStreamFollow::next_result` owns the retry loop and treats only `StreamError::Unavailable` as transient. It polls the same stream again after a 10 ms delay, so the cursor's `next` sequence is retained by Rust and is not reconstructed from JavaScript. A successful item is encoded only after the Rust stream returns it. Terminal stream errors (including authorization errors) are converted to one failure result and are not retried.

The retry delay and active poll are both wrapped by `run_with_cancellation`. `NativeStreamFollow::close` marks the cursor closed, cancels its token, and takes the transport stream. Abort in `native.ts` calls both `cancellation.cancel()` and `follow.close()`. Thus cancellation during an active poll or the delay releases the stream and ends the native iterator; the repeated-unavailable test exercises this bounded path. The checked follow tests are at `rust/crates/stream-napi/src/lib.rs:330-439` and `:1125-1179` in the candidate.

The canonical Rust gRPC cursor has the same cursor-preserving behavior in `RecordCursor`: on retryable stream status it advances endpoint preference, sleeps 10 ms, drops only the active endpoint, and reopens from `cursor.next`. `retryable` excludes peer `Cancelled`, permission, and other terminal statuses. This path is `rust/crates/stream/src/grpc.rs:408-467` and `:537-591`.

The main-branch TypeScript ConnectRPC adapter (`typescript/packages/stream/src/grpc.ts`) does not implement a separate transient retry loop. It passes the original request once, tracks `next` only for response validation, forwards `AbortSignal`, returns on an already-aborted signal, and suppresses cancellation in its catch path. Recovery therefore remains a native/Rust concern; adding a second TypeScript retry loop would risk duplicate cursor policy.

## Observer/options audit

Main's observer change is provider-local: `StreamGrpcOptions.observer` is sent through `observeInterceptors`, and `HttpStreamProviderOptions.observer` wraps each HTTP route with `observed`. `StreamEnvironment` itself has no observer field on `origin/main`; `DefaultStreamProvider` in the candidate selects native or HTTP and currently stores only endpoint/token/CA. Consequently, a consumer constructing a provider directly can observe HTTP/gRPC calls, while automatic `Stream.fromEnv` dispatch has no public observer option to preserve or drop. If the post-main integration adds `observer` to the environment, the minimal required parity is to retain it in `DefaultStreamProvider` and pass it to the selected HTTP provider and the native adapter's per-operation observer boundary. The Rust native operation authority must remain unchanged; do not duplicate retry or protocol logic in TypeScript.

## CI preflight

`origin/main` removes the old Stream-native qualification workflow from the ordinary qualification graph. Pull requests and pushes run the generic `qualification` matrix only; the deleted downstream native/Rust/package lanes are no longer ordinary-lane dependencies. Release assembly now calls `qualification.yml` and agent-host qualification explicitly, and waits for both before assembly. Publish workflows accept successful qualification runs from `workflow_dispatch` or `release`, not only `push`. This is a deliberate downstream-lane change, not an inadvertent ordinary PR dependency. The ordinary lane therefore cannot by itself qualify the native Stream package; the package receipt must remain an explicit release/feature qualification gate.

No tests or builds were run by this read-only audit. No production source was changed.

## Post-main WASM/lock closure check

The main commit changes the Stream producer inputs that must be included in any post-integration reproducibility receipt: `Cargo.lock` blob `e6257a32e510efddb95130499e7538a989978ec7`, `rust/crates/stream/Cargo.toml` blob `378d6dece6891f133d3124d84a61d4ac1a3a0f1e`, and `scripts/build-wasm.mjs` (14 added / 2 removed lines relative to the 7c source). The exact main Stream WASM blob is `951ce1c334d12b604a6d5d498a33b361e940d835` at `typescript/packages/stream/generated/wasm/acyclic_stream_wasm_bg.wasm`; the path is `generated/wasm` (without `src`). A fresh post-merge package receipt must hash this generated output from the exact source before staging, then assert source-generated-package-installed byte equality.
