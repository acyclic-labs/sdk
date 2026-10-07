# Current Actors PHP protobuf/gRPC prototype (2026-10-07)

This is the current-source cohort. It is separate from the immutable historical `rendered/actors.proto` message-only experiment in the parent directory. The current proto is rendered by the Rust-owned `acyclic-actors` build script from `Q:\sdk\work\sdkgen-main-port-current`; it contains `ActorsService` and all eight RPC declarations.

## Source and build identity

- Source checkout observed at build: `f8bf8daf64fae0c04366fd1e5326053dde3d4e9e`; the checkout advanced to `81eb6ef75df47223275e02f7c2ee0091e788fed5` after the build, with the producer files used here committed there.
- Cargo package: `acyclic-actors 0.2.0`; pinned Rust toolchain `1.98.1`; `cargo check --locked -p acyclic-actors` passed in an isolated target directory.
- Exact build output (`OUT_DIR`): `.tmp-current-actors-target/debug/build/acyclic-actors-c7e322b1cd979429/out` in the task worktree. The durable copies are `actors.proto` and `acyclic-actors-v1.bin` here.
- Proto SHA-256: `D568A12B4D07DCAFEF8502CAF61BCFA3E2BC514357D45A5291B4542E0D6B59DE`.
- Descriptor SHA-256: `C565B7D1FA43B3E96FC068EC7DB687E504603A377E9E592D15033F15CD02E9E9`.

## PHP generation

Official Protocol Buffers `protoc` 31.1 (Linux distribution SHA-256 `caaf8517e57c57d34a7d6f0544172d9051abf58556aa35c70d3fb0d824b8cfbb`; Windows companion was `128705333700EF804992656829B558E919686944D948BDEAD6CAEDF011FC238B`) and the gRPC PHP plugin binary published by `s1lver/grpc-php-plugin-binary` 1.0.0 (compiled from gRPC v1.53.0; plugin SHA-256 `ABE25E334173F3949693FD680695491CDA46DCCB37C1F424DCEB9666934AA3B5`) generated the checked-in evidence under `generated/`.

The generated `ActorsServiceClient.php` has these exact typed caller methods: `CreateActor`, `UpdateActor`, `InspectActor`, `AddSubscription`, `RemoveSubscription`, `ResumeSubscription`, `CheckpointActor`, and `InvokeActor`. The generated message classes preserve the Rust contract's `uint64` fields and PHP's generated `int|string` representation, including `handler_timeout_millis`, `configuration_revision`, `delivered_cursor`, `completed_cursor`, `recoverable_cursor`, `expected_configuration_revision`, and `SubscriptionStart.cursor`. All 29 generated PHP files pass `php -l` under WSL PHP 8.1.2.

The archived message-only proto evidence remains unchanged and must not be used as current Actors-service evidence. No contract mirror or handwritten ABI was added.

## Alef classification

The current WSL Alef source adapter remains `SOURCE_ADAPTER_GENERATED_BUT_NOT_COMPILABLE` (88 errors). The errors cluster into shared adapter gaps: Arc ownership/borrow shape mismatches, missing `Default`/`Deserialize` semantic type support, repeated Vec PHP conversion, and metadata conversion reference/type mismatches. This is evidence about the maintained source adapter, not a change to the Rust producer or generated PHP contract.

## Fresh Composer/native runtime qualification

The fresh runtime cohort is recorded under `runtime-qualification-20261007/`. It installs Composer `google/protobuf v4.33.6` and `grpc/grpc 1.82.0`, builds PECL protobuf 4.33.6 and gRPC 1.82.2 extensions, and runs the generated client against a live descriptor-driven server using the exact current proto/descriptor above. All eight RPCs pass with bearer authentication; optional `uint64` presence, unauthenticated status 16, server status 7, and in-flight cancellation status 1 are evidenced in `terminal-runtime.txt` and `server-events.jsonl`.

The runtime cohort intentionally records a PHP limitation: the largest signed-safe `uint64` value (`9223372036854775807`) round-trips, while the full unsigned maximum (`18446744073709551615`) is rejected by the current PHP 8.1 protobuf setter or decodes as `-1` in the pure-PHP path. Generated PHPDoc remains `int|string`; the current Rust descriptor has no nominal Rust ID-brand metadata, so no handwritten nominal wrapper was introduced. See `runtime-receipt.json` and `u64-upper-bound-limitation.txt`.

`terminal-negative.txt` records nominal request and nested message rejection against the same generated extraction. There is no raw native handle in this generated client surface; semantic Rust ID branding remains unavailable because the producer descriptor has no brand metadata for its string/uint64 ID fields.

The full unsigned-64 gate remains **FAIL**. `runtime-qualification-20261007/terminal-u64-boundaries.txt` tests `0`, `9223372036854775807`, and `18446744073709551615` through construction/round-trip and service boundary. Native PECL protobuf rejects the full maximum; pure-PHP protobuf clamps it and fails the live service assertion. Brick\Math 0.12.3 was tested as a maintained arbitrary-precision value type, but generated protobuf setters do not accept it as a wire integer. No ordinary alternate PHP CLI was available in WSL, and the maintained protobuf 64-bit path uses `intval()`. Therefore this cohort is runtime evidence with a signed-safe boundary and an explicit full-u64 failure, not full unsigned qualification.
