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
