# Ruby/PHP maintained-generator viability

This is source-only qualification evidence against the exact Actors UniFFI native producer used by the macOS arm64 cohort. It does not add a handwritten ABI mirror or alter the producer.

## Maintained-option inventory

The pinned official UniFFI 0.31.0 backend list is Kotlin, Swift, Python, and Ruby. The exact binary rejects both `--language c` and `--language php` with `possible values: kotlin, swift, python, ruby`; this is a generator capability result, rather than a claim that PHP as a runtime is impossible.

The maintained options investigated for PHP were:

- Mozilla `cbindgen` 0.29.4 (MPL-2.0): a Rust-to-C header generator. It produced only standard includes from the Actors UniFFI crate because the exported ABI is proc-macro generated.
- [`xberg-io/alef`](https://github.com/xberg-io/alef) 0.107.0 (MIT): a source-driven polyglot generator with PHP native-extension and C-FFI targets. Against the exact Actors source it successfully extracted IR and emitted a PHP native-extension adapter, but that adapter directly depends on `acyclic-actors-uniffi` source and does not consume UniFFI metadata or an existing cdylib. Windows compilation is blocked by `ext-php-rs`'s stable-channel `abi_vectorcall` feature; WSL compilation reaches the producer and then fails with 88 generated Arc/semantic-type/PHP argument mismatches (raw output hash is in the receipt).
- [`ext-php-rs`](https://github.com/extphprs/ext-php-rs) (MIT/Apache-2.0): a Rust-to-native-PHP-extension framework using PHP-specific Rust attributes. It requires a producer-side adapter and does not consume an existing UniFFI metadata contract.
- [`FFIMe`](https://github.com/ircmaxell/FFIMe) and [`klitsche/ffigen`](https://packagist.org/packages/klitsche/ffigen) (MIT / package WIP): PHP FFI wrapper generators that require a C header as input; neither emits a header from UniFFI metadata.
- [`ant-ffi`](https://github.com/maidsafe/ant-ffi) (archived, domain-specific): demonstrates PHP FFI usage but maintains a custom C ABI and is not a reusable UniFFI PHP backend.

This leaves a precise prototype boundary: no maintained option tested here can consume the existing Actors UniFFI metadata and emit a PHP consumer without adding a producer-side adapter or handwritten ABI mirror. That is a current pipeline gap, not an exclusion of PHP itself.

Remote protobuf coverage is a separate viable path. The canonical rendered Actors `.proto` was passed to pinned `protoc` 31.1 with `--php_out`, producing typed PHP message classes for `NestedActor`, `RegisterActor`, and `SubscriptionStart`. The source contract has no `service` declaration, so this does not generate an ActorsClient or cancellation path; official gRPC PHP generation requires a `.proto` service and `grpc_php_plugin` ([PHP gRPC basics](https://grpc.io/docs/languages/php/basics/)). The generated messages demonstrate remote schema coverage, while the embedded UniFFI metadata gap remains.
## Ruby: executable unary/type path

- Generator: Mozilla `uniffi_bindgen` 0.31.0 from the locked `uniffi = 0.31.0` source, MPL-2.0, repository `https://github.com/mozilla/uniffi-rs`; Cargo.lock registry checksum for `uniffi_bindgen` is `4ed0150801958d4825da56a41c71f000a457ac3a4613fa9647df78ac4b6b6881`.
- Target: exact macOS arm64 producer `libacyclic_actors_uniffi.dylib`, SHA256 `7604093B31926938B10C7E70B7AD783D3E98121016EA14DF8B60ECB3DE495155`, built from source revision `371bb4170e16aca973176b6756a261ee5add7297` with Rust 1.98.1.
- Runtime: macOS arm64 Ruby 2.6.10 with task-local `ffi` 1.17.2-arm64-darwin; generated Ruby source SHA256 `B6CFC6B45A11A23B81D8269B5561D4CAB66516B0B4BBD2E26A9D29E00E28DA13`.
- Command: `actors-uniffi-bindgen generate --language ruby --no-format --metadata-no-deps --config /tmp/ruby-config.toml <producer-dylib>` where `ruby-config.toml` sets `bindings.ruby.cdylib_path` to the producer's absolute path.
- Terminal result: `RUBY_ACTOR_ID_TYPE=AcyclicActorsUniffi::ActorId`, `RUBY_ACTOR_ID_VALUE=ruby-fixture`.
- Cancellation value object result: `RUBY_CANCEL_BEFORE=false`, `RUBY_CANCEL_AFTER=true`.

The generated Ruby source exposes the typed `ActorsClient` methods and `CancellationHandle`, but UniFFI 0.31.0's Ruby backend emits the async Actors methods as synchronous wrappers around a `uint64` future handle and then calls `consumeIntoOptionalTypeActorObservation` on that integer. Against the real fixture this fails before issuing a request: `NoMethodError: undefined method consumeIntoOptionalTypeActorObservation for 105553120178704:Integer`, and fixture state remains `started=0, aborted=0, active=0`. Therefore Ruby has a qualified unary/type path and cancellation-handle value semantics, but no valid in-flight cancellation qualification for these async Actors methods with this maintained generator revision.

## PHP: maintained C ABI path and generator capability evidence

- Maintained C ABI generator exercised: Mozilla `cbindgen` 0.29.4, MPL-2.0, repository `https://github.com/mozilla/cbindgen`.
- Command: `cbindgen --lang c --output target/actors-uniffi.h` from the exact `actors-uniffi` crate.
- Result: exit 0, header SHA256 `CB16FEEB6243454A636876FA5A6E046595E73F112EAA59DAC9381732696D6452`, containing only the standard includes and **zero** `acyclic`/`uniffi_` declarations.
- Cause: UniFFI's exported C ABI is emitted by proc-macro scaffolding and is present in the compiled native library metadata/symbols, not as Rust `pub extern "C"` items that cbindgen can render. PHP's FFI extension would require a manually maintained declaration of the UniFFI `RustBuffer`, `RustCallStatus`, opaque handles, and async/future ABI. That would be a handwritten contract mirror, outside this prototype's allowed scope.
- Exact official generator checks against the same producer and `uniffi-bindgen 0.31.0` binary (SHA256 `79E3D19ADCE7A8E6A81805769E0A85B7A26B429EDD9604445EDAC5CDF2D91A0`):
  - `--language c`: exit 2, `invalid value 'c'`, possible values Kotlin/Swift/Python/Ruby.
  - `--language php`: exit 2, `invalid value 'php'`, possible values Kotlin/Swift/Python/Ruby.
- Task-owned WSL runtime is now installed from pinned Ubuntu Jammy packages: PHP 8.1.2 with `FFI` enabled and `php-config` 8.1.2; a libc `strlen` FFI smoke call returned `6`. The Alef adapter reached compilation against the exact producer before failing on generated adapter type/signature mismatches. Local Windows PHP 8.5.11 still reports `PHP_FFI_EXTENSION=absent`.

No package or producer files are modified by this evidence.

## Ruby: current source candidate re-export qualification (2026-10-07)

The current Actors candidate at `Q:/sdk/work/sdkgen-main-port-current` uses a Rust-owned facade and maintained UniFFI Ruby source patch. The facade materializes the nominal `ActorId`, domain roots, request records, presence, readonly accessors, and all eight operations through Rust metadata; no handwritten Ruby ABI mirror is used. Windows and WSL generated Ruby packages pass the all-eight fixture, full-u64, typed-error, and pending cancellation probes. WSL uses a task-owned local TLS fixture bound to `0.0.0.0`, reached at `127.0.0.1`, with a SAN certificate, so this scope has no Windows localhost dependency.

Exact artifact hashes, source pre/post hashes, runtime versions, and terminal output are in [`ruby-candidate-reexports-runtime-20261007.json`](ruby-candidate-reexports-runtime-20261007.json) and [`ruby-candidate-reexports-runtime-terminal-20261007.txt`](ruby-candidate-reexports-runtime-terminal-20261007.txt). The historical macOS receipt remains [`ruby-async-sourcefix-20261007-receipt.json`](ruby-async-sourcefix-20261007-receipt.json).
