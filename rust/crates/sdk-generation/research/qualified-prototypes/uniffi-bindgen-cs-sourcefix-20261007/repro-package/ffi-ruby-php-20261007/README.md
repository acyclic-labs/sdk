# Ruby/PHP maintained-generator viability

This is source-only qualification evidence against the exact Actors UniFFI native producer used by the macOS arm64 cohort. It does not add a handwritten ABI mirror or alter the producer.

## Maintained-option inventory

The pinned official UniFFI 0.31.0 backend list is Kotlin, Swift, Python, and Ruby; there is no PHP backend in that release. For PHP the maintained OSS option investigated here is Mozilla cbindgen 0.29.4, which can generate C declarations from ordinary Rust `pub extern "C"` items but cannot see UniFFI proc-macro-generated declarations. A PHP FFI consumer would therefore need an independently handwritten UniFFI header/adapter, which is outside the allowed prototype scope.
## Ruby: executable unary/type path

- Generator: Mozilla `uniffi_bindgen` 0.31.0 from the locked `uniffi = 0.31.0` source, MPL-2.0, repository `https://github.com/mozilla/uniffi-rs`; Cargo.lock registry checksum for `uniffi_bindgen` is `4ed0150801958d4825da56a41c71f000a457ac3a4613fa9647df78ac4b6b6881`.
- Target: exact macOS arm64 producer `libacyclic_actors_uniffi.dylib`, SHA256 `7604093B31926938B10C7E70B7AD783D3E98121016EA14DF8B60ECB3DE495155`, built from source revision `371bb4170e16aca973176b6756a261ee5add7297` with Rust 1.98.1.
- Runtime: macOS arm64 Ruby 2.6.10 with task-local `ffi` 1.17.2-arm64-darwin; generated Ruby source SHA256 `B6CFC6B45A11A23B81D8269B5561D4CAB66516B0B4BBD2E26A9D29E00E28DA13`.
- Command: `actors-uniffi-bindgen generate --language ruby --no-format --metadata-no-deps --config /tmp/ruby-config.toml <producer-dylib>` where `ruby-config.toml` sets `bindings.ruby.cdylib_path` to the producer's absolute path.
- Terminal result: `RUBY_ACTOR_ID_TYPE=AcyclicActorsUniffi::ActorId`, `RUBY_ACTOR_ID_VALUE=ruby-fixture`.
- Cancellation value object result: `RUBY_CANCEL_BEFORE=false`, `RUBY_CANCEL_AFTER=true`.

The generated Ruby source exposes the typed `ActorsClient` methods and `CancellationHandle`, but UniFFI 0.31.0's Ruby backend emits the async Actors methods as synchronous wrappers around a `uint64` future handle and then calls `consumeIntoOptionalTypeActorObservation` on that integer. Against the real fixture this fails before issuing a request: `NoMethodError: undefined method consumeIntoOptionalTypeActorObservation for 105553120178704:Integer`, and fixture state remains `started=0, aborted=0, active=0`. Therefore Ruby has a qualified unary/type path and cancellation-handle value semantics, but no valid in-flight cancellation qualification for these async Actors methods with this maintained generator revision.

## PHP: maintained C ABI path is blocked without a handwritten contract

- Maintained C ABI generator exercised: Mozilla `cbindgen` 0.29.4, MPL-2.0, repository `https://github.com/mozilla/cbindgen`.
- Command: `cbindgen --lang c --output target/actors-uniffi.h` from the exact `actors-uniffi` crate.
- Result: exit 0, header SHA256 `CB16FEEB6243454A636876FA5A6E046595E73F112EAA59DAC9381732696D6452`, containing only the standard includes and **zero** `acyclic`/`uniffi_` declarations.
- Cause: UniFFI's exported C ABI is emitted by proc-macro scaffolding and is present in the compiled native library metadata/symbols, not as Rust `pub extern "C"` items that cbindgen can render. PHP's FFI extension would require a manually maintained declaration of the UniFFI `RustBuffer`, `RustCallStatus`, opaque handles, and async/future ABI. That would be a handwritten contract mirror, outside this prototype's allowed scope.
- Local PHP 8.5.11 also reports `PHP_FFI_EXTENSION=absent`; this is secondary evidence only. The empty cbindgen header is the reproducible semantic blocker, independent of installing PHP.

No package or producer files are modified by this evidence.