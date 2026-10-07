# Dart, Ruby, and PHP binding research

Snapshot: 2026-10-07  
Authority checkout: `C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`  
Purpose: identify a maintained binding/generator path without moving contract,
validation, transport, cancellation, or lifecycle behavior out of the Rust
Actors client.

This is research evidence, not a qualification receipt. A language is not
qualified until an installable package built from this checkout loads the
current native artifact and exercises the typed remote facade, errors,
cancellation, recovery, streams where supported, `u64`, bytes, and lifecycle.

## Current upstream candidates

| Language | Maintained upstream path and observed pin | License | What it actually generates | Fit for this repository |
| --- | --- | --- | --- | --- |
| Dart | Dart `ffigen` **23.0.0** with `ffi` **2.2.0** | BSD-3-Clause | Dart bindings from C, Objective-C, or Swift headers. It can consume a Rust library only after Rust exposes a C ABI. | Viable ABI generator, but it is not a Rust-domain projection generator. The Rust facade must own the C header and opaque-handle semantics. |
| Ruby | Mozilla UniFFI **0.32.2** (`uniffi`, `uniffi_bindgen`; exact crate family must be locked together) | MPL-2.0 | Generated Ruby bindings from UniFFI proc-macro metadata or UDL; Ruby is maintained but receives fewer new features than Kotlin, Swift, and Python. | Best direct candidate of these three for a Rust component. It still needs a Rust-owned UniFFI facade because current Actors `ts-rs` declarations are not UniFFI metadata. |
| PHP | `ext-php-rs` **0.16.0**, with its `cargo-php` tooling pinned separately | MIT or Apache-2.0, at the consumer's choice | PHP extension functions/classes and IDE stubs from Rust attributes and conversion traits; it is an extension binding, not a client-contract generator. | Possible native extension adapter, but no maintained async/stream client generator. PHP's dynamic/Zend scalar model needs explicit full-`u64`, bytes, error, and lifecycle decisions. |

The versions above are current upstream observations at the snapshot date. They
must be recorded in a future qualification manifest with checksums and the
exact Rust/native toolchain. Existing repository references to UniFFI 0.31.x
are historical prototype pins; they should not be mixed with 0.32.x runtime,
metadata, or bindgen crates.

Primary sources:

- Dart [`ffigen`](https://pub.dev/packages/ffigen) and [`ffi`](https://pub.dev/packages/ffi); the Dart-maintained generator source is [`dart-lang/native/pkgs/ffigen`](https://github.com/dart-lang/native/tree/main/pkgs/ffigen).
- Mozilla [UniFFI guide](https://mozilla.github.io/uniffi-rs/latest/), [Ruby configuration](https://mozilla.github.io/uniffi-rs/next/ruby/configuration.html), [async/future support](https://mozilla.github.io/uniffi-rs/latest/futures.html), and the [0.32.2 crates documentation](https://docs.rs/uniffi/latest/uniffi/).
- [`ext-php-rs` guide and API](https://docs.rs/ext-php-rs/latest/ext_php_rs/) and its [upstream repository](https://github.com/extphprs/ext-php-rs).
- The lower-level Ruby alternative [Magnus 0.9.1](https://docs.rs/magnus/latest/magnus/) is MIT, but it binds Rust extension code manually and does not replace UniFFI's generated component interface.

## Strong type boundary

The Rust source remains authoritative for `ActorId`, `CodeSha256`,
`PositiveU64`, enum unknown-value handling, optional presence, bytes, `u64`,
error payloads, and the eight `actors::client::Client` operations. A foreign
binding may add idiomatic wrappers, but it must not recreate these predicates
or independently encode the wire protocol.

### Dart

`ffigen` parses C-compatible declarations. It cannot read the Rust domain
types or infer that a string is an `ActorId`, that a byte array is exactly 32
bytes and non-zero, or that zero is invalid for `PositiveU64`. The smallest
truthful path is a Rust-owned C ABI facade with opaque handles and constructors
that return a typed error/status. The C header must be generated from that
facade or otherwise kept in the same Rust-owned source closure; a handwritten
Dart schema would become a second authority.

The Dart public layer can then expose nominal wrapper classes around generated
opaque pointers. It must keep the Rust object alive through every asynchronous
call and provide an explicit close/drop operation. `dart:ffi`'s
[`Finalizable`](https://api.dart.dev/dart-ffi/Finalizable-class.html) and
[`NativeFinalizer`](https://api.dart.dev/dart-ffi/NativeFinalizer-class.html)
help with fallback cleanup, but the API still needs deterministic `close()`.
`NativeCallable.listener` delivers callbacks to the creating isolate and does
not wait for a return value; it is not an async future or cancellation model by
itself. A Rust-owned operation handle with `poll`/completion, `cancel`, and
`free` is therefore required for non-blocking calls and streams.

### Ruby

UniFFI's generated Ruby surface can carry records, enums, objects, errors,
optional values, byte buffers, and `u64` values through the Rust-owned
component interface. A nominal constructor should be a Rust method or custom
type conversion, so invalid IDs, digests, and positive limits are rejected by
Rust before transport. Ruby itself remains dynamically typed; generated code
does not provide a TypeScript-like compile-time proof. `uniffi.toml` custom
types are suitable for ergonomic wrappers only when their lift/lower functions
are thin and Rust remains the predicate authority.

UniFFI supports Ruby async calls through its generated future machinery, but
the runtime model is different from a native Ruby `async/await`: Ruby has no
native async/await, Ruby-to-Rust polling can use a Fiber scheduler, and
Rust-to-Ruby async callbacks use a new OS thread per invocation. UniFFI's
guide also states that cancellation is not directly supported as a universal
feature. The current Rust cancellation token must therefore be part of an
explicit Rust-owned operation/cancellation object; generated Ruby code must
call Rust cancellation and release the future handle in all completion, error,
and drop paths. Do not claim `AbortSignal`-like cancellation from UniFFI alone.

### PHP

`ext-php-rs` converts Rust values through `FromZval`/`IntoZval` and supports
Rust-backed PHP classes, functions, enums, and generated IDE stubs. This is a
good runtime conversion boundary for Rust constructors and errors, but the
PHP stub is not a source contract. Export only Rust domain methods and use
Rust conversion implementations for nominal IDs, digest length/content,
positive limits, options, and enum admission.

The crate has conversions for `u64`, but PHP exposes Zend integers rather than
an independent unsigned-64 type. Qualification must test `0`, `1`, the
largest platform integer, and `u64::MAX`; if the public PHP representation
cannot preserve the full range, expose a Rust-owned string/opaque value rather
than silently narrowing. Bytes should use PHP binary strings through the
binding's binary conversion, with Rust retaining length and content checks.

`ext-php-rs` has no maintained Rust-future/stream client generator. PHP
extension calls are normally synchronous from userland. A future/stream handle
could be exposed as a Rust-backed PHP class with explicit `next`, `cancel`, and
`close`, but that adapter must call the canonical Rust client and must not
implement transport or retry behavior in PHP. Its Windows documentation also
requires nightly Rust for the vectorcall ABI, which conflicts with a stable
Rust-only release matrix unless the target policy explicitly excludes that
path or accepts the required toolchain.

## Async, stream, and lifecycle conclusions

The three candidates do not provide one common generated streaming contract:

* Dart FFI gives calls and callbacks, not a Future/stream abstraction. Use
  Rust-owned handles and explicit completion/cancellation/free operations.
* Ruby UniFFI has generated futures and Ruby Fiber integration, but Ruby
  cancellation is not a built-in guarantee. Expose cancellation through a
  Rust-owned operation object and test dropped futures and repeated cancel.
* PHP `ext-php-rs` has no equivalent maintained async client layer. Start with
  unary operations or expose a Rust-owned pollable object; do not emulate
  streams in PHP arrays or background threads without a Rust lifecycle owner.

For every language, the Rust operation must remain the sole owner of remote
transport, authentication, retry/recovery, stream backpressure, and
cancellation state. The foreign object only owns a binding handle and forwards
inputs, outputs, and lifecycle events. Qualification must cover cancellation
before dispatch, during transport, after completion, cancellation races,
double-close, and dropping a client while an operation is pending.

## Recommendation and gating order

Ruby is the strongest direct maintained generator candidate, subject to a
Rust-owned UniFFI facade and explicit cancellation/lifecycle tests. Dart is a
reasonable later ABI target after a C-compatible Rust facade and an operation
handle design are accepted. PHP should remain a native-extension prototype
until full-`u64`, async/stream, and Windows toolchain decisions are recorded.

None of these generators may be used to infer or reconstruct the Actors wire
contract. The source-owned Rust facade, its generated metadata/header, the
canonical `actors::client::Client`, and its error/cancellation behavior remain
the only accepted authority.
