# Embedded binding comparison

This comparison is source-bound to the Rust SDK worktree and records the
embedded boundary separately from remote SDK generation. The crate and tool
versions below come from the checked-in manifests or the local Cargo metadata
for those manifests. A binding is considered qualified only when a generated
or packaged artifact is installed and a foreign consumer invokes it; compiling
a Rust crate or inspecting generated declarations alone is not enough.

## Recommendation

Use the explicit Rust C ABI as the baseline for broad embedded distribution.
Keep N-API for the existing Node/Bun native packages and `wasm-bindgen` for
browser packages. Keep UniFFI as a focused option for Swift/Kotlin/Python/Ruby
facades after each language has an installed consumer receipt. None of these
boundaries is a second contract: the Rust implementation owns behavior and
the binding exposes handles, records, or wire values from that implementation.

| boundary | pinned implementation | evidence in this checkout | decision |
| --- | --- | --- | --- |
| C / C++ / ctypes Python | `cbindgen 0.29.4`, Rust crate `acyclic-sdk-embedded-prototype 0.2.0` | `research/acceptance/embedded/release-abi-20261004.json` records C, Python ctypes, C++/CTest, clean-prefix installation, two reproducible builds, and tamper rejection; the current crate has 16 Rust unit tests passing with `--all-features` | baseline embedded ABI |
| Swift / Kotlin / Python / Ruby | `uniffi 0.32.2` with `tokio` feature | `src/uniffi_polling.rs` is a proc-macro façade over the canonical `MemoryStream`; the all-feature Rust run passes 16 unit tests, including the generated-style object, cancellation, terminal-state, and provider tests | targeted façade where generated language package and installed consumer are separately qualified |
| Browser WASM | `wasm-bindgen 0.2.117`, `wasm-bindgen-futures 0.4.67`, `serde-wasm-bindgen 0.6.5` in the locked graph | `research/acceptance/filesystem-installed-rust-wasm-browser-20261006.json` passes installed browser smoke and multitab checks; `stream-wasm-release-20261005-final.json` passes 13 memory tests and export validation | browser Rust adapter |
| Node / Bun native | N-API-RS `napi 3.12.7`, `napi-derive 3.6.8`, `napi-build 2.4.4` in the locked graph | `filesystem-installed-native-20261005-final.json` passes an installed `win32-x64` package for workspace create, write/read, sync, diff, and cancellation; `stream-installed-package-native-20261005.json` passes the installed native gRPC package for ten RPCs, TLS, serialization, cancellation, and recovery | native JS adapter |

## C ABI and cbindgen

The embedded prototype is Apache-2.0. `cbindgen` 0.29.4 is MPL-2.0. The
Rust façade exports opaque handles, owned buffers, explicit release functions,
and status values. It does not expose Rust references, futures, protobuf
layouts, or panics. The C and Python consumers exercise append/read/follow,
owned output, cancellation, and stale-handle behavior; the C++ consumer adds
blocked-pull wakeup and cross-thread cancellation through the thin wrapper.

The historical local release receipt is concrete: Rust 1.98.1, Clang 17.0.2,
CMake 3.26.4, and Ninja produced two byte-identical packages after timestamp
normalization. Its clean-prefix CTest run passed 3/3 and the C/Python/C++
consumer receipts all invoked the same DLL. The receipt is a qualification of
the recorded source revision `37822fd23090c2b44de9bfc0eddd8368f62ed95d`, not
an assertion that every later working-tree revision has that artifact.

The current source also passes the native unit surface with:

```text
cargo test --offline --locked --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --all-features --lib
```

The full command's 16 unit tests passed. Its doctest phase could not run
because the installed Rust 1.98.1 toolchain lacks `rustdoc.exe`; that is a
toolchain installation gap, not a passing doctest claim.

The trade-off is API work. C ABI headers are stable and widely consumable, but
each target still needs an idiomatic wrapper and ownership tests. The
eight-RID native release aggregation and current package receipt remain
separate release work; this comparison does not promote an old local receipt
to current-package evidence.

## UniFFI

The embedded manifest pins UniFFI 0.32.2 and the Cargo metadata reports MPL-2.0
for `uniffi`, `uniffi_core`, `uniffi_macros`, and `uniffi_meta`. The prototype
uses `#[uniffi::Object]`, `#[uniffi::Record]`, `#[uniffi::Enum]`,
`#[uniffi::Error]`, and `#[uniffi::export]` directly over Rust types. This
avoids a separately authored UDL contract for the prototype.

The façade intentionally exposes a polling object because UniFFI has no direct
Rust `Stream` mapping in this prototype. `open_reader`, `next`, `cancel`, and
the terminal statuses route to the same canonical `MemoryStream` provider as
the C ABI. The Rust unit test `generated_style_object_calls_canonical_stream`
passes, and cancellation remains terminal on repeated calls.

The repository does not currently pin a `uniffi-bindgen` executable, check in
generated language bindings, or provide an installed Swift/Kotlin/Python/Ruby
consumer receipt for this façade. `tests/uniffi_python_consumer.py` is a
consumer harness that expects a generated directory and library; it is not by
itself evidence that generation or installation completed. UniFFI is therefore
qualified here as a Rust façade prototype, not as a released multi-language
embedded package.

## WASM and N-API

The browser package uses `wasm-bindgen --target web` over a Rust `wasm32`
build. The current Filesystem receipt records the WASM, JavaScript, and
TypeScript declaration hashes, Chrome headless smoke, IndexedDB and OPFS
coverage, 18 exported objects, and 12 multitab consistency/recovery cases.
The Stream receipt records the locked Rust build, wasm-bindgen 0.2.117, and 13
memory tests. These are installed/browser boundary receipts; they do not make
the browser package a native embedded ABI.

The N-API package uses the Rust `filesystem-napi` and `sdk-stream-native`
crates. Cargo metadata reports Apache-2.0 for the repository crates and MIT
for the N-API-RS crates. The Filesystem receipt verifies package resolution
from an installed `win32-x64` companion and checks capability discovery,
workspace operations, synchronization, diff, and cancellation. The Stream
receipt verifies package installation, the Rust native gRPC default, ten RPC
operations, TLS, serialization, cancellation, and endpoint recovery. The
generated `binding.d.ts` begins with the N-API-RS generator marker; the
behavior remains in Rust.

N-API is the best fit for Node/Bun distribution because it maps directly to
the host's native module ABI and can select platform companions. It still
requires a per-platform artifact matrix and Node ABI loader checks. WASM is
the best fit for browsers because it has no native loader, but it has browser
runtime and memory constraints. Neither boundary should be used to translate
arbitrary Rust algorithms into a second TypeScript implementation.

## Qualification gaps and next bounded work

1. Add a pinned UniFFI binding-generation command and generated-package
   receipts for each language that adopts the façade. The first useful target
   is Python because the repository already has a consumer harness; then
   qualify Swift or Kotlin with the same Rust scenarios.
2. Install the Rust 1.98.1 rustdoc component on the release qualification
   runner and rerun doctests from the same locked source revision.
3. Keep native package aggregation and all supported RID installed-consumer
   checks tied to one source digest. Existing single-platform receipts do not
   substitute for that aggregate proof.
4. Reuse the same append/follow/read/cancel/recovery vectors across C ABI,
   UniFFI, N-API, and WASM receipts so a binding cannot silently change Rust
   semantics.

## License scope

The repository's embedded crate is Apache-2.0. The binding tools are separate
upstream dependencies: cbindgen and UniFFI are MPL-2.0, wasm-bindgen is
MIT OR Apache-2.0, serde-wasm-bindgen is MIT, and N-API-RS crates are MIT.
These are metadata observations from the pinned local dependency graphs, not a
replacement for the release's full third-party license bundle.
