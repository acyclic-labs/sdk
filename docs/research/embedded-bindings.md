# Embedded bindings research

This note records the bounded embedded proof for the Rust SDK migration. It is deliberately
separate from the remote SDK contract: the embedded path exposes a small, language-neutral ABI
over the real Rust `MemoryStream` engine, while the Rust engine remains the only implementation of
append, replay, follow, cancellation, validation, and provider errors.

## Decision

Use a versioned C ABI as the embedded interop seam and generate its C header with pinned
[`cbindgen` 0.29.4](https://docs.rs/cbindgen/0.29.4/cbindgen/). Keep the ABI pull-based (`next`),
with explicit `cancel` and `close`, and make all returned bytes caller-owned with an explicit
release function. This directly follows [`ffi/abi-policy.md`](../../ffi/abi-policy.md): no Rust
references, futures, trait objects, layouts, or panics cross the boundary.

The working proof is
[`rust/crates/sdk-embedded-prototype`](../../rust/crates/sdk-embedded-prototype/). It creates a
Tokio runtime and a real `acyclic_stream::MemoryStream`, then exposes:

| Operation | Boundary behavior |
| --- | --- |
| `acyclic_embedded_engine_open/close` | Owns and drops one process-local Rust engine ID. IDs are monotonic and stale close calls are no-ops. |
| `acyclic_embedded_engine_append` | Copies a path and payload into the Rust provider and returns the committed sequence range. |
| `acyclic_embedded_reader_open` | Starts a Rust-owned finite replay or live follow task and returns a monotonic reader ID. |
| `acyclic_embedded_reader_next` | Pulls a record, end, pending, cancellation, capacity, or provider error. Invalid or stale IDs return `InvalidArgument` without dereferencing them. |
| `acyclic_embedded_reader_cancel/close` | Wakes and aborts the Rust task; repeated calls are safe and stale IDs are ignored. |
| `acyclic_*_result_release` | Checks a monotonic allocation ID, then drops the Rust-owned allocation. `acyclic_buffer_release` returns `InvalidArgument` for a mismatched or repeated release. |

The prototype tests append/read, follow after a live append, cancellation, invalid input, and the
panic containment path. It does not claim that any language binding is production-qualified yet.
Handles are integer IDs rather than arbitrary pointers, so invalid and stale foreign values fail
closed before any Rust object is accessed. Reader IDs retain the engine internally, so closing the
engine ID does not invalidate a live reader. A caller must not retain input pointers after an ABI
call, must pass a genuinely readable `ptr + len` region for every nonempty input, and must treat
returned pointers as read-only until the matching release call. The ABI can validate null and
length combinations, but cannot probe an arbitrary foreign address safely; invalid input memory is
therefore an explicit caller precondition. Handle lookup itself never dereferences the foreign ID.

## UniFFI comparison

The research baseline pins the current UniFFI line as `uniffi = "=0.32.2"` (the version used for
the comparison; it is not added as a second interface model in this prototype). UniFFI is a good
fit for generated object/value bindings in its first-party Kotlin, Swift, Python, and Ruby targets.
Its [async model](https://mozilla.github.io/uniffi-rs/next/internals/async-overview.html) requires
foreign-runtime integration, and its [foreign traits](https://mozilla.github.io/uniffi-rs/latest/foreign_traits.html)
are object callbacks with ownership and `Send + Sync` requirements. UniFFI does not provide a
first-class Rust `Stream`/`Sink` mapping; the open
[`Stream` support issue](https://github.com/mozilla/uniffi-rs/issues/2755) is evidence against
making a live stream the shared contract.

A UniFFI Python sketch would therefore have to turn the live reader into a polling object and
invent a second ownership/error convention around it:

```toml
[dependencies]
uniffi = "=0.32.2"
```

That can be useful as a language-specific experiment, but it does not solve the cross-language
embedded boundary. It also introduces a second generated interface alongside the remote protocol.
The C ABI proof keeps one explicit contract that Python, C#, Dart, JVM, Swift, and other adapters
can consume without pretending that a Rust future or trait object is portable.

UniFFI's supported language list must be read narrowly. Python, Kotlin, Swift, and Ruby are the
maintained first-party targets; C#, Go, Dart, Java, and Node bindings are ecosystem integrations
with their own support and runtime constraints. None is qualified by this proof. A target becomes
qualified only after it consumes the generated header through a real adapter and passes the
language-neutral conformance suite.

## Why cbindgen is the narrow proof

[`cbindgen`](https://docs.rs/cbindgen/0.29.4/cbindgen/) generates a C/C++ declaration file from
explicit `extern "C"` symbols. It does not translate Rust async behavior, generate Python or
Swift packages, or make an ABI safe by itself. Those semantics are implemented here first in Rust:

* the async task is internal and communicates through a bounded synchronous message queue;
* the foreign caller only sees opaque integer IDs, a status enum, sequence numbers, and owned bytes;
* a bounded queue admits at most 64 records and reserves one terminal error slot plus one cancellation wakeup slot, reporting `Capacity` instead of growing without a limit;
* every exported entry point catches unwinding and turns it into `Panic` or a safe no-op;
* cancellation wakes both the async task and a blocked pull, while `close` consumes the handle exactly once;

This is intentionally smaller than exposing the entire stream protocol. A future ABI revision can
add an explicit handshake and capability bitmap, but it must preserve the symbol and wire identity
rules before adding operations. The generated header is produced in Cargo's `OUT_DIR` by the build
script; generation is part of every clean build, so a stale checked-in header cannot silently become
the source of truth.

## Other tools considered

| Tool | Finding for embedded behavior |
| --- | --- |
| [UniFFI](https://github.com/mozilla/uniffi-rs) | Strong generated value/object bindings; async needs foreign runtime integration and no first-class stream bridge. |
| [cbindgen](https://github.com/mozilla/cbindgen) | Small, maintained C/C++ declaration generator; requires this explicit ABI design and language adapters. |
| [Diplomat](https://rust-diplomat.github.io/diplomat/) | Useful tagged, mostly unidirectional bridge for selected C/C++/WASM/JS/Dart/Kotlin/Python shapes; callbacks and async are constrained, so it does not cover this live reader proof. |
| [Interoptopus](https://docs.rs/interoptopus/latest/interoptopus/) | Generates selected C/C#/Python surfaces and advertises callbacks, but its target support and async story are not broad enough to make it the canonical embedded seam. |
| N-API/WASM adapters | Appropriate host-specific facades; they should call the Rust-owned contract and must not become independent stream implementations. |

The result is a deliberate split: cbindgen plus a small ABI is the portable baseline; UniFFI,
N-API, WASM, or a host-specific generator may sit above it where that target's runtime semantics
are proven. No generator is allowed to turn arbitrary Rust algorithms into separately maintained
language implementations.

## Foreign-consumer evidence

The generated header was compiled with Clang against the release Windows cdylib by
[`tests/c_consumer.c`](../../rust/crates/sdk-embedded-prototype/tests/c_consumer.c). The consumer
performed append, finite read, payload validation, repeated buffer release, follow, cancellation,
repeated close, and stale-reader rejection. The same sequence ran through
[`tests/python_consumer.py`](../../rust/crates/sdk-embedded-prototype/tests/python_consumer.py)
using Python `ctypes` against the produced `acyclic_sdk_embedded_prototype.dll`.

The C consumer also asserts the exported `AcyclicBuffer` layout at compile time. Rust registry
lookups are released before any `block_on` call, so no registry or reader mutex is held across a
future. Concurrent cancel/close operations only take the reader task mutex after removing or
looking up the ID, avoiding registry/task lock cycles.

The generated header was regenerated twice from the same clean release inputs and had identical
SHA-256 bytes in the verification run:

```text
18B07471660357296969E9B32C2891B5367949D21F5F736957D3BDF65197D677
```

## Filesystem and Harness proof

[`rust/crates/sdk-embedded-filesystem`](../../rust/crates/sdk-embedded-filesystem/) is a separate
standalone package that calls the canonical Rust `MemoryFs` composition. Its bounded test creates
two ephemeral volumes, checks out both with private overlays, mounts them through `MountedView`,
routes a path to the scratch checkout, writes bytes through `create_file`, and observes both mounts
in the resulting snapshot. With the `harness` feature it also executes the real
`HarnessBuilder` admission boundary and verifies that missing execution bindings are rejected.
The package has no generated or handwritten TypeScript behavior and keeps both probes in Rust.

The release verification commands were:

```text
cargo test --manifest-path rust/crates/sdk-embedded-filesystem/Cargo.toml
cargo test --manifest-path rust/crates/sdk-embedded-filesystem/Cargo.toml --features harness
```

These are consumer proofs, not language qualifications. Swift, C++, Go, and other adapters still
need their own generated-header bindings and the same conformance scenarios before being marked
supported.

## Reproduction

From the SDK worktree, run:

```text
cargo test --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml
```

The standalone crate has its own `[workspace]` boundary and does not edit the SDK root manifest or
lockfile. A clean build resolves the pinned cbindgen release, regenerates the C header, compiles
the actual `acyclic-stream` dependency, and runs the seven conformance tests in the prototype.
