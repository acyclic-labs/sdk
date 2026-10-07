# C# current-head C8 cohort: `799214a1`

This cohort is generated from the clean Rust source snapshot `Q:\sdk\work\sdkgen-actors-c8-799214a1-source`, archived from revision `799214a1ec` (`799214a1`). It is separate from `current-03bb-20261007`; no receipt from that historical cohort is reused here.

The native producer was built in WSL with the pinned workspace lockfile:

```text
CARGO_TARGET_DIR=/tmp/actors-c8-799-target RUSTC_WRAPPER= RUST_MIN_STACK=16777216 \
  cargo build --release --locked -p acyclic-actors-uniffi --features bindgen --bin actors-uniffi-bindgen
CARGO_TARGET_DIR=/tmp/actors-c8-799-target RUSTC_WRAPPER= RUST_MIN_STACK=16777216 \
  cargo build --release --locked -p acyclic-actors-uniffi --features bindgen --lib
```

The maintained C# source generator is `uniffi-bindgen-cs-sourcefix-20261007` at `e10ce410eb3a10cc19c7928b93ea8d84e038c034`, binary SHA-256 `59a191f74bec339c37be15ab70b0bf638578665b33a057c2d6ba5171298d7f55`, built with Rust `1.98.1`. It generated the domain and facade components from the same native producer and `uniffi.toml`; `merge.py` combines those generated components only to provide one C# assembly for this two-component cdylib. No contract types or methods are hand-authored.

The Windows task-local .NET SDK `8.0.425` compiled the managed assembly. The executable qualification run used the same assembly and the Linux `.so` from the producer build under the task-local WSL .NET 8 runtime.

Terminal evidence in this directory covers all eight Actors operations, `ulong.MaxValue` through request/service boundaries, three true in-flight `CancellationToken` cancellations with server abort cleanup, the Rust-owned `CurrentHeadMarker` false rejection, and expected C# compile failures for raw handles, readonly mutation, nominal record mismatch, and request-type mismatch. The Rust marker test is the semantic source-of-truth because the generated C# projection aliases that custom type to its wire `bool` representation.

Platform status is Linux and Windows qualified, with a macOS arm64 runtime cohort, for this exact source/native cohort. Windows used the separately built `acyclic_actors_uniffi.dll`; macOS used the separately built `libacyclic_actors_uniffi.dylib` and task-local .NET runtime 8.0.31. All8, full-u64, cancellation/server-abort, Rust CurrentHeadMarker, and negative checks are recorded per platform. The macOS negative compile uses the same exact managed assembly under the Windows SDK because the macOS SDK artifact was unavailable from the task host. Historical receipts remain historical.


