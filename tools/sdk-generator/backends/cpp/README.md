# C++ descriptor backend

This producer generates C++ messages and gRPC client/server bindings from
immutable Rust authority exports using protoc 35.0 and the official gRPC 1.82.0
C++ plugin. Its compiler, plugin and runtime use the same upstream dependency
revisions, recorded in `toolchains/toolchain.json`. The currently admitted host
is Windows x64; executable pins describe the observed build.

```sh
node tools/sdk-generator/backends/cpp/src/generate.mjs \
  --source-root /accepted-source --authority /accepted-export \
  --protoc /protoc --cpp-plugin /grpc_cpp_plugin --output /new-package
```

Generation stages verified descriptor snapshots, requires complete per-family
bindings and records every payload hash. It checks authority inputs, tool
binaries, producer sources and package metadata again before writing a receipt.
Generated headers live in `include/` and sources in `src/`. The package includes
license, notice, authority manifest and CMake installation/export templates.
It exposes `Acyclic::Transport` with exact protobuf 35.0.0 and gRPC 1.82.0 CMake
dependencies. The producer and routine CI download no native dependencies.

`toolchains/build-runtime.cmake` configures, builds and installs unchanged
upstream gRPC and its matched dependencies. Supply admitted `GRPC_SOURCE`,
`ABSL_ROOT_DIR`, `PROTOBUF_ROOT_DIR`, `BORINGSSL_ROOT_DIR`, `CARES_ROOT_DIR`,
`RE2_ROOT_DIR` and `ZLIB_ROOT_DIR`, plus owned `RUNTIME_BUILD` and
`RUNTIME_INSTALL` paths through CMake `-D` arguments, then invoke the file
with `cmake -P`. Run inside the MSVC x64 developer environment. The recipe
uses one worker, static libraries and the shared MSVC runtime consistently
across all dependencies. Dependency downloads and unrelated language plugins
are disabled. Raw archive hashes and exact source revisions are pinned;
callers must admit those sources before running the recipe.

Two independent maintained generations against accepted foundation
`5f13157414a5f48425007ffd957ea7d09611e5fe` emit identical 17-file package
payloads and identical receipts. The SDK builds and installs through CMake.
An independent consumer finds only the installed package and matched runtime.
It passes complete Rust file descriptors after removing source comments and
Buf image tag 8042, populated serialization for all 25 RPC message pairs,
bytes, unsigned maximum wire values, optional zero and oneof switching/clearing.
Its generated clients and servers execute all 25 methods over local TCP.
Server interceptors independently verify actual method paths, stream types and
call counts; streaming calls check two distinct responses in order. A positive
type control compiles, and three independent invalid byte/integer assignments
fail with their intended compiler type errors. The executed fixture sources
live in `tests/fixtures/consumer/`.

This establishes installed CMake consumption and native loopback transport for
the accepted Actors, Workers and Stream exports. Deterministic archive admission
and a reusable installed qualifier remain pending, as do Rust-backed RPC,
TLS/authentication, cancellation/recovery, remaining families/platforms and
embedded runtime qualification. Historical protoc 28.3 source-only evidence is
retained separately from the matched native runtime evidence.
