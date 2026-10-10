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
payloads and identical receipts. A fresh source tree staged only from admitted
archive bytes builds and installs
through CMake. An independent consumer finds the installed package and a private
copy of the matched runtime. All 1,618 runtime files and all SDK source bytes
match their admitted inputs after execution.
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
the accepted Actors, Workers and Stream exports. Two deterministic archives
have identical SHA256
`8bfb26bfadb6a328df08d35192e25f12edf162ed3fd9da6caf032163830fd7de`.
`src/package.mjs` admits that archive against an external digest, the generation
receipt, complete payload inventory, maintained producer/tool/template pins and
accepted Rust authority. Admission rejects links, traversal, missing/extra files
and payload or receipt drift. Rust-backed RPC, TLS/authentication,
cancellation/recovery, remaining families/platforms and embedded runtime
qualification remain outstanding. Historical protoc 28.3 source-only evidence is
retained separately from the matched native runtime evidence.

Archive admission command:

```sh
node tools/sdk-generator/backends/cpp/src/package.mjs \
  --package /package.tar.gz --sha256 <external-sha256> \
  --receipt /generation-receipt.json --authority /accepted-export
```

The maintained installed qualifier `src/qualify.mjs` passes a fresh archive
installation with all native controls. It admits external inventories for 7,377
MSVC/Windows SDK and build-tool files and 1,618 matched runtime files. The host
inventory covers the recorded compiler binaries, include and library search
directories; system OS files are outside this inventory. The qualifier invokes
pinned tools directly, uses one worker, copies the runtime into its private
output, disables CMake package registries and rejects inherited compiler flags.
It verifies exact compiled source sets and installed dependency prefixes, then
checks every source/runtime/compiler input again before emitting its receipt.
The currently qualified host uses MSVC 19.44.35228 and Windows SDK 10.0.26100.0.

```sh
node tools/sdk-generator/backends/cpp/src/qualify.mjs \
  --package /package.tar.gz --sha256 <external-sha256> \
  --receipt /generation-receipt.json --authority /accepted-export \
  --runtime-root /matched-runtime --runtime-inventory /runtime-inventory.json \
  --host-inventory /host-inventory.json --output /new-qualification
```

`src/inventory.mjs` reproduces the admitted host and runtime inventory bytes.
Recording a new inventory does not admit it; qualification requires its checksum
to match `toolchains/qualification.json`. The host environment JSON records
`include`, `lib` and `libpath` arrays, SDK/root/version values, and a `tools`
map for cl, link, lib, rc, mt, cmake and ninja executable paths.

```sh
node tools/sdk-generator/backends/cpp/src/inventory.mjs \
  --kind host --environment /host-environment.json --output /new-host-index.json
node tools/sdk-generator/backends/cpp/src/inventory.mjs \
  --kind runtime --root /matched-runtime --output /new-runtime-index.json
```
