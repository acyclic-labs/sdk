# C++ descriptor backend

This producer generates C++ messages and gRPC client/server bindings from
immutable Rust authority exports using maintained protoc 28.3 and the official
gRPC 1.82.0 C++ plugin. It copies verified descriptor snapshots into isolated
staging, admits tool binaries by host-specific hashes, requires complete
per-family output, and retains input, tool, code and payload hashes.

```sh
node tools/sdk-generator/backends/cpp/src/generate.mjs \
  --source-root /accepted-source --authority /accepted-export \
  --protoc /protoc --cpp-plugin /grpc_cpp_plugin --output /new-package
```

Generated headers live in `include/` and sources in `src/`. The package contains
its license, notice, authority manifest and CMake installation/export templates.
It exposes `Acyclic::Transport` and requires exact protobuf/gRPC runtime versions.
No runtime dependency is downloaded by the producer or routine CI.

`toolchains/CMakeLists.txt` builds the unchanged upstream plugin sources against
an admitted protobuf 28.3 build and its matching Abseil libraries. Pass
`PROTOBUF_SOURCE`, `PROTOBUF_BUILD`, `GRPC_SOURCE` and `ABSL_ROOT_DIR`; build with
MSVC x64, the static runtime and one worker. The current executable pin records
the observed Windows build, not a cross-host reproducible compiler claim.

Independent generation against accepted foundation
`5f13157414a5f48425007ffd957ea7d09611e5fe` emits identical twelve-file native
binding payloads. A native protobuf consumer passes complete file descriptors
after removing source comments and Buf image tag 8042, populated serialization
for all 25 RPC message pairs, bytes, unsigned maximum wire values, optional zero
and oneof switching/clearing. These are source-tree message controls.

Installed package, native gRPC runtime/client/server execution, independent
negative compiles, Rust-backed RPC, TLS/authentication, cancellation/recovery,
remaining families/platforms and embedded runtime qualification remain pending.
