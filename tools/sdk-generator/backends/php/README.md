# PHP transport generator

The producer consumes verified Rust descriptor snapshots through protoc 28.3
and the maintained gRPC 1.82.0 PHP generator. Its source belongs in `src/`,
offline controls in `tests/`, installed consumers in `tests/fixtures/consumer/`,
package metadata in `templates/package/`, and admission pins in `toolchains/`.
Generated packages, downloads and caches belong outside this tooling tree.

```sh
node tools/sdk-generator/backends/php/src/generate.mjs \
  --source-root /accepted-source --authority /rust-export --protoc /protoc \
  --php-plugin /grpc_php_plugin --output /new-package
node --test tools/sdk-generator/backends/php/tests/generate.test.mjs
```

The plugin is built from unchanged official gRPC sources with protobuf 28.3 and
its exact Abseil submodule. `toolchains/toolchain.json` retains source commits,
archive hashes and the admitted Windows executable hash. Build the source-only
`toolchains/CMakeLists.txt` with `PROTOBUF_SOURCE`, `GRPC_SOURCE` and
`ABSL_ROOT_DIR` pointing to those verified sources, using MSVC x64 and Ninja:

```sh
cmake -S tools/sdk-generator/backends/php/toolchains -B /owned-build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DPROTOBUF_SOURCE=/protobuf \
  -DGRPC_SOURCE=/grpc -DABSL_ROOT_DIR=/abseil
cmake --build /owned-build --target grpc_php_plugin --parallel 1
```

Six offline controls cover descriptor-only staging, input/tool admission,
overlapping destinations, compiler failure, missing output and collisions.
Actual independent generation on accepted foundation
`5f13157414a5f48425007ffd957ea7d09611e5fe` produces identical 105-file payloads.

Prototype fresh offline Composer installation on PHP 8.5.11 passes complete
Actors/Workers/Stream file descriptors after removing source comments and
Buf image tag 8042, bytes, unsigned wire bits, optional-zero presence,
oneof switching/clearing and every generated client method's path, message
types and unary/server-streaming shape. Maintained gRPC serialization methods
process nonempty samples. Three independent invalid assignments reject arrays
for Actor bytes, Worker bytes and an optional Stream integer. These are runtime
checks. Native gRPC 1.82.0 client creation and shutdown also pass.

The pure PHP runtime uses signed integers for unsigned wire values on this
64-bit host: `-1` carries the uint64 maximum bit pattern. Passing its maximum
unsigned decimal string saturates instead, so decimal-string bounds are not
qualified. Generated clients have maintained message-class argument types.

Reusable canonical packaging and installed qualification are still being
implemented. The prototype evidence is not a completed qualification receipt.
Rust-backed RPC, TLS/authentication, cancellation/recovery, remaining
families/platforms and the Rust embedded ABI remain outstanding.
