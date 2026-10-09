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

Fifty-seven offline controls cover descriptor-only staging, input/tool admission,
overlapping destinations, compiler failure, archive and dependency safety,
offline installation and prevention of success receipts after failures or drift.
Actual independent generation on accepted foundation
`5f13157414a5f48425007ffd957ea7d09611e5fe` produces identical 105-file payloads.

Fresh offline Composer installation on PHP 8.5.11 passes complete
Actors/Workers/Stream file descriptors after removing source comments and
Buf image tag 8042, bytes, unsigned wire bits, optional-zero presence,
oneof switching/clearing and every generated client method's path, message
types and unary/server-streaming shape. Maintained gRPC serialization methods
process nonempty samples. Three independent invalid assignments reject arrays
for Actor bytes, Worker bytes and an optional Stream integer. These are runtime
checks. Native gRPC 1.82.0 client creation and shutdown also pass. Four isolated
generated-client mutations fail for their intended path, request-content,
response-type and streaming-shape errors. Actual ZIP traversal, link and CRC
faults are rejected before dependency extraction.

The pure PHP runtime uses signed integers for unsigned wire values on this
64-bit host: `-1` carries the uint64 maximum bit pattern. Passing its maximum
unsigned decimal string saturates instead, so decimal-string bounds are not
qualified. Generated clients have maintained message-class argument types.

The reusable qualifier builds a sorted, fixed-time archive with maintained
bsdtar and timestamp-free gzip. Independent runs reproduce archive SHA-256
`50d85aadd88283df77f53b06fe72d73605d4bf7c61be37209aa84e5d3a557967`.
Composer 2.10.3 resolves exact package metadata offline, writes a lock, then
installs the newly built SDK archive and two verified raw dependency ZIPs into
a fresh cache and vendor tree. Native libzip checks ZIP member kinds, lengths
and CRCs; admission rejects unsafe paths, collisions and unexpected metadata.
No previously extracted package is reused.

```sh
node tools/sdk-generator/backends/php/src/qualify.mjs \
  --package /new-package --authority /rust-export --php-home /php-runtime \
  --composer /composer.phar --grpc-extension /php_grpc.dll --archiver /tar.exe \
  --cache /verified-raw-dependencies --output /new-qualification
```

The entire runtime, Composer, extension, archiver and dependency archives are
pinned. PHP runs with `-n`; homes, temporary directories and Composer settings
are isolated. The authoritative autoloader must resolve exclusively inside the
new installation. Complete package, dependency, vendor and autoloader byte
inventories are checked before and after controls. All intended diagnostics
and completion markers must pass before a receipt is written. Receipts retain
tool, code, input, archive, installed file, project and log hashes. Failures
preserve logs and partial output. Inputs must remain exclusively owned during
execution.

Routine CI runs affected offline Node controls and downloads no PHP tools or
dependencies. The backend registry scopes shared-reader changes and keeps
isolated PHP changes out of Rust and TypeScript build inputs.

Rust-backed RPC, TLS/authentication, cancellation/recovery, remaining
families/platforms and the Rust embedded ABI remain outstanding.
