# C++ SDK generation lane

This directory is the owned qualification lane for the C++ **remote** SDK. It
is deliberately not an installable package yet: generated output is not checked
in until the pinned Protobuf and gRPC generators have produced code and a real
CMake build has linked it against the matching runtimes.

The contract is emitted by the Rust source-of-truth pipeline as a descriptor
set and protobuf sources. `Generate.ps1` consumes those files and invokes the
upstream C++ generators. It does not author a second contract and it does not
translate Rust algorithms into C++.

## Pinned toolchain

| component | pin | role |
| --- | --- | --- |
| Protocol Buffers | `36.2` | `protoc`, C++ message generator and runtime family |
| gRPC C++ | `1.80.0` | `grpc_cpp_plugin` and C++ runtime family |
| C++ language | C++17 | minimum for the generated transport target |
| CMake | `>=3.26` | configure and package build |

The Protobuf and gRPC versions must be resolved together in CI. The C++
runtime ABI is not treated as portable across unrelated releases; generated
code and runtime are built from the same pinned lane. The script refuses an
unversioned `protoc` and refuses to run without `grpc_cpp_plugin`.

The pinned source release is [gRPC C++ 1.80.0](https://github.com/grpc/grpc/releases/tag/v1.80.0)
(verified release commit `f5e2d6e`). The cached `build/grpc-source-1.56.2`
probe is an older source tree and cannot qualify this lane.

## Generation

From a checkout containing the Rust-emitted `proto/` tree:

```powershell
.\cpp\Generate.ps1 `
  -Protoc C:\toolchains\protobuf-36.2\bin\protoc.exe `
  -GrpcCppPlugin C:\toolchains\grpc-1.80.0\bin\grpc_cpp_plugin.exe
```

The output is placed under `build/sdk-cpp/generated` and includes raw
`*.pb.h/*.pb.cc` and `*.grpc.pb.h/*.grpc.pb.cc` transport bindings. The script
also writes `generation-receipt.json` containing the Protobuf/gRPC pins, every
input proto hash, a source digest, and generated-file hashes. CMake refuses
generated output without that receipt or with mismatched pins. The output is
not a facade and is not publishable until the qualification checks below pass.

The qualification host retains the pinned generator at
`Q:/sdk/build/protobuf-36.2/bin/protoc.exe` and the pinned plugin at
`Q:/sdk/build/grpc-install-1.80.0-vs-clean/bin/grpc_cpp_plugin.exe`.
The current Rust-owned proto tree has been generated successfully into
`sdk/build/sdk-cpp-generated-current`. The available gRPC installation was
compiled against Protobuf 6.31.1, while the generator emits 7.36.2, and its
MSVC runtime mode is `/MD` while the available Protobuf 36.2 runtime is `/MT`.
The generated source consumer therefore fails closed on the exact Protobuf
version guard and runtime-library mismatch. A remote package is not qualified
until gRPC 1.80.0 is rebuilt against the same Protobuf and MSVC runtime mode.

## Qualification gates

1. Generate from the Rust-owned descriptor/proto output.
2. Compare the generated descriptor digest with
   `compatibility/manifest.json`.
3. Configure and build the generated target with the matching Protobuf/gRPC
   runtime using CMake and Clang or the platform C++ compiler.
4. Run the shared vectors for Actors unary calls, Stream server streams,
   Objects client streams, and Filesystem import/export. Verify deadlines,
   cancellation, idempotency, recovery, oneof/optional presence, bytes, and
   unknown-field behavior.
5. Test installation from the produced archive in a clean consumer project.

Until all five gates pass, this remains a transport-only prototype. Embedded
behavior belongs behind the versioned C ABI in `ffi/`; a C++ wrapper must sit
above that ABI and must not expose Rust or protobuf C++ layouts.

The scoped consumer harness at `cpp/consumer` compiles every generated source,
links the pinned runtime packages, and round-trips an Actors
`CreateActorRequest` while checking its descriptor identity. Configure it with
`ACYCLIC_GENERATED_ROOT`, `Protobuf_DIR`, `utf8_range_DIR`, `absl_DIR`, and
`gRPC_DIR`; the harness must be run against a gRPC build whose Protobuf and
MSVC runtime pins match the generator receipt.
