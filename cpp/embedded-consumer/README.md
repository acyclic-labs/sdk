# C++ embedded ABI consumer

This is a native consumer smoke test for the real Rust `cdylib` produced by
`rust/crates/sdk-embedded-prototype`. It is separate from the remote gRPC C++
lane and does not provide a C++ facade. The test checks the ABI policy surface:
version handshake, opaque engine/reader handles, copied input, owned output,
explicit result-buffer release, and close operations.

Build the Rust prototype first, then configure this directory with the release
DLL, import library, and generated cbindgen header:

```powershell
cargo build --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml

$embedded = Resolve-Path rust/crates/sdk-embedded-prototype/target/release
$header = Get-ChildItem rust/crates/sdk-embedded-prototype/target/release/build `
  -Recurse -Filter acyclic_embedded_prototype.h | Select-Object -First 1

cmake -S cpp/embedded-consumer -B build/cpp-embedded-consumer `
  -G Ninja `
  -DCMAKE_CXX_COMPILER=clang++ `
  -DACYCLIC_EMBEDDED_ROOT=$embedded `
  -DACYCLIC_EMBEDDED_HEADER=$header.FullName
cmake --build build/cpp-embedded-consumer
ctest --test-dir build/cpp-embedded-consumer --output-on-failure

cmake --install build/cpp-embedded-consumer --prefix build/cpp-embedded-install
cmake -S cpp/embedded-consumer/install-consumer `
  -B build/cpp-embedded-install-consumer -G Ninja `
  -DCMAKE_CXX_COMPILER=clang++ `
  -DCMAKE_PREFIX_PATH=$PWD/build/cpp-embedded-install
cmake --build build/cpp-embedded-install-consumer
$env:PATH = "$PWD/build/cpp-embedded-install/bin;$env:PATH"
build/cpp-embedded-install-consumer/acyclic_cpp_installed_consumer.exe
```

The installed package exposes a thin RAII C++ facade in
`<acyclic/embedded.hpp>`. It only owns handles and result buffers and forwards
to the Rust C ABI; it does not reimplement stream behavior. The install
consumer proves that the wrapper, generated header, import library, and DLL can
be consumed from a clean CMake prefix.

This is a local Windows smoke only. It does not qualify remote SDK generation
or ABI stability across releases.

For a reproducible source-bound package check, run
`portable-package-test.ps1`. It builds the Rust release library from the
locked crate, configures and runs all CTest cases, installs into an isolated
prefix, checks the exact installed file set, builds the clean install consumer,
and writes a receipt containing a digest of every source input plus installed
artifact hashes. The script keeps all outputs under `cpp/embedded-consumer/.build`
so a root-level build directory cannot be mistaken for a package artifact.

The smoke and installed-consumer test passed on the qualification host on 2026-10-03 with Rust 1.98.1,
Clang 17.0.2, CMake 3.26.4, and Ninja. The
`embedded_abi_cpp_raw_generated_header` test also compiles and runs a C++20
consumer against the generated header directly, without the RAII adapter or a
consumer-side `extern "C"`/typedef shim. The Rust `cbindgen` build is configured
with C++ compatibility, so the generated header provides the fixed-width enum
branch and C linkage itself.

The current generated header used by that run has SHA-256
`64837E5A4E468E9748969025D3631E39B69E9DE7F77D66D26E7876247B90C109`.

The retained current-source package is bound to isolated revision
`41d14f38eaa2aee665f378b3ffe8c7f5e31921d9` and its source state is recorded in
`Q:/sdk/embedded-package-bundle/receipts/embedded-runtime-closure-current.json`.
The native package contains the Rust DLL and import library; the foreign
consumers only own ABI handles and result buffers.

The current owner build is recorded in
[package-manifest.json](package-manifest.json), including the replacement
header, DLL, import-library hashes and the exact local qualification result.
