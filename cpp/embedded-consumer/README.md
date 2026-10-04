# C and C++ embedded ABI consumer

This directory consumes the Rust-owned `cdylib` produced by
`rust/crates/sdk-embedded-prototype`. The Rust crate is the ABI authority:
foreign code owns opaque handles and result buffers, while stream behavior,
cancellation, lifetime rules, and error values remain in Rust.

The crate metadata currently defines package `acyclic-sdk-embedded-prototype`
version `0.2.0`, Rust `1.98`, ABI version `1`, and a `cdylib` plus `rlib`
output. The generated C header comes from the crate's pinned `cbindgen`
build dependency. This prototype is installed from a generated package
prefix; it is not published to a registry.

## Build an installed package

Run the platform recipe from the repository root. Keep its output outside the
source checkout so a clean-prefix consumer cannot accidentally see source or
build files:

```powershell
pwsh -File cpp/embedded-consumer/portable-package-test.ps1
```

```bash
bash cpp/embedded-consumer/portable-package-test.sh
```

The recipe builds the locked Rust crate in release mode, generates the header,
builds and runs the C++ CTest suite, installs a clean CMake prefix, and then
invokes all three foreign consumers against the installed runtime:

- C checks layout, append/read, owned output release, and stale-handle safety.
- Python loads the installed C ABI with `ctypes` and checks append/follow,
  owned buffers, cancellation, and stale handles.
- C++ checks the installed RAII facade, blocked-pull wakeup, cross-thread
  cancellation, and the clean-prefix CMake package.

The recipe writes `platform-package.json` beside the isolated build output.
That receipt records the Rust revision, the hashes of every Rust and consumer
source input, the generated package files, and the invoked behavior checks.
The exact installed prefix contains:

```text
include/acyclic/embedded.hpp
include/acyclic_embedded_prototype.h
lib/<platform runtime>
lib/cmake/AcyclicEmbedded/AcyclicEmbeddedConfig.cmake
lib/cmake/AcyclicEmbedded/AcyclicEmbeddedConfigVersion.cmake
```

Consumers do not set Rust feature flags. The producer selects the target
toolchain internally. On Linux musl, the recipe also selects the pinned musl
compiler and requests a shared musl runtime so the generated `cdylib` remains
loadable; the receipt verifies the musl loader for each executable.

## Direct CMake development loop

For a local iteration, build the Rust release output and configure this
directory with the generated release header and library:

```powershell
cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml

$embedded = Resolve-Path rust/crates/sdk-embedded-prototype/target/release
$header = Get-ChildItem rust/crates/sdk-embedded-prototype/target/release/build `
  -Recurse -Filter acyclic_embedded_prototype.h | Select-Object -First 1

cmake -S cpp/embedded-consumer -B build/cpp-embedded-consumer -G Ninja `
  -DCMAKE_CXX_COMPILER=clang++ `
  -DACYCLIC_EMBEDDED_ROOT=$embedded `
  -DACYCLIC_EMBEDDED_HEADER=$header.FullName
cmake --build build/cpp-embedded-consumer
ctest --test-dir build/cpp-embedded-consumer --output-on-failure
```

The installed-consumer test uses the generated CMake package rather than
including repository paths. This keeps package integration separate from the
in-tree C++ tests and catches missing headers, runtime files, or package
configuration.

The generated header is a C header. C++ consumers include it through the
facade's `extern "C"` boundary and retain the generator's conditional typedef
compatibility branch; they do not duplicate the Rust ABI declarations.

Platform receipts are the qualification record. A local Windows or Unix run
is useful for development, while the central release verifier accepts a
platform only after its receipt, installed runtime hash, source identity, C,
Python, and C++ behavior checks all match the generated source revision.