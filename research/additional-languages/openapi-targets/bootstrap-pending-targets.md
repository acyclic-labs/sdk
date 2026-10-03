# C and Dart bootstrap qualification notes

These lanes use generated sources derived from
`Q:/sdk/tmp-openapi-integrated2/artifacts/workers.json`; no new contract or
route is authored here. The technical qualification is bounded by the
per-target evidence in `receipt.json`.

## C

The first CMake attempt used the Visual Studio generator and was blocked by
the host compiler/development-header selection. The follow-up used the
working LLVM and Ninja toolchain found on this host:

- LLVM clang 17.0.2 at `C:/Program Files/LLVM/bin/clang.exe`;
- CMake 3.26.4 and Ninja 1.11.1 from `C:/Strawberry/c/bin`;
- official curl-for-Windows 8.22.0_2 x64 development archive;
- archive SHA-256 `7c8c6b953b4eb2953d2bdc08cca1d5f09a964e9f86c361693559400c9a6d6db0`.

CMake configure and Ninja build pass with the pinned curl include and import
library. OpenAPI Generator 7.25.0 emits one C constructor local-name
collision in `acyclic_workers_v1_job_observation.c`; the Rust-owned stage
emits the anchor-checked `apply-c-compile-adaptation.ps1`, which changes only
that local name, then the generated package builds successfully. The CMake
install prefix and portable archive carry a per-component license manifest
for the generated source, cJSON, and curl files.

The recorded C consumer sends the canonical `AQID` bytes boundary and passes
the 200 response (`b2s=` / `AQID`), the max uint64 decimal string
`18446744073709551615`, and the canonical 409 fixture. The generated C package
has no native package-level field; the portable install adds explicit package
metadata through `LICENSES.json`, with each component scope and license-file
hash recorded in the research manifest.

## Dart

The official stable Dart 3.13.5 SDK archive is now used for the generated
package test. The Rust-owned stage repairs the two empty enum anchors, emits
package-level `Apache-2.0` metadata, pins the current cached `test` line, and
emits `tool/rust_owned_package_smoke.dart`. Offline resolution upgrades
`frontend_server_client` to its AOT-aware 4.0.0 line; analysis, the emitted
smoke test, and `dart test --concurrency=1` pass all 70 generated tests. The
earlier Dart 3.8.3 missing-snapshot failure remains recorded as historical
evidence. A separate compiled Dart consumer passes the 200 bytes round trip,
max uint64 decimal-string preservation, and canonical 409 mapping against the
same Rust fixtures. The portable archive carries the generated package and
complete resolved-dependency license evidence.

The generated Rust reqwest package is also qualified by an isolated compiled
consumer: it passes base64 bytes, maximum uint64 decimal-string preservation,
and typed 409 error decoding. The per-target license and archive manifests
remain recorded in `receipt.json`; they do not make a global Apache claim for
third-party dependencies.

## Julia

The Rust-owned `sdk-openapi --julia-adaptation` emits the Workers package
source from the `ContractSpec` route and field anchors. The official Julia
1.13.1 Windows portable archive is pinned by URL, archive hash, and executable
hash. The packaged `AcyclicWorkers` 1.0.0 archive installs offline with its
stdlib-only `Project.toml`/`Manifest.toml` and Apache-2.0 package license.
Eight packaged tests pass against the loopback fixtures: request and response
bytes, exact maximum uint64, typed 409 errors, and pre-transport cancellation.
The target remains an HTTP projection; Julia and all stdlib/transitive runtime
licenses are tracked outside the generated package’s Apache-2.0 scope.
