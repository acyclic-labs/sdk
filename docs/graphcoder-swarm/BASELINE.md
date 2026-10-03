# Qualification baseline observations

The first official installed-Harness attempts are retained in
[baseline-installed-harness-2026-10-03.json](baseline-installed-harness-2026-10-03.json).
They are baseline evidence only and cannot satisfy the final matrix.

The repository's official path is `scripts/check-harness-package.sh`. It builds
fresh Harness WASM, compiles the public TypeScript package, packs the npm
archive, installs that archive in an isolated consumer, runs native/WASM event
equivalence and transport/package tests, packages the Rust closure, and then
runs the Rust package suite. The first attempt exposed the required workspace
Objects build prerequisite. After that prerequisite was built, the installed
consumer passed 40 tests with 92 expectations, including native/WASM event
equivalence and package artifact resolution.

The same run's Rust package suite failed before producing valid qualification
evidence because the Windows host had many concurrent Cargo/rustc builds and
the compiler saw only metadata stubs for `std` and `test`. The run therefore
did not reach `run-harness-conformance.mjs`; Rust package and provider
conformance must be rerun with host contention cleared. Compilation or the
passing TypeScript consumer suite does not substitute for those runtime lanes.

The pinned toolchain was present and internally complete during that failure:
`rustc`/`cargo` were `1.98.1` from `rust-toolchain.toml`, and the target sysroot
contained matching `libstd` and `libtest` `.rlib` and `.rmeta` files. This rules
out a missing-toolchain-file explanation for the recorded run. The retry must
use a unique `CARGO_TARGET_DIR`, `CARGO_BUILD_JOBS=2`, and no overlapping
Cargo/rustc lane so the metadata-stub result can be classified as either a
repeatable package/toolchain problem or host contention.
