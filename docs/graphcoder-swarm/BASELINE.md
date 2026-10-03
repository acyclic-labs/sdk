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
