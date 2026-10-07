# Machines Rust-owned TypeScript scenario receipt

This receipt ties the executable Rust scenario to the existing `tsify`/wasm-bindgen declaration generator. The Rust example creates a deterministic `CreateMachine`, exercises the canonical `SimulatedMachines` provider, and emits the resulting mutation and page payload. The generated declaration is copied from `scripts/build-machines-wasm.mjs` staging output; no hand-authored TypeScript contract or website file is an input. The install tarball and dependency cache live in an external qualification staging directory and are not vendored here.

Generated declaration evidence includes `CreateIn`, `SuspensionIn`, `ExpirationIn`, readonly `ObservationOut[]`/`PageOut.machines`, and `validatePageSize`. `rustdoc_profiles` remains unchanged because it owns package/target/feature Rustdoc availability, while scenario source is crate-owned and qualified by Cargo.

Qualification: `cargo check --manifest-path Cargo.toml -p acyclic-machines --example machines-typescript-consumer --locked`; direct executable run; staged generated-WASM `validatePageSize` boundary runtime (1/256 accepted, 0/257/fractional/NaN/infinity rejected); `bun x tsc -p typescript/packages/machines/tsconfig.json --noEmit --pretty false`; `bun test typescript/packages/machines/test --timeout 30000` (23 pass, 0 fail).

The generator now emits generated/scenarios/machines/typescript-consumer.ts from the Rust example JSON. The copied receipt artifact generated-typescript-consumer.ts typechecks and executes against the installed @acyclic-labs/machines@0.2.0; its runtime assertion observes a created outcome, one listed machine, and after-idle suspension. See generated-snippet-qualification.txt for hashes and results.
