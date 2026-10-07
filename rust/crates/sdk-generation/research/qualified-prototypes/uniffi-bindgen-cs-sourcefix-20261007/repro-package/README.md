# Source-only C# reproducibility package

This package is the smallest retained input set for the qualified C# backend
source patch. It contains the Rust `acyclic-actors-uniffi` 0.2.0 manifest,
lockfile, facade source, exact `cargo metadata` output, the five-template
source patch, generated C# and configuration, and the three managed probes.
It intentionally does not vendor the upstream generator checkout and does not
contain a DLL, native binary, Cargo target, managed `bin`/`obj`, or a second
managed backend/registry.

## Pinned inputs

- Rust foundation revision: `371bb4170e16aca973176b6756a261ee5add7297`.
- Rust package: `acyclic-actors-uniffi` version `0.2.0`, Rust 1.98, UniFFI
  `=0.31.0`, `publish = false`, Apache-2.0.
- C# generator: NordSecurity `uniffi-bindgen-cs`, tag
  `v0.11.0+v0.31.0`, commit `e10ce410eb3a10cc19c7928b93ea8d84e038c034`,
  MPL-2.0.

The generator source is intentionally external. Reproduce against a clean
checkout at that exact commit, apply `generator/generator-source.patch`, and
build the pinned binary with `cargo build --release --locked
--bin uniffi-bindgen-cs`. The patch was derived from the tracked diff only and
changes exactly five templates: `Async.cs`, `Helpers.cs`, `ObjectTemplate.cs`,
`TopLevelFunctionTemplate.cs`, and `macros.cs` (69 insertions, 16 deletions).
The raw-handle constructor fix is part of `ObjectTemplate.cs`; the untracked
upstream staging files `.cargo-ok` and `ObjectTemplate.cs.patch` are excluded.

Generation consumes the Rust cdylib produced from the copied Cargo package and
uses `generated/uniffi.toml`. The resulting source must hash to the value in
`SHA256SUMS.txt`. The generated code uses the maintained UniFFI future/cancel
symbols, full-width `ulong`, optional values, nominal validated types, and
compiler-visible typed errors. The patch adds an optional
`CancellationToken` to generated async declarations and maps native status 3
to `OperationCanceledException` carrying the request token. It keeps the Rust
facade's existing nullable `CancellationHandle` argument and the probes pass
`null`; no managed handle adapter or operation registry is added.

## Qualification receipts

The external source qualification already produced these results from this
exact generated source and patch:

- `managed/AllEightProgram.cs`: compile PASS and runtime PASS for connect,
  create, update, inspect, inspect request, add, remove, resume, checkpoint,
  and invoke; no manual Rust cancellation handle.
- `managed/Program.cs`: compile PASS and three gated cancellation iterations;
  continuation-map baseline 0, peaks 1/1/1, after each iteration 0;
  fixture state started=3, aborted=3, active=0; each await raised
  `OperationCanceledException` carrying the request token.
- `managed/RawHandleNegative.cs`: expected external-assembly compile failure
  CS1729, proving the internal marker constructor cannot be forged by a
  consumer.

The pending probe reads the exclusive fixture options file
`Q:\sdk\work\root-pending-actors-fixture-options.json`; that external fixture
is deliberately not copied into this source-only package. `receipt.json` in
the parent qualification directory records the complete run provenance.

`SHA256SUMS.txt` covers every retained input and probe; the receipt is intentionally excluded from its own manifest. `receipt.json` records the package
scope and exact observed hashes. No production Rust or SDK files were changed
by assembling this package.

