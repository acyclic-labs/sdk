# @acyclic-labs/fs changelog

## 0.2.0 - Unreleased

- Breaking candidate: memory and native compositions use canonical Objects v2. Existing v1 Objects roots are rejected without upgrade or overwrite; Stream history and atomic publication remain intact.
- Exposes exact-original-base `beginTransactionAt` across hosted, native and browser/memory bindings. Durable retries reuse their owned generation, original operation key and mutation transcript instead of starting at a fresh head after reload.
- Qualifies native adapters through an installed compiled companion without module mocks; generated native/WASM declarations expose the original-base transaction API.
- Adds six-platform, source-bound NAPI companion production and release admission, canonical optional dependencies, retained compiler/runtime receipts, and installed-archive native behavior qualification. Publication requires all six architecture proofs; a local pass is not release qualification.

## 0.1.5 - 2026-09-25

- Aligns the native and browser clients with the qualified SDK 0.1.5 release.

## 0.1.4 - 2026-09-25

- Aligns the native and browser clients with the qualified SDK 0.1.4 release.

## 0.1.3 - 2026-09-25

- Makes the standalone package test build its distribution first, so clean
  checkouts exercise the native and browser clients reliably.

## 0.1.2 - 2026-09-25

- Aligns the native and browser workspace clients with the 0.1.2 SDK release.

## 0.1.1 - 2026-09-24

- First coordinated SDK release of versioned, forkable workspaces across
  browser, memory, hosted, and native providers.
- Native-view and projection updates improve source-backed mount behavior;
  platform guarantees remain explicit in each import and provider.
