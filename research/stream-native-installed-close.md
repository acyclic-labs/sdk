# Stream native installed close qualification

This receipt covers the current pinned Windows Stream N-API artifact built from the `rust-source-foundation/sdk` checkout. The package was staged in a temporary fixture and resolved by package name from a separate temporary consumer.

- Source checkout: `C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk`
- Source: `rust/crates/stream-napi/src/lib.rs`
- Source SHA-256: `6B9257AA172D1154B29B1B8FEFEDC289FF17AC5BAE324B5BD4F5D57B700A9C98`
- Generated declarations: `typescript/packages/stream/generated/native/binding.d.ts`
- Declaration SHA-256: `65264A0F4F133381FC6C14EBB364D93FCB4B02EE7623CF574BC516EFEF0F33E`
- Temporary package fixture: `C:/Users/varun/AppData/Local/Temp/stream-native-qual-8962e9e5132b4a85ad7d96d85dadf89f/package-root`
- Package: `@acyclic-labs/stream-native-qual@0.2.0`
- Package tarball: `acyclic-labs-stream-native-qual-0.2.0.tgz`
- Tarball SHA-256: `39DD8EB138058E47A9F946ABB3F6A0607DAB0E61399C2E4C32E496C2194B4078`
- Tarball SHA-512 SRI: `sha512-wArEYl7lLNYmv0aplT3mxrGpcjGiMcoUkmP4OabsYP4ZOxvtaDcNA2n2BE4xOOfFTcm47XdMOOOoS8PrFgpHoA==`
- `package.json` SHA-256: `8AEBF11F7CF0DB97B88D5E034ED9A50C8A36AC93C7C4B3DB33AB019AD0BB4D46`
- N-API loader `binding.cjs` SHA-256: `D176F7E0466931DB9E07EFA3F00CD1AA80653D9E66D881F7AE2FDE69C2355B58`
- Generated declaration `binding.d.ts` SHA-256: `CA35185D8E8BF34CBEC4D867D4DA8BE116D7703FE4E27CB68C529919C735D50F`
- Windows native artifact `index.win32-x64-msvc.node` SHA-256: `D4D3693311D5958B2AD825DBD4BDF804B4211CB093B45B49EFA668F605505547`

The exact installed-consumer command was:

```text
node C:\Users\varun\AppData\Local\Temp\stream-native-qual-8962e9e5132b4a85ad7d96d85dadf89f\installed-close.mjs
```

The fixture used the maintained Connect TLS server and the package-name resolver. It passed with `transport: grpc`, `binding: 0.2.0`, a pending `nextResult()` resolving after `close()`, and observed server-side follow stream release. The copied test source is [stream-native-installed-close.mjs](stream-native-installed-close.mjs).

This receipt proves the Windows package fixture only. It does not claim native qualification for other operating systems or architectures.

## Production package integration audit

The installed fixture exercises the current Rust N-API artifact and generated declarations, but the production Stream package still has no native companion loader. The current gap is bounded to these files:

- `typescript/packages/stream/generated/native/binding.d.ts` exists, while `binding.cjs` and a checked-in or staged `.node` artifact do not.
- `scripts/stream-napi-types.mjs` generates declarations only; it does not build or stage an installable native package.
- `typescript/packages/stream/package.json` has no `./native` export or platform optional dependencies.
- `typescript/packages/stream/src/index.ts` and `src/client.ts` expose memory, HTTP, and Connect gRPC entry points only; no native loader or default native selection is wired.

The minimal future integration surface is a new `typescript/packages/stream/src/native.ts` loader/provider, a `./native` package export and platform companion metadata in `typescript/packages/stream/package.json`, and an explicit async native entry point in `src/client.ts` or `src/index.ts`. The existing synchronous `StreamClient.fromEnv` contract should not be silently changed until that async loading decision is coordinated. The current receipt therefore establishes the Windows artifact and cancellation behavior without asserting production package integration or support for unqualified platforms.

## Production integration update

The package now contains the coordinated thin integration surface:

- `typescript/packages/stream/src/native.ts` loads the maintained N-API `binding.cjs`, adapts canonical protobuf requests, projects unary responses through the existing Rust/WASM `projectMemoryResponse`, and maps Rust error metadata to the public Stream error codes. Follow cancellation calls the Rust-owned `NativeStreamFollow.close()` and preserves the existing `AbortSignal` provider contract.
- `typescript/packages/stream/src/default.ts` selects the installed native provider lazily for Node/Bun and retains the existing WASM-backed HTTP provider for browsers or when the optional native companion is absent. `Stream.fromEnv` keeps its synchronous construction API.
- `typescript/packages/stream/package.json` exports `./native`, declares the qualified Windows x64 companion, and exposes the Windows-only native staging script `scripts/build-stream-native.mjs`.

The staging script rejects every target except `x86_64-pc-windows-msvc`; no other platform or architecture is claimed. TypeScript compilation passed with `bunx tsc -p typescript/packages/stream/tsconfig.json --pretty false --noEmit`. The installed Windows fixture receipt above remains the runtime evidence for the native artifact and follow release behavior.

## Installed production package qualification

The integrated package was staged and consumed under its production identity `@acyclic-labs/stream@0.2.0` from:

`C:/Users/varun/AppData/Local/Temp/stream-prod-qual-2ea3cb5c58a34e7087bae696f86ab0c3/consumer`

The maintained N-API loader and Windows companion were generated with `@napi-rs/cli` from the current Stream manifest and pinned target. Recorded hashes are:

- package tarball `acyclic-labs-stream-0.2.0.tgz`: `FA6FDB946CF99514CE9210D0B2EF4FA186E83ACFBA7864EC1D0158043F596B5F`
- `generated/native/binding.cjs`: `2F42DA4093D32B527F8F6D0AF4D8676F93CCB0F4C9F4B89622B659A0A9EC809D`
- Windows companion `index.win32-x64-msvc.node`: `D4D3693311D5958B2AD825DBD4BDF804B4211CB093B45B49EFA668F605505547`
- staged package `package.json`: `707D8CF788D39BE14F0D00B5B76943A21A5F3CF141E873DF434846D1FFF7A98F`

The exact native/default consumer command was:

```text
node C:\Users\varun\AppData\Local\Temp\stream-prod-qual-2ea3cb5c58a34e7087bae696f86ab0c3\consumer\installed-default.mjs
```

It passed with:

```json
{"schema":"acyclic.stream.native.installed-default.v1","transport":"grpc","defaultEntry":"Stream.fromEnv","nativeSelected":true,"pendingFollowClosed":true,"serverStreamDropped":true,"installedPackage":"@acyclic-labs/stream"}
```

The exact browser fallback consumer command was:

```text
node C:\Users\varun\AppData\Local\Temp\stream-prod-qual-2ea3cb5c58a34e7087bae696f86ab0c3\consumer\browser-fallback.mjs
```

It passed with:

```json
{"schema":"acyclic.stream.browser.fallback.v1","selected":"http-wasm","process":"browser","tail":"0","requests":1}
```

The browser fixture removed the Node runtime marker and exercised the existing HTTP adapter with a Rust/WASM validated tail response. The native receipt remains Windows x64 evidence only.

The consumer sources are preserved as [stream-native-installed-default.mjs](stream-native-installed-default.mjs) and [stream-browser-fallback.mjs](stream-browser-fallback.mjs); they contain no native binary or generated package payload.

## Metadata and lockfile audit

`cargo metadata --locked --no-deps --format-version 1` passed. `Cargo.lock` records the current `acyclic-stream-napi` dependency set, including the qualification-only `rcgen` and `tonic` dev dependencies. The current `rust/crates/stream-napi/Cargo.toml` does not yet contain a Rust-owned `[package.metadata.napi] targets` declaration; the parent Rust coordination step must add the qualified `x86_64-pc-windows-msvc` target before release metadata is treated as complete.

## Review corrections and release metadata

The Rust manifest now owns the qualified target declaration, and `scripts/build-stream-native.mjs` reads it through `cargo metadata --locked`; the builder rejects targets absent from that manifest. A release build generated `typescript/packages/stream/generated/native/native-targets.json` with schema `acyclic.stream.native-targets.v1`, source revision, manifest digest, selected Rust target, and native artifact digest. The release package no longer declares an unrepresented optional companion dependency; the generated loader first loads the staged qualified artifact from the package itself.

Native follow installs its abort listener before `openFollowResult`, so cancellation during a delayed open cancels the Rust token and suppresses the expected cancellation result. The preserved delayed-open regression is [stream-native-delayed-open-abort.mjs](stream-native-delayed-open-abort.mjs), which passed with `opened: true`, `cancellationObserved: true`, and `completed: true`.

The browser entry now uses an opaque fixed dynamic import for the Node-only native facade. A real browser-target bundle completed with `bun build ... --target=browser` and contained no `node:buffer`, `node:module`, or `node:url` references. Native availability fallback only recognizes a missing production `generated/native/binding.cjs`; transitive native loader failures propagate.

The release-staged package was repacked after these corrections. Its SHA-256 values are:

- package tarball `acyclic-labs-stream-0.2.0.tgz`: `9224D1922D229AC5DD0BF4BE0CF8368A483A69CFA603DA47551CD072B16A2E39`
- `generated/native/native-targets.json`: `C9C351EE3F868AFFCB9EA0037502E1515A4F8CEDAC5423827495DEC0EDFC5FB4`
- release Windows artifact `generated/native/index.win32-x64-msvc.node`: `F32E3A6D26EBDCC158B074D651A8A5624EAB0F114CCFB1D25C1F7541973C28D2`
