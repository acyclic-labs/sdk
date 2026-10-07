# Filesystem N-API strict-u32 qualification

- Source: `rust/crates/filesystem-napi/src/lib.rs`  
  SHA-256: `AE75B4A83E6939D659EE3BBF1D9DDBC0F1F2E99068DC0D3FF17EC8A291A18C36`
- ABI: `target/debug/acyclic_fs_napi.dll`  
  SHA-256: `06625731FD0A959EDDA97EB7B3918B53B629597741C5777CB00DE348428B31DB`
- Generated declarations: `typescript/packages/filesystem/generated/native/binding.d.ts`  
  SHA-256: `2E3A0E2635A72B003186977FAA4F9A32ADC847A1D245C77E4E6C206C1E8F92B0`
- Rust qualification: pinned Rust 1.98.1 unit suite, 12 passing tests.
- Declaration freshness: `bun scripts/filesystem-napi-types.mjs write` followed by `bun scripts/filesystem-napi-types.mjs check` passed. The checked-in declaration is generated from the current source and exposes `NapiU32` as `number`.
- Runtime qualification: `node scripts/check-filesystem-napi.mjs` passed on `win32-x64`.
- Covered runtime inputs: `workspace.planExtents` method input, flat `NativeFs.open` `maximumEntries`, and nested `NativeFs.createVolume` `limits.maximumPathBytes`, with the complete limits object supplied. Each rejects `-1`, `0.5`, `NaN`, `Infinity`, and `4294967296` with `expected a finite integer in the u32 range`; `4294967295` is accepted for `maximumEntries`.

## TypeScript adapter handoff

The browser and native adapters now forward u32 bounds to the generated Rust
boundary. Positive-integer checks were removed from `native.ts`,
`change-set-adapter.ts`, `transaction-adapter.ts`, and the shared join/rebase
result module. The `workspaceOperations` read-byte check remains because it
protects a bigint request invariant; the native object-cache byte check remains
because that public number is converted to bigint. Response byte copying,
result parsing, and bigint handling remain adapter responsibilities.

- `native.ts`: `69AB0A6D1B0D836048E77FAA387E00F4885F799184354C9C507D5E4D3B941612`
- `workspace-results.ts`: `97632977D89DC5F69EB2447F61B081199170A90644E3F80892C282B88FC28336`
- `change-set-adapter.ts`: `6C50EF1CA92FDB58EDB9C8887D2397826038478D3B40E72CDA82791D380B8676`
- `transaction-adapter.ts`: `E822047444E4644237D9AF57A7BC4E51FFF098FBAE13A2BB24B1047BF25E3E45`
- Focused regression: `bun test test/workspace-operations.test.ts` (4 passing), including zero and fractional join-bound inputs forwarded to Rust.
- TypeScript gates: `bun x tsc -p tsconfig.json --noEmit --pretty false`, `bun x tsc -p tsconfig.type-tests.json --pretty false`, and `bun test test/wasm-adapter.test.ts` (15 passing).
