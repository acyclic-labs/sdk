# Filesystem main-port runtime qualification — enum-final staging candidate — 2026-10-07

This records the enum-final staging candidate based on `4f9447472ad3288ef51cd9d7e5dbc9e7d8785c41` (`origin/main`) in `Q:\sdk\work\filesystem-main-port`. It binds the shared exact-u32 admission helper, native N-API and WASM numeric input boundaries, exact Rust u64 work-counter serialization, finite Rust-owned file/status projections, TypeScript adapter validator deletions, and focused Rust-admission regressions. The primary dependency checkout was reused only through temporary local junctions; no dependency files are part of this port. Artifact hashes below bind this dirty candidate pending the coordinator's main-based signed commit.

## Source identity

- `rust/crates/filesystem/src/numeric.rs` — `51276AA1FF76487DF226F6C6000F951842DBF0F2FEE7948796B4FBDE8FF05551`
- `rust/crates/filesystem/src/lib.rs` — `7448757A5A238E2EB40920ADE8CC5A2F77676B5F94F3A21B4A94B14335E2E60A`
- `rust/crates/filesystem-napi/src/lib.rs` — `68222789B7DF446D20DB4F5C261CBCAE3A2A1334FDF5D68F8511CAEA1ABD7F52`
- `rust/crates/filesystem-wasm/src/lib.rs` — `BA04104024B60632BC7E4E8DA7FA6B81BCA2A8BDFC2883255BCDCA0136FA692D`
- `typescript/packages/filesystem/src/native.ts` — `1594BDAA362752ED93F8DF365CEC86AED5AB11C07ABFDB1885CC44FF836112D2`
- `typescript/packages/filesystem/src/workspace-results.ts` — `97632977D89DC5F69EB2447F61B081199170A90644E3F80892C282B88FC28336`
- `typescript/packages/filesystem/src/change-set-adapter.ts` — `6C50EF1CA92FDB58EDB9C8887D2397826038478D3B40E72CDA82791D380B8676`
- `typescript/packages/filesystem/src/transaction-adapter.ts` — `E822047444E4644237D9AF57A7BC4E51FFF098FBAE13A2BB24B1047BF25E3E45`
- `typescript/packages/filesystem/src/wasm-adapter.ts` — `31C2706966B030805599C23430CF33341972EEBC65630F1BFB75A0FB7BA0F313`
- `typescript/packages/filesystem/src/workspace-operations.ts` — `2D39BB0520DAE24CA80E31783F07A97897BA924993381F533E0D85F9ABE6D671`
- `typescript/packages/filesystem/test/workspace-operations.test.ts` — `6A5812ACA4E989ED814C20AB6B049460982EE0C7810B9BAAC88C8DC020B467B9`
- `typescript/packages/filesystem/src/binding-results.ts` — `EAEB8BC1F4B1972A08CCA3172D4A960CC1B6BC0B94855155A8FC13950BEEF762`
- `typescript/packages/filesystem/src/contracts.ts` — `FBD071BAB6DDBE653771D32038B5E97D711A36E475BE0B58ED107D4E1997DA12`
- `typescript/packages/filesystem/src/hosted.ts` — `EA00DC8FECF822FC60E5006DD3B20DC17FE081A0E48690F9A1E38495D18D80DF`
- `typescript/packages/filesystem/src/native.ts` — `51E932009741EF4E3355BEEBBC4ED0C10124F341E6BA498EEFDB081BF7BD69EC`
- `typescript/packages/filesystem/src/wasm-adapter.ts` — `94EE4D01477D8AD0BAE2CCA31DCE746DC5ACA2CD84765CA76D584F4D9FCC1BEB`
- `typescript/packages/filesystem/src/workspace-copies.ts` — `7611211CE18A203D4988DC3E4C5867B84A807B14C8EEBD8FFF2EC2D512A1F040`
- `typescript/packages/filesystem/test/wasm-adapter.test.ts` — `3BB0F5671B828D61F864EC1D92DFA032FAEBCD948070182B8C4FD8F2689FE87F`
- `typescript/packages/filesystem/test/binding-results.test.ts` — `DD9E066EAEA2EA2967238EB7AF1A9710C53242FCD0580E66E38390382295A5BA`

## Fresh artifacts

- N-API debug DLL: `target/debug/acyclic_fs_napi.dll`, SHA-256 `40124FA4A95C31194214C46A45D12F84BA395F3FA0CD026099E41AAF40EE2DF3`.
- Native companion package: `target/filesystem-native-companion-enum-final3/acyclic-fs-0.2.0-win32-x64.node`, SHA-256 `40124FA4A95C31194214C46A45D12F84BA395F3FA0CD026099E41AAF40EE2DF3` (fresh ABI and adapter qualification passed).
- N-API generated declaration: `typescript/packages/filesystem/generated/native/binding.d.ts`, SHA-256 `177390A9D6CEB1E2869CDA7B44C5D8489D3DD1B2045FAFBFA9A1CCD14630CD3F` (rendered by the module's exported `render(_, root)` callback; its public `NapiU32` declaration remains `number` and the Rust-owned file/status enums are present).
- WASM bindgen output: `target/filesystem-wasm-runtime/acyclic_fs_wasm_bg.wasm`, SHA-256 `63242635057724D7A293773D00E724BEDEA429F76BCBC2D2F5E653CBD50972C8`.
- WASM JavaScript glue: `target/filesystem-wasm-runtime/acyclic_fs_wasm.js`, SHA-256 `258200FE8CCD23EB897A47721B3C5978036C6DB1964F1B0F2733E7A37B77C6B1`.
- The fresh WASM files were copied into the tracked package output (the generated `.wasm` changed from the stale committed binary):
  - `typescript/packages/filesystem/generated/wasm/acyclic_fs_wasm_bg.wasm` — `63242635057724D7A293773D00E724BEDEA429F76BCBC2D2F5E653CBD50972C8`
  - `typescript/packages/filesystem/generated/wasm/acyclic_fs_wasm_bg.wasm.d.ts` — `2DE36970D9E5A4687352DADD8FCBCDD418A99F0B7B7B520FD8A0D4513F5D0E2D`
  - `typescript/packages/filesystem/generated/wasm/acyclic_fs_wasm.d.ts` — `893062A9D674F772D6F8F8104D845C664EA6FEE80AAA53D730563FDA4962FA57`
  - `typescript/packages/filesystem/generated/wasm/acyclic_fs_wasm.js` — `258200FE8CCD23EB897A47721B3C5978036C6DB1964F1B0F2733E7A37B77C6B1`

## Checks

- `cargo check -p acyclic-fs-napi --locked` — passed.
- `cargo check -p acyclic-fs-napi --tests --locked` — passed.
- `cargo check -p acyclic-fs-wasm --locked --target wasm32-unknown-unknown` — passed.
- `cargo check -p acyclic-fs-wasm --tests --locked --target wasm32-unknown-unknown` — passed.
- `cargo test -p acyclic-fs --lib numeric --locked` — 2 passed, 0 failed.
- `rustfmt --edition 2024 --check` passed for the three owned Rust sources.
- `bun x tsc -p tsconfig.json --noEmit --pretty false` — passed with the reused dependency tree.
- `bun x tsc -p tsconfig.type-tests.json --pretty false` — passed.
- Exported N-API `render(_, root)` callback — generated `binding.d.ts` and matched the tracked declaration byte-for-byte.
- Fresh packed-package 18-case runtime admission matrix — passed for both WASM and N-API. `0`, `1`, and `u32::MAX` reached the expected Rust downstream outcomes; negative, fractional, non-finite, overflow, null, string, boolean, undefined, bigint, boxed-number, and symbol values were all rejected at the boundary. Matrix SHA-256: `CB96AE0AAAC89CB52EBEFAEABC5C793E746152CA0C1D3905A1CED26688C05B98`.
- Fresh N-API ABI qualification — passed on `win32-x64`.
- Fresh native TypeScript adapter qualification — passed on `win32-x64`.
- Fresh native packaged boundary matrix — primitive numbers and all rejected non-number values matched the strict N-API conversion behavior.
- `cargo test -p acyclic-fs-napi native_work_json_preserves_full_u64_values --locked` — 1 passed, 0 failed; `u64::MAX` counters remained decimal strings in native JSON.
- Filesystem TypeScript suite — 70 passed, 0 failed, 692 assertions across 14 files with the frozen Bun lock/cache fixture for `fast-check` 4.10.2 and `pure-rand` 8.4.2.
- Strong-type focused suite — 45 passed, 0 failed, 295 assertions across binding results, WASM adapters, workspace copies/operations, and hosted contract tests after the exact-u64 and finite-union projection port. The complete suite also passed with the frozen cache fixture.
- The tracked WASM package output hashes match the fresh bindgen output above; no stale generated WASM was retained.
- The packed WASM boundary retains generated `number` declarations while Rust rejects non-primitive-number inputs before core u32 admission; native work counters are emitted and accepted as canonical decimal strings and exposed publicly as `bigint`.
- Fresh npm package after the enum-final source port: `target/npm-artifacts-enum-final2/acyclic-labs-fs-0.2.0.tgz`, SHA-256 `0D29C4E2BAF5D33F9CF11C070506A9F7618544F65808D137BB2CE02A64DB5BEC`. The packed archive was extracted and used for the 18-case matrix above.

The shared helper accepts exactly finite integral values in `0..=u32::MAX` and rejects negative, fractional, non-finite, and overflowing values. N-API and WASM inputs delegate to that helper; positive-operation policy remains in the Rust core. TypeScript adapters retain result/transport adaptation and no longer pre-validate values already admitted by those Rust boundaries.

## Final-main closure

At qualification time `origin/main` was `fd8272d97ffc20fea444e37f100e738d94e0dbc4`. The selected filesystem source and test paths have no diff between that revision and the staging base `4f9447472ad3288ef51cd9d7e5dbc9e7d8785c41`; the newer revision's changes are outside this filesystem closure. The recorded source hashes therefore remain applicable when this patch is applied to the final main revision.
