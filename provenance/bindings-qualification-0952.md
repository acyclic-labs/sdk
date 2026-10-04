# Harness binding qualification receipt

Source revision: `0952a50e900851c0e8457319bb606b5a84f04ef7`

Worktree: `C:\Users\varun\.codex\worktrees\fork-binding-qualification`

Branch: `codex/fork-binding-qualification`

## Commands and results

- `bun install --frozen-lockfile` initially failed because the lockfile did not contain the `typescript/packages/graphcoder` workspace. The verbose diagnostic reported the missing workspace and `@acyclic-labs/graphcoder` package mapping.
- `bun install` updated only `bun.lock` with the missing GraphCoder workspace entry and package mapping. The lockfile delta was 1,384 bytes and contained no dependency-version changes.
- `bun install --frozen-lockfile` then passed: 112 installs, 177 packages, no changes.
- `bun run --cwd typescript/packages/harness build:wasm` passed with `wasm-bindgen 0.2.117`.
- `bun test typescript/packages/harness/test/native-wasm-equivalence.test.ts` passed: 4 tests, 77 assertions, 0 failures.
- `bun run check:generated` passed with exit code 0 after materializing the ignored WASM artifacts for all six packages.

The first `check:generated` attempt failed only because inference, machines, and objects had no local ignored runtime artifacts. Their package build scripts were run, and the complete check was rerun successfully.

## Harness artifact digests

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm` | 6,980,005 | `8B2979C5CBD5A74BF07C49AF731021281812CDD01B158C692281F5F476F595DB` |
| `typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js` | 75,922 | `14421B3DF5A34BF867441EA4659FED47DC99D071367BA83B816242260234D1EA` |
| `typescript/packages/harness/generated/wasm/acyclic_harness_wasm.d.ts` | 43,100 | `489DC650710BD26523244AC4E6FB73CBACA51336A2976B8A357BC49B993A6FC7` |
| `typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm.d.ts` | 9,456 | `D23595D181315240DA6AA924E1E793B442B8AA63BD01408F2CFC9BCBEC700F0D` |

## Binding ownership evidence

- Rust source owns the WASM shape in `rust/crates/harness/src/wasm.rs`, including `WasmModelBoundaryReferences`, `forkSeedFromReport`, and `prepareModelRequest`.
- Rust source owns fork validation and `original_request_digest` in `rust/crates/harness/src/fork.rs`.
- TypeScript preserves the generated WASM shape in `typescript/packages/harness/src/fork.ts`; it does not redeclare the Rust model-boundary fields.
- `typescript/packages/harness/test/native-wasm-equivalence.test.ts` compares raw WASM and typed facade results for recursive model requests, model-boundary envelopes, malformed values, and original request digests.

No source changes besides the lockfile reproducibility fix and this receipt are included. The generated runtime files are ignored build artifacts and are not committed.
