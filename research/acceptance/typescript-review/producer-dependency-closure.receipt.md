# Producer dependency closure acceptance receipt

This receipt records the scoped source-authority negative review for the Rust
examples producer and docs importer. It is intentionally retained as evidence
for the authority semantics decision; it does not qualify a production build.

## Forged dirty-source authority witness

The target worktree HEAD was unchanged at
`b98fb2b450a1a3c69c37eec6fdbcb5876f700844`, while the tracked file
`rust/crates/sdk-docs/src/lib.rs` was dirty (` M rust/crates/sdk-docs/src/lib.rs`).
The authority manifest was generated from the dirty bytes and supplied with its
matching configured digest:

- source closure digest: `sha256:9c1f67f0e07a3ecac933a5139303b8e2759878a546be15d1b18f35c911ff9d4e`
- authority file digest: `sha256:69a55a4b472784e813ce8944610972d8d9d37fe78f32defb665209aaa633dd33`
- resulting docs bundle SHA-256: `46BF0D851BF2E96CD693F6EB33E375B51F9B38A76E2E6092AA057319061FF3DE`
- command exit: `0`

Exact command used:

```text
Q:\sdk\.tmp-sdk-docs-target-current\debug\sdk-docs.exe --repo-root C:\Users\varun\.codex\worktrees\rust-sdk-docs-source\sdk --output Q:\sdk\.tmp-docs-authority-dirty-tracked-output.json --examples-bundle Q:\sdk\.tmp-docs-authority-dirty-tracked --source-authority Q:\sdk\.tmp-forged-source-authority-tracked.json --source-authority-sha256 sha256:69a55a4b472784e813ce8944610972d8d9d37fe78f32defb665209aaa633dd33 --source-revision b98fb2b450a1a3c69c37eec6fdbcb5876f700844
```

The acceptance means the current verifier binds the supplied authority bytes,
the live dirty bytes, and the current HEAD label. It does not prove that those
bytes are Git blob content for that revision. Production semantics must either
require a clean revision/blob binding or label this as a separately trusted
captured snapshot.

Evidence files retained in the shared review workspace:

- `.tmp-docs-authority-dirty-tracked.log` SHA-256 `E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855`
- `.tmp-docs-authority-dirty-tracked-output.json` SHA-256 `46BF0D851BF2E96CD693F6EB33E375B51F9B38A76E2E6092AA057319061FF3DE`
- `.tmp-forged-source-authority-tracked.json` SHA-256 `69A55A4B472784E813CE8944610972D8D9D37FE78F32DEFB665209AAA633DD33`

## Dynamic producer mutation fixture

`producer-dependency-closure.acceptance.test.ts` now copies the workspace
manifest and lock, `rust-toolchain.toml`, `.cargo/config.toml`, the complete
`rust/crates` tree excluding build targets, and `plugin`. Its baseline assertion
requires one source file from each of the 13 resolved local packages. It then
mutates `rust/crates/objects/src/lib.rs`, requires the compiled producer to
reject the live closure, and requires the manifest to be absent from the failed
output directory. The runtime assertion remains pending a fresh owner binary;
the collector now compiles past the earlier repair, but the fresh build fails
while normalizing Cargo dependency paths: Cargo reports ordinary `C:\...`
paths while the collector canonicalizes the source root to `\\?\C:\...`, so
`sdk-contract-wire` is incorrectly reported as escaping the source root.
Evidence: `.tmp-sdk-examples-build-current3.log`, SHA-256
`735C9547745FCBD6C5A6C62EED210627DB6D1631E46EAB4FF2D0DE44EABB9EC8`.

After that path repair, a fresh binary built successfully with SHA-256
`31AC30207DA94FC28DC8284AAC153317BD1502712601EE470E487C0463B96C42`.
Its initial compiled/live check passed after a stabilized rebuild, but the
clean baseline generation failed at the final source race gate: expected
`sha256:6bcb3bcebaff5a71b8e4446580805eb6f23fbfc65f167c764bec54eb5365b231`,
observed
`sha256:583166cb3e57331a03985a87469f1d11c3c6653c1414da79727f74d86df0f2cc`.
Source-owned inputs changed during generation (the run was concurrent with
other workspace activity), so the source race gate correctly rejected the
baseline. The core mutation case remains blocked until the owner can establish
a stable snapshot, including whether generated inputs must be pre-materialized
or excluded/relocated. The baseline log is
`.tmp-sdk-examples-current4-currentroot-second.log`.

## Clean Git binding retest

After the docs owner added Git blob verification, a rebuilt `sdk-docs` binary
(SHA-256 `1437FC0E22E1B459BF72DF5479012445703C9F2812707C0D7059583BD88FDF00`)
rejected a newly forged authority for the current dirty tracked docs source.
Exit was `1` with `SDK examples source authority revision does not contain
current bytes`. The authority digest was
`sha256:9bc495e4b207bcb2a2dc0348670f13be6c18b855765f05e7fa595fc98c387b85`;
the rejection log SHA-256 was
`2E0903A12D7735E1BE805EFD7A2AFAFBFF9FA9DABC25CF360883C18BCAA148AC`.
The earlier exit-0 witness remains retained as historical evidence of the
pre-repair behavior.

## Immutable snapshot producer proof

To remove live workspace races from the producer acceptance, a fixed snapshot at
`Q:\sdk\.tmp-sdk-examples-immutable-snapshot-current` was used. It included the
workspace manifests, lock, toolchain/configuration, and all local crates while
excluding targets, `.git`, `node_modules`, and `plugin`. The owner binary was
built twice with:

```text
cargo build --manifest-path Q:/sdk/.tmp-sdk-examples-immutable-snapshot-current/rust/crates/sdk-examples/Cargo.toml --locked --offline --bin sdk-examples
```

Both builds passed; the binary SHA-256 was
`FE7F882C862999708AD2D609CC23604F8B82D17C26D46947C03D274864547FCC`. Build log
SHA-256 values were `F4BE5243A9A1A1ABEEBCF1FAAFABE706F0CC205CC027DAEAB62D2E821972C342`
and `81EDD2B773A4D3342D4E4AA265677B32B30527622B8617D56A3D307BDEDF792A`.

The unchanged snapshot generated successfully (exit `0`) into
`Q:\sdk\.tmp-sdk-examples-immutable-baseline-current`. Its manifest SHA-256 was
`2821F35D6EDEAC644E4A1FE8FCC401B526E62072371B14EDACCF155D755C45B2`, and the
manifest's source closure was
`sha256:0586d9ab640241fd9389b4fffebd17f630a684c9911e8c08b5e94759bbd301ed`.

A separate copy was mutated only by appending a comment to
`rust/crates/objects/src/lib.rs` (mutated file SHA-256
`10B98B58F02ACFD471ABE9E0BB8A5D979B5F3DEA2C4DDC6AF42A4B58F3AEB6BD`). Running
the same binary against the mutated copy exited `1` with
`live example source closure does not match compiled producer`; the diagnostic
reported compiled
`sha256:0586d9ab640241fd9389b4fffebd17f630a684c9911e8c08b5e94759bbd301ed` and
observed
`sha256:00d328c2e97da32267b62697d31c2f95079511bb3c8acba943141e326f78fcb9`.
The failed output directory and `sdk-examples-manifest.json` were absent. The
mutation log SHA-256 was
`06A0D67FC14C320C53BC348D78153F93FA495D8103848BE610128EA3D04BA708`.

This baseline/mutation pair is an immutable-capture proof independent of
concurrent edits in the live owner worktree.

The successful baseline manifest contained no absolute Windows paths. Its
receipt artifact paths remained portable `qualification/consumers/...` and
`qualification/packages/...` paths.
