# Direct production exact_u32_from_f64 proof — filesystem c8 / ba8d1fa

This artifact is bound to the actual filesystem production crate at
`C:/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/work/filesystem-main-port-c8`.
The source revision is `ba8d1fa931912e69a9c6e591423552b58fe57557`.

Inventory captured after the proof:

- `rust/crates/filesystem/src/numeric.rs`: SHA-256 `F7841AD39FC70618DF72F2F6746B03F6DCC74D3016FBD74C8B93F43B34102CAF`
- `rust/crates/filesystem/src/lib.rs`: SHA-256 `1BD52C28F32698CC7EF0D1C1F81616060461A531C7522BC2D2FAA90AC9F5B1DB`
- positive harness: SHA-256 `4E4D0A887CEDB5B6A06E6B02C068A044EB667681DF79659E514AAFAAD8AF204F`
- negative harness: SHA-256 `97465BE5149D55F3231D5AAF89940C3927571174F6D92FA52DDB464BEA3466BB`

The Kani harness calls exported production symbol `acyclic_fs::exact_u32_from_f64`; it does not copy that function. For every `u64` bit pattern interpreted with `f64::from_bits`, the positive theorem checks acceptance iff finite, integral, nonnegative, and at most `u32::MAX`; on acceptance it checks `f64::from(returned_u32) == input` numerically, otherwise it checks `Err`.

Pinned run: Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, CaDiCaL 3.0.0, default unwind 2, offline, one build job, target under `/tmp/kani-exact-u32-f64-c8-ba8d1fa-proof`.

Positive raw output: `exact-u32-f64-c8-ba8d1fa-kani068.raw.txt` (terminal marker present; `0 of 14 failed (1 unreachable)`; `VERIFICATION: SUCCESSFUL`; one harness complete).

Negative control raw output: `exact-u32-f64-c8-ba8d1fa-negative-kani068.raw.txt` (terminal marker present; expected `1 of 10 failed`; intentional assertion that `exact_u32_from_f64(0.5)` is `Ok`; this is the expected mutation failure, not a production theorem failure).

The old `latest265` receipts in the parent artifact remain historical and are not relabeled by this source identity record.
