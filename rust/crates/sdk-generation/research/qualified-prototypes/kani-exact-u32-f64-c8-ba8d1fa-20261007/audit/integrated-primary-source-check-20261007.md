# Integrated filesystem source check — 2026-10-07

This is an audit of the current SDK foundation checkout after integration. It does not create a proof receipt and does not retag the c8 proof.

## Observed integrated checkout

- Checkout: `C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk`
- Branch: `codex/rust-source-foundation`
- HEAD: `371bb4170e16aca973176b6756a261ee5add7297`
- `rust/crates/filesystem/src/numeric.rs` SHA-256: `8BB27FDFCFF40AD0C5D76C92E017C1E6D491EDE22A0EED5853DFD02EDD35C349`
- `rust/crates/filesystem/src/lib.rs` SHA-256: `5CBF6257B489D4F9F3A04D98389A9232D8582C7D49FB3DA0967EA3CE0BC4D8E2`

Both filesystem files are currently uncommitted in the integrated checkout (`numeric.rs` is untracked and `lib.rs` is modified). The exported symbol is present at `acyclic_fs::exact_u32_from_f64`.

## Binding result

The preserved c8 proof receipt is bound to revision `ba8d1fa931912e69a9c6e591423552b58fe57557`, with `numeric.rs` SHA-256 `F7841AD39FC70618DF72F2F6746B03F6DCC74D3016FBD74C8B93F43B34102CAF` and `lib.rs` SHA-256 `1BD52C28F32698CC7EF0D1C1F81616060461A531C7522BC2D2FAA90AC9F5B1DB`. The integrated hashes differ. Therefore the c8 all-bit PASS and its negative control remain historical source-bound evidence and are not claims about this integrated checkout.

The integrated function currently compares against `u32::MAX as f64` (line 10) and casts with `number as u32` (line 13). This is a source-level property difference from the c8 proof source, which used the explicitly preserved `f64::from(u32::MAX)` bound. No theorem is transferred across this mismatch; a new receipt requires a proof against the integrated source after its production source is frozen.

## Path correction

The filesystem integration checkout for this review is `C:/Users/varun/.codex/worktrees/filesystem-main-review/sdk`, not the older `rust-source-foundation/sdk` WIP checkout described above. See `integrated-root-path-cd6-source-identity-20261007.md`: the corrected checkout matches the proven production hashes exactly. The older WIP mismatch remains recorded as an observation of that separate checkout only.
