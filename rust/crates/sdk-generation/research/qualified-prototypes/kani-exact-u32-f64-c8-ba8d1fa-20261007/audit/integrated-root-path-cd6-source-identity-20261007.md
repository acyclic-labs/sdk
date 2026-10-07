# Corrected integrated filesystem source binding — 2026-10-07

The authoritative integrated checkout for this review is `C:/Users/varun/.codex/worktrees/filesystem-main-review/sdk`.

- HEAD: `cd6ab86bf9f5fbfa8eeb416b6604de379df67903`
- `rust/crates/filesystem/src/numeric.rs`: `F7841AD39FC70618DF72F2F6746B03F6DCC74D3016FBD74C8B93F43B34102CAF`
- `rust/crates/filesystem/src/lib.rs`: `1BD52C28F32698CC7EF0D1C1F81616060461A531C7522BC2D2FAA90AC9F5B1DB`

These two production source hashes exactly match the source inventory in `receipt.c8-ba8d1fa.json`, whose proof was executed against revision `ba8d1fa931912e69a9c6e591423552b58fe57557`. The integrated checkout therefore reuses that theorem only by exact production-file hash identity; no new Kani solver run was performed here. The newer commit identity is recorded and does not replace the original tested revision.

The bound claim remains limited to `acyclic_fs::exact_u32_from_f64` and the theorem in the original receipt. It does not certify unrelated files, the whole integrated checkout, or future edits. A change to either production file hash requires a new source-bound proof.
