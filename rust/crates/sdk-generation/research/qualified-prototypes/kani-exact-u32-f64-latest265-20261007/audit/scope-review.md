# Scope review

Source authority is the Windows worktree at
C:/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/work/filesystem-main-port-latest265,
revision f775b8465d836cf69b358f9fa9ebd8edbfa819db. Windows Git reports the
production numeric.rs and lib.rs paths clean; their captured SHA-256 values are
recorded in receipt.in-progress.json. The linked-worktree Git metadata is not
usable from the WSL mount, so the Windows Git identity is authoritative.

The harness closes over only:
- acyclic_fs::exact_u32_from_f64 imported from the production crate;
- arbitrary u64 bits interpreted with f64::from_bits;
- the stated finite/integral/nonnegative/u32-range predicate;
- Kani assertions on Result admission and numeric f64 re-embedding.

IEEE cases:
- +0 and -0 satisfy the numeric predicate and are accepted; f64 equality
  intentionally treats signed zero as the same numeric integer value.
- finite subnormals and ordinary fractional values fail fract()==0.0 and are
  rejected, unless they are zero.
- NaN and both infinities fail is_finite() and are rejected.
- negative finite values fail the nonnegative predicate.
- values above u32::MAX fail the upper bound.
- every accepted u32 re-embeds to the same numeric f64; the assertion is
  independent of the implementation's cast expression.

Cutover rerun plan:
1. Preserve this f775 source-bound receipt and codegen result unchanged.
2. After the filesystem source freeze is explicitly identified, refresh the
   source inventory and hashes from that exact checkout.
3. Run the same harness through a new source-bound receipt and retain the raw
   terminal output and exit marker.
4. Report only the refreshed result; this artifact cannot be relabeled as a
   proof of a later source revision.

