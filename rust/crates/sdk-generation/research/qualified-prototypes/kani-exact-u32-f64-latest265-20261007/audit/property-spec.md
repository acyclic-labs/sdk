# Exact u32 admission theorem

Source-bound target: acyclic_fs::exact_u32_from_f64 from
C:/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk/work/filesystem-main-port-latest265/rust/crates/filesystem/src/numeric.rs.

The single harness quantifies an unconstrained u64, interprets it with
f64::from_bits, and invokes the exported production function. Its theorem
is:

- if the production input is finite, integral, and in [0, u32::MAX], the
  result is accepted and the returned u32 re-embeds as the same numeric f64;
- otherwise the result is Err.

The re-embedding assertion independently checks exact numeric preservation;
it does not use a duplicated conversion algorithm or rely solely on an
expected cast expression. Numeric equality includes signed zero semantics.
The quantified domain covers NaNs, infinities, signed zero, subnormals,
negative values, fractional values, and out-of-range values.

No proof has been executed yet because the Actors CBMC process
PTY 17660 / PID 1031718 remains active.
