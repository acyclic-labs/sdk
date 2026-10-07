# Direct-production Kani proof integration (filesystem, c2)

This isolated branch adds cfg(kani) declarations and proof harnesses that call the
filesystem crate's production functions directly. It does not duplicate either
implementation algorithm.

Source freeze:

- Git revision: c2e2d8ef80ab88a989c1f5955e6f35881f0274cd
- Worktree: C:\Users\varun\.codex\tmp\kani-proof-filesystem-c2
- Toolchain: Kani 0.68.0, CBMC 6.11.0, nightly-2026-08-21, CaDiCaL 3.0.0
- Kani runs used one job and offline dependency resolution.

Production bindings:

- src/kani_proofs.rs calls crate::exact_u32_from_f64 for every symbolic u64
  IEEE-754 bit pattern. The positive theorem states that Ok(value) is equivalent
  to finite, integral, nonnegative, in-range input and independently asserts
  f64::from(value) == number.
- src/operation_window/kani_reconcile_limits_proof.rs calls the private
  validate_reconcile_limits from its parent module. The theorem states that the
  result is Ok(()) exactly when all three production limit fields are nonzero,
  and checks the production JoinLimit error on invalid input.

Evidence:

- audit/exact-u32-positive.raw.txt: 0 of 7 failed, VERIFICATION:- SUCCESSFUL.
- audit/reconcile-limits-positive.raw.txt: 0 of 94 failed (2 unreachable),
  VERIFICATION:- SUCCESSFUL.
- audit/exact-u32-negative.raw.txt: expected failure, 1 of 10 failed, proving
  the assertion that the production function accepts 0.5 is false.
- audit/reconcile-limits-negative.raw.txt: expected failure, 1 of 91 failed
  (2 unreachable), proving the assertion that zero generations are accepted is
  false.
- The shell tee wrapper did not preserve a numeric cargo exit marker in two raw
  logs; the receipt records the terminal tool exit and Kani verification marker
  separately.

Scope limits:

These are proofs of the two named production functions and their stated
pre/postconditions. They do not prove arbitrary filesystem behavior, transport
semantics, stateful mutation, or unrelated SDK/generated code.
