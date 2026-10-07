# Actual-production Kani milestone (2026-10-07)

This scoped prototype imports the current `acyclic-actors` production crate by
path and calls its public `validate_add_subscription`, `validate_create`, and
`validate_update` functions. The harness does not copy or reimplement their
validation algorithm, and it does not claim any response projection or
transport property.

The completed source-bound result is `production_validate_add_subscription_cursor`:
for every symbolic full-width `u64` cursor, the actual production validator
returns `Ok(())` for a fixed otherwise-valid request. Kani 0.68.0 / CBMC 6.11.0
reported 0 failed checks and terminal exit 0.

The combined ActorLimits harness compiled the actual production crate but was
solver-inconclusive: CBMC spent its run unwinding the production validator's
internal `HashSet` SipHash/fastrand path before reaching the assertions. The
receipt records that limitation and preserves the raw interrupted log. It is not
reported as an ActorLimits proof. The generated wire fields
`delivered_cursor`, `completed_cursor`, `recoverable_cursor`, `failed_cursor`,
checkpoint fields, configuration revision, and optional response actor remain
unproved because this current production crate exposes no projection/accessor
function for them.

Build output was placed under `/tmp/kani-actual-target-20261007` on Linux ext4;
the repository prototype contains source, receipt, and raw text evidence only.
