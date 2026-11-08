# Original allocation custody across a refund unwind

`shrinking_allocation_commits_custody_before_refund_callback` calls the shipped
`ReadAllocation::{release,resize_retained,reconcile_capacity}` paths. It retires
actual admitted backing, injects an unwind after the original meter refunds its
credit, and checks both zero and partial retained custody before dropping the
same allocation owner. Every path must retain precisely the unreleased claim,
preserve the original callback panic and leave zero outstanding credit on drop.
The property evaluates all six cases before reporting custody violations.

Run from the repository root. The retained patch restores the complete original
production file from `e85420c325d53887e9d8e10b81190f08761cc2ef`, while keeping the
same property and original meter. The inverse also restores that revision's
Darwin production dependency, retaining current tests, so the original Linux-only
borrowed constructor cannot turn the semantic control into a compilation failure:

The inverse also restores that pin's original closed-spawn argument body. This
keeps the Unix dependency consistent with the original Linux-only borrowed
constructor while retaining current tests. Dependency restoration is not a
semantic negative.

```sh
git apply rust/proofs/allocation-shrink/original-custody.patch
git diff --exit-code e85420c325d53887e9d8e10b81190f08761cc2ef -- rust/crates/native-runtime/src/meter/allocation.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(=tests::reservations::shrinking_allocation_commits_custody_before_refund_callback)'
# Require failure: "allocation retained ownership precedes original refund callback",
# with both release sizes reported for each of the three original paths.
git apply -R rust/proofs/allocation-shrink/original-custody.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(=tests::reservations::shrinking_allocation_commits_custody_before_refund_callback)'
# Require one pass and no selected skips.
```

All three paths share the same custody transition. Shrinking performs one scalar
subtraction, commits one scalar retained count, then invokes the original meter
once. The before/after callback count is one; added syscalls, durability barriers,
round trips, allocations, copies and waits are zero. The transition is constant
work independent of native history or population, and adds no progress dependency.
This property proves synchronous retained-credit custody, not native IO completion
packet ownership, a meter that corrupts its own ledger, or whole-application pause.
