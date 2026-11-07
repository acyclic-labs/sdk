# Original allocation owner before admission

`allocation_owner_precedes_original_credit_admission` injects an unwind at the
original ledger's owner-selection callback. It uses the shipped `ReadBuffer`
allocator, actual existing backing through `ReadAllocation::adopt`, and the
original `ReadAllocationReservation::allocate` conversion. No failed owner
selection may leave charged memory or a spent allocation attempt. The property
checks every path before reporting violations and preserves the original panic.

Run from the repository root. The retained patch restores the complete original
production file from `4a3122d094a1a82d7605b4fcb8e4cadb106365d4`. It also restores
that revision's Darwin production dependency, retaining the current tests. This
keeps its Linux-only borrowed constructor consistent with the original member
storage; an unavailable constructor cannot stand in for the owner-order failure:

```sh
git apply rust/proofs/allocation-owner/original-owner-order.patch
git diff --exit-code 4a3122d094a1a82d7605b4fcb8e4cadb106365d4 -- rust/crates/native-runtime/src/meter/allocation.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(=tests::reservations::allocation_owner_precedes_original_credit_admission)'
# Require failure: "original allocation owner selection precedes credit admission",
# reporting buffer, adopt and reserved paths; a compiler error is not evidence.
git apply -R rust/proofs/allocation-owner/original-owner-order.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(=tests::reservations::allocation_owner_precedes_original_credit_admission)'
# Require one pass and no selected skips.
```

The shared owner selection moves before admission. Each successful constructor
still makes one owner callback, at most one Arc clone, and one admission callback;
there are no added syscalls, barriers, round trips, allocations, copies or waits.
Failure before admission performs no credit refund. Unconsumed reservations keep
their existing single refund. Work remains constant and has no wait dependency.
The check covers owner selection, not arbitrary corruption inside a meter's own
admission callback or native completion packet custody.
