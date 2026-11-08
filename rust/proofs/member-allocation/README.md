# Darwin member storage on its original allocation ledger

`member_storage_preserves_original_credit_and_unwind` exercises the production
member buffer with the shared `ReadBudgetMeter`. It crosses normal and panicking
callers with normal and panicking allocation-return callbacks. It checks the
actual retained capacity, one allocation, one original credit return, zero final
memory and preservation of the caller's original panic. Refused admission adds
neither a refund nor retained storage. The existing isolated self-execution
primitive contains an original destructor abort.

The buffer precedes its borrowed `ReadAllocation` field: Rust's field drop order
frees its physical storage before the original ledger guard returns the credit.
The shared guard owns capacity reconciliation and secondary callback unwinds.
There is no second allocation ledger or custom member destructor.

The retained inverse restores the original Darwin production source from
`ae447d369b915aaf26256a3119446ccecdb9c378`, keeping the current test module. Run on
macOS from the repository root:

```sh
git apply --check rust/proofs/member-allocation/original-members.patch
git apply rust/proofs/member-allocation/original-members.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(member_storage_preserves_original_credit_and_unwind)'
# Require original member credit return failure and a destructor cleanup abort.
# Compilation failure is not a semantic negative.
git apply -R rust/proofs/member-allocation/original-members.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(member_storage_preserves_original_credit_and_unwind)'
# Require one selected pass and no selected skips.
```

Restore the inverse even if a command fails. Native original and restored
execution remains required; source identity and formatting alone establish no
macOS acceptance.

On an exact-capacity metered allocation, allocator calls remain one; original
allocation-return callbacks fall from two to one by removing the zero refund.
Added syscalls, durability barriers, round trips, allocations, copies and Arc
clones are zero. Allocation bookkeeping is constant work and introduces no wait,
polling or history. Initializing the PID buffer remains proportional to its
capacity. This control does not establish constant-time group enumeration or
whole-group retirement progress.
