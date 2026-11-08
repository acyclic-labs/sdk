# Original allocation custody for closed-spawn arguments

`closed_spawn_arguments_preserve_original_credit_and_unwind` enters the public
`ProcessTree::spawn_closed_measured` path with the shared `ReadBudgetMeter`.
It crosses a successful examination, a refused examination and an examination
unwind with normal and panicking original allocation-return callbacks. Successful
admission starts a real `/bin/true` child and retires it through `ProcessTree`.
Refusal and unwind before spawn must perform zero native writes. Each case
requires one allocation and one refund, zero final retained memory, preservation
of the original examination panic, and allocation denial without a new refund
or child. No child output is synthesized.

The borrowed `ReadAllocation` is declared before the argument vector, so reverse
local drop order destroys the vector before returning its original credit.
Capacity reconciliation uses the shared guard. A callback unwind cannot bypass
that guard, and a secondary refund panic cannot replace an examination unwind.

The retained inverse was captured with `git diff -R` against the complete
production file at `9ba2d099dd157f70bdffff34adfc4218a9ffa03b`; the property and
shared meter observations remain present. From the repository root on Unix:

```sh
git apply --check rust/proofs/closed-spawn-arguments/original-arguments.patch
git apply rust/proofs/closed-spawn-arguments/original-arguments.patch
git diff --exit-code 9ba2d099dd157f70bdffff34adfc4218a9ffa03b -- rust/crates/native-runtime/src/process_tree/unix.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(closed_spawn_arguments_preserve_original_credit_and_unwind)'
# Require one selected failure containing original argument examination unwind
# and closed spawn leaked argument credit. Compilation failure is not a negative.
git apply -R rust/proofs/closed-spawn-arguments/original-arguments.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(closed_spawn_arguments_preserve_original_credit_and_unwind)'
# Require one selected pass and no selected skips.
```

Restore the inverse on every exit. Native execution on each Unix platform is
required; source identity alone establishes no runtime acceptance.

Allocation bookkeeping is constant work: one original allocation, one capacity
reconciliation and one original refund, without waits or population/history
state. Pointer-vector construction remains proportional to the supplied argument
count. Before and after, it uses one allocator call and one pointer copy per
program/argument; added allocations, Arc clones, payload copies, syscalls,
durability barriers and round trips are zero. Reconciliation charges actual
allocator capacity, including excess on a refused completed adoption. The existing
native child retirement event remains the only child wait.

The manual-ledger class census is
`rg -n 'meter\.(allocate|release)' rust/crates/native-runtime/src/process_tree --glob '*.rs' --glob '!**/*tests.rs'`.
No direct allocation/refund pair remains in process-tree production code. Other
native subsystems have separate retained-resource contracts and are not qualified
by this argument-storage property.
