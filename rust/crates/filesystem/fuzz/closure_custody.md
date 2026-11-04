# Original closure work and output custody

The closure walker borrows the original accounting frame for physical reads and
incremental diffs. Returned proof allocations keep independent leases on that
same ledger. A withdrawn future drops its authority frame before releasing the
shared accounting lock; the frame restores the original counters and scratch.
Re-entry fails with the typed accounting-handoff error instead of waiting.

`minimal_generation_has_a_complete_authenticated_closure` delegates reads to the
real memory store, checks prior work at every physical admission, adds seven codec
units per read and requires all seven at the outer receipt. It checks that proof
output and independent live backing retain their original funding after the
meter retires, including retirement after cancellation. The existing frame and
retained-allocation controls cover withdrawal and shared backing retirement.

The kept `mutations/closure_original_frame.patch` restores the former production
read's replacement-frame construction. The same real-store relation must reject
it before the first physical read. Restore the production source after applying
the patch, then require the relation to pass:

```sh
cargo test -p acyclic-fs --no-default-features --features local,memory,distributed --lib kernel::closure::tests::minimal_generation_has_a_complete_authenticated_closure -- --exact
```

Lease transfer, combination and frame borrowing take constant work and introduce
no waits, reads, copies or durability barriers. An ordinary shared meter admits
its value and funding controls plus promotion of the original ledger, each once;
native meters retain the existing original work without these allocations.
Closure traversal remains proportional to admitted objects and records. The
membership relation keeps its linear copy and consuming-cursor bounds and the
old quadratic-move and repeated-seek negative controls at 512 entries, with
complete identity-bit coverage retained separately in the same relation.
