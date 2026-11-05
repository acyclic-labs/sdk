# Physical allocation on native charging

Run `python3 rust/proofs/qualify.py native-charge` from the repository root.

The retained mutation inserts an allocation directly in production
`KernelAccounting::charge`. It must fail the unchanged physical-allocation
assertion in `native_charge_matches_the_old_path_at_every_completed_prefix`.
The shared qualifier restores exact source bytes and requires the original
property to pass; compilation failures, empty selections and timeouts do not
qualify the mutation.

The property invokes actual native accounting with bounded or unbounded work,
nested resource scopes, invocation views, rechecks, cancellation and poisoned
ledgers. It compares completed-prefix errors, receipts and live allocation with
the actual check-then-charge path and measures the real allocator around each
call. Its existing 128 generated cases are retained. The redundant fixed
injection example, test-only thread-local flag and charge hook are removed.

Zero allocator calls and bytes prove this component's physical heap cost, not
complete operation complexity. The separate original-leaf mutex oracle and
concurrent accounting models remain. No native I/O, full-tree, platform or
deployment acceptance follows from this property alone.
