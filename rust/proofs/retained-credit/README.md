# Retained credit conservation

`kernel::allocation::retained::tests::retained_credit_conserves_full_width_ownership_transitions`
calls the production `KernelAccounting`, `AllocationLedger` and
`RetainedAllocation`. It generates full-width retained/scratch amounts within
the original ledger's representable capacity, then checks invalid scratch
retirement, invalid splitting, split/absorb, cancellation and caller/guard
retirement orders. No second accounting implementation supplies the result.

The original read-only ledger observation checks exact live credit, peak and
admission operations. Split/absorb and retirement must leave charged work
unchanged. This replaces the fixed 73/127-byte shared-peak example; the separate
physical-value destruction, admission-refusal and native concurrent-retirement
checks remain because they cover different boundaries.

The native adapter already has
[`empty_retirement_preserves_capacity_receipts_and_resource_credit`](../../crates/filesystem/src/materializer/allocation_tests.rs).
It calls actual `NativeMaterializationWork` and `NativeAllocation` across nested
resource scopes, cancellation and split/drop orders, checking exact original
capacity, receipts and resource credit. Native guards reject over-retirement
against their own byte ownership; standalone scratch instead shares a ledger
with retained leases, which is the distinct fence exercised here. The native
property uses small amounts and does not extend this full-width arithmetic
claim to native resource admission.

Run `python3 rust/proofs/qualify.py retained-credit` from the repository root. The
kept mutation removes the production scratch/retained fence. The unchanged
property must fail at `scratch retirement accepted retained credit`. A compile
error, signal, empty selection or timeout is not a qualifying negative result.
The driver restores the exact original source before its positive check.

This is a component accounting property, not a physical allocator, provider,
replication, restart or whole-operation cost proof. Its transitions have no
asynchronous wait. The production transition bodies operate on one ledger and
at most two guards without a population traversal; exact charged work alone
does not prove physical allocation or instruction counts. Native concurrent
retirement is covered separately by `materializer/allocation_model.rs`.
