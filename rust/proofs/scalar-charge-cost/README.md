# Scalar charging avoids unrelated arithmetic

Run `python3 rust/proofs/qualify.py scalar-charge-cost` from the repository root.

The existing generated property invokes the actual standalone kernel accounting
methods with full-width prior work, item count and budget. Each of 128 generated
invocations covers both live and cancelled callers. Compare complete typed work
or cancellation errors and every receipt field with the general production
charge method. The scalar method must perform one addition, or zero after caller
cancellation, and allocate zero heap objects and bytes on every result path.

The retained mutation replaces the actual `WorkCounters::charge_items` body with
general charging of an item-only receipt. It preserves accounting behavior but
adds unrelated zero-field arithmetic. Qualification requires the intended
arithmetic-count assertion to fail, restores exact original source bytes, then
requires the same property to pass. Compilation errors, empty selections and
timeouts do not qualify.

This property subsumes the fixed `item_only_charge_does_one_addition_and_old_path_fails_the_same_bound`
example. Direct typed results and receipts replace Debug-string and JSON
comparisons; setup allocation is excluded from operation measurement.

This establishes scalar standalone arithmetic and allocation counts, not a
complete native ledger, storage, cancellation-tree or whole-filesystem cost
proof. The operation introduces no wait or asynchronous producer. Native
single-leaf charging and retirement have separate controls.
