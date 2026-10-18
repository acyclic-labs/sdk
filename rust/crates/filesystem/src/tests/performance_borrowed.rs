use super::*;
use proptest::prelude::*;

fn baseline(observed: WorkCounters, budget: WorkBudget) -> Result<(), WorkError> {
    let fields = [
        (
            "authority_records_read",
            observed.authority_records_read,
            budget.authority_records_read,
        ),
        (
            "authority_records_appended",
            observed.authority_records_appended,
            budget.authority_records_appended,
        ),
        (
            "authority_bytes_read",
            observed.authority_bytes_read,
            budget.authority_bytes_read,
        ),
        (
            "authority_bytes_written",
            observed.authority_bytes_written,
            budget.authority_bytes_written,
        ),
        (
            "object_probes",
            observed.object_probes,
            budget.object_probes,
        ),
        (
            "backend_read_operations",
            observed.backend_read_operations,
            budget.backend_read_operations,
        ),
        (
            "backend_write_operations",
            observed.backend_write_operations,
            budget.backend_write_operations,
        ),
        (
            "durability_operations",
            observed.durability_operations,
            budget.durability_operations,
        ),
        ("page_reads", observed.page_reads, budget.page_reads),
        ("page_writes", observed.page_writes, budget.page_writes),
        (
            "object_bytes_read",
            observed.object_bytes_read,
            budget.object_bytes_read,
        ),
        (
            "object_bytes_written",
            observed.object_bytes_written,
            budget.object_bytes_written,
        ),
        ("bytes_hashed", observed.bytes_hashed, budget.bytes_hashed),
        ("bytes_copied", observed.bytes_copied, budget.bytes_copied),
        (
            "bytes_encoded",
            observed.bytes_encoded,
            budget.bytes_encoded,
        ),
        (
            "source_bytes_read",
            observed.source_bytes_read,
            budget.source_bytes_read,
        ),
        (
            "source_path_components",
            observed.source_path_components,
            budget.source_path_components,
        ),
        (
            "source_entries_visited",
            observed.source_entries_visited,
            budget.source_entries_visited,
        ),
        ("output_bytes", observed.output_bytes, budget.output_bytes),
        (
            "items_examined",
            observed.items_examined,
            budget.items_examined,
        ),
        (
            "items_returned",
            observed.items_returned,
            budget.items_returned,
        ),
        (
            "allocation_operations",
            observed.allocation_operations,
            budget.allocation_operations,
        ),
        (
            "peak_allocation_bytes",
            observed.peak_allocation_bytes,
            budget.peak_allocation_bytes,
        ),
        (
            "materializations",
            observed.materializations,
            budget.materializations,
        ),
    ];
    for (counter, observed, maximum) in fields {
        if observed > maximum {
            return Err(WorkError::BudgetExceeded {
                counter,
                observed,
                maximum,
            });
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn borrowed_verification_preserves_all_bytes_errors_and_priority(values in prop::array::uniform24(any::<u64>()), maxima in prop::array::uniform24(any::<u64>()), unbounded in any::<bool>()) {
        let make = |values: [u64; 24]| {
            let mut counters = WorkCounters::UNCHARGED;
            for (field, value) in counter_fields(&mut counters).into_iter().zip(values) { *field = value; }
            counters
        };
        let maxima = if unbounded { [u64::MAX; 24] } else { maxima };
        // Every invocation gets separately constructed counter and budget values.
        let _ = acyclic_test_support::differential(
            || { let (work, budget) = (make(values), make(maxima)); (baseline(work, budget), serde_json::to_vec(&work).unwrap()) },
            || { let (work, budget) = (make(values), make(maxima)); (work.verify_within(&budget), serde_json::to_vec(&work).unwrap()) },
        );
    }
}

#[test]
fn completed_kernel_work_borrows_the_original_budget_without_allocating() {
    use crate::{
        kernel::allocation::KernelAccounting,
        performance::{VERIFICATION_BUDGET, VERIFICATION_PASSES},
    };
    for by_value in [false, true] {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let KernelAccounting::Standalone { budget, .. } = &accounting else {
            unreachable!()
        };
        let original = std::ptr::from_ref(budget) as usize;
        VERIFICATION_PASSES.with(|count| count.set(0));
        VERIFICATION_BUDGET.with(|address| address.set(0));
        let allocations = acyclic_test_support::measure(|| {
            let delta = WorkCounters {
                items_examined: 7,
                bytes_copied: 11,
                ..WorkCounters::UNCHARGED
            };
            if by_value {
                // The original production body: same work, verification count,
                // output and allocations, but a copied 192-byte budget.
                let KernelAccounting::Standalone {
                    work,
                    budget,
                    cancellation,
                    ..
                } = &mut accounting
                else {
                    unreachable!()
                };
                work.try_add_assign(&delta).unwrap();
                work.verify(*budget).unwrap();
                cancellation.check().unwrap();
            } else {
                accounting.record_completed(delta).unwrap();
            }
        });
        assert_eq!(VERIFICATION_PASSES.with(std::cell::Cell::get), 1);
        let bound = || {
            assert_eq!(
                VERIFICATION_BUDGET.with(std::cell::Cell::get),
                original,
                "verification copied the original budget"
            );
        };
        assert_eq!(
            std::panic::catch_unwind(bound).is_ok(),
            !by_value,
            "original by-value verification escaped the borrowing invariant"
        );
        assert_eq!(allocations.count_total, 0);
        assert_eq!(allocations.bytes_total, 0);
        assert_eq!(accounting.spent().items_examined, 7);
        assert_eq!(accounting.spent().bytes_copied, 11);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn item_only_kernel_charge_preserves_exact_errors_and_receipts(prior in 0u64..u64::MAX, count in any::<u64>(), limit in any::<u64>(), cancelled in any::<bool>()) {
        use crate::kernel::allocation::KernelAccounting;
        let run = |old| {
            let cancellation = crate::CancellationToken::new();
            let mut accounting = KernelAccounting::standalone(WorkBudget { items_examined: limit, ..WorkBudget::UNBOUNDED }, &cancellation);
            // Completed work is retained even when it exceeds its allowance.
            let _ = accounting.record_completed(WorkCounters { items_examined: prior, ..WorkCounters::UNCHARGED });
            if cancelled { cancellation.cancel(); }
            let result = if old { accounting.charge(WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED }) }
                else { accounting.charge_items(count) };
            (result.map_err(|error| format!("{error:?}")), serde_json::to_vec(&accounting.spent()).unwrap())
        };
        let _ = acyclic_test_support::differential(|| run(true), || run(false));
    }
}

#[test]
fn item_only_charge_does_one_addition_and_old_path_fails_the_same_bound() {
    use crate::{kernel::allocation::KernelAccounting, performance::ADDITIVE_STEPS};
    for old in [true, false] {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        ADDITIVE_STEPS.with(|count| count.set(0));
        let allocations = acyclic_test_support::measure(|| {
            if old {
                accounting
                    .charge(WorkCounters {
                        items_examined: 7,
                        ..WorkCounters::UNCHARGED
                    })
                    .unwrap();
            } else {
                accounting.charge_items(7).unwrap();
            }
        });
        let steps = ADDITIVE_STEPS.with(std::cell::Cell::get);
        assert_eq!(steps, if old { 46 } else { 1 });
        assert_eq!(
            steps == 1,
            !old,
            "zero-field arithmetic control escaped the exact bound"
        );
        assert_eq!(allocations.count_total, 0);
        assert_eq!(allocations.bytes_total, 0);
        assert_eq!(accounting.spent().items_examined, 7);
    }
}
