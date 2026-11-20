use super::*;
use proptest::prelude::*;

#[test]
fn completed_kernel_work_borrows_the_original_budget_without_allocating() {
    use crate::{
        kernel::allocation::KernelAccounting,
        performance::{VERIFICATION_BUDGET, VERIFICATION_PASSES},
    };
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let KernelAccounting::Standalone { budget, .. } = &accounting else {
        unreachable!()
    };
    let original = std::ptr::from_ref(budget) as usize;
    VERIFICATION_PASSES.with(|count| count.set(0));
    VERIFICATION_BUDGET.with(|address| address.set(0));
    let allocations = acyclic_test_support::measure(|| {
        accounting
            .record_completed(WorkCounters {
                items_examined: 7,
                bytes_copied: 11,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
    });
    assert_eq!(VERIFICATION_PASSES.with(std::cell::Cell::get), 1);
    assert_eq!(
        VERIFICATION_BUDGET.with(std::cell::Cell::get),
        original,
        "verification copied the original budget"
    );
    assert_eq!(allocations.count_total, 0);
    assert_eq!(allocations.bytes_total, 0);
    assert_eq!(accounting.spent().unwrap().items_examined, 7);
    assert_eq!(accounting.spent().unwrap().bytes_copied, 11);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn item_only_kernel_charge_preserves_exact_errors_receipts_and_cost(prior in 0u64..u64::MAX, count in any::<u64>(), limit in any::<u64>()) {
        use crate::{kernel::allocation::KernelAccounting, performance::ADDITIVE_STEPS};
        let run = |old, cancelled| {
            let cancellation = crate::CancellationToken::new();
            let mut accounting = KernelAccounting::standalone(WorkBudget { items_examined: limit, ..WorkBudget::UNBOUNDED }, &cancellation);
            // Completed work is retained even when it exceeds its allowance.
            let _ = accounting.record_completed(WorkCounters { items_examined: prior, ..WorkCounters::UNCHARGED });
            if cancelled { cancellation.cancel(); }
            ADDITIVE_STEPS.with(|steps| steps.set(0));
            let mut result = Ok(());
            let allocations = acyclic_test_support::measure(|| {
                result = if old { accounting.charge(WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED }) }
                    else { accounting.charge_items(count) };
            });
            if !old {
                assert_eq!(ADDITIVE_STEPS.with(std::cell::Cell::get), usize::from(!cancelled),
                    "scalar charge performed unrelated counter arithmetic");
            }
            assert_eq!(allocations.count_total, 0);
            assert_eq!(allocations.bytes_total, 0);
            let result = result.map_err(|error| match error {
                crate::FsError::Work(error) => Ok(error),
                crate::FsError::Cancelled(error) => Err(error),
                error => panic!("unexpected accounting error: {error:?}"),
            });
            (result, accounting.spent().unwrap())
        };
        for cancelled in [false, true] {
            prop_assert_eq!(run(true, cancelled), run(false, cancelled));
        }
    }
}
