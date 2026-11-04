use super::*;

#[test]
fn finite_admission_checks_every_declared_work_allowance() -> Result<(), serde_json::Error> {
    assert!(WorkBudget::default().has_finite_allowances());
    assert!(!WorkBudget::UNBOUNDED.has_finite_allowances());
    let finite = serde_json::to_value(WorkBudget::default())?;
    let fields = finite
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("work counters must be an object"))?;
    assert_eq!(fields.len(), 24);
    for field in fields.keys() {
        let mut changed = finite.clone();
        changed[field] = serde_json::json!(u64::MAX);
        let budget: WorkBudget = serde_json::from_value(changed)?;
        assert!(
            !budget.has_finite_allowances(),
            "unbounded {field} was admitted"
        );
    }
    Ok(())
}

#[test]
fn budget_intersection_preserves_every_declared_allowance() -> Result<(), serde_json::Error> {
    let fields = serde_json::to_value(WorkBudget::default())?;
    let names = fields
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("work counters must be an object"))?;
    let mut left = fields.clone();
    let mut right = fields.clone();
    let mut expected = fields.clone();
    for (index, name) in names.keys().enumerate() {
        let low = (index + 1) as u64;
        let high = low + 24;
        let (a, b) = if index % 2 == 0 {
            (low, high)
        } else {
            (high, low)
        };
        left[name] = serde_json::json!(a);
        right[name] = serde_json::json!(b);
        expected[name] = serde_json::json!(low);
    }
    let left: WorkBudget = serde_json::from_value(left)?;
    let right: WorkBudget = serde_json::from_value(right)?;
    let expected: WorkBudget = serde_json::from_value(expected)?;
    assert_eq!(left.intersect_budget(right), expected);
    assert_eq!(right.intersect_budget(left), expected);
    assert_eq!(left.intersect_budget(WorkBudget::UNBOUNDED), left);
    assert_eq!(
        left.intersect_budget(WorkBudget::default()),
        WorkBudget::default()
    );
    Ok(())
}

#[derive(Debug, Error, PartialEq)]
enum OuterError {
    #[error("inner")]
    Inner,
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[test]
fn budgets_fail_on_the_exact_counter() {
    let observed = WorkCounters {
        page_reads: 2,
        ..WorkCounters::default()
    };
    let budget = WorkBudget {
        page_reads: 1,
        ..WorkBudget::default()
    };
    assert_eq!(
        observed.verify(budget),
        Err(WorkError::BudgetExceeded {
            counter: "page_reads",
            observed: 2,
            maximum: 1,
        })
    );
    let observed = WorkCounters {
        source_entries_visited: 3,
        ..WorkCounters::default()
    };
    let budget = WorkBudget {
        source_entries_visited: 2,
        ..WorkBudget::default()
    };
    assert_eq!(
        observed.verify(budget),
        Err(WorkError::BudgetExceeded {
            counter: "source_entries_visited",
            observed: 3,
            maximum: 2,
        })
    );
}

#[test]
fn nested_failure_accounting_never_substitutes_a_sentinel_receipt() {
    let failure = OperationFailure::new(
        (),
        WorkCounters {
            page_reads: 1,
            ..WorkCounters::default()
        },
    );
    let mapped = failure.map_with_prior_work(
        WorkCounters {
            page_reads: u64::MAX,
            ..WorkCounters::default()
        },
        |()| OuterError::Inner,
    );
    assert_eq!(mapped.error, OuterError::Work(WorkError::Overflow));
    assert_eq!(mapped.work.page_reads, u64::MAX);
    assert_ne!(*mapped.work, WorkCounters::UNBOUNDED);
}

/// Every counter of [`WorkCounters`], as mutable references in declaration
/// order, so a test can set one counter at a time.
fn counter_fields(work: &mut WorkCounters) -> [&mut u64; 24] {
    [
        &mut work.authority_records_read,
        &mut work.authority_records_appended,
        &mut work.authority_bytes_read,
        &mut work.authority_bytes_written,
        &mut work.object_probes,
        &mut work.backend_read_operations,
        &mut work.backend_write_operations,
        &mut work.durability_operations,
        &mut work.page_reads,
        &mut work.page_writes,
        &mut work.object_bytes_read,
        &mut work.object_bytes_written,
        &mut work.bytes_hashed,
        &mut work.bytes_copied,
        &mut work.bytes_encoded,
        &mut work.source_bytes_read,
        &mut work.source_path_components,
        &mut work.source_entries_visited,
        &mut work.output_bytes,
        &mut work.items_examined,
        &mut work.items_returned,
        &mut work.allocation_operations,
        &mut work.peak_allocation_bytes,
        &mut work.materializations,
    ]
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn full_width_work_arithmetic_preserves_exact_sums_and_refusals(
        left in proptest::array::uniform24(proptest::prop_oneof![proptest::strategy::Just(0_u64), proptest::strategy::Just(u64::MAX), proptest::num::u64::ANY]),
        right in proptest::array::uniform24(proptest::prop_oneof![proptest::strategy::Just(0_u64), proptest::strategy::Just(u64::MAX), proptest::num::u64::ANY]),
        maxima in proptest::array::uniform24(proptest::num::u64::ANY),
    ) {
        let make = |values: [u64; 24]| {
            let mut work = WorkCounters::UNCHARGED;
            for (field, value) in counter_fields(&mut work).into_iter().zip(values) {
                *field = value;
            }
            work
        };
        // Test every field alone, then mixed fields. The wider arithmetic is
        // independent of the production overflow flags and additive macro.
        for selected in 0..=24 {
            let values = |input: [u64; 24]| {
                if selected == 24 { input } else {
                    let mut only = [0; 24];
                    only[selected] = input[selected];
                    only
                }
            };
            let a = values(left);
            let b = values(right);
            let prior = make(a);
            let delta = make(b);
            let mut sum = WorkCounters::UNCHARGED;
            let mut overflow = false;
            for (index, field) in counter_fields(&mut sum).into_iter().enumerate() {
                let wide = if index == 22 {
                    u128::from(a[index].max(b[index]))
                } else { u128::from(a[index]) + u128::from(b[index]) };
                overflow |= wide > u128::from(u64::MAX);
                *field = wide as u64;
            }
            let expected = if overflow { Err(WorkError::Overflow) } else { Ok(sum) };
            proptest::prop_assert_eq!(prior.checked_add(delta), expected,
                "full-width work sum disagreed with wider arithmetic");
            let mut added = prior;
            proptest::prop_assert_eq!(added.try_add_assign(&delta), expected.map(|_| ()));
            proptest::prop_assert_eq!(added, expected.unwrap_or(prior));
            // The separate borrowed-verification property checks exact budget
            // errors and their priority against an independent field oracle.
            let mut below = sum;
            let field = selected.min(23);
            let limits = counter_fields(&mut below);
            *limits[field] = limits[field].saturating_sub(1);
            for budget in [WorkBudget::UNBOUNDED, make(maxima), sum, below] {
                let admitted = expected.and_then(|sum| sum.verify(budget).map(|()| sum));
                proptest::prop_assert_eq!(prior.admit(&delta, &budget), admitted.map(|_| ()));
                let mut charged = prior;
                proptest::prop_assert_eq!(charged.charge(&delta, &budget), admitted.map(|_| ()));
                proptest::prop_assert_eq!(charged, admitted.unwrap_or(prior));
            }
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
#[path = "performance_native.rs"]
mod native;
