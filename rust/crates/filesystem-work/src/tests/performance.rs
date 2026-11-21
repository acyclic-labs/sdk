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
    for (low, name) in (1_u64..).zip(names.keys()) {
        let high = low + 24;
        let (a, b) = if low % 2 == 1 {
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
    assert_eq!(mapped.error, OuterError::Inner);
    assert_eq!(
        mapped.work(),
        Err(WorkObservationError::Work(WorkError::Overflow))
    );
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
        // Test every field alone, full-width mixed fields, then nonzero mixed
        // fields whose sums cannot overflow. The wider arithmetic is independent
        // of the production overflow flags and additive macro.
        for selected in 0..26 {
            let values = |input: [u64; 24]| {
                match selected {
                    24 => input,
                    25 => input.map(|value| (value / 2).max(1)),
                    _ => {
                        let mut only = [0; 24];
                        only[selected] = input[selected];
                        only
                    }
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
                match u64::try_from(wide) {
                    Ok(value) => *field = value,
                    Err(_) => overflow = true,
                }
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
mod native {
    mod borrowed {
        use super::super::{WorkBudget, WorkCounter, WorkCounters, WorkError, counter_fields};
        use proptest::prelude::*;

        fn baseline(observed: WorkCounters, budget: WorkBudget) -> Result<(), WorkError> {
            let fields = [
                (
                    WorkCounter::AuthorityRecordsRead,
                    observed.authority_records_read,
                    budget.authority_records_read,
                ),
                (
                    WorkCounter::AuthorityRecordsAppended,
                    observed.authority_records_appended,
                    budget.authority_records_appended,
                ),
                (
                    WorkCounter::AuthorityBytesRead,
                    observed.authority_bytes_read,
                    budget.authority_bytes_read,
                ),
                (
                    WorkCounter::AuthorityBytesWritten,
                    observed.authority_bytes_written,
                    budget.authority_bytes_written,
                ),
                (
                    WorkCounter::ObjectProbes,
                    observed.object_probes,
                    budget.object_probes,
                ),
                (
                    WorkCounter::BackendReadOperations,
                    observed.backend_read_operations,
                    budget.backend_read_operations,
                ),
                (
                    WorkCounter::BackendWriteOperations,
                    observed.backend_write_operations,
                    budget.backend_write_operations,
                ),
                (
                    WorkCounter::DurabilityOperations,
                    observed.durability_operations,
                    budget.durability_operations,
                ),
                (
                    WorkCounter::PageReads,
                    observed.page_reads,
                    budget.page_reads,
                ),
                (
                    WorkCounter::PageWrites,
                    observed.page_writes,
                    budget.page_writes,
                ),
                (
                    WorkCounter::ObjectBytesRead,
                    observed.object_bytes_read,
                    budget.object_bytes_read,
                ),
                (
                    WorkCounter::ObjectBytesWritten,
                    observed.object_bytes_written,
                    budget.object_bytes_written,
                ),
                (
                    WorkCounter::BytesHashed,
                    observed.bytes_hashed,
                    budget.bytes_hashed,
                ),
                (
                    WorkCounter::BytesCopied,
                    observed.bytes_copied,
                    budget.bytes_copied,
                ),
                (
                    WorkCounter::BytesEncoded,
                    observed.bytes_encoded,
                    budget.bytes_encoded,
                ),
                (
                    WorkCounter::SourceBytesRead,
                    observed.source_bytes_read,
                    budget.source_bytes_read,
                ),
                (
                    WorkCounter::SourcePathComponents,
                    observed.source_path_components,
                    budget.source_path_components,
                ),
                (
                    WorkCounter::SourceEntriesVisited,
                    observed.source_entries_visited,
                    budget.source_entries_visited,
                ),
                (
                    WorkCounter::OutputBytes,
                    observed.output_bytes,
                    budget.output_bytes,
                ),
                (
                    WorkCounter::ItemsExamined,
                    observed.items_examined,
                    budget.items_examined,
                ),
                (
                    WorkCounter::ItemsReturned,
                    observed.items_returned,
                    budget.items_returned,
                ),
                (
                    WorkCounter::AllocationOperations,
                    observed.allocation_operations,
                    budget.allocation_operations,
                ),
                (
                    WorkCounter::PeakAllocationBytes,
                    observed.peak_allocation_bytes,
                    budget.peak_allocation_bytes,
                ),
                (
                    WorkCounter::Materializations,
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
            fn borrowed_verification_preserves_exact_errors_and_priority(values in prop::array::uniform24(any::<u64>()), maxima in prop::array::uniform24(any::<u64>()), unbounded in any::<bool>()) {
                let make = |values: [u64; 24]| {
                    let mut counters = WorkCounters::UNCHARGED;
                    for (field, value) in counter_fields(&mut counters).into_iter().zip(values) { *field = value; }
                    counters
                };
                let maxima = if unbounded { [u64::MAX; 24] } else { maxima };
                for selected in 0..25 {
                    let (work, budget) = if selected == 24 {
                        // Retain the full-width mixed inputs and their error priority.
                        (make(values), make(maxima))
                    } else {
                        // A refusal on every field cannot be hidden by an earlier one.
                        let mut work = WorkCounters::UNCHARGED;
                        let mut budget = WorkBudget::UNBOUNDED;
                        let observed = values[selected].max(1);
                        *counter_fields(&mut work)[selected] = observed;
                        *counter_fields(&mut budget)[selected] = observed - 1;
                        (work, budget)
                    };
                    prop_assert_eq!(work.verify_within(&budget), baseline(work, budget),
                        "budget refusal lost the exact counter or priority");
                }
            }
        }
    }
}
