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
    let names = fields.as_object()
        .ok_or_else(|| serde::de::Error::custom("work counters must be an object"))?;
    let mut left = fields.clone();
    let mut right = fields.clone();
    let mut expected = fields.clone();
    for (index, name) in names.keys().enumerate() {
        let low = (index + 1) as u64;
        let high = low + 24;
        let (a, b) = if index % 2 == 0 { (low, high) } else { (high, low) };
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
    assert_eq!(left.intersect_budget(WorkBudget::default()), WorkBudget::default());
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

#[test]
fn every_multiline_counter_addition_fails_closed_on_overflow() {
    for (left, right) in [
        (
            WorkCounters {
                authority_records_appended: u64::MAX,
                ..WorkCounters::default()
            },
            WorkCounters {
                authority_records_appended: 1,
                ..WorkCounters::default()
            },
        ),
        (
            WorkCounters {
                authority_bytes_written: u64::MAX,
                ..WorkCounters::default()
            },
            WorkCounters {
                authority_bytes_written: 1,
                ..WorkCounters::default()
            },
        ),
        (
            WorkCounters {
                backend_read_operations: u64::MAX,
                ..WorkCounters::default()
            },
            WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            },
        ),
        (
            WorkCounters {
                backend_write_operations: u64::MAX,
                ..WorkCounters::default()
            },
            WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::default()
            },
        ),
    ] {
        assert_eq!(left.checked_add(right), Err(WorkError::Overflow));
    }
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

#[test]
fn in_place_charges_equal_a_verified_sum_for_every_counter() {
    for field in 0..24 {
        let mut prior = WorkCounters::default();
        let mut delta = WorkCounters::default();
        let mut budget = WorkBudget::UNBOUNDED;
        *counter_fields(&mut prior)[field] = 7;
        *counter_fields(&mut delta)[field] = 5;
        for maximum in [11, 12, 13] {
            *counter_fields(&mut budget)[field] = maximum;
            let expected = prior
                .checked_add(delta)
                .and_then(|sum| sum.verify(budget).map(|()| sum));
            assert_eq!(prior.admit(&delta, &budget), expected.map(|_| ()));
            let mut charged = prior;
            let outcome = charged.charge(&delta, &budget);
            assert_eq!(outcome, expected.map(|_| ()));
            assert_eq!(charged, expected.unwrap_or(prior));
            let mut added = prior;
            assert_eq!(added.try_add_assign(&delta), Ok(()));
            assert_eq!(Ok(added), prior.checked_add(delta));
            assert_eq!(added.verify_ref(&budget), added.verify(budget));
        }

        // Peak allocation is a maximum and never overflows; every other
        // counter fails closed and leaves the counters unchanged.
        *counter_fields(&mut prior)[field] = u64::MAX;
        let mut charged = prior;
        let sum = prior.checked_add(delta);
        if field == 22 {
            assert!(sum.is_ok());
            continue;
        }
        assert_eq!(sum, Err(WorkError::Overflow));
        assert_eq!(
            charged.charge(&delta, &WorkBudget::UNBOUNDED),
            Err(WorkError::Overflow)
        );
        assert_eq!(charged.try_add_assign(&delta), Err(WorkError::Overflow));
        assert_eq!(charged, prior);
    }
}
