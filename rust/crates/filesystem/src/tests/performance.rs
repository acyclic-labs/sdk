use super::*;

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

#[test]
fn in_place_item_charges_equal_a_verified_sum() {
    let mut budget = WorkBudget::UNBOUNDED;
    budget.items_examined = 5;
    let mut work = WorkCounters {
        page_reads: 2,
        items_examined: 1,
        ..WorkCounters::default()
    };
    let sum = work
        .checked_add(WorkCounters {
            items_examined: 3,
            ..WorkCounters::default()
        })
        .and_then(|sum| sum.verify(budget).map(|()| sum));
    assert_eq!(work.charge_items(3, &budget), Ok(()));
    assert_eq!(Ok(work), sum);

    let before = work;
    assert_eq!(
        work.charge_items(2, &budget),
        Err(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 6,
            maximum: 5,
        })
    );
    assert_eq!(
        work, before,
        "a refused charge leaves the counters unchanged"
    );

    // An earlier counter already past its bound is reported first, exactly
    // as verifying the complete sum does.
    budget.page_reads = 1;
    assert_eq!(
        work.charge_items(1, &budget),
        Err(WorkError::BudgetExceeded {
            counter: "page_reads",
            observed: 2,
            maximum: 1,
        })
    );
    assert_eq!(work, before);
    work.items_examined = u64::MAX;
    assert_eq!(
        work.charge_items(1, &WorkBudget::UNBOUNDED),
        Err(WorkError::Overflow)
    );
}

#[test]
fn in_place_allocation_charges_equal_a_verified_peak() {
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 100;
    let mut work = WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: 40,
        ..WorkCounters::default()
    };
    assert_eq!(work.charge_allocation(2, 30, &budget), Ok(()));
    assert_eq!(
        (work.allocation_operations, work.peak_allocation_bytes),
        (3, 40)
    );
    assert_eq!(work.charge_allocation(1, 90, &budget), Ok(()));
    assert_eq!(
        (work.allocation_operations, work.peak_allocation_bytes),
        (4, 90)
    );
    let before = work;
    assert_eq!(
        work.charge_allocation(1, 101, &budget),
        Err(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 101,
            maximum: 100,
        })
    );
    assert_eq!(work, before);
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
