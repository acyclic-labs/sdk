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
