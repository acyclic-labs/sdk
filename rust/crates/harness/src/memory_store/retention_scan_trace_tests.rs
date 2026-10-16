use super::*;
use crate::accounting_trace::{self as trace, Event};

#[test]
fn cancellation_at_each_observation_preserves_the_complete_operation_trace() -> Result<()> {
    let store = fixture(2)?;
    let path = "files/new";
    let work = WorkCounters {
        items_examined: 1,
        source_bytes_read: 23,
        ..Default::default()
    };
    for cancel_at in [Some(0), Some(1), Some(2), Some(3), Some(4), None] {
        let cancellations = [CancellationToken::default(), CancellationToken::default()];
        let before = PreparationIo::from_budget(budget(1024, 1024 * 1024), &cancellations[0])?;
        let after = PreparationIo::from_budget(budget(1024, 1024 * 1024), &cancellations[1])?;
        let (original, original_trace) =
            trace::capture(&cancellations[0], cancel_at, || scan(&store, path, &before));
        let (actual, actual_trace) = trace::capture(&cancellations[1], cancel_at, || {
            store.validate_retained_path(path, Some(&after))
        });
        trace::assert_matches(&original_trace, &actual_trace);
        assert_eq!(
            original.err().map(|error| error.to_string()),
            actual.err().map(|error| error.to_string())
        );
        assert_eq!(before.receipt(), after.receipt());
        assert_eq!(before.owned_bytes()?, after.owned_bytes()?);
        if cancel_at == Some(1) {
            assert_eq!(
                actual_trace,
                vec![
                    Event::Preflight(work),
                    Event::CancellationObserved(false),
                    Event::ChargeAttempt(work),
                    Event::CancellationObserved(true),
                ]
            );
            assert_eq!(after.receipt(), WorkCounters::UNCHARGED);
        }
    }
    Ok(())
}

#[test]
#[should_panic(expected = "preflight/charge trace changed")]
fn omitting_one_preflight_is_rejected_even_with_identical_results_and_charges() {
    let store = fixture(1).unwrap();
    let path = "files/new";
    let cancellations = [CancellationToken::default(), CancellationToken::default()];
    let before = PreparationIo::from_budget(budget(1024, 1024 * 1024), &cancellations[0]).unwrap();
    let broken = PreparationIo::from_budget(budget(1024, 1024 * 1024), &cancellations[1]).unwrap();
    let work = WorkCounters {
        items_examined: 1,
        source_bytes_read: 23,
        ..Default::default()
    };
    let (original, original_trace) =
        trace::capture(&cancellations[0], None, || scan(&store, path, &before));
    // Broken one-entry scan: charge still re-admits, so all the former final
    // outcome assertions pass despite the missing preflight and observation.
    let (actual, broken_trace) = trace::capture(&cancellations[1], None, || broken.charge(work));
    assert!(original.is_ok());
    assert!(actual.is_ok());
    assert_eq!(before.receipt(), broken.receipt());
    assert_eq!(before.owned_bytes().unwrap(), broken.owned_bytes().unwrap());
    assert_eq!(
        original_trace,
        vec![
            Event::Preflight(work),
            Event::CancellationObserved(false),
            Event::ChargeAttempt(work),
            Event::CancellationObserved(false),
            Event::Charged(work),
        ]
    );
    trace::assert_matches(&original_trace, &broken_trace);
}
