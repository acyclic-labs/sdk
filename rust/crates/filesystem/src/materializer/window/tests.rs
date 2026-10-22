use super::*;
use crate::{WorkBudget, WorkError};

const APPENDS: u64 = 10;
const COPIED: u64 = 100;
const PEAK: u64 = 1 << 20;

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_appended: APPENDS,
        bytes_copied: COPIED,
        peak_allocation_bytes: PEAK,
        ..crate::performance::BOUNDARY_BUDGET
    }
}

fn root() -> NativeMaterializationWork {
    NativeMaterializationWork::new(budget(), &crate::CancellationToken::new()).unwrap()
}

fn appends(count: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: count,
        ..WorkCounters::UNCHARGED
    }
}

fn exceeded(error: NativeTreeMaterializationError, counter: &str) {
    let shown = format!("{error:?}");
    assert!(
        shown.contains(
            &format!(
                "{:?}",
                WorkError::BudgetExceeded {
                    counter: "",
                    observed: 0,
                    maximum: 0
                }
            )[..16]
        ) && shown.contains(&format!("counter: {counter:?}")),
        "expected {counter} to exceed its window, got {shown}"
    );
}

/// One run of a session: a run window holding an effect window, like a Pi run that
/// dispatches its root effect. Both charge appends directly and bytes through the
/// plain-ledger runtime meter.
fn run(session: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
    let run = session.invocation_window()?;
    run.record_admitted(appends(APPENDS - 2))?;
    let effect = run.invocation_window()?;
    effect.record_admitted(appends(APPENDS))?;
    effect
        .runtime_meter()
        .examine(0, COPIED)
        .map_err(|error| effect.runtime_error(error))?;
    run.record_admitted(appends(2))?;
    run.runtime_meter()
        .examine(0, COPIED)
        .map_err(|error| run.runtime_error(error))
}

#[test]
fn every_run_of_a_long_session_gets_its_own_full_window() {
    const RUNS: u64 = 16;
    let session = root();
    for _ in 0..RUNS {
        run(&session).unwrap();
    }
    let receipt = session.receipt();
    // The shared ledger still records every run: only the checks are windowed.
    assert_eq!(receipt.authority_records_appended, RUNS * 2 * APPENDS);
    assert_eq!(receipt.bytes_copied, RUNS * 2 * COPIED);
    assert!(receipt.authority_records_appended > APPENDS);
    // Unwindowed work on the session keeps its own full budget.
    session.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        session.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn the_same_session_without_windows_exceeds_the_shared_budget() {
    // Negative control for the session above: every run on the shared ledger itself.
    let session = root();
    session.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        session.record_admitted(appends(APPENDS)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn one_run_over_its_own_window_still_fails() {
    let session = root();
    run(&session).unwrap();
    let over = session.invocation_window().unwrap();
    over.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        over.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
    let copied = session.invocation_window().unwrap();
    let meter = copied.runtime_meter();
    meter.examine(0, COPIED).unwrap();
    exceeded(
        copied.runtime_error(meter.examine(0, 1).unwrap_err()),
        "bytes_copied",
    );
}

#[test]
fn an_enclosing_window_is_never_charged_for_a_nested_one() {
    let session = root();
    let outer = session.invocation_window().unwrap();
    outer.record_admitted(appends(APPENDS - 4)).unwrap();
    let inner = outer.invocation_window().unwrap();
    inner.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        inner.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
    // A clone keeps its window, and the outer window still has exactly its own remainder.
    let alias = outer.clone();
    alias.record_admitted(appends(4)).unwrap();
    assert_eq!(outer.remaining().unwrap().authority_records_appended, 0);
    exceeded(
        outer.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn reservations_and_refunds_stay_exact_across_windows() {
    let session = root();
    let window = session.invocation_window().unwrap();
    let reservation = window.reserve_work(appends(APPENDS)).unwrap();
    assert_eq!(window.remaining().unwrap().authority_records_appended, 0);
    drop(reservation);
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    assert_eq!(
        session.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    window.record_admitted(appends(3)).unwrap();
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 3
    );
    assert_eq!(
        session.remaining().unwrap().authority_records_appended,
        APPENDS
    );
}

#[test]
fn a_reservation_held_by_one_window_never_blocks_a_fresh_window() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let held = first.reserve_work(appends(APPENDS)).unwrap();
    assert_eq!(first.remaining().unwrap().authority_records_appended, 0);
    let second = session.invocation_window().unwrap();
    assert_eq!(
        second.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    second.record_admitted(appends(APPENDS)).unwrap();
    assert_eq!(second.remaining().unwrap().authority_records_appended, 0);
    let third = session.invocation_window().unwrap();
    third.record_admitted(appends(1)).unwrap();
    assert_eq!(first.remaining().unwrap().authority_records_appended, 0);
    drop(held);
    assert_eq!(
        first.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    assert_eq!(second.remaining().unwrap().authority_records_appended, 0);
    assert_eq!(
        third.remaining().unwrap().authority_records_appended,
        APPENDS - 1
    );
}

#[test]
fn a_window_holding_its_full_reservation_cannot_charge() {
    let session = root();
    let window = session.invocation_window().unwrap();
    let _held = window.reserve_work(appends(APPENDS)).unwrap();
    exceeded(
        window.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn live_allocation_is_never_forgotten_by_a_new_window() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let held = first.claim(PEAK / 2, 1, WorkCounters::UNCHARGED).unwrap();
    drop(first);
    let second = session.invocation_window().unwrap();
    let live = second.live_allocation_bytes();
    assert!(live >= PEAK / 2);
    assert!(
        second
            .claim(PEAK - live + 1, 1, WorkCounters::UNCHARGED)
            .is_err()
    );
    drop(held);
    second.claim(PEAK / 2, 1, WorkCounters::UNCHARGED).unwrap();
}

#[test]
fn window_layout_matches_its_claim() {
    let session = root();
    let before = session.live_allocation_bytes();
    let window = session.invocation_window().unwrap();
    let expected =
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeWorkWindow>()).unwrap()
            as u64;
    assert_eq!(session.live_allocation_bytes(), before + expected);
    drop(window);
    assert_eq!(session.live_allocation_bytes(), before);
}
