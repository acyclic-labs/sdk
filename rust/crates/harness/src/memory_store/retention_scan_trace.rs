use acyclic_fs::{CancellationToken, FsError, WorkCounters};
use std::cell::RefCell;

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Preflight,
    Charge,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Event {
    Preflight(WorkCounters),
    ChargeAttempt(WorkCounters),
    CancellationObserved(bool),
    Charged(WorkCounters),
}

struct Observer {
    events: Vec<Event>,
    cancellation: CancellationToken,
    cancel_at: Option<usize>,
    observations: usize,
}

thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
}

pub(crate) fn before(phase: Phase, delta: &WorkCounters) {
    let cancel = OBSERVER.with(|state| {
        let mut state = state.borrow_mut();
        let observer = state.as_mut()?;
        observer.events.push(match phase {
            Phase::Preflight => Event::Preflight(*delta),
            Phase::Charge => Event::ChargeAttempt(*delta),
        });
        let cancel = (observer.cancel_at == Some(observer.observations))
            .then(|| observer.cancellation.clone());
        observer.observations += 1;
        cancel
    });
    // The injected token has no registered callbacks, and cancellation happens
    // outside the observer borrow. No observation reads the flag a second time.
    if let Some(cancel) = cancel {
        cancel.cancel();
    }
}

pub(crate) fn after(phase: Phase, delta: &WorkCounters, result: &Result<(), FsError>) {
    OBSERVER.with(|state| {
        if let Some(observer) = state.borrow_mut().as_mut() {
            // Both borrowed entrypoints check cancellation before any work or
            // budget validation. Their result therefore records that actual
            // cancellation verdict without adding another atomic flag read.
            observer.events.push(Event::CancellationObserved(matches!(
                result,
                Err(FsError::Cancelled(_))
            )));
            if matches!(phase, Phase::Charge) && result.is_ok() {
                observer.events.push(Event::Charged(*delta));
            }
        }
    });
}

pub(crate) fn capture<R>(
    cancellation: &CancellationToken,
    cancel_at: Option<usize>,
    run: impl FnOnce() -> R,
) -> (R, Vec<Event>) {
    OBSERVER.with(|state| {
        let mut state = state.borrow_mut();
        assert!(state.is_none(), "nested operation trace");
        *state = Some(Observer {
            events: Vec::new(),
            cancellation: cancellation.clone(),
            cancel_at,
            observations: 0,
        });
    });
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.with(|state| {
                state.borrow_mut().take();
            });
        }
    }
    let reset = Reset;
    let result = run();
    let events = OBSERVER.with(|state| state.borrow_mut().take().unwrap().events);
    drop(reset);
    (result, events)
}

pub(crate) fn assert_matches(expected: &[Event], actual: &[Event]) {
    assert_eq!(actual, expected, "preflight/charge trace changed");
}
