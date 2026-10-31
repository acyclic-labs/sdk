use super::ProcessWaitOutcome;
#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(not(loom))]
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct State {
    outcome: Option<ProcessWaitOutcome>,
    waker: Option<Waker>,
}

/// Completion and waker registration share one lock: settlement cannot pass
/// between the pending check and registration. The first terminal event wins.
#[derive(Default)]
pub(super) struct Signal(Mutex<State>);

impl Signal {
    pub(super) fn settle(&self, outcome: ProcessWaitOutcome) {
        let waker = {
            let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if state.outcome.is_some() {
                return;
            }
            state.outcome = Some(outcome);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) fn poll(&self, context: &mut Context<'_>) -> Poll<ProcessWaitOutcome> {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(outcome) = &state.outcome {
            Poll::Ready(outcome.clone())
        } else {
            if state
                .waker
                .as_ref()
                .is_none_or(|waker| !waker.will_wake(context.waker()))
            {
                state.waker = Some(context.waker().clone());
            }
            Poll::Pending
        }
    }
}
