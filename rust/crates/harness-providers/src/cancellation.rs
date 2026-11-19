use std::task::{Context, Poll, Waker};

use crate::sync::{Mutex, MutexGuard};

#[derive(Default)]
struct State {
    cancelled: bool,
    waiter: Option<Waker>,
}

/// One waiter, with cancellation and registration in the same critical section.
/// Lock order: this is the only lock; release it before polling or waking callers.
#[derive(Default)]
pub(crate) struct Cancellation(Mutex<State>);

impl Cancellation {
    fn state(&self) -> MutexGuard<'_, State> {
        // The critical sections only update a flag or move a waker. Waker
        // callbacks run after unlocking, so the guard always holds valid state.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn cancel(&self) {
        let waiter = {
            let mut state = self.state();
            state.cancelled = true;
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    pub(crate) fn poll(&self, cx: &Context<'_>) -> Poll<()> {
        // RawWaker clone/drop callbacks may reenter the caller, just like wake.
        let mut next = Some(cx.waker().clone());
        let result = {
            let mut state = self.state();
            if state.cancelled {
                Poll::Ready(())
            } else {
                if !state
                    .waiter
                    .as_ref()
                    .is_some_and(|w| w.will_wake(cx.waker()))
                {
                    std::mem::swap(&mut state.waiter, &mut next);
                }
                Poll::Pending
            }
        };
        drop(next);
        result
    }
}
