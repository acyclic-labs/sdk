use super::sync::Mutex;
use crate::StreamError;
use std::{
    future::poll_fn,
    task::{Context, Poll, Waker},
};

pub(super) struct Gate {
    // Lock order: never nested with provider, cursor, or signal locks.
    // Polling providers and waking tasks happen after this lock is released.
    state: Mutex<(bool, Option<Waker>)>,
}
impl Gate {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new((false, None)),
        }
    }
    pub(super) fn set(&self, enabled: bool) -> Result<(), StreamError> {
        let wake = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            if state.0 == enabled {
                return Ok(());
            }
            state.0 = enabled;
            state.1.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    fn poll_changed(&self, expected: bool, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        let wake = cx.waker().clone();
        let previous = {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Err(StreamError::Unavailable));
            };
            if state.0 != expected {
                return Poll::Ready(Ok(()));
            }
            state.1.replace(wake)
        };
        drop(previous);
        Poll::Pending
    }
    pub(super) async fn changed(&self, expected: bool) -> Result<(), StreamError> {
        poll_fn(|cx| self.poll_changed(expected, cx)).await
    }
}
