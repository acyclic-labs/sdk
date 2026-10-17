use super::sync::Mutex;
use crate::StreamError;
use std::{
    collections::VecDeque,
    future::poll_fn,
    task::{Context, Poll, Waker},
};

pub(super) struct Signal<T> {
    // Lock order: never nested with cursor, admission, or another signal lock.
    // Wakers run only after unlocking; no guard crosses an await.
    state: Mutex<(VecDeque<T>, Option<Waker>)>,
}
impl<T> Signal<T> {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new((VecDeque::new(), None)),
        }
    }
    pub(super) fn send(&self, value: T) -> Result<(), StreamError> {
        let wake = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            state.0.push_back(value);
            state.1.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    pub(super) fn try_next(&self) -> Result<Option<T>, StreamError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .0
            .pop_front())
    }
    pub(super) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, StreamError>> {
        let wake = cx.waker().clone();
        let previous = {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Err(StreamError::Unavailable));
            };
            if let Some(value) = state.0.pop_front() {
                return Poll::Ready(Ok(value));
            }
            state.1.replace(wake)
        };
        drop(previous);
        Poll::Pending
    }
    pub(super) async fn next(&self) -> Result<T, StreamError> {
        poll_fn(|cx| self.poll(cx)).await
    }
}
