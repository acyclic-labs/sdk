use super::sync::{Arc, Mutex};
use crate::StreamError;
use std::{
    collections::VecDeque,
    future::poll_fn,
    task::{Context, Poll, Waker},
};

pub(crate) struct Signal<T> {
    // Lock order: never nested with cursor, admission, or another signal lock.
    // Wakers run only after unlocking; no guard crosses an await.
    state: Mutex<(VecDeque<T>, Option<Waker>)>,
}

/// The original release observation follows the backing's final owner.
pub(crate) struct Guard<T> {
    sender: Arc<Signal<T>>,
    value: Option<T>,
}
impl<T> Guard<T> {
    pub(crate) fn new(sender: Arc<Signal<T>>, value: T) -> Self {
        Self {
            sender,
            value: Some(value),
        }
    }
}
impl<T> Drop for Guard<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            let _ = self.sender.send(value);
        }
    }
}
impl<T> Signal<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new((VecDeque::new(), None)),
        }
    }
    pub(crate) fn send(&self, value: T) -> Result<(), StreamError> {
        let wake = {
            let mut state = self.state.lock().map_err(|_| StreamError::unavailable())?;
            state.0.push_back(value);
            state.1.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    pub(crate) fn try_next(&self) -> Result<Option<T>, StreamError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamError::unavailable())?
            .0
            .pop_front())
    }
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, StreamError>> {
        let wake = cx.waker().clone();
        let previous = {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Err(StreamError::unavailable()));
            };
            if let Some(value) = state.0.pop_front() {
                return Poll::Ready(Ok(value));
            }
            state.1.replace(wake)
        };
        drop(previous);
        Poll::Pending
    }
    pub(crate) async fn next(&self) -> Result<T, StreamError> {
        poll_fn(|cx| self.poll(cx)).await
    }
}
