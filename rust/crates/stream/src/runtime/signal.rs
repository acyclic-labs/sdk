#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
use super::sync::Arc;
use super::sync::Mutex;
use crate::StreamError;
use std::{
    collections::VecDeque,
    task::{Context, Waker},
};

pub(crate) struct Signal<T> {
    // Lock order: never nested with cursor, admission, or another signal lock.
    // Wakers run only after unlocking; no guard crosses an await.
    state: Mutex<(VecDeque<T>, Option<Waker>)>,
}

/// The original release observation follows the backing's final owner.
#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) struct Guard<T> {
    sender: Arc<Signal<T>>,
    value: Option<T>,
}
#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
impl<T> Guard<T> {
    pub(crate) fn new(sender: Arc<Signal<T>>, value: T) -> Self {
        Self {
            sender,
            value: Some(value),
        }
    }
}
#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
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
        self.send_with(|| value)
    }
    /// Creates the event under the same lock used to receive it. The closure
    /// may update atomics, but must not acquire locks or wake tasks.
    pub(crate) fn send_with(&self, value: impl FnOnce() -> T) -> Result<(), StreamError> {
        let wake = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            state.0.push_back(value());
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
            .map_err(|_| StreamError::Unavailable)?
            .0
            .pop_front())
    }
    /// Receives at most one event and arms the next publication atomically.
    /// The second result names buffered work that needs another owner turn.
    pub(crate) fn take_and_listen(
        &self,
        cx: &mut Context<'_>,
    ) -> Result<(Option<T>, bool), StreamError> {
        let wake = cx.waker().clone();
        let (value, buffered, previous) = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            let value = state.0.pop_front();
            (value, !state.0.is_empty(), state.1.replace(wake))
        };
        drop(previous);
        Ok((value, buffered))
    }
    #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
    pub(crate) fn pending(&self) -> usize {
        self.state.lock().unwrap().0.len()
    }
}

#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
impl<T> Signal<T> {
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> std::task::Poll<Result<T, StreamError>> {
        use std::task::Poll;
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
    pub(crate) async fn next(&self) -> Result<T, StreamError> {
        std::future::poll_fn(|cx| self.poll(cx)).await
    }
}
