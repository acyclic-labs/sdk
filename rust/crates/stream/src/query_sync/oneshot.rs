//! Modeled one-shot completion: result and wake registration share one lock.
use loom::sync::Mutex;
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::{Poll, Waker},
};

// Leaf lock, never held while executing work or invoking a waker.
struct State<T> {
    value: Option<T>,
    closed: bool,
    waker: Option<Waker>,
}

pub struct Sender<T>(Option<Arc<Mutex<State<T>>>>);

pub fn channel<T: Send + 'static>() -> (
    Sender<T>,
    impl Future<Output = Result<T, &'static str>> + Send,
) {
    let state = Arc::new(Mutex::new(State {
        value: None,
        closed: false,
        waker: None,
    }));
    let sender = Sender(Some(state.clone()));
    let receiver = poll_fn(move |cx| {
        let mut state = state.lock().unwrap();
        if let Some(value) = state.value.take() {
            Poll::Ready(Ok(value))
        } else if state.closed {
            Poll::Ready(Err("sender dropped"))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    });
    (sender, receiver)
}

impl<T> Sender<T> {
    pub fn send(mut self, value: T) {
        let state = self.0.take().unwrap();
        let waker = {
            let mut state = state.lock().unwrap();
            state.value = Some(value);
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(state) = &self.0 {
            let waker = {
                let mut state = state.lock().unwrap();
                state.closed = true;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
