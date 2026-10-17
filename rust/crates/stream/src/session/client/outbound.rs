use super::{Mutex, StreamError, gate};
use std::{
    collections::VecDeque,
    future::poll_fn,
    task::{Context, Poll, Waker},
};

struct State<T> {
    frames: VecDeque<(T, usize)>,
    bytes: usize,
    closed: bool,
    reader: Option<Waker>,
}

pub(super) struct Outbound<T> {
    // Lock order: never nested with another SDK lock, including closed's gate.
    // Wake callbacks and queued-value destruction happen after unlocking.
    state: Mutex<State<T>>,
    frames: usize,
    bytes: usize,
    closed: gate::Gate,
}

impl<T> Outbound<T> {
    pub(super) fn new(frames: usize, bytes: usize) -> Self {
        Self {
            state: Mutex::new(State {
                frames: VecDeque::new(),
                bytes: 0,
                closed: false,
                reader: None,
            }),
            frames,
            bytes,
            closed: gate::Gate::new(),
        }
    }
    pub(super) fn push(&self, value: T, bytes: usize) -> Result<(), StreamError> {
        let wake = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            if state.closed {
                return Err(StreamError::Unavailable);
            }
            if state.frames.len() >= self.frames || bytes > self.bytes - state.bytes {
                return Err(StreamError::Capacity);
            }
            state.bytes += bytes;
            state.frames.push_back((value, bytes));
            state.reader.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, StreamError>> {
        let wake = cx.waker().clone();
        let previous = {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Err(StreamError::Unavailable));
            };
            if state.closed {
                return Poll::Ready(Err(StreamError::Unavailable));
            }
            if let Some((value, bytes)) = state.frames.pop_front() {
                state.bytes -= bytes;
                return Poll::Ready(Ok(value));
            }
            state.reader.replace(wake)
        };
        drop(previous);
        Poll::Pending
    }
    pub(super) async fn next(&self) -> Result<T, StreamError> {
        poll_fn(|cx| self.poll(cx)).await
    }
    pub(super) fn close(&self) -> Result<(), StreamError> {
        let (frames, wake) = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            state.closed = true;
            state.bytes = 0;
            (std::mem::take(&mut state.frames), state.reader.take())
        };
        self.closed.set(true)?;
        if let Some(wake) = wake {
            wake.wake();
        }
        drop(frames);
        Ok(())
    }
    pub(super) async fn cancelled(&self) -> Result<(), StreamError> {
        self.closed.changed(false).await
    }
}
