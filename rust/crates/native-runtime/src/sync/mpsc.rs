//! Bounded-channel shim for Loom; production uses std's bounded channel.
use loom::sync::{Condvar, Mutex};
pub(crate) use std::sync::mpsc::{RecvError, TrySendError};
use std::{collections::VecDeque, sync::Arc};

struct State<T> {
    items: VecDeque<T>,
    capacity: usize,
    sender: bool,
    receiver: bool,
}
struct Channel<T> {
    // The channel lock never nests an admission or completion lock. Admission
    // may hold its lock while calling try_send; recv releases before notifying.
    state: Mutex<State<T>>,
    ready: Condvar,
}
pub(crate) struct SyncSender<T>(Arc<Channel<T>>);
pub(crate) struct Receiver<T>(Arc<Channel<T>>);

pub(crate) fn sync_channel<T>(capacity: usize) -> (SyncSender<T>, Receiver<T>) {
    assert_ne!(capacity, 0, "native queues have positive capacity");
    let channel = Arc::new(Channel {
        state: Mutex::new(State {
            items: VecDeque::new(),
            capacity,
            sender: true,
            receiver: true,
        }),
        ready: Condvar::new(),
    });
    (SyncSender(channel.clone()), Receiver(channel))
}

impl<T> SyncSender<T> {
    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut state = self.0.state.lock().unwrap();
        if !state.receiver {
            return Err(TrySendError::Disconnected(value));
        }
        if state.items.len() == state.capacity {
            return Err(TrySendError::Full(value));
        }
        state.items.push_back(value);
        self.0.ready.notify_one();
        Ok(())
    }
}
impl<T> Receiver<T> {
    pub(crate) fn recv(&self) -> Result<T, RecvError> {
        let mut state = self.0.state.lock().unwrap();
        loop {
            if let Some(value) = state.items.pop_front() {
                return Ok(value);
            }
            if !state.sender {
                return Err(RecvError);
            }
            state = self.0.ready.wait(state).unwrap();
        }
    }
}
impl<T> Drop for SyncSender<T> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().sender = false;
        self.0.ready.notify_one();
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().receiver = false;
    }
}
