//! Bounded-channel shim for Loom; production uses std's bounded channel.
use loom::sync::{Condvar, Mutex};
pub use std::sync::mpsc::{RecvError, TrySendError};
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
/// The sending half of a bounded channel.
pub struct SyncSender<T>(Arc<Channel<T>>);
/// The receiving half of a bounded channel.
pub struct Receiver<T>(Arc<Channel<T>>);

/// A channel holding at most `capacity` values. A zero capacity admits no
/// value: every `try_send` is `Full`, as std's rendezvous channel is with no
/// receiver blocked in `recv`.
pub fn sync_channel<T>(capacity: usize) -> (SyncSender<T>, Receiver<T>) {
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
    /// Queues `value` if the channel has room and its receiver is alive.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let Ok(mut state) = self.0.state.lock() else {
            return Err(TrySendError::Disconnected(value));
        };
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
    /// Waits for the next value, or for every sender to drop.
    pub fn recv(&self) -> Result<T, RecvError> {
        let mut state = self.0.state.lock().map_err(|_| RecvError)?;
        loop {
            if let Some(value) = state.items.pop_front() {
                return Ok(value);
            }
            if !state.sender {
                return Err(RecvError);
            }
            state = self.0.ready.wait(state).map_err(|_| RecvError)?;
        }
    }
}
impl<T> Drop for SyncSender<T> {
    fn drop(&mut self) {
        // Drop cannot return a channel error, but must still wake a receiver.
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sender = false;
        self.0.ready.notify_one();
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .receiver = false;
    }
}
