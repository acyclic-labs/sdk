//! Bounded watch-change queue whose storage follows its occupancy.
//!
//! `std::sync::mpsc::sync_channel(capacity)` allocates every slot when it is
//! created. A watch admits up to tens of thousands of queued changes, but an
//! idle watch holds none, so the eager array made each open watch retain
//! megabytes. This queue keeps the same `try_send`/`try_recv` contract: a full
//! queue refuses the change, a dropped receiver disconnects senders, and the
//! receiver observes disconnection only once the queue is empty and every
//! sender is gone.
use std::collections::VecDeque;
use std::sync::mpsc::{TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Retained storage beyond this many slots is released when the queue drains.
const RETAINED_SLOTS: usize = 64;

struct State<T> {
    queue: VecDeque<T>,
    senders: usize,
    receiver: bool,
}

struct Shared<T> {
    capacity: usize,
    state: Mutex<State<T>>,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Sending half; clones share the bound.
pub(crate) struct QueueSender<T>(Arc<Shared<T>>);

/// Receiving half.
pub(crate) struct QueueReceiver<T>(Arc<Shared<T>>);

/// Creates a queue admitting at most `capacity` undelivered items.
pub(crate) fn bounded<T>(capacity: usize) -> (QueueSender<T>, QueueReceiver<T>) {
    let shared = Arc::new(Shared {
        capacity,
        state: Mutex::new(State {
            queue: VecDeque::new(),
            senders: 1,
            receiver: true,
        }),
    });
    (QueueSender(Arc::clone(&shared)), QueueReceiver(shared))
}

impl<T> QueueSender<T> {
    /// Queues `value` unless the queue is full, its receiver is gone, or the
    /// allocator refuses the slot; a refused slot reports `Full`.
    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut state = self.0.lock();
        if !state.receiver {
            return Err(TrySendError::Disconnected(value));
        }
        if state.queue.len() >= self.0.capacity || state.queue.try_reserve(1).is_err() {
            return Err(TrySendError::Full(value));
        }
        state.queue.push_back(value);
        Ok(())
    }
}

impl<T> Clone for QueueSender<T> {
    fn clone(&self) -> Self {
        self.0.lock().senders += 1;
        Self(Arc::clone(&self.0))
    }
}

impl<T> Drop for QueueSender<T> {
    fn drop(&mut self) {
        self.0.lock().senders -= 1;
    }
}

impl<T> QueueReceiver<T> {
    /// Removes the oldest queued item.
    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut state = self.0.lock();
        match state.queue.pop_front() {
            Some(value) => {
                if state.queue.is_empty() && state.queue.capacity() > RETAINED_SLOTS {
                    state.queue = VecDeque::new();
                }
                Ok(value)
            }
            None if state.senders == 0 => Err(TryRecvError::Disconnected),
            None => Err(TryRecvError::Empty),
        }
    }

    #[cfg(test)]
    fn retained_slots(&self) -> usize {
        self.0.lock().queue.capacity()
    }
}

impl<T> Drop for QueueReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.receiver = false;
        // Queued items drop with the receiver, as they would with a channel.
        state.queue = VecDeque::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A fresh queue sized like a watch's default bound retains no slots.
    /// Negative control: the eager `sync_channel` retains its full array.
    #[test]
    fn idle_queue_retains_no_slots() {
        let (_sender, receiver) = bounded::<[u8; 256]>(16_384);
        assert_eq!(receiver.retained_slots(), 0);
        let (sender, receiver) = bounded(16_384);
        for value in 0..1_000u32 {
            sender.try_send(value).unwrap();
        }
        assert!(receiver.retained_slots() >= 1_000);
        while receiver.try_recv().is_ok() {}
        assert_eq!(receiver.retained_slots(), 0);
    }

    #[derive(Clone, Debug)]
    enum Step {
        Send(u8),
        Receive,
        CloneSender,
        DropSender,
        DropReceiver,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => any::<u8>().prop_map(Step::Send),
            4 => Just(Step::Receive),
            1 => Just(Step::CloneSender),
            1 => Just(Step::DropSender),
            1 => Just(Step::DropReceiver),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        /// Every observable result equals the standard bounded channel's.
        #[test]
        fn matches_the_standard_bounded_channel(
            capacity in 1usize..6,
            steps in proptest::collection::vec(step(), 0..64),
        ) {
            let (sender, receiver) = bounded(capacity);
            let (std_sender, std_receiver) = std::sync::mpsc::sync_channel(capacity);
            let (mut senders, mut std_senders) = (vec![sender], vec![std_sender]);
            let (mut receiver, mut std_receiver) = (Some(receiver), Some(std_receiver));
            for step in steps {
                match step {
                    Step::Send(value) => {
                        if let (Some(sender), Some(std_sender)) = (senders.last(), std_senders.last()) {
                            prop_assert_eq!(
                                format!("{:?}", sender.try_send(value)),
                                format!("{:?}", std_sender.try_send(value))
                            );
                        }
                    }
                    Step::Receive => {
                        if let (Some(receiver), Some(std_receiver)) = (&receiver, &std_receiver) {
                            prop_assert_eq!(receiver.try_recv(), std_receiver.try_recv());
                        }
                    }
                    Step::CloneSender => {
                        if let (Some(sender), Some(std_sender)) = (senders.last(), std_senders.last()) {
                            let (sender, std_sender) = (sender.clone(), std_sender.clone());
                            senders.push(sender);
                            std_senders.push(std_sender);
                        }
                    }
                    Step::DropSender => {
                        senders.pop();
                        std_senders.pop();
                    }
                    Step::DropReceiver => {
                        receiver = None;
                        std_receiver = None;
                    }
                }
            }
        }
    }
}
