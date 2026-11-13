//! Watch-change queue whose storage follows its occupancy.
//!
//! Every accepted change is retained until received. Storage grows geometrically
//! under its original native allocation guard; a denied allocation leaves the
//! queue unchanged. Receiver and sender retirement retain standard channel semantics.
use crate::sync::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::sync::mpsc::{TryRecvError, TrySendError};
use std::sync::{Arc, PoisonError};

/// Retained storage beyond this many slots is released when the queue drains.
const RETAINED_SLOTS: usize = 64;

struct State<T> {
    queue: VecDeque<T>,
    allocation: Option<crate::NativeAllocation>,
    senders: usize,
    receiver: bool,
}

impl<T> State<T> {
    fn reserve_slot(&mut self) -> bool {
        if self.queue.len() < self.queue.capacity() {
            return true;
        }
        let Some(capacity) = self.queue.capacity().max(2).checked_mul(2) else {
            return false;
        };
        if let Some(allocation) = &mut self.allocation {
            crate::kernel::reserve_native_buffer(&mut self.queue, capacity, allocation).is_ok()
        } else {
            self.queue
                .try_reserve_exact(capacity - self.queue.len())
                .is_ok()
        }
    }

    fn release_backing(&mut self) {
        // Destroy the actual storage before releasing its custody.
        self.queue = VecDeque::new();
        if let Some(allocation) = &mut self.allocation {
            allocation.release_deferred(allocation.bytes());
        }
    }
}

struct Shared<T> {
    state: Mutex<State<T>>,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Sending half; clones share the original queue and allocation custody.
pub(crate) struct QueueSender<T>(Arc<Shared<T>>);

/// Receiving half.
pub(crate) struct QueueReceiver<T>(Arc<Shared<T>>);

/// Creates an initially empty queue with optional original allocation custody.
pub(crate) fn channel<T>(
    allocation: Option<crate::NativeAllocation>,
) -> (QueueSender<T>, QueueReceiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            allocation,
            senders: 1,
            receiver: true,
        }),
    });
    (QueueSender(Arc::clone(&shared)), QueueReceiver(shared))
}

impl<T> QueueSender<T> {
    /// Queues `value` unless its receiver is gone or allocation admission fails.
    /// A denied allocation reports `Full` without modifying accepted items.
    pub(crate) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut state = self.0.lock();
        if !state.receiver {
            return Err(TrySendError::Disconnected(value));
        }
        if !state.reserve_slot() {
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
    #[cfg(all(test, unix))]
    pub(crate) fn sender_for_test(&self) -> QueueSender<T> {
        self.0.lock().senders += 1;
        QueueSender(Arc::clone(&self.0))
    }

    /// Removes the oldest queued item.
    pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut state = self.0.lock();
        match state.queue.pop_front() {
            Some(value) => {
                if state.queue.is_empty() && state.queue.capacity() > RETAINED_SLOTS {
                    state.release_backing();
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
        state.release_backing();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Idle queues retain no slots; large drained backing retires its custody.
    #[test]
    fn idle_queue_retains_no_slots() {
        let (_sender, receiver) = channel::<[u8; 256]>(None);
        assert_eq!(receiver.retained_slots(), 0);
        let (sender, receiver) = channel(None);
        for value in 0..65_537u32 {
            sender.try_send(value).unwrap();
        }
        assert!(receiver.retained_slots() >= 65_537);
        for value in 0..65_537u32 {
            assert_eq!(receiver.try_recv(), Ok(value));
        }
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(receiver.retained_slots(), 0);
    }

    #[test]
    fn native_queue_custody_tracks_backing_and_retries_denied_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                peak_allocation_bytes: 4_096,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let baseline = work.live_allocation_bytes();
        let allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED).unwrap();
        let (sender, receiver) = channel(Some(allocation));
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "idle capacity is not physical storage"
        );
        for value in 0..128u64 {
            sender.try_send(value).unwrap();
        }
        assert_eq!(work.live_allocation_bytes(), baseline + 1_024);
        let refused = sender.try_send(128);
        assert!(
            matches!(refused, Err(TrySendError::Full(128))),
            "queue growth admitted replacement while original backing was still live: {refused:?}"
        );
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + 1_024,
            "denied growth retained no bytes"
        );
        for value in 0..128u64 {
            assert_eq!(receiver.try_recv(), Ok(value));
        }
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "drained large backing was released"
        );
        sender.try_send(129).unwrap();
        assert_eq!(receiver.try_recv(), Ok(129));
        assert!(
            work.live_allocation_bytes() > baseline,
            "small retained backing stays charged"
        );
        drop(receiver);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert!(matches!(
            sender.try_send(130),
            Err(TrySendError::Disconnected(130))
        ));

        // Wrapped live entries must move in order, with both backings funded.
        for denied in [true, false] {
            let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget {
                    peak_allocation_bytes: baseline + if denied { 8 } else { 12 },
                    ..crate::WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
            let (sender, receiver) = channel::<u8>(Some(allocation));
            for value in 0..4 {
                sender.try_send(value)?;
            }
            for value in 0..2 {
                assert_eq!(receiver.try_recv(), Ok(value));
            }
            for value in 4..6 {
                sender.try_send(value)?;
            }
            assert_eq!(receiver.retained_slots(), 4);
            assert_eq!(work.live_allocation_bytes(), baseline + 4);
            let before = work.receipt();
            let mut result = Ok(());
            let physical = acyclic_test_support::measure(|| {
                result = sender.try_send(6);
            });
            if denied {
                assert!(
                    matches!(result, Err(TrySendError::Full(6))),
                    "queue growth admitted replacement while original backing was still live: {result:?}"
                );
                assert_eq!(physical.count_total, 0);
                assert_eq!(receiver.retained_slots(), 4);
                assert_eq!(work.receipt(), before);
            } else {
                result?;
                assert_eq!(physical.count_total, 1);
                assert_eq!(physical.bytes_total, 8);
                assert_eq!(receiver.retained_slots(), 8);
                assert_eq!(work.live_allocation_bytes(), baseline + 8);
                assert_eq!(work.receipt().peak_allocation_bytes, baseline + 12);
                assert_eq!(work.receipt().bytes_copied, before.bytes_copied + 4);
            }
            for value in 2..if denied { 6 } else { 7 } {
                assert_eq!(receiver.try_recv(), Ok(value));
            }
            sender.try_send(7)?;
            assert_eq!(receiver.try_recv(), Ok(7));
            drop(receiver);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
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
        /// Every observable result equals the standard unbounded channel's.
        #[test]
        fn matches_the_standard_channel(
            steps in proptest::collection::vec(step(), 0..64),
        ) {
            let (sender, receiver) = channel(None);
            let (std_sender, std_receiver) = std::sync::mpsc::channel();
            let (mut senders, mut std_senders) = (vec![sender], vec![std_sender]);
            let (mut receiver, mut std_receiver) = (Some(receiver), Some(std_receiver));
            for step in steps {
                match step {
                    Step::Send(value) => {
                        if let (Some(sender), Some(std_sender)) = (senders.last(), std_senders.last()) {
                            prop_assert_eq!(
                                format!("{:?}", sender.try_send(value)),
                                format!("{:?}", std_sender.send(value).map_err(|error| TrySendError::Disconnected(error.0)))
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
