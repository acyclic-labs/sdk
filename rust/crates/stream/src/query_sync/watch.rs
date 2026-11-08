//! Loom watch model: version check and waiter registration precede parking.

use loom::sync::{RwLock, RwLockReadGuard};
use std::{
    future::poll_fn,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
};

// Lock order: caller's projection/visibility/state -> value. Wakers register
// under value and wake after release; no value guard survives an await.
struct State<T> {
    value: RwLock<Value<T>>,
}

struct Value<T> {
    data: T,
    version: u64,
    senders: usize,
    waiters: Vec<Waiter>,
}

struct Waiter {
    // All slot access holds State::value; the atomic only makes the shared
    // position Sync. Removing another waiter can move this one in O(1).
    slot: Arc<AtomicUsize>,
    wake: Waker,
}

struct Registration<'a, T> {
    state: &'a State<T>,
    entry: Option<Arc<AtomicUsize>>,
}

impl<T> Registration<'_, T> {
    fn register(&mut self, value: &mut Value<T>, wake: Waker) -> Option<Waker> {
        if let Some(slot) = &self.entry
            && let Some(waiter) = value.waiters.get_mut(slot.load(Ordering::Relaxed))
            && Arc::ptr_eq(&waiter.slot, slot)
        {
            return Some(if waiter.wake.will_wake(&wake) {
                wake
            } else {
                std::mem::replace(&mut waiter.wake, wake)
            });
        }
        let slot = Arc::new(AtomicUsize::new(value.waiters.len()));
        value.waiters.push(Waiter {
            slot: Arc::clone(&slot),
            wake,
        });
        self.entry = Some(slot);
        None
    }
}

impl<T> Drop for Registration<'_, T> {
    fn drop(&mut self) {
        let Some(slot) = &self.entry else {
            return;
        };
        let removed = {
            let mut value = self
                .state
                .value
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let index = slot.load(Ordering::Relaxed);
            // A publication or final publisher loss may already have taken
            // this entry. Identity prevents a reused position from aliasing it.
            if !value
                .waiters
                .get(index)
                .is_some_and(|waiter| Arc::ptr_eq(&waiter.slot, slot))
            {
                return;
            }
            let removed = value.waiters.swap_remove(index);
            if let Some(moved) = value.waiters.get(index) {
                moved.slot.store(index, Ordering::Relaxed);
            }
            removed
        };
        drop(removed);
    }
}

/// Watch publisher, matching the production subset of Tokio watch.
pub struct Sender<T>(Arc<State<T>>);

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0
            .value
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .senders += 1;
        Self(Arc::clone(&self.0))
    }
}

/// One subscriber's independently consumed version.
pub struct Receiver<T> {
    state: Arc<State<T>>,
    seen: u64,
}

/// Borrowed value; its Loom read guard tracks the entire borrow.
pub struct Ref<'a, T>(RwLockReadGuard<'a, Value<T>>);

impl<T> Deref for Ref<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.data
    }
}

/// Creates a watch whose initial value is already seen by its first subscriber.
pub fn channel<T>(data: T) -> (Sender<T>, Receiver<T>) {
    let sender = Sender(Arc::new(State {
        value: RwLock::new(Value {
            data,
            version: 0,
            senders: 1,
            waiters: Vec::new(),
        }),
    }));
    let receiver = sender.subscribe();
    (sender, receiver)
}

impl<T> Sender<T> {
    /// Counts live subscribers, excluding all publisher handles.
    pub fn receiver_count(&self) -> usize {
        let senders = self
            .0
            .value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .senders;
        Arc::strong_count(&self.0).saturating_sub(senders)
    }

    /// Subscribes at the current version.
    pub fn subscribe(&self) -> Receiver<T> {
        let seen = self
            .0
            .value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .version;
        Receiver {
            state: self.0.clone(),
            seen,
        }
    }

    /// Borrows the current value without changing any subscriber's version.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(self
            .0
            .value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Replaces the value and pushes a wake to all registered subscribers.
    pub fn send_replace(&self, data: T) -> T {
        let mut previous = data;
        self.send_modify(|value| std::mem::swap(value, &mut previous));
        previous
    }

    /// Mutates and publishes, even when the value compares equal.
    pub fn send_modify(&self, update: impl FnOnce(&mut T)) {
        self.send_if_modified(|value| {
            update(value);
            true
        });
    }

    /// Mutates under the value lock and publishes only when requested.
    pub fn send_if_modified(&self, update: impl FnOnce(&mut T) -> bool) -> bool {
        let waiters = {
            let mut value = self
                .0
                .value
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !update(&mut value.data) {
                return false;
            }
            value.version = value.version.wrapping_add(1);
            std::mem::take(&mut value.waiters)
        };
        for waiter in waiters {
            waiter.wake.wake();
        }
        true
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waiters = {
            let mut value = self
                .0
                .value
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            value.senders -= 1;
            if value.senders != 0 {
                return;
            }
            std::mem::take(&mut value.waiters)
        };
        for waiter in waiters {
            waiter.wake.wake();
        }
    }
}

impl<T> Receiver<T> {
    /// Consumes the current version while borrowing its value.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let value = self
            .state
            .value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.seen = value.version;
        Ref(value)
    }

    /// Waits for an unseen publication or reports that its publisher vanished.
    pub async fn changed(&mut self) -> Result<(), std::io::Error> {
        let mut registration = Registration {
            state: &self.state,
            entry: None,
        };
        poll_fn(|cx| {
            let wake = cx.waker().clone();
            let mut value = self
                .state
                .value
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.seen != value.version {
                self.seen = value.version;
                return Poll::Ready(Ok(()));
            }
            if value.senders == 0 {
                return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
            }
            let previous = registration.register(&mut value, wake);
            drop(value);
            drop(previous);
            Poll::Pending
        })
        .await
    }
}
