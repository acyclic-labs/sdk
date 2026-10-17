//! Loom watch model: version check and waiter registration precede parking.

use loom::sync::{RwLock, RwLockReadGuard};
use std::{
    future::poll_fn,
    ops::Deref,
    sync::Arc,
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
    waiters: Vec<Waker>,
}

/// Watch publisher, matching the production subset of Tokio watch.
pub struct Sender<T>(Arc<State<T>>);

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
    /// Counts live subscribers; the only other owner is this publisher.
    pub fn receiver_count(&self) -> usize {
        Arc::strong_count(&self.0).saturating_sub(1)
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
        let waiters = {
            let mut value = self
                .0
                .value
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            update(&mut value.data);
            value.version = value.version.wrapping_add(1);
            std::mem::take(&mut value.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
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
            std::mem::take(&mut value.waiters)
        };
        for waiter in waiters {
            waiter.wake();
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
        poll_fn(|cx| {
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
            if !value.waiters.iter().any(|old| old.will_wake(cx.waker())) {
                value.waiters.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }
}
