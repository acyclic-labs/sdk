//! Async locks whose cooperative-budget yield happens before acquisition.
//!
//! Tokio's semaphore takes a lock's permits and only then checks the task's
//! cooperative budget, returning `Pending` while it keeps them. A future that
//! is polled once on an exhausted budget and then left unpolled holds the lock
//! until it is dropped, so every later writer waits on a future nobody polls.
//! Spending the budget first means a budget yield holds nothing; the
//! acquisition itself then runs unconstrained, waiting only on the lock.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(loom)]
use super::locks as inner;
#[cfg(not(loom))]
use tokio::sync as inner;

/// The lock is held by another owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("lock is busy")]
pub struct Busy;

/// Async mutex; a cooperative-budget yield never holds the lock.
pub struct Mutex<T>(inner::Mutex<T>);

/// Async read/write lock; a cooperative-budget yield never holds the lock.
pub struct RwLock<T>(inner::RwLock<T>);

impl<T> Mutex<T> {
    /// Constructs an unlocked value.
    pub fn new(value: T) -> Self {
        Self(inner::Mutex::new(value))
    }

    /// Acquires exclusive ownership.
    pub fn lock(&self) -> impl Future<Output = impl std::ops::DerefMut<Target = T> + '_> + '_ {
        Acquire::new(self.0.lock())
    }

    /// Attempts immediate exclusive ownership without registering a waiter.
    pub fn try_lock(&self) -> Result<impl std::ops::DerefMut<Target = T> + '_, Busy> {
        self.0.try_lock().map_err(|_| Busy)
    }

    /// Consumes an unborrowed mutex.
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

impl<T> RwLock<T> {
    /// Constructs an unlocked value.
    pub fn new(value: T) -> Self {
        Self(inner::RwLock::new(value))
    }

    #[cfg(all(test, loom))]
    pub fn stalled_reader_control(value: T) -> Self {
        Self(inner::RwLock::stalled_reader_control(value))
    }

    /// Acquires shared ownership.
    pub fn read(&self) -> impl Future<Output = impl std::ops::Deref<Target = T> + '_> + '_ {
        Acquire::new(self.0.read())
    }

    /// Acquires exclusive ownership.
    pub fn write(&self) -> impl Future<Output = impl std::ops::DerefMut<Target = T> + '_> + '_ {
        Acquire::new(self.0.write())
    }

    /// Attempts immediate exclusive ownership without registering a waiter.
    pub fn try_write(&self) -> Result<impl std::ops::DerefMut<Target = T> + '_, Busy> {
        self.0.try_write().map_err(|_| Busy)
    }
}

pin_project_lite::pin_project! {
    /// Spends one unit of cooperative budget, yielding while holding nothing,
    /// then polls the acquisition without a second budget check. The
    /// acquisition is stored once, in place, so wrapping adds no frame.
    #[must_use = "futures do nothing unless polled"]
    struct Acquire<F> {
        spent: bool,
        #[pin]
        acquisition: budget::Unconstrained<F>,
    }
}

impl<F> Acquire<F> {
    fn new(acquisition: F) -> Self {
        Self {
            spent: false,
            acquisition: budget::unconstrained(acquisition),
        }
    }
}

impl<F: Future> Future for Acquire<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        if !*this.spent {
            std::task::ready!(budget::proceed(cx));
            *this.spent = true;
        }
        this.acquisition.poll(cx)
    }
}

#[cfg(not(loom))]
mod budget {
    use std::task::{Context, Poll};
    pub use tokio::task::coop::{Unconstrained, unconstrained};

    pub fn proceed(cx: &mut Context<'_>) -> Poll<()> {
        tokio::task::coop::poll_proceed(cx).map(|spent| spent.made_progress())
    }
}

/// Loom has no cooperative budget, so the model owns one: an exhausted task
/// yields exactly once, waking itself as Tokio does, and is then refilled.
#[cfg(loom)]
pub mod budget {
    use std::{
        cell::Cell,
        task::{Context, Poll},
    };

    loom::thread_local! {
        static EXHAUSTED: Cell<bool> = Cell::new(false);
        static AFTER_ACQUIRE: Cell<bool> = Cell::new(false);
    }

    pub type Unconstrained<F> = F;

    pub fn unconstrained<F>(future: F) -> F {
        future
    }

    /// Makes this model thread's next budget check yield.
    pub fn exhaust() {
        EXHAUSTED.with(|exhausted| exhausted.set(true));
    }

    /// Consumes an exhausted budget, waking the task as Tokio does.
    pub(in crate::query_sync) fn yields(cx: &mut Context<'_>) -> bool {
        let exhausted = EXHAUSTED.with(|exhausted| exhausted.replace(false));
        if exhausted {
            cx.waker().wake_by_ref();
        }
        exhausted
    }

    /// Negative control: this model thread skips the budget check before
    /// acquiring; the modeled lock checks after taking its permits, the order
    /// Tokio's semaphore uses.
    pub fn check_after_acquire() {
        AFTER_ACQUIRE.with(|after| after.set(true));
    }

    pub(in crate::query_sync) fn after_acquire() -> bool {
        AFTER_ACQUIRE.with(Cell::get)
    }

    pub fn proceed(cx: &mut Context<'_>) -> Poll<()> {
        if !after_acquire() && yields(cx) {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use futures::FutureExt as _;

    /// Spends this task's cooperative budget without yielding to the runtime,
    /// as a task does when every operation it polls is already ready.
    fn exhaust_budget() {
        while tokio::task::coop::consume_budget().now_or_never().is_some() {}
    }

    #[tokio::test]
    async fn a_budget_yield_holds_no_lock() {
        let lock = super::RwLock::new(());
        exhaust_budget();
        let mut reader = Box::pin(lock.read());
        assert!(
            reader.as_mut().now_or_never().is_none(),
            "budget did not yield"
        );
        assert!(lock.try_write().is_ok(), "a parked reader kept the lock");

        let lock = super::Mutex::new(());
        exhaust_budget();
        let mut locking = Box::pin(lock.lock());
        assert!(
            locking.as_mut().now_or_never().is_none(),
            "budget did not yield"
        );
        assert!(lock.try_lock().is_ok(), "a parked locker kept the lock");
    }
}
