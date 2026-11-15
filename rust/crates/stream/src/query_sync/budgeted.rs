//! Async locks whose cooperative-budget yield happens before acquisition.
//!
//! Tokio's semaphore takes a lock's permits and only then checks the task's
//! cooperative budget, returning `Pending` while it keeps them. A future that
//! is polled once on an exhausted budget and then left unpolled holds the lock
//! until it is dropped, so every later writer waits on a future nobody polls.
//! Spending the budget first means a budget yield holds nothing; the
//! acquisition itself then runs unconstrained, waiting only on the lock.

#[cfg(loom)]
use super::locks as inner;
#[cfg(not(loom))]
use tokio::sync as inner;

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
    pub async fn lock(&self) -> impl std::ops::DerefMut<Target = T> + '_ {
        acquire(self.0.lock()).await
    }

    /// Attempts immediate exclusive ownership without registering a waiter.
    pub fn try_lock(&self) -> Result<impl std::ops::DerefMut<Target = T> + '_, ()> {
        self.0.try_lock().map_err(|_| ())
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
    pub(crate) fn stalled_reader_control(value: T) -> Self {
        Self(inner::RwLock::stalled_reader_control(value))
    }

    /// Acquires shared ownership.
    pub async fn read(&self) -> impl std::ops::Deref<Target = T> + '_ {
        acquire(self.0.read()).await
    }

    /// Acquires exclusive ownership.
    pub async fn write(&self) -> impl std::ops::DerefMut<Target = T> + '_ {
        acquire(self.0.write()).await
    }

    /// Attempts immediate exclusive ownership without registering a waiter.
    pub fn try_write(&self) -> Result<impl std::ops::DerefMut<Target = T> + '_, ()> {
        self.0.try_write().map_err(|_| ())
    }
}

/// Spends one unit of cooperative budget, yielding while holding nothing,
/// then acquires without a second budget check.
async fn acquire<F: std::future::Future>(acquisition: F) -> F::Output {
    #[cfg(all(loom, not(target_arch = "wasm32")))]
    if crate::verification::fault_is(crate::verification::Fault::BudgetAfterAcquire) {
        // Negative control: the order Tokio's semaphore uses.
        let guard = acquisition.await;
        budget::spend().await;
        return guard;
    }
    budget::spend().await;
    budget::unconstrained(acquisition).await
}

#[cfg(not(loom))]
mod budget {
    pub(super) use tokio::task::coop::{consume_budget as spend, unconstrained};
}

/// Loom has no cooperative budget, so the model owns one: an exhausted task
/// yields exactly once, waking itself as Tokio does, and is then refilled.
#[cfg(loom)]
pub(crate) mod budget {
    use std::{cell::Cell, task::Poll};

    loom::thread_local! {
        static EXHAUSTED: Cell<bool> = const { Cell::new(false) };
    }

    /// Makes this model thread's next budget check yield.
    pub(crate) fn exhaust() {
        EXHAUSTED.with(|exhausted| exhausted.set(true));
    }

    pub(super) async fn spend() {
        std::future::poll_fn(|cx| {
            if EXHAUSTED.with(|exhausted| exhausted.replace(false)) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }

    pub(super) fn unconstrained<F>(future: F) -> F {
        future
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
