//! Runtime-independent cooperative cancellation for native and browser work.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use thiserror::Error;

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Weak<CancellationWaiter>>>,
}

#[derive(Default)]
struct CancellationWaiter {
    waker: Mutex<Option<Waker>>,
}

/// Cloneable cancellation source and observation token.
///
/// Cancellation is monotonic. It is an execution fact only and never durable
/// filesystem authority. Backends check it before externally visible work and
/// async waits wake promptly when cancellation wins a race.
#[derive(Clone, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    /// Creates one live token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically marks the token cancelled and wakes every registered waiter.
    pub fn cancel(&self) {
        if self.state.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let waiters = match self.state.waiters.lock() {
            Ok(mut waiters) => std::mem::take(&mut *waiters),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        for waiter in waiters {
            let Some(waiter) = waiter.upgrade() else {
                continue;
            };
            let waker = match waiter.waker.lock() {
                Ok(mut waker) => waker.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(waker) = waker {
                let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
            }
        }
    }

    /// Returns whether cancellation has occurred.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Fails if cancellation has already occurred.
    ///
    /// # Errors
    ///
    /// Returns [`CancellationError`] after this token is cancelled.
    pub fn check(&self) -> Result<(), CancellationError> {
        if self.is_cancelled() {
            Err(CancellationError)
        } else {
            Ok(())
        }
    }

    /// Resolves once cancellation occurs.
    #[must_use]
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            waiter: Arc::new(CancellationWaiter::default()),
            registered: false,
        }
    }
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    waiter: Arc<CancellationWaiter>,
    registered: bool,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.token.is_cancelled() {
            return Poll::Ready(());
        }
        let mut waiters = match this.token.state.waiters.lock() {
            Ok(waiters) => waiters,
            Err(poisoned) => poisoned.into_inner(),
        };
        if this.token.is_cancelled() {
            return Poll::Ready(());
        }
        let mut waker = match this.waiter.waker.lock() {
            Ok(waker) => waker,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            *waker = Some(context.waker().clone());
        }
        if !this.registered {
            waiters.push(Arc::downgrade(&this.waiter));
            this.registered = true;
        }
        Poll::Pending
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }
        let mut waiters = match self.token.state.waiters.lock() {
            Ok(waiters) => waiters,
            Err(poisoned) => poisoned.into_inner(),
        };
        waiters.retain(|waiter| {
            waiter
                .upgrade()
                .is_some_and(|waiter| !Arc::ptr_eq(&waiter, &self.waiter))
        });
        let mut waker = match self.waiter.waker.lock() {
            Ok(waker) => waker,
            Err(poisoned) => poisoned.into_inner(),
        };
        *waker = None;
    }
}

/// Stable cooperative-cancellation outcome.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("filesystem operation was cancelled")]
pub struct CancellationError;

#[cfg(test)]
#[path = "tests/cancellation.rs"]
mod tests;
