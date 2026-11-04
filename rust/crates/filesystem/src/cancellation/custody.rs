//! Synchronous custody over the shared async mutex and native wake primitive.

use super::CancellationToken;
use crate::demand::DemandError;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::sync::{Mutex, MutexGuard};

#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_WAIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Default)]
pub(crate) struct Custody<T>(Mutex<T>);

impl<T> Custody<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }

    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        self.0.try_lock().ok()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
        self.lock_with_wait(None)
            .expect("unconditional custody cannot be cancelled")
    }

    /// Cancellation only abandons acquisition; it never revokes held custody.
    /// The observer owns its wait registrations, independently of native birth.
    pub(crate) fn lock_with_wait(
        &self,
        wait: Option<&CancellationToken>,
    ) -> Result<MutexGuard<'_, T>, DemandError> {
        if wait.is_some_and(CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        if let Some(guard) = self.try_lock() {
            return Ok(guard);
        }
        // Both futures register before observing their state. NativeWake retains
        // a wake between the final observation and entry into the OS wait.
        // No executor is entered: synchronous callers may already run in one.
        let wake = Arc::new(acyclic_native_runtime::NativeWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut acquire = std::pin::pin!(tokio::task::coop::unconstrained(self.0.lock()));
        let mut cancelled = std::pin::pin!(async {
            match wait {
                Some(wait) => wait.cancelled().await,
                None => std::future::pending().await,
            }
        });
        loop {
            if cancelled.as_mut().poll(&mut context).is_ready() {
                return Err(DemandError::Cancelled);
            }
            if let Poll::Ready(guard) = acquire.as_mut().poll(&mut context) {
                return Ok(guard);
            }
            #[cfg(test)]
            BEFORE_WAIT.with(|hook| {
                if let Some(observe) = hook.borrow_mut().take() {
                    observe();
                }
            });
            wake.wait(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_and_cancellation_between_registration_and_wait_are_retained() {
        // Exercise the actual queue and native wake, including simultaneous
        // readiness/cancellation and a caller already running in an executor.
        for (release, cancel) in [(true, false), (false, true), (true, true)] {
            let custody = Custody::new(0);
            let mut held = Some(custody.lock());
            let token = CancellationToken::new();
            std::thread::scope(|threads| {
                let (entering, entered) = std::sync::mpsc::channel();
                let (resume, resumed) = std::sync::mpsc::channel();
                let waiting = token.clone();
                let custody = &custody;
                let waiter = threads.spawn(move || {
                    BEFORE_WAIT.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move || {
                            entering.send(()).expect("live wait observer");
                            resumed.recv().expect("live wait controller");
                        }));
                    });
                    futures::executor::block_on(async {
                        match custody.lock_with_wait(Some(&waiting)) {
                            Ok(mut guard) => {
                                *guard += 1;
                                true
                            }
                            Err(DemandError::Cancelled) => false,
                            Err(error) => panic!("unexpected custody failure: {error}"),
                        }
                    })
                });
                entered.recv().expect("wait registered");
                if release {
                    drop(held.take());
                }
                if cancel {
                    token.cancel();
                }
                resume.send(()).expect("registered wait resumes");
                assert_eq!(waiter.join().expect("wait returned"), !cancel);
            });
            drop(held);
            assert_eq!(*custody.lock(), i32::from(!cancel));
        }
    }
}
