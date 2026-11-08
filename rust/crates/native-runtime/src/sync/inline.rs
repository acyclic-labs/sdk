//! Locks whose storage is included in their owner's retained allocation.
//!
//! The standard pthread mutex allocates backing storage at first acquisition.
//! Keep that storage inline while preserving poisoning and condition waits.
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LockResult, PoisonError, TryLockError, TryLockResult};

#[derive(Debug, Default)]
/// A poison-preserving mutex with its native lock storage inside the value.
///
/// Acquiring an uncontended lock needs no separate backing allocation. Contended
/// waits use the shared parking service. Poison is advisory, as with
/// [`std::sync::Mutex`]; callers can recover the original value from its error.
pub struct Mutex<T> {
    inner: parking_lot::Mutex<T>,
    poisoned: AtomicBool,
}

impl<T> Mutex<T> {
    /// Creates a mutex without allocating its lock storage separately.
    pub const fn new(value: T) -> Self {
        Self {
            inner: parking_lot::Mutex::new(value),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Waits for ownership, returning the held guard even if an owner panicked.
    pub fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        self.guard(self.inner.lock())
    }

    /// Attempts ownership once without waiting.
    pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        self.guard(self.inner.try_lock().ok_or(TryLockError::WouldBlock)?)
            .map_err(TryLockError::Poisoned)
    }

    fn guard<'a>(&'a self, inner: parking_lot::MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        checked(MutexGuard {
            inner,
            poisoned: &self.poisoned,
            panicking: std::thread::panicking(),
        })
    }
}

#[derive(Debug)]
/// Exclusive ownership of an inline mutex; dropping it publishes poison before
/// releasing the lock if its owner started unwinding while holding it.
pub struct MutexGuard<'a, T> {
    inner: parking_lot::MutexGuard<'a, T>,
    poisoned: &'a AtomicBool,
    panicking: bool,
}

fn checked<T>(guard: MutexGuard<'_, T>) -> LockResult<MutexGuard<'_, T>> {
    // The held mutex orders poison publication before the next acquisition.
    if guard.poisoned.load(Ordering::Relaxed) {
        Err(PoisonError::new(guard))
    } else {
        Ok(guard)
    }
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        if !self.panicking && std::thread::panicking() {
            self.poisoned.store(true, Ordering::Relaxed);
        }
        // The inner guard unlocks only after publishing poison.
    }
}

#[derive(Debug, Default)]
/// A condition variable paired with an inline mutex.
///
/// Notifications are not retained. Waiters must check their predicate while
/// holding the mutex and recheck it after waking. Like the native parking
/// condition, each instance must use only one mutex at a time.
pub struct Condvar(parking_lot::Condvar);

impl Condvar {
    /// Creates an empty condition variable.
    pub const fn new() -> Self {
        Self(parking_lot::Condvar::new())
    }

    /// Releases ownership until notified, then reacquires the same mutex and
    /// reports its poison state.
    pub fn wait<'a, T>(&self, mut guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        self.0.wait(&mut guard.inner);
        checked(guard)
    }

    /// Waits for notification or the requested duration, then reacquires the
    /// same mutex. A poison error retains both the guard and timeout result.
    pub fn wait_timeout<'a, T>(
        &self,
        mut guard: MutexGuard<'a, T>,
        timeout: std::time::Duration,
    ) -> LockResult<(MutexGuard<'a, T>, parking_lot::WaitTimeoutResult)> {
        let elapsed = self.0.wait_for(&mut guard.inner, timeout);
        checked(guard)
            .map(|guard| (guard, elapsed))
            .map_err(|error| PoisonError::new((error.into_inner(), elapsed)))
    }

    /// Wakes one waiter, if present.
    pub fn notify_one(&self) {
        self.0.notify_one();
    }

    /// Wakes every currently registered waiter.
    pub fn notify_all(&self) {
        self.0.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn condition_reacquisition_preserves_original_poison_and_value() {
        let mutex = Mutex::new(0);
        let changed = Condvar::new();
        std::thread::scope(|scope| {
            let mut guard = mutex.lock().unwrap();
            assert!(matches!(mutex.try_lock(), Err(TryLockError::WouldBlock)));
            let producer = scope.spawn(|| {
                let mut guard = mutex.lock().unwrap();
                *guard = 7;
                changed.notify_all();
                panic!("original locked producer failed");
            });
            let mut observed_poison = false;
            while *guard == 0 {
                guard = match changed.wait(guard) {
                    Ok(next) => next,
                    Err(error) => {
                        observed_poison = true;
                        error.into_inner()
                    }
                };
            }
            assert!(observed_poison);
            assert_eq!(*guard, 7);
            drop(guard);
            assert!(producer.join().is_err());
        });
        assert!(matches!(mutex.try_lock(), Err(TryLockError::Poisoned(_))));
        assert_eq!(*mutex.lock().unwrap_err().into_inner(), 7);

        // Acquiring a lock while unwinding must not newly poison it.
        struct DuringUnwind<'a>(&'a Mutex<u8>);
        impl Drop for DuringUnwind<'_> {
            fn drop(&mut self) {
                *self.0.lock().unwrap() = 9;
            }
        }
        let clean = Mutex::new(0);
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _during = DuringUnwind(&clean);
            panic!("original outer failure");
        }));
        assert!(failed.is_err());
        assert_eq!(*clean.lock().unwrap(), 9);
    }
}
