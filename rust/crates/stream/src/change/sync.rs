#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Mutex};

#[cfg(loom)]
mod checked {
    use std::{
        cell::Cell,
        ops::{Deref, DerefMut},
    };
    loom::thread_local! { static HELD: Cell<bool> = Cell::new(false); }

    // One lock rank: publication locks never nest, including across instances.
    // Thread ownership distinguishes re-entry from another thread's contention.
    pub(crate) struct Mutex<T>(loom::sync::Mutex<T>);
    pub(crate) struct Guard<'a, T>(loom::sync::MutexGuard<'a, T>);
    pub(crate) fn assert_unlocked() {
        assert!(
            !HELD.with(Cell::get),
            "wake must run outside the publication lock"
        );
    }
    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Self(loom::sync::Mutex::new(value))
        }
        pub(crate) fn lock(&self) -> std::sync::LockResult<Guard<'_, T>> {
            assert_unlocked();
            let guard = self.0.lock();
            HELD.with(|held| held.set(true));
            guard
                .map(Guard)
                .map_err(|poison| std::sync::PoisonError::new(Guard(poison.into_inner())))
        }
    }
    impl<T> Deref for Guard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }
    impl<T> DerefMut for Guard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }
    impl<T> Drop for Guard<'_, T> {
        fn drop(&mut self) {
            HELD.with(|held| held.set(false));
        }
    }
}
#[cfg(loom)]
pub(crate) use checked::Mutex;
#[cfg(all(loom, test))]
pub(crate) use checked::assert_unlocked;
