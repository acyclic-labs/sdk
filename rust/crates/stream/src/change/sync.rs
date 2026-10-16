#[cfg(acyclic_stream_loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(acyclic_stream_loom))]
pub(crate) use std::sync::{Arc, Mutex};

#[cfg(acyclic_stream_loom)]
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
        HELD.with(|held| assert!(!held.get(), "wake must run outside the publication lock"));
    }
    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Self(loom::sync::Mutex::new(value))
        }
        pub(crate) fn lock(&self) -> Result<Guard<'_, T>, ()> {
            assert_unlocked();
            let guard = self.0.lock().map_err(|_| ())?;
            HELD.with(|held| held.set(true));
            Ok(Guard(guard))
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
#[cfg(acyclic_stream_loom)]
pub(crate) use checked::{Mutex, assert_unlocked};
