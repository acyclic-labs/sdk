//! Publication lock order: state (1) < cursor key (2) < journal tail (3).
//! These synchronous guards never cross an await. Release in reverse order.
#[cfg(objects_loom)]
pub(crate) use loom::sync::Arc;
#[cfg(all(objects_loom, feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(objects_loom)]
use loom::sync::{Mutex as RawMutex, MutexGuard as RawGuard};
#[cfg(not(objects_loom))]
pub(crate) use std::sync::Arc;
#[cfg(all(not(objects_loom), feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(objects_loom))]
use std::sync::{Mutex as RawMutex, MutexGuard as RawGuard};
use std::{
    ops::{Deref, DerefMut},
    sync::{LockResult, PoisonError},
};

#[cfg(all(debug_assertions, not(objects_loom)))]
std::thread_local! { static HELD: std::cell::Cell<u8> = const { std::cell::Cell::new(0) }; }
#[cfg(objects_loom)]
loom::thread_local! { static HELD: std::cell::Cell<u8> = std::cell::Cell::new(0); }

#[cfg(any(debug_assertions, objects_loom))]
struct Rank<const ORDER: u8>(u8);
#[cfg(any(debug_assertions, objects_loom))]
impl<const ORDER: u8> Drop for Rank<ORDER> {
    fn drop(&mut self) {
        HELD.with(|held| {
            assert_eq!(held.get(), ORDER, "lock order: out-of-order release");
            held.set(self.0);
        });
    }
}

#[derive(Debug)]
pub(crate) struct Mutex<T, const ORDER: u8 = 1>(RawMutex<T>);
impl<T: Default, const ORDER: u8> Default for Mutex<T, ORDER> {
    fn default() -> Self {
        Self::new(T::default())
    }
}
impl<T, const ORDER: u8> Mutex<T, ORDER> {
    pub(crate) fn new(value: T) -> Self {
        Self(RawMutex::new(value))
    }
    #[track_caller]
    pub(crate) fn lock(&self) -> LockResult<MutexGuard<'_, T, ORDER>> {
        #[cfg(any(debug_assertions, objects_loom))]
        HELD.with(|held| assert!(held.get() < ORDER, "lock order: acquisition inversion"));
        let wrap = |inner| MutexGuard {
            inner,
            #[cfg(any(debug_assertions, objects_loom))]
            _rank: Rank(HELD.with(|held| held.replace(ORDER))),
        };
        match self.0.lock() {
            Ok(inner) => Ok(wrap(inner)),
            Err(error) => Err(PoisonError::new(wrap(error.into_inner()))),
        }
    }
}

pub(crate) struct MutexGuard<'a, T, const ORDER: u8 = 1> {
    // Field order unlocks before retiring this thread's rank.
    inner: RawGuard<'a, T>,
    #[cfg(any(debug_assertions, objects_loom))]
    _rank: Rank<ORDER>,
}
impl<T, const ORDER: u8> Deref for MutexGuard<'_, T, ORDER> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}
impl<T, const ORDER: u8> DerefMut for MutexGuard<'_, T, ORDER> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

#[cfg(all(test, not(objects_loom), debug_assertions))]
mod tests {
    use super::*;

    #[test]
    fn objects_lock_order_checker_rejects_inversion_and_restores_after_unwind() {
        let state = Mutex::<_, 1>::new(());
        let cursor = Mutex::<_, 2>::new(());
        let error = std::panic::catch_unwind(|| {
            let _cursor = cursor.lock().unwrap();
            let _state = state.lock().unwrap();
        });
        assert!(error.is_err());
        let _state = state.lock().unwrap();
        // Unwind poisoned the inner lock; retaining that guard still tracks rank.
        let _cursor = cursor.lock().unwrap_or_else(PoisonError::into_inner);
    }
}
