//! Owned async locking: Tokio in production, instrumented ownership under Loom.
#[cfg(not(acyclic_harness_loom))]
pub(crate) use crate::sync::Arc;
#[cfg(not(acyclic_harness_loom))]
pub(crate) use tokio::sync::{Mutex, OwnedMutexGuard};

#[cfg(not(acyclic_harness_loom))]
pub(crate) fn lock_owned<T>(mutex: Arc<Mutex<T>>) -> impl Future<Output = OwnedMutexGuard<T>> {
    mutex.lock_owned()
}

#[cfg(acyclic_harness_loom)]
pub(crate) use modeled::{Arc, Mutex, OwnedMutexGuard, lock_owned};

#[cfg(acyclic_harness_loom)]
mod modeled {
    pub use loom::sync::Arc;
    use loom::sync::{Condvar, Mutex as DataMutex};
    use std::ops::{Deref, DerefMut};

    // Loom has no owned async guard. Move the value into the owning guard and
    // use its instrumented condition variable for lock admission and release.
    // This private data lock is never held while user code or a wake executes.
    pub struct Mutex<T> {
        value: DataMutex<Option<T>>,
        available: Condvar,
    }

    impl<T> Mutex<T> {
        pub fn new(value: T) -> Self {
            Self {
                value: DataMutex::new(Some(value)),
                available: Condvar::new(),
            }
        }
    }

    pub struct OwnedMutexGuard<T> {
        mutex: Arc<Mutex<T>>,
        value: Option<T>,
    }

    pub async fn lock_owned<T>(mutex: Arc<Mutex<T>>) -> OwnedMutexGuard<T> {
        let value = {
            let mut value = mutex.value.lock().unwrap();
            while value.is_none() {
                value = mutex.available.wait(value).unwrap();
            }
            value.take()
        };
        OwnedMutexGuard { mutex, value }
    }

    impl<T> Deref for OwnedMutexGuard<T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.value.as_ref().unwrap()
        }
    }

    impl<T> DerefMut for OwnedMutexGuard<T> {
        fn deref_mut(&mut self) -> &mut T {
            self.value.as_mut().unwrap()
        }
    }

    impl<T> Drop for OwnedMutexGuard<T> {
        fn drop(&mut self) {
            {
                let mut value = self.mutex.value.lock().unwrap();
                assert!(value.is_none());
                *value = self.value.take();
            }
            self.mutex.available.notify_one();
        }
    }
}
