//! Owned async locking uses the same Tokio primitive in ordinary and model builds.
#[doc(hidden)]
pub use std::sync::Arc;
#[doc(hidden)]
pub use tokio::sync::{Mutex, OwnedMutexGuard};

#[doc(hidden)]
pub fn lock_owned<T>(mutex: Arc<Mutex<T>>) -> impl Future<Output = OwnedMutexGuard<T>> {
    mutex.lock_owned()
}
