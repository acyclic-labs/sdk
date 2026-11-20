//! Model builds instrument the same queue and completion locks as native runs.
mod locks;
pub use locks::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
pub use std::sync::mpsc;

#[cfg(loom)]
pub mod mpsc;

/// Reference counts whose release orders what their holders wrote, as an
/// atomic's would.
#[cfg(loom)]
pub use loom::sync::Arc;
#[cfg(not(loom))]
pub use std::sync::Arc;

/// Atomic primitives that Loom instruments in model builds.
pub mod atomic {
    #[cfg(loom)]
    pub use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};
    #[cfg(not(loom))]
    pub use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};
}

/// Pays the portable parking service's one-time per-thread initialization.
///
/// Linux futexes and Windows SRW locks retain their storage inline. The
/// portable locks allocate their process-wide table and this thread's parker
/// on the first wait, so service initialization pays that cost before any
/// measured request is admitted.
#[cfg(not(loom))]
pub fn initialize_parking() {
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let mutex = Mutex::new(());
        let guard = mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = Condvar::new().wait_timeout(guard, std::time::Duration::ZERO);
    }
}

/// Drops a shared reference after its holder has published through it. The
/// thread can be preempted before the release, so Loom explores another
/// thread running first.
pub fn release<T: ?Sized>(reference: std::sync::Arc<T>) {
    #[cfg(loom)]
    loom::thread::yield_now();
    drop(reference);
}
