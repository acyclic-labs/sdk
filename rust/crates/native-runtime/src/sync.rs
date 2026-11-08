//! Model builds instrument the same queue and completion locks as native runs.
#[cfg(not(loom))]
pub use inline::{Condvar, Mutex, MutexGuard};
#[cfg(loom)]
pub use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
pub(crate) use std::sync::mpsc;

#[cfg(not(loom))]
#[path = "sync/inline.rs"]
mod inline;

#[cfg(loom)]
#[path = "sync/mpsc.rs"]
pub(crate) mod mpsc;

/// Reference counts whose release orders what their holders wrote, as an
/// atomic's would.
#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;

pub(crate) mod atomic {
    #[cfg(loom)]
    pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};
    #[cfg(not(loom))]
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};
}

/// Drops a shared reference after its holder has published through it. The
/// thread can be preempted before the release, so Loom explores another
/// thread running first.
pub(crate) fn release<T: ?Sized>(reference: std::sync::Arc<T>) {
    #[cfg(loom)]
    loom::thread::yield_now();
    drop(reference);
}
