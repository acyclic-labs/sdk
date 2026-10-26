//! Model builds instrument the same queue and completion locks as native runs.
#[cfg(acyclic_native_runtime_loom)]
pub(crate) use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(acyclic_native_runtime_loom))]
pub(crate) use std::sync::{Condvar, Mutex, MutexGuard, mpsc};

#[cfg(acyclic_native_runtime_loom)]
pub(crate) mod mpsc;

/// Reference counts whose release orders what their holders wrote, as an
/// atomic's would.
#[cfg(acyclic_native_runtime_loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(acyclic_native_runtime_loom))]
pub(crate) use std::sync::Arc;

pub(crate) mod atomic {
    #[cfg(acyclic_native_runtime_loom)]
    pub(crate) use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
    #[cfg(not(acyclic_native_runtime_loom))]
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
}

/// Drops a shared reference after its holder has published through it. The
/// thread can be preempted before the release, so Loom explores another
/// thread running first.
pub(crate) fn release<T: ?Sized>(reference: std::sync::Arc<T>) {
    #[cfg(acyclic_native_runtime_loom)]
    loom::thread::yield_now();
    drop(reference);
}
