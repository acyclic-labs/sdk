//! Model builds instrument the same queue and completion locks as native runs.
#[cfg(acyclic_native_runtime_loom)]
pub(crate) use loom::sync::Mutex;
#[cfg(not(acyclic_native_runtime_loom))]
pub(crate) use std::sync::{Mutex, mpsc};

#[cfg(acyclic_native_runtime_loom)]
pub(crate) mod mpsc;

/// Drops a shared reference after its holder has published through it. The
/// thread can be preempted before the release, so Loom explores another
/// thread running first.
pub(crate) fn release<T: ?Sized>(reference: std::sync::Arc<T>) {
    #[cfg(acyclic_native_runtime_loom)]
    loom::thread::yield_now();
    drop(reference);
}
