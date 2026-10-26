// Resource nodes include their lock storage in the funded node layout. The
// standard pthread mutex can allocate on its first acquisition on Darwin;
// parking_lot keeps this storage inline on every host. Loom replaces only the
// primitive, so resource admission and unwind handling remain production code.
#[cfg(acyclic_fs_loom)]
pub(crate) use loom::sync::{Mutex, MutexGuard};
#[cfg(not(acyclic_fs_loom))]
pub(crate) use parking_lot::{Mutex, MutexGuard};

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::LockResult<MutexGuard<'_, T>> {
    #[cfg(acyclic_fs_loom)]
    return mutex.lock();
    #[cfg(not(acyclic_fs_loom))]
    Ok(mutex.lock())
}
