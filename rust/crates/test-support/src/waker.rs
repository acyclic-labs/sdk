//! Wakers whose clone and drop callbacks run for each original reference.
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Waker};

struct Callbacks {
    cloned: Box<dyn Fn() + Send + Sync>,
    retired: Box<dyn Fn() + Send + Sync>,
}

unsafe fn clone(data: *const ()) -> RawWaker {
    // SAFETY: every live reference owns one Arc; borrowing must not consume it.
    let owner = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Callbacks>()) });
    (owner.cloned)();
    raw(Arc::clone(&owner))
}

unsafe fn wake(data: *const ()) {
    // SAFETY: wake consumes exactly the reference supplied to this callback.
    drop(unsafe { Arc::from_raw(data.cast::<Callbacks>()) });
}

unsafe fn wake_by_ref(_: *const ()) {}

unsafe fn retire(data: *const ()) {
    // SAFETY: drop consumes exactly one reference, including callback unwind.
    let owner = unsafe { Arc::from_raw(data.cast::<Callbacks>()) };
    (owner.retired)();
}

fn raw(owner: Arc<Callbacks>) -> RawWaker {
    RawWaker::new(
        Arc::into_raw(owner).cast(),
        &RawWakerVTable::new(clone, wake, wake_by_ref, retire),
    )
}

/// Observes every clone/drop callback, even while another waker remains alive.
/// Waking is a no-op; consuming wakes still release their original reference.
/// Callbacks may re-enter the observed operation or unwind independently.
pub fn callbacks(
    cloned: impl Fn() + Send + Sync + 'static,
    retired: impl Fn() + Send + Sync + 'static,
) -> Waker {
    let owner = Arc::new(Callbacks {
        cloned: Box::new(cloned),
        retired: Box::new(retired),
    });
    // SAFETY: the vtable preserves one Arc owner per live waker, is thread-safe,
    // borrows for clone/wake_by_ref and consumes for drop/wake, also on unwind.
    unsafe { Waker::from_raw(raw(owner)) }
}
