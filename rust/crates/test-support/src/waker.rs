//! Wakers whose clone and drop callbacks run for each original reference.
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// A waker that unparks the thread which creates it.
pub fn thread_waker() -> Waker {
    Waker::from(Arc::new(ThreadWake(std::thread::current())))
}

/// Drives a future on this thread and parks between its wake notifications.
pub fn complete<T>(future: impl Future<Output = T>) -> T {
    let waker = thread_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            // A wake before park leaves the token set, so none is lost.
            Poll::Pending => std::thread::park(),
        }
    }
}

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
