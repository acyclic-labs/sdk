//! Owned async locking uses the same Tokio primitive in ordinary and model builds.
#[doc(hidden)]
pub use std::sync::Arc;
#[doc(hidden)]
pub use tokio::sync::{Mutex, OwnedMutexGuard};

#[doc(hidden)]
pub fn lock_owned<T>(mutex: Arc<Mutex<T>>) -> impl Future<Output = OwnedMutexGuard<T>> {
    mutex.lock_owned()
}

/// Drive the production owned mutex using modeled wake and park events.
/// Tokio's internal atomics are not Loom-instrumented.
#[cfg(loom)]
#[doc(hidden)]
pub fn block_on<F: Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Wake, Waker};

    struct Unpark(loom::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    // Tokio may clone a waker while holding its uninstrumented wait-list lock.
    // Clone/drop only std::Arc here; the model event is the actual wake.
    let waker = Waker::from(Arc::new(Unpark(loom::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => loom::thread::park(),
        }
    }
}
