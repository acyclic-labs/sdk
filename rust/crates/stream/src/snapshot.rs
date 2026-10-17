use std::{
    future::{Future, poll_fn},
    task::Poll,
};

mod job;
#[cfg(acyclic_loom)]
pub(crate) mod model;

// Implementations acquire locks first, then synchronously update their cursor.
// Cancellation before readiness must leave the cursor unchanged. This module is
// the only place stream snapshots are polled: pending acquisitions are dropped
// within the poll and restarted under independent ownership, never in a Stream.
pub(crate) trait Snapshot: Send + 'static {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    const OBSERVE: bool = false;
    type Output: Send + 'static;
    fn read(&mut self) -> impl Future<Output = Self::Output> + Send;
}

pub(crate) fn read<S: Snapshot>(
    state: S,
    #[cfg(acyclic_loom)] retain_pending: bool,
) -> impl Future<Output = (S, S::Output)> + Send {
    let read = read_owned(
        state,
        #[cfg(acyclic_loom)]
        retain_pending,
    );
    // Observation can pause after a read completes. Keep it outside the
    // cancellable probe so a cursor is never advanced and then retried.
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    let read = if S::OBSERVE {
        futures::future::Either::Left(acyclic_native_runtime::testing::observe_read(read))
    } else {
        futures::future::Either::Right(read)
    };
    read
}

async fn read_owned<S: Snapshot>(
    mut state: S,
    #[cfg(acyclic_loom)] retain_pending: bool,
) -> (S, S::Output) {
    #[cfg(acyclic_loom)]
    if retain_pending {
        let result = state.read().await;
        return (state, result);
    }
    loop {
        let ready = poll_fn(|cx| {
            let future = std::pin::pin!(state.read());
            Poll::Ready(future.poll(cx))
        })
        .await;
        if let Poll::Ready(result) = ready {
            return (state, result);
        }
        let (job, completed) = job::new(state);
        let worker = job.run();
        #[cfg(acyclic_loom)]
        model::spawn(worker);
        #[cfg(all(not(acyclic_loom), not(target_arch = "wasm32")))]
        if tokio::runtime::Handle::try_current().is_ok() {
            #[cfg(feature = "test-support")]
            drop(acyclic_native_runtime::testing::spawn(worker));
            #[cfg(not(feature = "test-support"))]
            drop(tokio::spawn(worker));
        } else {
            std::thread::spawn(move || futures::executor::block_on(worker));
        }
        #[cfg(all(not(acyclic_loom), target_arch = "wasm32"))]
        wasm_bindgen_futures::spawn_local(worker);
        let (returned, result) = completed.await;
        if let Some(result) = result {
            return (returned, result);
        }
        // Executor shutdown cancelled the read, after dropping its lock future.
        // The next consumer poll owns the cursor again and may use any executor.
        state = returned;
    }
}

#[cfg(acyclic_loom)]
pub(crate) fn park<F: Future>(future: F) -> F::Output {
    use std::{
        mem::ManuallyDrop,
        sync::Arc,
        task::{Context, Wake, Waker},
    };
    struct Thread(loom::thread::Thread);
    impl Wake for Thread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Thread(loom::thread::current())));
    let mut future = ManuallyDrop::new(Box::pin(future));
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            drop(ManuallyDrop::into_inner(future));
            return value;
        }
        loom::thread::park();
    }
}
