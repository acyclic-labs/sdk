use std::{
    future::Future,
    task::{Context, Poll},
};

// Reserve the logical operation on the calling thread before dispatching its
// future. The worker's first poll replaces the temporary waker; readiness is
// retained by the future even if it arrives before that poll.
pub(super) fn start<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let mut future = Box::pin(future);
    let ready = future
        .as_mut()
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()));
    async move {
        match ready {
            Poll::Ready(value) => value,
            Poll::Pending => future.await,
        }
    }
}
