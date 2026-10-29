use futures::Stream;
use futures::future::AbortHandle;
use futures::stream::Abortable;

/// The response owner also owns cancellation of the original request producer.
pub(super) struct InputGuard(AbortHandle);

pub(super) fn input<S: Stream>(input: S) -> (Abortable<S>, InputGuard) {
    let (handle, registration) = AbortHandle::new_pair();
    (Abortable::new(input, registration), InputGuard(handle))
}

impl Drop for InputGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}
