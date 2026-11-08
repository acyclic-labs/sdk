//! Owned custody of one exact keyed commit, independent of its waiters.
use crate::StreamProviderExt as _;
use crate::operation_claim::Signal;
use crate::{CommitOutcome, CommitRequest, OperationTermination, StreamError, StreamProvider};
use std::sync::{Arc, OnceLock};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
#[cfg(not(target_arch = "wasm32"))]
pub use native::NativePending;

struct Completion<R, H> {
    request: CommitRequest,
    result: OnceLock<R>,
    terminal: Signal,
    // Request, result and event storage retire before their original funding.
    _storage: H,
}

struct Producer<R, H>(Arc<Completion<R, H>>);
impl<R, H> Producer<R, H> {
    fn publish(self, result: R) {
        // There is exactly one producer; no waiter can replace its result.
        assert!(self.0.result.set(result).is_ok());
    }
}
impl<R, H> Drop for Producer<R, H> {
    fn drop(&mut self) {
        self.0.terminal.publish(if self.0.result.get().is_some() {
            OperationTermination::Released
        } else {
            OperationTermination::ProducerDropped
        });
    }
}

/// One exact commit and its retained terminal result.
///
/// Clones observe the same producer; no clone retries. Producer loss is an
/// explicit unavailable outcome, never a wait for a successor or transport EOF.
#[must_use = "retain the handle or its request identity to recover an ambiguous outcome"]
pub struct Pending<R = Result<CommitOutcome, StreamError>, H = ()> {
    completion: Arc<Completion<R, H>>,
}
impl<R, H> Clone for Pending<R, H> {
    fn clone(&self) -> Self {
        Self {
            completion: Arc::clone(&self.completion),
        }
    }
}
impl<R, H> Pending<R, H> {
    fn prepare(request: CommitRequest, storage: H) -> (Self, Producer<R, H>) {
        let completion = Arc::new(Completion {
            request,
            result: OnceLock::new(),
            terminal: Signal::new(),
            _storage: storage,
        });
        (
            Self {
                completion: Arc::clone(&completion),
            },
            Producer(completion),
        )
    }
    /// Exact submitted bytes, conditions, deadline and recovery identity.
    #[must_use]
    pub fn request(&self) -> &CommitRequest {
        &self.completion.request
    }

    /// Moves the exact terminal value and its funding to the sole remaining
    /// owner. A live producer or another observer leaves this handle intact.
    /// No native error, receipt or allocation guard is cloned.
    pub fn try_take(self) -> Result<(CommitRequest, Option<R>, H), Self> {
        if !self.completion.terminal.is_terminated() {
            return Err(self);
        }
        match Arc::try_unwrap(self.completion) {
            Ok(completion) => Ok((
                completion.request,
                completion.result.into_inner(),
                completion._storage,
            )),
            Err(completion) => Err(Self { completion }),
        }
    }

    /// Borrows the original result without copying its receipt or native error.
    /// Cancelling a wait never cancels the producer or consumes its result.
    /// Completion wakes every registered observer independently, with constant
    /// work per observer and no dependency on another observer being polled.
    pub async fn result(&self) -> Result<&R, StreamError> {
        let completion = &self.completion;
        completion.terminal.terminated().await;
        completion.result.get().ok_or(StreamError::Unavailable)
    }
}
impl Pending {
    /// Transfers one canonical commit to the supplied executor immediately.
    /// Losing every waiter cannot withdraw an already submitted publication.
    pub fn submit<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        request: CommitRequest,
        executor: &dyn crate::runtime::Executor,
    ) -> Self {
        let command = request.clone();
        let (pending, producer) = Self::prepare(request, ());
        executor.spawn(Box::pin(async move {
            producer.publish(provider.commit(command).await);
        }));
        pending
    }
    /// Waits for the retained portable receipt. Native callers borrow the same
    /// completion with [`Self::result`] to preserve its original allocation.
    pub async fn wait(&self) -> Result<CommitOutcome, StreamError> {
        self.result().await?.clone()
    }
}
