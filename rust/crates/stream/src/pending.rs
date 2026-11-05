//! Owned custody of one exact keyed commit, independent of its waiters.
use crate::StreamProviderExt as _;
use crate::completion::{Completion, Producer};
use crate::{CommitOutcome, CommitRequest, StreamError, StreamProvider};
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
#[cfg(not(target_arch = "wasm32"))]
pub use native::NativePending;

/// One exact commit and its retained terminal result.
///
/// Clones observe the same producer; no clone retries. Producer loss is an
/// explicit unavailable outcome, never a wait for a successor or transport EOF.
#[must_use = "retain the handle or its request identity to recover an ambiguous outcome"]
pub struct Pending<R = Result<CommitOutcome, StreamError>, H = ()> {
    completion: Completion<R, H, CommitRequest>,
}
impl<R, H> Clone for Pending<R, H> {
    fn clone(&self) -> Self {
        Self {
            completion: self.completion.clone(),
        }
    }
}
impl<R, H> Pending<R, H> {
    fn prepare(request: CommitRequest, storage: H) -> (Self, Producer<R, H, CommitRequest>) {
        let (completion, producer) = Completion::prepare(request, storage);
        (Self { completion }, producer)
    }
    /// Exact submitted bytes, conditions, deadline and recovery identity.
    #[must_use]
    pub fn request(&self) -> &CommitRequest {
        self.completion.identity()
    }

    /// Moves the exact terminal value and its funding to the sole remaining
    /// owner. A live producer or another observer leaves this handle intact.
    /// No native error, receipt or allocation guard is cloned.
    pub fn try_take(self) -> Result<(CommitRequest, Option<R>, H), Self> {
        match self.completion.try_take() {
            Ok(parts) => Ok(parts),
            Err(completion) => Err(Self { completion }),
        }
    }

    /// Borrows the original result without copying its receipt or native error.
    /// Cancelling a wait never cancels the producer or consumes its result.
    /// Each completion wakes one waiter, which passes the wake onward in O(1).
    pub async fn result(&self) -> Result<&R, StreamError> {
        self.completion
            .result()
            .await
            .map_err(|_| StreamError::Unavailable)
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
