//! Owned custody of one exact keyed commit, independent of its waiters.
use crate::{BoxProviderFuture, CommitOutcome, CommitRequest, StreamError, StreamProvider};
use futures::{FutureExt as _, channel::oneshot, future::Shared};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// One submitted commit and its shared terminal result.
///
/// Submission immediately transfers the exact request to the executor. Dropping
/// or cancelling any waiter never cancels publication. Clones observe the same
/// result; no clone retries. A transport failure remains an ambiguous outcome:
/// recover or replay [`Self::request`] with its original identity and bytes.
#[derive(Clone)]
#[must_use = "retain the handle or its request identity to recover an ambiguous outcome"]
pub struct Pending {
    request: Arc<CommitRequest>,
    outcome: Shared<BoxProviderFuture<'static, Result<CommitOutcome, StreamError>>>,
}

impl Pending {
    /// Submits one canonical commit on the supplied executor. Staged publication
    /// uses this same handle once its canonical commit is ready.
    pub fn submit<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        request: CommitRequest,
        executor: &dyn crate::runtime::Executor,
    ) -> Self {
        let request = Arc::new(request);
        let owned = Arc::clone(&request);
        let (send, receive) = oneshot::channel();
        executor.spawn(Box::pin(async move {
            let outcome = provider.commit((*owned).clone()).await;
            let _ = send.send(outcome);
        }));
        let outcome: BoxProviderFuture<'static, _> =
            Box::pin(async move { receive.await.unwrap_or(Err(StreamError::Unavailable)) });
        Self {
            request,
            outcome: outcome.shared(),
        }
    }

    /// Exact submitted bytes, conditions, deadline and recovery identity.
    #[must_use]
    pub fn request(&self) -> &CommitRequest {
        &self.request
    }

    /// Waits without consuming this handle. Cancelling the wait leaves both
    /// the owned submission and its eventual result available to other waiters.
    pub async fn wait(&self) -> Result<CommitOutcome, StreamError> {
        self.outcome.clone().await
    }
}

impl Future for Pending {
    type Output = Result<CommitOutcome, StreamError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().outcome).poll(cx)
    }
}
