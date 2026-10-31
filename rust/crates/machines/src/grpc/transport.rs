//! Typed public RPC ownership, shared by native and thread-affine transports.

use super::*;
use acyclic_stream::{ProviderPlatform, ProviderTask};
use futures::{Future, Stream};
use tonic::Response;

include!(concat!(env!("OUT_DIR"), "/machines-client.rs"));

/// Authentication and physical transport belong to the embedding platform.
/// The provider retains all admission, decoding and settlement in one core.
pub trait Endpoint: ProviderPlatform + Clone + 'static {
    /// An authenticated client for the next original RPC.
    type Client: RpcClient;
    /// The platform's original deadline event owner.
    type Deadline: Deadline;
    /// Resolve original credentials before dispatch, never after admission loss.
    fn client(&self) -> impl Future<Output = Result<Self::Client, ProviderError>> + ProviderTask;
    /// Admit one absolute file-operation deadline.
    fn absolute_deadline(&self, expiry: u64) -> Result<Self::Deadline, ProviderError>;
}

/// One platform deadline, retained through credentials, dispatch and output.
/// Implementations use their existing clock event; they never renew the budget.
pub trait Deadline: ProviderPlatform + 'static {
    /// Remaining original budget for the transport's deadline metadata.
    fn remaining(&self) -> Duration;
    /// Stop observation on the original expiry event, retaining admitted uncertainty.
    fn run<F>(&self, future: F) -> impl Future<Output = Option<F::Output>> + ProviderTask
    where
        F: Future + ProviderTask,
        F::Output: ProviderTask;
}

/// Observe the producer until its result or the caller's separately chosen expiry.
pub(super) async fn run<D: Deadline, F>(deadline: Option<&D>, future: F) -> Option<F::Output>
where
    F: Future + ProviderTask,
    F::Output: ProviderTask,
{
    match deadline {
        Some(deadline) => deadline.run(future).await,
        None => Some(future.await),
    }
}

/// Read one frame without imposing native `Send` on browser response producers.
pub(super) trait Messages<T>: Stream<Item = Result<T, Status>> + Unpin {
    fn message(&mut self) -> impl Future<Output = Result<Option<T>, Status>> + ProviderTask
    where
        Self: ProviderTask,
        T: ProviderTask,
    {
        async move { self.next().await.transpose() }
    }
}
impl<T, S: Stream<Item = Result<T, Status>> + Unpin> Messages<T> for S {}

#[cfg(not(target_arch = "wasm32"))]
impl Deadline for crate::provider::deadline::GuestDeadline {
    fn remaining(&self) -> Duration {
        self.remaining()
    }
    async fn run<F>(&self, future: F) -> Option<F::Output>
    where
        F: Future + ProviderTask,
        F::Output: ProviderTask,
    {
        self.run(future).await
    }
}
