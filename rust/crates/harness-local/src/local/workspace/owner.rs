//! Who acts on a checkout view: an agent hosted by this process. Views,
//! mounts and the git facade take this one boundary.
use super::*;
use crate::runtime::TaskContext;
use acyclic_stream::BoxProviderFuture;

/// The authority a view's mounts, captures and git commands act under.
pub(in crate::local) trait ViewOwner: Send + Sync {
    /// Fails once this owner may no longer act on the view.
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>>;
}

/// A tool call of an agent turn. The turn's actor commit is its single
/// writer, so the call itself carries no lease to verify.
impl ViewOwner for TaskContext {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

/// An agent of this process's native-host runtime. The runtime's exclusive
/// workspace ownership lock is its fence; the agent's committed volumes are
/// its grants, so there is no per-call lease to verify.
pub(in crate::local) struct AgentOwner;

impl ViewOwner for AgentOwner {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }
}
