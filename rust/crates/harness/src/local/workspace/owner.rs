//! Who acts on a checkout view: an agent hosted by this process. Views,
//! mounts and the git facade take this one boundary.
use super::*;
use crate::runtime::TaskContext;
use acyclic_stream::BoxProviderFuture;

/// The authority a view's mounts, captures and git commands act under.
pub(in crate::local) trait ViewOwner: Send + Sync {
    /// Fails once this owner may no longer act on the view.
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>>;
    /// Identity of `operation`'s publication window under this owner.
    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId;
}

fn identity(domain: &str, fields: &[&[u8]]) -> acyclic_fs::OperationLeaseId {
    let mut hash = blake3::Hasher::new_derive_key(domain);
    for field in fields {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field);
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationLeaseId::from_bytes(identity)
}

/// A tool call of an agent turn. The turn's actor commit is its single
/// writer, so the call itself carries no lease to verify.
impl ViewOwner for TaskContext {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId {
        identity("acyclic.local.git-task/v1", &[&operation.into_bytes()])
    }
}

/// An agent of this process's native-host runtime. The runtime's exclusive
/// workspace ownership lock is its fence; the agent's committed volumes are
/// its grants, so there is no per-call lease to verify.
pub(in crate::local) struct AgentOwner(pub(in crate::local) crate::agent::AgentPath);

impl ViewOwner for AgentOwner {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId {
        identity(
            "acyclic.local.git-agent/v1",
            &[self.0.as_str().as_bytes(), &operation.into_bytes()],
        )
    }
}
