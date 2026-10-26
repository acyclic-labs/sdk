//! Who acts on a checkout view: a live task admission, or an agent hosted by
//! this process. Views, mounts and the git facade take this one boundary.
use super::*;
use crate::runtime::{OwnedTaskMailbox, TaskContext};
use acyclic_stream::BoxProviderFuture;

/// The authority a view's mounts, captures and git commands act under.
pub(in crate::local) trait ViewOwner: Send + Sync {
    /// Fails once this owner may no longer act on the view.
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>>;
    /// Identity of `operation`'s publication window under this owner.
    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId;
    /// Journal recording this owner's captures, when it has one.
    fn journal(&self) -> Result<Option<Arc<crate::durable_host::TaskJournalOwner<LocalStream>>>> {
        Ok(None)
    }
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

impl ViewOwner for Arc<dyn OwnedTaskMailbox> {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move { self.verify_owner(None)?.await })
    }

    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId {
        let (task, fence) = self.task_binding();
        lease_identity(task, operation, fence)
    }
}

pub(super) fn lease_identity(
    task: TaskId,
    operation: OperationId,
    fence: &crate::scheduler::LeaseFence,
) -> acyclic_fs::OperationLeaseId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.git-task-fence/v1");
    hash.update(&task.into_bytes());
    hash.update(&operation.into_bytes());
    for field in [&fence.placement, &fence.reservation_id] {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field.as_bytes());
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationLeaseId::from_bytes(identity)
}

impl ViewOwner for TaskContext {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move { task_owner(self)?.verify().await })
    }

    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId {
        self.owned_mailbox.as_ref().map_or_else(
            || identity("acyclic.local.git-task/v1", &[&operation.into_bytes()]),
            |owner| owner.lease(operation),
        )
    }

    fn journal(&self) -> Result<Option<Arc<crate::durable_host::TaskJournalOwner<LocalStream>>>> {
        self.ordinary_journal_owner::<LocalStream>().map(Some)
    }
}

fn task_owner(context: &TaskContext) -> Result<&Arc<dyn OwnedTaskMailbox>> {
    context
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))
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
