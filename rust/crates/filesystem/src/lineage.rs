//! Durable direct-parent workspace lineage.
//!
//! Immutable generations retain content ancestry, while this module records the
//! customer-facing workspace relationship needed for recursive agent trees and
//! direct-parent publication authorization.  The record is deliberately small
//! and storage-neutral so distributed authority backends can persist it with
//! the same compare-and-swap discipline as workspace heads.

use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ForkOptions, GenerationId, IdempotencyKey, Workspace,
    WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Mutex;
use thiserror::Error;

const LINEAGE_VERSION: u32 = 1;

/// Durable identity and direct parent of one SDK workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceLineageRecord {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Child workspace described by this record.
    pub workspace_id: WorkspaceId,
    /// Canonical workspace name needed to reopen it after process restart.
    pub workspace_name: String,
    /// Direct parent allowed to receive publication from the child.
    pub parent_workspace_id: Option<WorkspaceId>,
    /// Canonical direct-parent name needed for restart recovery.
    pub parent_workspace_name: Option<String>,
    /// Exact parent generation from which the child was created.
    pub fork_generation: GenerationId,
    /// Exact first generation in the child workspace, used for local diffs.
    pub initial_generation: GenerationId,
}

/// Durable optimistic-concurrency adapter for workspace lineage.
pub trait WorkspaceLineageStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the record for one workspace, if registered.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<WorkspaceLineageRecord>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a record.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Failure while recording or authenticating workspace lineage.
#[derive(Debug, Error)]
pub enum WorkspaceLineageError<E: std::error::Error + 'static> {
    /// The durable adapter failed.
    #[error("workspace-lineage store failed: {0}")]
    Store(E),
    /// The workspace operation failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Persisted state uses an unsupported version or mismatched identity.
    #[error("workspace-lineage state is incompatible")]
    IncompatibleState,
    /// An existing workspace is already bound to different lineage.
    #[error("workspace lineage conflicts with its durable registration")]
    ConflictingRegistration,
    /// Publication was requested by a workspace other than the direct parent.
    #[error("only the direct parent may receive workspace publication")]
    UnauthorizedJoin,
    /// A lineage walk encountered a cycle or exceeded the caller's bound.
    #[error("workspace lineage traversal exceeded its safe bound")]
    TraversalLimit,
}

/// Core recursive-workspace graph over one durable lineage adapter.
pub struct WorkspaceGraph<S> {
    store: S,
}

impl<S> WorkspaceGraph<S> {
    /// Creates a graph over a durable state adapter.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Borrows the underlying adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S: WorkspaceLineageStore> WorkspaceGraph<S> {
    /// Registers a root workspace with no publication parent.
    pub async fn register_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if let Some(existing) = self
            .store
            .load(workspace.id())
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            if existing.version == LINEAGE_VERSION
                && existing.workspace_id == workspace.id()
                && existing.workspace_name == workspace.name().as_str()
                && existing.parent_workspace_id.is_none()
                && existing.parent_workspace_name.is_none()
            {
                return Ok(existing);
            }
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        let generation = workspace.head().await?;
        self.register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            workspace_id: workspace.id(),
            workspace_name: workspace.name().as_str().to_owned(),
            parent_workspace_id: None,
            parent_workspace_name: None,
            fork_generation: generation.id(),
            initial_generation: generation.id(),
        })
        .await
    }

    /// Forks a real SDK workspace and durably records its direct parent.
    pub async fn fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Workspace<A, O>, WorkspaceLineageError<S::Error>> {
        let generation = parent.head().await?;
        let child = parent
            .fork(
                destination,
                ForkOptions::from_generation(generation.clone(), idempotency_key),
            )
            .await?;
        let initial_generation = child.head().await?;
        self.register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            workspace_id: child.id(),
            workspace_name: child.name().as_str().to_owned(),
            parent_workspace_id: Some(parent.id()),
            parent_workspace_name: Some(parent.name().as_str().to_owned()),
            fork_generation: generation.id(),
            initial_generation: initial_generation.id(),
        })
        .await?;
        Ok(child)
    }

    /// Verifies that `parent` is the child's exact direct publication target.
    pub async fn authorize_join(
        &self,
        child: WorkspaceId,
        parent: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let record = self.required(child).await?;
        if record.parent_workspace_id != Some(parent) {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        Ok(record)
    }

    /// Resolves one durable workspace identity to its canonical lineage record.
    ///
    /// This is the restart-safe lookup used by thin adapters that retain only
    /// workspace IDs in compatibility state.
    pub async fn resolve(
        &self,
        workspace: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        self.required(workspace).await
    }

    /// Returns the direct-parent chain, nearest parent first.
    pub async fn ancestors(
        &self,
        workspace: WorkspaceId,
        maximum: u32,
    ) -> Result<Vec<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        let mut next = Some(workspace);
        let mut seen = BTreeSet::new();
        let mut records = Vec::new();
        while let Some(id) = next {
            if !seen.insert(id) || records.len() >= maximum as usize {
                return Err(WorkspaceLineageError::TraversalLimit);
            }
            let record = self.required(id).await?;
            next = record.parent_workspace_id;
            if id != workspace {
                records.push(record);
            }
        }
        Ok(records)
    }

    async fn required(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let record = self
            .store
            .load(workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        if record.version != LINEAGE_VERSION || record.workspace_id != workspace_id {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        Ok(record)
    }

    async fn register(
        &self,
        record: WorkspaceLineageRecord,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if let Some(existing) = self
            .store
            .load(record.workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            if existing.version == LINEAGE_VERSION
                && existing.workspace_id == record.workspace_id
                && existing.workspace_name == record.workspace_name
                && existing.parent_workspace_id == record.parent_workspace_id
                && existing.parent_workspace_name == record.parent_workspace_name
                && existing.fork_generation == record.fork_generation
                && existing.initial_generation == record.initial_generation
            {
                return Ok(existing);
            }
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        if self
            .store
            .compare_and_swap(record.workspace_id, 0, record.clone())
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            return Ok(record);
        }
        let existing = self.required(record.workspace_id).await?;
        if existing.parent_workspace_id == record.parent_workspace_id
            && existing.workspace_name == record.workspace_name
            && existing.parent_workspace_name == record.parent_workspace_name
            && existing.fork_generation == record.fork_generation
            && existing.initial_generation == record.initial_generation
        {
            Ok(existing)
        } else {
            Err(WorkspaceLineageError::ConflictingRegistration)
        }
    }
}

/// Process-local lineage adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryWorkspaceLineageStore {
    records: Mutex<BTreeMap<WorkspaceId, WorkspaceLineageRecord>>,
}

impl MemoryWorkspaceLineageStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local lineage synchronization failure.
#[derive(Debug, Error)]
#[error("workspace-lineage memory store is unavailable")]
pub struct MemoryWorkspaceLineageStoreError;

impl WorkspaceLineageStore for MemoryWorkspaceLineageStore {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.records
            .lock()
            .map_err(|_| MemoryWorkspaceLineageStoreError)
            .map(|records| records.get(&workspace_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MemoryWorkspaceLineageStoreError)?;
        let revision = records
            .get(&workspace_id)
            .map_or(0, |record| record.revision);
        if revision != expected_revision {
            return Ok(false);
        }
        records.insert(workspace_id, replacement);
        Ok(true)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{Digest, WorkspaceName};

    fn workspace(name: &str) -> WorkspaceId {
        WorkspaceId::derive([9; 16], &WorkspaceName::new(name).expect("valid name"))
    }

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
    }

    #[tokio::test]
    async fn direct_parent_authorization_and_recursive_walk_are_durable() {
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        let root = workspace("root");
        let child = workspace("child");
        let grandchild = workspace("grandchild");
        graph
            .register(WorkspaceLineageRecord {
                version: LINEAGE_VERSION,
                revision: 1,
                workspace_id: root,
                workspace_name: "root".to_owned(),
                parent_workspace_id: None,
                parent_workspace_name: None,
                fork_generation: generation(1),
                initial_generation: generation(1),
            })
            .await
            .expect("root registration");
        for (workspace_id, parent_workspace_id, byte) in [(child, root, 2), (grandchild, child, 3)]
        {
            graph
                .register(WorkspaceLineageRecord {
                    version: LINEAGE_VERSION,
                    revision: 1,
                    workspace_id,
                    workspace_name: if workspace_id == child {
                        "child".to_owned()
                    } else {
                        "grandchild".to_owned()
                    },
                    parent_workspace_id: Some(parent_workspace_id),
                    parent_workspace_name: Some(if parent_workspace_id == root {
                        "root".to_owned()
                    } else {
                        "child".to_owned()
                    }),
                    fork_generation: generation(byte),
                    initial_generation: generation(byte),
                })
                .await
                .expect("child registration");
        }
        graph
            .authorize_join(grandchild, child)
            .await
            .expect("direct parent");
        assert!(matches!(
            graph.authorize_join(grandchild, root).await,
            Err(WorkspaceLineageError::UnauthorizedJoin)
        ));
        let ancestors = graph
            .ancestors(grandchild, 3)
            .await
            .expect("bounded ancestry");
        assert_eq!(
            ancestors
                .iter()
                .map(|record| record.workspace_id)
                .collect::<Vec<_>>(),
            vec![child, root]
        );
    }
}
