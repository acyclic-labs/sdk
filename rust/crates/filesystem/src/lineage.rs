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
const MAXIMUM_LINEAGE_CAS_ATTEMPTS: usize = 16;

/// Durable identity and direct parent of one SDK workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceLineageRecord {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Whether the planned child authority and its initial generation exist.
    pub ready: bool,
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

fn lineage_shape_is_valid(record: &WorkspaceLineageRecord) -> bool {
    let parent_shape = match (
        record.parent_workspace_id,
        record.parent_workspace_name.as_deref(),
    ) {
        (None, None) => record.fork_generation == record.initial_generation,
        (Some(parent_id), Some(parent_name)) => {
            parent_id != record.workspace_id && crate::WorkspaceName::new(parent_name).is_ok()
        }
        _ => false,
    };
    record.version == LINEAGE_VERSION
        && record.revision > 0
        && crate::WorkspaceName::new(&record.workspace_name).is_ok()
        && parent_shape
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
    /// Optimistic registration contention exceeded the bounded retry policy.
    #[error("workspace lineage registration remained contended")]
    Contended,
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
            if lineage_shape_is_valid(&existing)
                && existing.ready
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
            ready: true,
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
        let destination =
            crate::WorkspaceName::new(destination.as_ref()).map_err(WorkspaceError::from)?;
        let generation = parent.head().await?;
        let child_id = parent
            .fork_workspace_id(destination.as_str())
            .map_err(WorkspaceError::from)?;
        let planned = WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: false,
            workspace_id: child_id,
            workspace_name: destination.as_str().to_owned(),
            parent_workspace_id: Some(parent.id()),
            parent_workspace_name: Some(parent.name().as_str().to_owned()),
            fork_generation: generation.id(),
            initial_generation: generation.id(),
        };
        let planned = self.register(planned).await?;
        let child = parent
            .fork(
                destination.as_str(),
                ForkOptions::from_generation(generation.clone(), idempotency_key),
            )
            .await?;
        let initial_generation = child.head().await?;
        if child.id() != child_id {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        if planned.ready && planned.initial_generation != initial_generation.id() {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        let mut ready = planned;
        ready.ready = true;
        ready.initial_generation = initial_generation.id();
        self.promote_ready(ready).await?;
        Ok(child)
    }

    /// Registers a child created by another SDK-owned fork façade.
    ///
    /// Lazy workspaces use this after their source-overlay binding and physical
    /// workspace fork have both become durable. Repeating the exact lineage is
    /// idempotent; a competing parent or fork point is rejected.
    pub async fn register_existing_child<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let initial_generation = child.head().await?.id();
        self.register_existing_child_at_generation(
            parent,
            child,
            fork_generation,
            initial_generation,
        )
        .await
    }

    /// Registers an existing child after it has advanced beyond its initial
    /// fork generation, while still authenticating that exact fork point.
    pub async fn register_existing_child_at_generation<
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    >(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
        initial_generation: crate::GenerationId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        // Do not let a metadata adapter manufacture publication authority.
        // The child's authenticated initial generation must be the generation
        // root produced by an SDK fork from the claimed parent generation.
        parent.generation(fork_generation).await?;
        let initial = child.generation(initial_generation).await?;
        let expected_child = parent
            .fork_workspace_id(child.name().as_str())
            .map_err(WorkspaceError::from)?;
        if child.id() != expected_child {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        if initial.parents().await?.as_slice() != [fork_generation] {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        self.register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: child.id(),
            workspace_name: child.name().as_str().to_owned(),
            parent_workspace_id: Some(parent.id()),
            parent_workspace_name: Some(parent.name().as_str().to_owned()),
            fork_generation,
            initial_generation,
        })
        .await
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
        if record.workspace_id != workspace_id || !lineage_shape_is_valid(&record) {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if !record.ready {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        Ok(record)
    }

    async fn register(
        &self,
        record: WorkspaceLineageRecord,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if !lineage_shape_is_valid(&record) {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if let Some(existing) = self
            .store
            .load(record.workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            if lineage_shape_is_valid(&existing) && same_lineage(&existing, &record) {
                if record.ready && !existing.ready {
                    return self.promote_ready(record).await;
                }
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
        let existing = self
            .store
            .load(record.workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        if lineage_shape_is_valid(&existing)
            && same_lineage(&existing, &record)
            && existing.ready == record.ready
        {
            Ok(existing)
        } else if lineage_shape_is_valid(&existing)
            && same_lineage(&existing, &record)
            && record.ready
            && !existing.ready
        {
            self.promote_ready(record).await
        } else {
            Err(WorkspaceLineageError::ConflictingRegistration)
        }
    }

    async fn promote_ready(
        &self,
        ready: WorkspaceLineageRecord,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if !ready.ready || !lineage_shape_is_valid(&ready) {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        for _ in 0..MAXIMUM_LINEAGE_CAS_ATTEMPTS {
            let existing = self
                .store
                .load(ready.workspace_id)
                .await
                .map_err(WorkspaceLineageError::Store)?
                .ok_or(WorkspaceLineageError::IncompatibleState)?;
            if !lineage_shape_is_valid(&existing) || !same_lineage(&existing, &ready) {
                return Err(WorkspaceLineageError::ConflictingRegistration);
            }
            if existing.ready {
                return Ok(existing);
            }
            let mut replacement = ready.clone();
            replacement.revision = existing
                .revision
                .checked_add(1)
                .ok_or(WorkspaceLineageError::IncompatibleState)?;
            if self
                .store
                .compare_and_swap(ready.workspace_id, existing.revision, replacement.clone())
                .await
                .map_err(WorkspaceLineageError::Store)?
            {
                return Ok(replacement);
            }
        }
        Err(WorkspaceLineageError::Contended)
    }
}

fn same_lineage(left: &WorkspaceLineageRecord, right: &WorkspaceLineageRecord) -> bool {
    left.version == right.version
        && left.workspace_id == right.workspace_id
        && left.workspace_name == right.workspace_name
        && left.parent_workspace_id == right.parent_workspace_id
        && left.parent_workspace_name == right.parent_workspace_name
        && left.fork_generation == right.fork_generation
        && (!left.ready || !right.ready || left.initial_generation == right.initial_generation)
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
    use crate::{Digest, Fs, WorkspaceName};

    fn workspace(name: &str) -> WorkspaceId {
        WorkspaceId::derive([9; 16], &WorkspaceName::new(name).expect("valid name"))
    }

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
    }

    #[tokio::test]
    async fn malformed_lineage_shape_never_enters_the_graph() {
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        let child = workspace("child");
        let malformed = WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: child,
            workspace_name: "child".to_owned(),
            parent_workspace_id: Some(child),
            parent_workspace_name: None,
            fork_generation: generation(1),
            initial_generation: generation(2),
        };

        assert!(matches!(
            graph.register(malformed).await,
            Err(WorkspaceLineageError::IncompatibleState)
        ));
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
                ready: true,
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
                    ready: true,
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

    #[tokio::test]
    async fn a_durable_planned_child_is_completed_by_retry_after_a_crash() {
        let fs = Fs::memory();
        let parent = fs.create_workspace("root").await.expect("root");
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        graph.register_root(&parent).await.expect("register root");
        let parent_head = parent.head().await.expect("parent head");
        let child_id = parent
            .fork_workspace_id("child")
            .expect("derived child identity");
        let planned = WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: false,
            workspace_id: child_id,
            workspace_name: "child".to_owned(),
            parent_workspace_id: Some(parent.id()),
            parent_workspace_name: Some(parent.name().as_str().to_owned()),
            fork_generation: parent_head.id(),
            initial_generation: parent_head.id(),
        };
        graph.register(planned).await.expect("persist fork intent");
        assert!(matches!(
            graph.resolve(child_id).await,
            Err(WorkspaceLineageError::IncompatibleState)
        ));

        let child = graph
            .fork(&parent, "child", IdempotencyKey::from_bytes([31; 16]))
            .await
            .expect("retry completes child");
        assert_eq!(child.id(), child_id);
        let recovered = graph.resolve(child_id).await.expect("ready lineage");
        assert!(recovered.ready);
        assert_eq!(recovered.parent_workspace_id, Some(parent.id()));
        assert_eq!(
            recovered.initial_generation,
            child.head().await.expect("head").id()
        );
    }
}
