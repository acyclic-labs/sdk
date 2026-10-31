//! Durable direct-parent workspace lineage.
//!
//! Promoting a planned child publishes against one observed revision. A lost
//! conditional write returns [`WorkspaceLineageError::Contended`].

use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ForkOptions, GenerationId,
    IdempotencyKey, WorkBudget, Workspace, WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use thiserror::Error;

const LINEAGE_VERSION: u32 = 1;

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
            parent_id != record.workspace_id
                && crate::WorkspaceName::new(
                    parent_name,
                    &WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .is_ok()
        }
        _ => false,
    };
    record.version == LINEAGE_VERSION
        && record.revision > 0
        && crate::WorkspaceName::new(
            &record.workspace_name,
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .is_ok()
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
#[derive(Debug, Error, strum::IntoStaticStr)]
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
    /// The conditional publication lost its expected revision.
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
    pub async fn register_root<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
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
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
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
    pub async fn fork<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        &self,
        parent: &Workspace<A, O>,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Workspace<A, O>, WorkspaceLineageError<S::Error>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.lineage.fork",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                let destination = crate::WorkspaceName::new(
                    destination.as_ref(),
                    &WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .map_err(|failure| failure.error)?
                .value;
                let generation = parent
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                let child_id = parent
                    .volume
                    .fs
                    .workspace_id(destination.as_str())
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
                    .fork::<_, _, WorkspaceError>(
                        destination.as_str(),
                        ForkOptions::from_generation(generation.clone(), idempotency_key),
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                        &mut |_| {
                            std::future::ready(Ok(crate::OperationReceipt {
                                value: (),
                                work: crate::WorkCounters::UNCHARGED,
                            }))
                        },
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                let initial_generation = child
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
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
            })
            .await,
        )
    }

    /// Registers a child created by another SDK-owned fork façade.
    pub async fn register_existing_child<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    >(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let initial_generation = child
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .id();
        self.register_existing_child_at_generation(
            parent,
            child,
            fork_generation,
            initial_generation,
        )
        .await
    }

    /// Registers an existing child after it has advanced beyond its initial
    pub async fn register_existing_child_at_generation<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
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
        parent
            .generation(
                fork_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
        let initial = child
            .generation(
                initial_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let expected_child = parent
            .volume
            .fs
            .workspace_id(child.name().as_str())
            .map_err(WorkspaceError::from)?;
        if child.id() != expected_child {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        if initial
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .as_slice()
            != [fork_generation]
        {
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
        self.ancestry_until(workspace, None, maximum).await
    }

    /// Authenticate a workspace below an already authenticated repository root.
    /// The root itself need not have a lineage record: it is the caller's anchor.
    pub async fn authorize_descendant(
        &self,
        workspace: WorkspaceId,
        ancestor: WorkspaceId,
    ) -> Result<(), WorkspaceLineageError<S::Error>> {
        self.ancestry_until(workspace, Some(ancestor), u32::MAX)
            .await
            .map(|_| ())
    }

    async fn ancestry_until(
        &self,
        workspace: WorkspaceId,
        ancestor: Option<WorkspaceId>,
        maximum: u32,
    ) -> Result<Vec<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        let mut next = Some(workspace);
        let mut seen = BTreeSet::new();
        let mut records = Vec::new();
        while let Some(id) = next {
            if ancestor == Some(id) {
                return Ok(records);
            }
            if !seen.insert(id) || records.len() >= maximum as usize {
                return Err(WorkspaceLineageError::TraversalLimit);
            }
            let record = self.required(id).await?;
            next = record.parent_workspace_id;
            if id != workspace {
                records.push(record);
            }
        }
        if ancestor.is_some() {
            Err(WorkspaceLineageError::UnauthorizedJoin)
        } else {
            Ok(records)
        }
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

mod memory;
pub use memory::{MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError};

#[cfg(test)]
mod tests;
