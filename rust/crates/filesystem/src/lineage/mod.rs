//! Durable direct-parent workspace lineage.
//!
//! Promoting a planned child publishes against one observed revision. A lost
//! conditional write returns [`WorkspaceLineageError::Contended`].

use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ForkOptions, GenerationId,
    IdempotencyKey, Workspace, WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
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

fn lineage_shape_is_valid(
    record: &WorkspaceLineageRecord,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<bool, WorkspaceError> {
    fn valid_name(
        name: &str,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceError> {
        match crate::WorkspaceName::validate_with_accounting(name, accounting) {
            Ok(_) => Ok(true),
            Err(WorkspaceError::Name(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }
    accounting.check()?;
    let parent_shape = match (
        record.parent_workspace_id,
        record.parent_workspace_name.as_deref(),
    ) {
        (None, None) => record.fork_generation == record.initial_generation,
        (Some(parent_id), Some(parent_name)) => {
            parent_id != record.workspace_id && valid_name(parent_name, accounting)?
        }
        _ => false,
    };
    Ok(record.version == LINEAGE_VERSION
        && record.revision > 0
        && valid_name(&record.workspace_name, accounting)?
        && parent_shape)
}

/// Durable optimistic-concurrency adapter for workspace lineage.
pub trait WorkspaceLineageStore: acyclic_stream::ProviderPlatform {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the record for one workspace, if registered.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<WorkspaceLineageRecord>, Self::Error>>
    + acyclic_stream::ProviderTask;

    /// Atomically replaces `expected_revision`; zero creates a record.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> impl Future<Output = Result<bool, Self::Error>> + acyclic_stream::ProviderTask;
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
    /// Registers a root workspace using the caller's work and cancellation.
    pub async fn register_root<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
        W: crate::facade::FsWork,
    >(
        &self,
        workspace: &Workspace<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check().map_err(WorkspaceError::from)?;
            if let Some(existing) = self
                .store
                .load(workspace.id())
                .await
                .map_err(WorkspaceLineageError::Store)?
            {
                if lineage_shape_is_valid(&existing, &mut accounting)?
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
                .observed_generation_accounted(
                    crate::model::GenerationSelector::Head,
                    &mut accounting,
                )
                .await?;
            self.register_absent(
                WorkspaceLineageRecord {
                    version: LINEAGE_VERSION,
                    revision: 1,
                    ready: true,
                    workspace_id: workspace.id(),
                    workspace_name: workspace.name().as_str().to_owned(),
                    parent_workspace_id: None,
                    parent_workspace_name: None,
                    fork_generation: generation.id(),
                    initial_generation: generation.id(),
                },
                &mut accounting,
            )
            .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Forks a real SDK workspace and durably records its direct parent.
    pub async fn fork<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
        W: crate::facade::FsWork,
    >(
        &self,
        parent: &Workspace<A, O>,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Workspace<A, O>, WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.lineage.fork",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        let result = crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async {
                let (destination, child_id) = parent
                    .volume
                    .fs
                    .workspace_name(destination.as_ref(), &mut accounting)?;
                let generation = parent
                    .observed_generation_accounted(
                        crate::model::GenerationSelector::Head,
                        &mut accounting,
                    )
                    .await?;
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
                let planned = self.register(planned, &mut accounting).await?;
                let child = parent
                    .fork_accounted::<_, WorkspaceError>(
                        destination.as_str(),
                        ForkOptions::from_generation(generation.clone(), idempotency_key),
                        crate::PublicationPermit::Unrestricted,
                        &mut accounting,
                        (),
                    )
                    .await?;
                let initial_generation = child
                    .observed_generation_accounted(
                        crate::model::GenerationSelector::Head,
                        &mut accounting,
                    )
                    .await?;
                if child.id() != child_id {
                    return Err(WorkspaceLineageError::ConflictingRegistration);
                }
                if planned.ready && planned.initial_generation != initial_generation.id() {
                    return Err(WorkspaceLineageError::ConflictingRegistration);
                }
                if !planned.ready {
                    let expected_revision = planned.revision;
                    let mut ready = planned;
                    ready.ready = true;
                    ready.initial_generation = initial_generation.id();
                    self.promote_ready(ready, expected_revision, &mut accounting)
                        .await?;
                }
                Ok(child)
            })
            .await,
        );
        work.finish(result, &accounting)
    }

    /// Registers a child created by another SDK-owned fork façade.
    pub async fn register_existing_child<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
        W: crate::facade::FsWork,
    >(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        self.register_child(
            parent,
            child,
            fork_generation,
            crate::model::GenerationSelector::Head,
            work,
            cancellation,
        )
        .await
    }

    /// Registers an existing child at its authenticated initial generation.
    pub async fn register_existing_child_at_generation<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
        W: crate::facade::FsWork,
    >(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
        initial_generation: crate::GenerationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        self.register_child(
            parent,
            child,
            fork_generation,
            crate::model::GenerationSelector::Exact(initial_generation),
            work,
            cancellation,
        )
        .await
    }

    async fn register_child<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
        W: crate::facade::FsWork,
    >(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
        initial: crate::model::GenerationSelector,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .register_child_accounted(parent, child, fork_generation, initial, &mut accounting)
            .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn register_child_accounted<
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    >(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        fork_generation: crate::GenerationId,
        initial: crate::model::GenerationSelector,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        accounting.check().map_err(WorkspaceError::from)?;
        // Do not let a metadata adapter manufacture publication authority.
        // The child's authenticated initial generation must be the generation
        // root produced by an SDK fork from the claimed parent generation.
        let parent_generation = parent
            .observed_generation_accounted(
                crate::model::GenerationSelector::Exact(fork_generation),
                accounting,
            )
            .await?;
        let initial = child
            .observed_generation_accounted(initial, accounting)
            .await?;
        let expected_child = parent
            .volume
            .fs
            .workspace_id_for_name(child.name(), accounting)?;
        if child.id() != expected_child {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        let parents = initial.parents_accounted(accounting).await?;
        if parents.0.as_slice() != [parent_generation.id()] {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        self.register(
            WorkspaceLineageRecord {
                version: LINEAGE_VERSION,
                revision: 1,
                ready: true,
                workspace_id: child.id(),
                workspace_name: child.name().as_str().to_owned(),
                parent_workspace_id: Some(parent.id()),
                parent_workspace_name: Some(parent.name().as_str().to_owned()),
                fork_generation,
                initial_generation: initial.id(),
            },
            accounting,
        )
        .await
    }

    /// Verifies that `parent` is the child's exact direct publication target.
    pub async fn authorize_join<W: crate::facade::FsWork>(
        &self,
        child: WorkspaceId,
        parent: WorkspaceId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result: Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> = async {
            let record = self
                .lookup(child, &mut accounting)
                .await?
                .ok_or(WorkspaceLineageError::IncompatibleState)?;
            if record.parent_workspace_id != Some(parent) {
                return Err(WorkspaceLineageError::UnauthorizedJoin);
            }
            Ok(record)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Resolves one durable workspace identity to its canonical lineage record.
    pub async fn resolve<W: crate::facade::FsWork>(
        &self,
        workspace: WorkspaceId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result: Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> = async {
            self.lookup(workspace, &mut accounting)
                .await?
                .ok_or(WorkspaceLineageError::IncompatibleState)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Returns the direct-parent chain, nearest parent first.
    pub async fn ancestors<W: crate::facade::FsWork>(
        &self,
        workspace: WorkspaceId,
        maximum: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Vec<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .ancestry_until(workspace, None, maximum, &mut accounting)
            .await;
        work.finish(result, &accounting)
    }

    /// Authenticate a workspace below an already authenticated repository root.
    /// The root itself need not have a lineage record: it is the caller's anchor.
    /// Each visited workspace is loaded once; an absent starting workspace is
    /// refused from that same observation, without a second presence check.
    pub async fn authorize_descendant<W: crate::facade::FsWork>(
        &self,
        workspace: WorkspaceId,
        ancestor: WorkspaceId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceLineageError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .ancestry_until(workspace, Some(ancestor), u32::MAX, &mut accounting)
            .await
            .map(|_| ());
        work.finish(result, &accounting)
    }

    async fn ancestry_until(
        &self,
        workspace: WorkspaceId,
        ancestor: Option<WorkspaceId>,
        maximum: u32,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Vec<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        let mut next = Some(workspace);
        let mut seen = crate::kernel::allocation::fixed_index::FixedIndex::<bool>::default();
        let mut records = Vec::new();
        while let Some(id) = next {
            accounting.check().map_err(WorkspaceError::from)?;
            if ancestor == Some(id) {
                return Ok(records);
            }
            if ancestor.is_none() && records.len() >= maximum as usize {
                return Err(WorkspaceLineageError::TraversalLimit);
            }
            let visited = seen
                .get_or_try_insert_with(id.into_bytes(), accounting, |_| Ok(false))
                .map_err(WorkspaceError::from)?;
            if std::mem::replace(visited, true) {
                return Err(WorkspaceLineageError::TraversalLimit);
            }
            let record = match self.lookup(id, accounting).await? {
                // A workspace this lineage never registered is outside it,
                // which is a definite refusal rather than corrupt state.
                None if ancestor.is_some() && id == workspace => {
                    return Err(WorkspaceLineageError::UnauthorizedJoin);
                }
                None => return Err(WorkspaceLineageError::IncompatibleState),
                Some(record) => record,
            };
            next = record.parent_workspace_id;
            if ancestor.is_none() && id != workspace {
                records.push(record);
            }
        }
        if ancestor.is_some() {
            Err(WorkspaceLineageError::UnauthorizedJoin)
        } else {
            Ok(records)
        }
    }

    async fn lookup(
        &self,
        workspace_id: WorkspaceId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Option<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        accounting.check().map_err(WorkspaceError::from)?;
        let Some(record) = self
            .store
            .load(workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
        else {
            return Ok(None);
        };
        if record.workspace_id != workspace_id || !lineage_shape_is_valid(&record, accounting)? {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if !record.ready {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        Ok(Some(record))
    }

    async fn register(
        &self,
        record: WorkspaceLineageRecord,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if !lineage_shape_is_valid(&record, accounting)? {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if let Some(existing) = self
            .store
            .load(record.workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            if lineage_shape_is_valid(&existing, accounting)? && same_lineage(&existing, &record) {
                if record.ready && !existing.ready {
                    return self
                        .promote_ready(record, existing.revision, accounting)
                        .await;
                }
                return Ok(existing);
            }
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        self.register_absent(record, accounting).await
    }

    // Absence has already been observed. A rejected creation must observe the
    // winner, but another read before the conditional write buys no guarantee.
    async fn register_absent(
        &self,
        record: WorkspaceLineageRecord,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        accounting.check().map_err(WorkspaceError::from)?;
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
        if !lineage_shape_is_valid(&existing, accounting)? || !same_lineage(&existing, &record) {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        if existing.ready == record.ready {
            Ok(existing)
        } else if record.ready && !existing.ready {
            self.promote_ready(record, existing.revision, accounting)
                .await
        } else {
            Err(WorkspaceLineageError::ConflictingRegistration)
        }
    }

    async fn promote_ready(
        &self,
        mut ready: WorkspaceLineageRecord,
        expected_revision: u64,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        accounting.check().map_err(WorkspaceError::from)?;
        // The caller validated this lineage at the observed revision. Only the
        // conditional write can authorize replacing it; do not refresh the fence.
        ready.revision = expected_revision
            .checked_add(1)
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        if self
            .store
            .compare_and_swap(ready.workspace_id, expected_revision, ready.clone())
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            return Ok(ready);
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
