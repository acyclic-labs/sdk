//! Durable direct-parent workspace lineage.
//!
//! Immutable generations retain content ancestry, while this module records the
//! customer-facing workspace relationship needed for recursive agent trees and
//! direct-parent publication authorization.  The record is deliberately small
//! and storage-neutral so distributed authority backends can persist it with
//! the same compare-and-swap discipline as workspace heads.

use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, MemoryRecords};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ForkOptions, GenerationId, IdempotencyKey, Workspace,
    WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};

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
            parent_id != record.workspace_id && crate::WorkspaceName::new(parent_name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).is_ok()
        }
        _ => false,
    };
    record.version == LINEAGE_VERSION
        && record.revision > 0
        && crate::WorkspaceName::new(&record.workspace_name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).is_ok()
        && parent_shape
}

#[cfg(not(target_arch = "wasm32"))]
type LineageAllocation = crate::NativeAllocation;
#[cfg(target_arch = "wasm32")]
type LineageAllocation = ();
type LineageFailure<E> = (WorkspaceLineageError<E>, Option<LineageAllocation>);

struct LineageOwnedRecord {
    record: WorkspaceLineageRecord,
    allocation: Option<LineageAllocation>,
}

enum LineageRegistrationWork<'a> {
    Ordinary(std::marker::PhantomData<&'a ()>),
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    },
}

impl LineageRegistrationWork<'_> {
    const fn ordinary() -> Self { Self::Ordinary(std::marker::PhantomData) }

    fn copy_name<E: std::error::Error + 'static>(
        &mut self, name: &str,
    ) -> Result<(String, Option<LineageAllocation>), LineageFailure<E>> {
        match self {
            Self::Ordinary(_) => Ok((name.to_owned(), None)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation } => {
                let mut accounting = KernelAccounting::native(original, cancellation);
                let copied = crate::WorkCounters { bytes_copied: name.len() as u64,
                    ..crate::WorkCounters::UNCHARGED };
                accounting.admit(copied).map_err(lineage_filesystem_failure)?;
                let allocation = accounting.claim_owned(name.len() as u64, u64::from(!name.is_empty()))
                    .map_err(lineage_filesystem_failure)?;
                let mut owner = (String::new(), allocation);
                owner.0.try_reserve_exact(name.len()).map_err(|error|
                    lineage_filesystem_failure(crate::NativeTreeMaterializationError::AllocationFailed(error).into()))?;
                accounting.reconcile_owned(&mut owner.1, name.len() as u64, owner.0.capacity() as u64)
                    .map_err(lineage_filesystem_failure)?;
                owner.0.push_str(name);
                accounting.record_completed(copied).map_err(lineage_filesystem_failure)?;
                let KernelOwnedAllocation::NativeOriginal(allocation) = owner.1 else {
                    unreachable!("native name retains original custody")
                };
                Ok((owner.0, Some(allocation)))
            }
        }
    }

    fn clone_record<E: std::error::Error + 'static>(
        &mut self, source: &WorkspaceLineageRecord,
    ) -> Result<LineageOwnedRecord, LineageFailure<E>> {
        let name_owner = self.copy_name(&source.workspace_name)?;
        #[cfg(not(target_arch = "wasm32"))]
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<WorkspaceLineageRecord>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Native { original, cancellation } = self {
            KernelAccounting::native(original, cancellation).admit(copied).map_err(lineage_filesystem_failure)?;
        }
        let mut owned = LineageOwnedRecord {
            record: WorkspaceLineageRecord {
                version: source.version, revision: source.revision, ready: source.ready,
                workspace_id: source.workspace_id, workspace_name: name_owner.0,
                parent_workspace_id: source.parent_workspace_id, parent_workspace_name: None,
                fork_generation: source.fork_generation, initial_generation: source.initial_generation,
            },
            allocation: name_owner.1,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Native { original, cancellation } = self {
            KernelAccounting::native(original, cancellation).record_completed(copied).map_err(lineage_filesystem_failure)?;
        }
        if let Some(parent_name) = &source.parent_workspace_name {
            let parent_owner = self.copy_name(parent_name)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(mut incoming) = parent_owner.1 {
                owned.allocation.as_mut().expect("native metadata retains its name guard")
                    .absorb(&mut incoming).map_err(|error| lineage_filesystem_failure(error.into()))?;
            }
            owned.record.parent_workspace_name = Some(parent_owner.0);
        }
        Ok(owned)
    }

    fn shape<E: std::error::Error + 'static>(
        &mut self, record: &WorkspaceLineageRecord,
    ) -> Result<bool, LineageFailure<E>> {
        match self {
            Self::Ordinary(_) => Ok(lineage_shape_is_valid(record)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation } => {
                let valid_name = |name: &str| match crate::WorkspaceName::new(name, *original, cancellation) {
                    Ok(_) => Ok(true),
                    Err(WorkspaceError::Name(crate::WorkspaceNameError::Empty
                        | crate::WorkspaceNameError::Reserved | crate::WorkspaceNameError::TooLong
                        | crate::WorkspaceNameError::InvalidCharacter)) => Ok(false),
                    Err(error) => Err((WorkspaceLineageError::Workspace(error), None)),
                };
                let parent_shape = match (record.parent_workspace_id, record.parent_workspace_name.as_deref()) {
                    (None, None) => record.fork_generation == record.initial_generation,
                    (Some(parent), Some(name)) => parent != record.workspace_id && valid_name(name)?,
                    _ => false,
                };
                Ok(record.version == LINEAGE_VERSION && record.revision > 0
                    && valid_name(&record.workspace_name)? && parent_shape)
            }
        }
    }

    async fn load<S: WorkspaceLineageStore>(
        &mut self, store: &S, id: WorkspaceId,
    ) -> Result<Option<LineageOwnedRecord>, LineageFailure<S::Error>> {
        match self {
            Self::Ordinary(_) => store.load(id).await
                .map(|value| value.map(|record| LineageOwnedRecord { record, allocation: None }))
                .map_err(|error| (WorkspaceLineageError::Store(error), None)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation } => store.load_with_native_work(id, original, cancellation)
                .await.map(|value| value.map(|owned| {
                    let (record, allocation) = owned;
                    LineageOwnedRecord { record, allocation: Some(allocation) }
                })),
        }
    }

    async fn compare_and_swap<S: WorkspaceLineageStore>(
        &mut self, store: &S, id: WorkspaceId, expected: u64, owned: LineageOwnedRecord,
    ) -> Result<bool, LineageFailure<S::Error>> {
        match self {
            Self::Ordinary(_) => store.compare_and_swap(id, expected, owned.record).await
                .map_err(|error| (WorkspaceLineageError::Store(error), None)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation } => store.compare_and_swap_with_native_work(
                id, expected, (owned.record,
                    owned.allocation.expect("native replacement retains original custody")),
                original, cancellation,
            ).await,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn lineage_filesystem_failure<E: std::error::Error + 'static>(error: crate::FsError) -> LineageFailure<E> {
    (WorkspaceLineageError::Workspace(WorkspaceError::from(error)), None)
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

    /// Loads actual metadata with its producer-funded original allocation custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn load_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<Option<(WorkspaceLineageRecord, crate::NativeAllocation)>,
        (WorkspaceLineageError<Self::Error>, Option<crate::NativeAllocation>)>> + Send {
        async move {
            KernelAccounting::native(original, cancellation).check().map_err(lineage_filesystem_failure)?;
            original.cancellation().check().map_err(|error|
                (WorkspaceLineageError::Workspace(WorkspaceError::Cancelled(error)), None))?;
            let _ = workspace_id;
            Err((WorkspaceLineageError::NativeOriginalUnsupported, None))
        }
    }

    /// Transfers an original-funded replacement into the canonical revision CAS.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_swap_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: (WorkspaceLineageRecord, crate::NativeAllocation),
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<bool,
        (WorkspaceLineageError<Self::Error>, Option<crate::NativeAllocation>)>> + Send {
        async move {
            KernelAccounting::native(original, cancellation).check().map_err(lineage_filesystem_failure)?;
            original.cancellation().check().map_err(|error|
                (WorkspaceLineageError::Workspace(WorkspaceError::Cancelled(error)), None))?;
            let _ = (workspace_id, expected_revision, replacement);
            Err((WorkspaceLineageError::NativeOriginalUnsupported, None))
        }
    }
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
    /// Optimistic registration contention exceeded the bounded retry policy.
    #[error("workspace lineage registration remained contended")]
    Contended,
    /// Publication was requested by a workspace other than the direct parent.
    #[error("only the direct parent may receive workspace publication")]
    UnauthorizedJoin,
    /// A lineage walk encountered a cycle or exceeded the caller's bound.
    #[error("workspace lineage traversal exceeded its safe bound")]
    TraversalLimit,
    /// A custom adapter cannot fund its real metadata engine in the original work.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("workspace-lineage store does not support original native work")]
    NativeOriginalUnsupported,
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
        self.register_root_engine(workspace, &mut LineageRegistrationWork::ordinary())
            .await.map(|owned| owned.record).map_err(|(error, _)| error)
    }

    /// Registers the authentic root using the invocation's original native work.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn register_root_with_native_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(WorkspaceLineageRecord, crate::NativeAllocation), (WorkspaceLineageError<S::Error>, Option<crate::NativeAllocation>)> {
        let mut control = LineageRegistrationWork::Native { original, cancellation };
        let owned = self.register_root_engine(workspace, &mut control).await?;
        Ok((owned.record,
            owned.allocation.expect("native registration retains original custody")))
    }

    async fn register_root_engine<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self, workspace: &Workspace<A, O>, control: &mut LineageRegistrationWork<'_>,
    ) -> Result<LineageOwnedRecord, LineageFailure<S::Error>> {
        if let Some(existing) = control.load(&self.store, workspace.id()).await? {
            if control.shape(&existing.record)?
                && existing.record.ready
                && existing.record.workspace_id == workspace.id()
                && existing.record.workspace_name == workspace.name().as_str()
                && existing.record.parent_workspace_id.is_none()
                && existing.record.parent_workspace_name.is_none()
            {
                return Ok(existing);
            }
            return Err((WorkspaceLineageError::ConflictingRegistration, None));
        }
        let generation = match control {
            LineageRegistrationWork::Ordinary(_) => workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(|error| (WorkspaceLineageError::Workspace(error), None))?,
            #[cfg(not(target_arch = "wasm32"))]
            LineageRegistrationWork::Native { original, cancellation } =>
                workspace.head(*original, cancellation).await.map_err(|error| (error, None))
                    .map_err(|(error, allocation)| (WorkspaceLineageError::Workspace(error), allocation))?,
        };
        let (workspace_name, allocation) = control.copy_name(workspace.name().as_str())?;
        self.register_engine(LineageOwnedRecord {
            record: WorkspaceLineageRecord {
                version: LINEAGE_VERSION,
                revision: 1,
                ready: true,
                workspace_id: workspace.id(),
                workspace_name,
                parent_workspace_id: None,
                parent_workspace_name: None,
                fork_generation: generation.id(),
                initial_generation: generation.id(),
            },
            allocation,
        }, control).await
    }

    /// Forks a real SDK workspace and durably records its direct parent.
    pub async fn fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
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
                let destination = crate::WorkspaceName::new(destination.as_ref(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                let generation = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                let child_id = parent
                    .volume.fs.workspace_id(destination.as_str())
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
                    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                let initial_generation = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
        let initial_generation = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
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
        parent.generation(fork_generation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let initial = child.generation(initial_generation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let expected_child = parent
            .volume.fs.workspace_id(child.name().as_str())
            .map_err(WorkspaceError::from)?;
        if child.id() != expected_child {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        if initial.parents(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_slice() != [fork_generation] {
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

    /// Reopens one durable workspace and checks its exact identity.
    pub async fn workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self, fs: &crate::Fs<A, O>, workspace_id: WorkspaceId,
        budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> Result<Workspace<A, O>, WorkspaceLineageError<S::Error>> {
        let record = self.resolve(workspace_id).await?;
        let workspace = fs.open_workspace(&record.workspace_name, budget, cancellation).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        if workspace.id() != workspace_id { return Err(WorkspaceLineageError::IncompatibleState); }
        Ok(workspace)
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
        self.register_engine(LineageOwnedRecord { record, allocation: None },
            &mut LineageRegistrationWork::ordinary()).await
            .map(|owned| owned.record).map_err(|(error, _)| error)
    }

    async fn register_engine(
        &self, record: LineageOwnedRecord, control: &mut LineageRegistrationWork<'_>,
    ) -> Result<LineageOwnedRecord, LineageFailure<S::Error>> {
        if !control.shape(&record.record)? {
            return Err((WorkspaceLineageError::IncompatibleState, None));
        }
        if let Some(existing) = control.load(&self.store, record.record.workspace_id).await? {
            if control.shape(&existing.record)? && same_lineage(&existing.record, &record.record) {
                if record.record.ready && !existing.record.ready {
                    return self.promote_ready_engine(record, control).await;
                }
                return Ok(existing);
            }
            return Err((WorkspaceLineageError::ConflictingRegistration, None));
        }
        let replacement = control.clone_record(&record.record)?;
        if control.compare_and_swap(&self.store, record.record.workspace_id, 0, replacement).await? {
            return Ok(record);
        }
        let existing = control.load(&self.store, record.record.workspace_id).await?
            .ok_or((WorkspaceLineageError::IncompatibleState, None))?;
        if control.shape(&existing.record)? && same_lineage(&existing.record, &record.record)
            && existing.record.ready == record.record.ready
        {
            Ok(existing)
        } else if control.shape(&existing.record)? && same_lineage(&existing.record, &record.record)
            && record.record.ready && !existing.record.ready
        {
            self.promote_ready_engine(record, control).await
        } else {
            Err((WorkspaceLineageError::ConflictingRegistration, None))
        }
    }

    async fn promote_ready(
        &self,
        ready: WorkspaceLineageRecord,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        self.promote_ready_engine(LineageOwnedRecord { record: ready, allocation: None },
            &mut LineageRegistrationWork::ordinary()).await
            .map(|owned| owned.record).map_err(|(error, _)| error)
    }

    async fn promote_ready_engine(
        &self, ready: LineageOwnedRecord, control: &mut LineageRegistrationWork<'_>,
    ) -> Result<LineageOwnedRecord, LineageFailure<S::Error>> {
        if !ready.record.ready || !control.shape(&ready.record)? {
            return Err((WorkspaceLineageError::IncompatibleState, None));
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let existing = control.load(&self.store, ready.record.workspace_id).await?
                .ok_or((WorkspaceLineageError::IncompatibleState, None))?;
            if !control.shape(&existing.record)? || !same_lineage(&existing.record, &ready.record) {
                return Err((WorkspaceLineageError::ConflictingRegistration, None));
            }
            if existing.record.ready { return Ok(existing); }
            let mut replacement = control.clone_record(&ready.record)?;
            replacement.record.revision = existing.record.revision.checked_add(1)
                .ok_or((WorkspaceLineageError::IncompatibleState, None))?;
            let result = control.clone_record(&replacement.record)?;
            if control.compare_and_swap(&self.store, ready.record.workspace_id,
                existing.record.revision, replacement).await?
            {
                return Ok(result);
            }
        }
        Err((WorkspaceLineageError::Contended, None))
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
    records: MemoryRecords<WorkspaceId, MemoryLineageRecord>,
}

struct MemoryLineageRecord {
    owned: LineageOwnedRecord,
    entry_allocation: Option<LineageAllocation>,
}

impl MemoryWorkspaceLineageStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn load_engine(
        &self, workspace_id: WorkspaceId, control: &mut LineageRegistrationWork<'_>,
    ) -> Result<Option<LineageOwnedRecord>, LineageFailure<MemoryWorkspaceLineageStoreError>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let LineageRegistrationWork::Native { original, cancellation } = control {
            KernelAccounting::native(original, cancellation).charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(lineage_filesystem_failure)?;
        }
        let records = self.records.0.lock()
            .map_err(|_| (WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError), None))?;
        records.get(&workspace_id).map(|stored| control.clone_record(&stored.owned.record)).transpose()
    }

    fn compare_and_swap_engine(
        &self, workspace_id: WorkspaceId, expected_revision: u64,
        replacement: LineageOwnedRecord, _control: &mut LineageRegistrationWork<'_>,
    ) -> Result<bool, LineageFailure<MemoryWorkspaceLineageStoreError>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let LineageRegistrationWork::Native { original, cancellation } = _control {
            let allocation = replacement.allocation.as_ref()
                .ok_or_else(|| lineage_filesystem_failure(crate::NativeTreeMaterializationError::InvalidLayout.into()))?;
            let actual = replacement.record.workspace_name.capacity().checked_add(
                replacement.record.parent_workspace_name.as_ref().map_or(0, String::capacity))
                .ok_or_else(|| lineage_filesystem_failure(crate::WorkError::Overflow.into()))? as u64;
            if !allocation.work().shares_ledger(original) || allocation.bytes() < actual {
                return Err(lineage_filesystem_failure(crate::NativeTreeMaterializationError::InvalidLayout.into()));
            }
            KernelAccounting::native(original, cancellation).charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(lineage_filesystem_failure)?;
        }
        let mut records = self.records.0.lock()
            .map_err(|_| (WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError), None))?;
        if records.get(&workspace_id).map_or(0, |stored| stored.owned.record.revision) != expected_revision {
            return Ok(false);
        }
        let mut entry_allocation = None;
        #[cfg(not(target_arch = "wasm32"))]
        if !records.contains_key(&workspace_id) {
            if let LineageRegistrationWork::Native { original, cancellation } = _control {
                // Logical entry storage is known; opaque B-tree nodes are not guessed.
                let allocation = KernelAccounting::native(original, cancellation).claim_owned(
                    std::mem::size_of::<(WorkspaceId, MemoryLineageRecord)>() as u64, 0,
                ).map_err(lineage_filesystem_failure)?;
                let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                    unreachable!("native entry retains original custody")
                };
                entry_allocation = Some(allocation);
            }
        }
        if let Some(stored) = records.get_mut(&workspace_id) {
            // Replacing metadata reuses the actual existing entry, not a newly adopted one.
            entry_allocation = stored.entry_allocation.take();
        }
        records.insert(workspace_id, MemoryLineageRecord { owned: replacement, entry_allocation });
        Ok(true)
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
        self.load_engine(workspace_id, &mut LineageRegistrationWork::ordinary())
            .map(|value| value.map(|owned| owned.record))
            .map_err(|(error, _)| match error {
                WorkspaceLineageError::Store(error) => error,
                _ => unreachable!("ordinary memory load only fails at its lock"),
            })
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_engine(workspace_id, expected_revision,
            LineageOwnedRecord { record: replacement, allocation: None },
            &mut LineageRegistrationWork::ordinary())
            .map_err(|(error, _)| match error {
                WorkspaceLineageError::Store(error) => error,
                _ => unreachable!("ordinary memory compare-and-swap only fails at its lock"),
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self, workspace_id: WorkspaceId, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<(WorkspaceLineageRecord, crate::NativeAllocation)>,
        (WorkspaceLineageError<Self::Error>, Option<crate::NativeAllocation>)> {
        let mut control = LineageRegistrationWork::Native { original, cancellation };
        self.load_engine(workspace_id, &mut control).map(|value| value.map(|owned|
            (owned.record,
                owned.allocation.expect("native load retains original custody"))))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self, workspace_id: WorkspaceId, expected_revision: u64,
        replacement: (WorkspaceLineageRecord, crate::NativeAllocation),
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<bool, (WorkspaceLineageError<Self::Error>, Option<crate::NativeAllocation>)> {
        let (record, allocation) = replacement;
        self.compare_and_swap_engine(workspace_id, expected_revision,
            LineageOwnedRecord { record, allocation: Some(allocation) },
            &mut LineageRegistrationWork::Native { original, cancellation })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Digest, Fs, WorkspaceName};

    fn workspace(name: &str) -> WorkspaceId {
        WorkspaceId::derive([9; 16], &WorkspaceName::new(name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("valid name").value, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded workspace identity").value
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
        let parent = fs.create_workspace("root", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("root");
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        graph.register_root(&parent).await.expect("register root");
        let parent_head = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("parent head");
        let child_id = parent
            .volume.fs.workspace_id("child")
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
            child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("head").id()
        );
    }
}
