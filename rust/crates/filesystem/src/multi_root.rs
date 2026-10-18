//! Durable all-roots publication decisions.
//!
//! Each root still uses the ordinary immutable join/candidate APIs. This
//! coordinator validates every pinned root before recording one commit
//! decision, then rolls all roots forward idempotently. A changed target after
//! the decision pauses recovery instead of overwriting external work.

use crate::record_store::{MemoryRecords, next_revision};
use crate::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ConflictKey,
    ConflictSide, DefaultTextMergeDriver, DriverError, ForkOptions, GenerationId, IdempotencyKey,
    JoinOutcome, MemoryMergeResolutionCache, MergeDriver, MergeDriverRegistry,
    MergePlanResolutionError, MergeResolution, OperationId, PublicationPermit,
    PublicationReservation, ReservationOutcome, UnpublishedMergeCandidate, WorkBudget, Workspace,
    WorkspaceContextError, WorkspaceContextId, WorkspaceContextRegistry, WorkspaceContextStore,
    WorkspaceDelete, WorkspaceError, WorkspaceGraph, WorkspaceId, WorkspaceLineageError,
    WorkspaceLineageStore, WorkspaceName, WorkspaceRootId,
};
use acyclic_stream::ProviderTask;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Mutex;
use thiserror::Error;

pub(crate) const MULTI_ROOT_VERSION: u32 = 1;

/// One root pinned by an immutable cross-root merge plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRootMergeRoot {
    /// Workspace whose pinned generation is being published.
    pub source_workspace_id: WorkspaceId,
    /// Optional filtered compatibility fork authenticated as a direct child of `source_workspace_id`.
    pub merge_workspace_id: Option<WorkspaceId>,
    /// Child generation captured when planning began.
    pub source_generation: GenerationId,
    /// Exact direct-parent workspace.
    pub target_workspace_id: WorkspaceId,
    /// Target head that must validate before the commit decision.
    pub target_generation: GenerationId,
    /// Three-way common ancestor authenticated by the per-root join plan.
    pub base_generation: GenerationId,
    /// Caller-selected child-wins binding paths pinned with the publication journal.
    pub child_wins_bindings: BTreeSet<String>,
}

/// Immutable pinned plan for one logical multi-root publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRootMergePlan {
    /// Stable retry and recovery identity.
    pub operation_id: OperationId,
    /// Direct-parent context receiving the publication.
    pub parent_context_id: WorkspaceContextId,
    /// Exact child context being published.
    pub child_context_id: WorkspaceContextId,
    /// Non-empty ordered root set.
    pub roots: BTreeMap<WorkspaceRootId, MultiRootMergeRoot>,
}

/// Caller-selected declarative resolutions bound to an exact plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRootMergeCandidate {
    /// Exact immutable plan.
    pub plan: MultiRootMergePlan,
    /// Per-root conflict declarations. Empty maps mean no conflicts.
    pub resolutions: BTreeMap<WorkspaceRootId, BTreeMap<ConflictKey, MergeResolution>>,
}

/// Opaque durable reservation proving that one target was validated and fenced for an exact publication operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRootFence {
    /// Publisher-defined stable reservation token.
    pub token: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct WorkspacePublicationFence {
    target_workspace_id: WorkspaceId,
    reservation: PublicationReservation,
}

/// Durable logical publication phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MultiRootPublicationPhase {
    /// Every root is being checked before any durable commit decision.
    Validating,
    /// Typed conflicts are durable and await declarative resolution.
    Conflicted,
    /// A conflicted working state is being restored to its pre-merge state.
    Aborting,
    /// The all-roots roll-forward decision is durable.
    Committed,
    /// At least one root has been advanced.
    Publishing,
    /// Every root is durable.
    Applied,
    /// Unexpected concurrent target state requires reconciliation.
    Paused,
}

/// Complete recoverable cross-root publication record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRootPublication {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Validated unpublished candidate.
    pub candidate: MultiRootMergeCandidate,
    /// Durable decision/progress phase.
    pub phase: MultiRootPublicationPhase,
    /// Roots known durable under the commit decision.
    pub published_roots: BTreeSet<WorkspaceRootId>,
    /// Exact durable generation produced for each published root.
    pub published_generations: BTreeMap<WorkspaceRootId, GenerationId>,
    /// Per-root reservations acquired before the commit decision.
    pub fences: BTreeMap<WorkspaceRootId, MultiRootFence>,
    /// Immutable typed conflicts pinned during validation.
    pub conflicts: BTreeMap<WorkspaceRootId, crate::MergePlan>,
    /// Parent generations containing the durable conflict projection.
    pub projected_roots: BTreeMap<WorkspaceRootId, GenerationId>,
    /// Conflict keys explicitly accepted from the projected working copy.
    pub declared_conflicts: BTreeMap<WorkspaceRootId, BTreeSet<ConflictKey>>,
    /// Root that observed unexpected target state, if paused.
    pub paused_root: Option<WorkspaceRootId>,
}

/// Result of validating and reserving one root before the commit decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MultiRootPrepare {
    /// The exact target is reserved for publication.
    Ready(MultiRootFence),
    /// The exact target changed before it could be reserved.
    Stale,
    /// Immutable typed conflicts require a later declarative resolution.
    Conflicted(Box<crate::MergePlan>),
}

/// Exact resolved generation and its durable publication reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiRootConflictFinish {
    /// Current generation descending from the projected conflict state.
    pub generation: GenerationId,
    /// Reservation fencing that generation until the Applied decision.
    pub fence: MultiRootFence,
}

/// Durable optimistic store for publication decisions.
pub trait MultiRootPublicationStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one operation record.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<Option<MultiRootPublication>, Self::Error>> + ProviderTask;

    /// Atomically replaces one expected revision; zero creates the record.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask;

    /// Lists durable operations that may require restart recovery.
    fn list_operations(
        &self,
    ) -> impl Future<Output = Result<Vec<OperationId>, Self::Error>> + ProviderTask;

    /// Removes a terminal record only when its exact revision still matches.
    fn compare_and_delete(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask;

    /// Atomically claims the one publication slot for a parent context.
    fn claim_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask;

    /// Releases the exact parent claim after terminal cleanup.
    fn release_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask;
}

/// Root-specific publisher backed by the ordinary join and materializer APIs.
pub trait MultiRootPublisher: Send + Sync {
    /// Publisher error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Atomically validates and reserves the exact pinned target.
    fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> impl Future<Output = Result<MultiRootPrepare, Self::Error>> + ProviderTask;

    /// Releases a reservation when validation fails before a commit decision.
    fn release(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> impl Future<Output = Result<(), Self::Error>> + ProviderTask;

    /// Projects one root into its durable conflicted working state.
    fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: Option<&crate::MergePlan>,
    ) -> impl Future<Output = Result<GenerationId, Self::Error>> + ProviderTask;

    /// Restores one projected root to its exact pre-merge generation.
    fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> impl Future<Output = Result<GenerationId, Self::Error>> + ProviderTask;

    /// Validates explicitly staged conflict paths and returns their exact keys.
    fn declare_conflicts(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> impl Future<Output = Result<BTreeSet<ConflictKey>, Self::Error>> + ProviderTask;

    /// Captures the current resolved working generation during continue.
    fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> impl Future<Output = Result<MultiRootConflictFinish, Self::Error>> + ProviderTask;

    /// Idempotently rolls one root forward after the durable commit decision.
    fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> impl Future<Output = Result<MultiRootPublishRoot, Self::Error>> + ProviderTask;

    /// Reclaims terminal per-root recovery state after the all-roots Applied decision is durable.
    fn finalize(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
    ) -> impl Future<Output = Result<(), Self::Error>> + ProviderTask {
        async { Ok(()) }
    }
}

/// Resolves one authenticated workspace identity for core publication.
pub trait WorkspaceResolver<A, O>: Send + Sync {
    /// Resolver failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Opens the exact workspace or fails closed.
    fn resolve(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Workspace<A, O>, Self::Error>> + ProviderTask;
}

impl<A, O, S> WorkspaceResolver<A, O> for (crate::Fs<A, O>, WorkspaceGraph<S>)
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    S: WorkspaceLineageStore,
{
    type Error = WorkspaceLineageError<S::Error>;
    async fn resolve(&self, workspace_id: WorkspaceId) -> Result<Workspace<A, O>, Self::Error> {
        let record = self.1.resolve(workspace_id).await?;
        let workspace = self
            .0
            .open_workspace(
                &record.workspace_name,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        if workspace.id() != workspace_id {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        Ok(workspace)
    }
}

/// Applies one logically published root generation to its host checkout.
pub trait MultiRootMaterializer<A, O>: Send + Sync {
    /// Materialization failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Idempotently applies the exact published generation for one root.
    fn materialize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        workspace: &Workspace<A, O>,
        from: GenerationId,
        to: GenerationId,
    ) -> impl Future<Output = Result<(), Self::Error>> + ProviderTask;

    /// Reclaims terminal physical recovery state.
    fn finalize(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
    ) -> impl Future<Output = Result<(), Self::Error>> + ProviderTask {
        async { Ok(()) }
    }
}

/// SDK-owned publisher over ordinary immutable workspace joins.
pub struct WorkspaceMultiRootPublisher<A, O, R> {
    resolver: R,
    merge_drivers: Option<std::sync::Arc<MergeDriverRegistry>>,
    root_merge_drivers: BTreeMap<WorkspaceRootId, std::sync::Arc<MergeDriverRegistry>>,
    resolution_cache: Mutex<MemoryMergeResolutionCache>,
    marker: std::marker::PhantomData<fn() -> (A, O)>,
}

/// Workspace publisher composed with an injected physical materializer.
pub struct MaterializingWorkspaceMultiRootPublisher<A, O, R, M> {
    publisher: WorkspaceMultiRootPublisher<A, O, R>,
    materializer: M,
}

impl<A, O, R, M> MaterializingWorkspaceMultiRootPublisher<A, O, R, M> {
    /// Creates one composed logical and physical publisher.
    #[must_use]
    pub fn new(resolver: R, materializer: M) -> Self {
        Self {
            publisher: WorkspaceMultiRootPublisher::new(resolver),
            materializer,
        }
    }

    /// Uses one immutable-input driver registry for conflict resolution.
    #[must_use]
    pub fn with_merge_drivers(mut self, registry: std::sync::Arc<MergeDriverRegistry>) -> Self {
        self.publisher = self.publisher.with_merge_drivers(registry);
        self
    }

    /// Overrides merge-driver selection for one root in a multi-root plan.
    #[must_use]
    pub fn with_root_merge_drivers(
        mut self,
        root_id: WorkspaceRootId,
        registry: std::sync::Arc<MergeDriverRegistry>,
    ) -> Self {
        self.publisher = self.publisher.with_root_merge_drivers(root_id, registry);
        self
    }
}

/// Failure from a composed logical and physical root publication.
#[derive(Debug, Error)]
pub enum MaterializingWorkspaceMultiRootPublisherError<
    R: std::error::Error + 'static,
    M: std::error::Error + 'static,
> {
    /// Logical workspace validation or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceMultiRootPublisherError<R>),
    /// The exact published generation could not be recovered.
    #[error("published workspace generation is unavailable for materialization")]
    MissingPublishedGeneration,
    /// Physical checkout application failed and may be retried.
    #[error("root materialization failed: {0}")]
    Materializer(M),
}

impl<A, O, R> WorkspaceMultiRootPublisher<A, O, R> {
    /// Creates a publisher over an injected authenticated resolver.
    #[must_use]
    pub fn new(resolver: R) -> Self {
        Self {
            resolver,
            merge_drivers: None,
            root_merge_drivers: BTreeMap::new(),
            resolution_cache: Mutex::new(MemoryMergeResolutionCache::default()),
            marker: std::marker::PhantomData,
        }
    }

    /// Uses one immutable-input driver registry for conflict resolution.
    #[must_use]
    pub fn with_merge_drivers(mut self, registry: std::sync::Arc<MergeDriverRegistry>) -> Self {
        self.merge_drivers = Some(registry);
        self
    }

    /// Overrides merge-driver selection for one root in a multi-root plan.
    #[must_use]
    pub fn with_root_merge_drivers(
        mut self,
        root_id: WorkspaceRootId,
        registry: std::sync::Arc<MergeDriverRegistry>,
    ) -> Self {
        self.root_merge_drivers.insert(root_id, registry);
        self
    }

    /// Borrows the resolver capability.
    #[must_use]
    pub const fn resolver(&self) -> &R {
        &self.resolver
    }
}

/// Production workspace publisher failure.
#[derive(Debug, Error)]
pub enum WorkspaceMultiRootPublisherError<E: std::error::Error + 'static> {
    /// Workspace resolution failed.
    #[error("workspace resolution failed: {0}")]
    Resolver(E),
    /// Workspace planning, preview, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Declarative conflict resolution did not match the pinned plan.
    #[error("merge candidate validation failed: {0}")]
    Candidate(String),
    /// Validation found typed conflicts for which no complete resolution was supplied.
    #[error("merge requires declarative conflict resolutions")]
    Conflicted {
        /// Immutable exact alternatives suitable for presentation or drivers.
        plan: Box<crate::MergePlan>,
    },
    /// The supplied opaque fence does not describe these exact inputs.
    #[error("multi-root publication fence is invalid")]
    InvalidFence,
    /// Disposable validation state could not be reclaimed safely.
    #[error("multi-root validation workspace could not be reclaimed")]
    PreviewCleanup,
}

/// Authorization boundary for direct-parent and root-binding validation.
pub trait MultiRootPublicationAuthorizer: Send + Sync {
    /// Authorizer error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verifies that the plan publishes the exact child's bound roots only to its exact direct parent.
    fn authorize(
        &self,
        plan: &MultiRootMergePlan,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask;

    /// Verifies that an interactive transition is being requested by the exact direct parent that owns the durable merge state.
    fn authorize_transition(
        &self,
        plan: &MultiRootMergePlan,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask {
        async move {
            if caller_context_id != plan.parent_context_id {
                return Ok(false);
            }
            self.authorize(plan).await
        }
    }
}

impl<S: WorkspaceContextStore> MultiRootPublicationAuthorizer for WorkspaceContextRegistry<S> {
    type Error = WorkspaceContextError<S::Error>;

    async fn authorize(&self, plan: &MultiRootMergePlan) -> Result<bool, Self::Error> {
        let child = self
            .authorize_parent(plan.child_context_id, plan.parent_context_id)
            .await?;
        let parent = self.resolve(plan.parent_context_id).await?;
        Ok(plan.roots.iter().all(|(root_id, root)| {
            root.merge_workspace_id.is_none()
                && child.roots.get(root_id).is_some_and(|binding| {
                    binding.workspace_id == root.source_workspace_id
                        && binding.parent_workspace_id == Some(root.target_workspace_id)
                })
                && parent
                    .roots
                    .get(root_id)
                    .is_some_and(|binding| binding.workspace_id == root.target_workspace_id)
        }))
    }
}

/// Context and workspace-lineage authorizer for filtered compatibility forks.
pub struct LineageMultiRootPublicationAuthorizer<S> {
    store: S,
}

impl<S> LineageMultiRootPublicationAuthorizer<S> {
    /// Creates an authorizer over one shared durable core-state store.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }
}

/// Failure while authenticating both context and compatibility-fork lineage.
#[derive(Debug, Error)]
pub enum LineageMultiRootPublicationAuthorizationError<E: std::error::Error + 'static> {
    /// Context binding validation failed.
    #[error(transparent)]
    Context(#[from] WorkspaceContextError<E>),
    /// Workspace lineage validation failed.
    #[error(transparent)]
    Lineage(#[from] WorkspaceLineageError<E>),
}

impl<S> MultiRootPublicationAuthorizer for LineageMultiRootPublicationAuthorizer<S>
where
    S: WorkspaceContextStore
        + WorkspaceLineageStore<Error = <S as WorkspaceContextStore>::Error>
        + Clone,
{
    type Error = LineageMultiRootPublicationAuthorizationError<<S as WorkspaceContextStore>::Error>;

    async fn authorize(&self, plan: &MultiRootMergePlan) -> Result<bool, Self::Error> {
        let contexts = WorkspaceContextRegistry::new(self.store.clone());
        let child = contexts
            .authorize_parent(plan.child_context_id, plan.parent_context_id)
            .await?;
        let parent = contexts.resolve(plan.parent_context_id).await?;
        let graph = WorkspaceGraph::new(self.store.clone());
        for (root_id, root) in &plan.roots {
            if !child.roots.get(root_id).is_some_and(|binding| {
                binding.workspace_id == root.source_workspace_id
                    && binding.parent_workspace_id == Some(root.target_workspace_id)
            }) || parent
                .roots
                .get(root_id)
                .is_none_or(|binding| binding.workspace_id != root.target_workspace_id)
            {
                return Ok(false);
            }
            if graph
                .authorize_join(root.source_workspace_id, root.target_workspace_id)
                .await
                .is_err()
            {
                return Ok(false);
            }
            if let Some(filtered) = root.merge_workspace_id
                && graph
                    .authorize_join(filtered, root.source_workspace_id)
                    .await
                    .is_err()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// One root's publication result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiRootPublishRoot {
    /// Root became durable during this attempt.
    Published(GenerationId),
    /// Exact result was already durable.
    AlreadyPublished(GenerationId),
    /// Target no longer matches and was not overwritten.
    UnexpectedTarget,
}

/// Terminal coordinator outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Publication {
    /// Every root is durable.
    Applied(MultiRootPublication),
    /// Validation failed before a commit decision; no root was changed.
    StaleBeforeCommit(WorkspaceRootId),
    /// Typed conflicts are durable; no root was published.
    Conflicted(MultiRootPublication),
    /// Roll-forward paused on unexpected state after a commit decision.
    Paused(MultiRootPublication),
}

enum MultiRootValidation {
    Committed(MultiRootPublication),
    Stale(WorkspaceRootId, MultiRootPublication),
    Conflicted(MultiRootPublication),
}

/// Cross-root publication failure.
#[derive(Debug, Error)]
pub enum MultiRootPublicationError<
    S: std::error::Error + 'static,
    P: std::error::Error + 'static,
    A: std::error::Error + 'static,
> {
    /// Durable state failed.
    #[error("multi-root publication store failed: {0}")]
    Store(S),
    /// A root-specific join/materialization failed.
    #[error("multi-root publisher failed: {0}")]
    Publisher(P),
    /// Context lineage authorization failed.
    #[error("multi-root publication authorization failed: {0}")]
    Authorizer(A),
    /// Retry supplied different immutable inputs or malformed roots.
    #[error("multi-root publication candidate is invalid or conflicts with its retry identity")]
    InvalidCandidate,
    /// Durable state remained contended beyond the bounded retry policy.
    #[error("multi-root publication state remained contended")]
    Contended,
    /// Another unresolved publication already belongs to this parent context.
    #[error("parent context already has an unresolved multi-root publication")]
    ParentBusy,
}

type MultiRootResult<T, S, P, A> = Result<T, MultiRootPublicationError<S, P, A>>;

/// Core coordinator for one injected durable store and root publisher.
pub struct MultiRootPublicationCoordinator<S, P, A> {
    store: S,
    publisher: P,
    authorizer: A,
}

impl<S, P, A> MultiRootPublicationCoordinator<S, P, A> {
    /// Creates a coordinator.
    #[must_use]
    pub const fn new(store: S, publisher: P, authorizer: A) -> Self {
        Self {
            store,
            publisher,
            authorizer,
        }
    }
}

/// Process-local durable-state substitute for deterministic tests.
#[derive(Default)]
pub struct MemoryMultiRootPublicationStore {
    records: MemoryRecords<OperationId, MultiRootPublication>,
    parents: Mutex<BTreeMap<WorkspaceContextId, OperationId>>,
    #[cfg(test)]
    fail_claim_once: std::sync::atomic::AtomicBool,
}

/// Memory store synchronization failure.
#[derive(Debug, Error)]
#[error("multi-root publication memory store is unavailable")]
pub struct MemoryMultiRootPublicationStoreError;

impl MultiRootPublicationStore for MemoryMultiRootPublicationStore {
    type Error = MemoryMultiRootPublicationStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MultiRootPublication>, Self::Error> {
        self.records
            .load(&operation_id)
            .map_err(|_| MemoryMultiRootPublicationStoreError)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> Result<bool, Self::Error> {
        self.records
            .compare_and_swap(operation_id, expected_revision, replacement)
            .map_err(|_| MemoryMultiRootPublicationStoreError)
    }

    async fn list_operations(&self) -> Result<Vec<OperationId>, Self::Error> {
        self.records
            .keys()
            .map_err(|_| MemoryMultiRootPublicationStoreError)
    }

    async fn compare_and_delete(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.records
            .compare_and_delete(&operation_id, expected_revision)
            .map_err(|_| MemoryMultiRootPublicationStoreError)
    }

    async fn claim_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> Result<bool, Self::Error> {
        #[cfg(test)]
        if self
            .fail_claim_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(MemoryMultiRootPublicationStoreError);
        }
        let mut parents = self
            .parents
            .lock()
            .map_err(|_| MemoryMultiRootPublicationStoreError)?;
        if let Some(existing) = parents.get(&parent_context_id) {
            Ok(*existing == operation_id)
        } else {
            parents.insert(parent_context_id, operation_id);
            Ok(true)
        }
    }

    async fn release_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> Result<bool, Self::Error> {
        let mut parents = self
            .parents
            .lock()
            .map_err(|_| MemoryMultiRootPublicationStoreError)?;
        if parents.get(&parent_context_id) != Some(&operation_id) {
            return Ok(true);
        }
        parents.remove(&parent_context_id);
        Ok(true)
    }
}

mod helpers;
mod materializing;
mod publisher;
mod publisher_helpers;
use helpers::*;
mod coordinator;
mod validation;
use validation::is_pristine_unclaimed;
pub(crate) use validation::{validate_candidate, validate_journal};

#[cfg(test)]
#[path = "multi_root/tests/mod.rs"]
mod tests;
