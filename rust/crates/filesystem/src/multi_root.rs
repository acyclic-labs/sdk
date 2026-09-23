//! Durable all-roots publication decisions.
//!
//! Each root still uses the ordinary immutable join/candidate APIs. This
//! coordinator validates every pinned root before recording one commit
//! decision, then rolls all roots forward idempotently. A changed target after
//! the decision pauses recovery instead of overwriting external work.

use crate::async_storage::StorageFuture;
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
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Mutex;
use thiserror::Error;

const MULTI_ROOT_VERSION: u32 = 2;

/// One root pinned by an immutable cross-root merge plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRootMergeRoot {
    /// Workspace whose pinned generation is being published.
    pub source_workspace_id: WorkspaceId,
    /// Optional filtered compatibility fork authenticated as a direct child of
    /// `source_workspace_id`. Ordinary filesystem joins leave this absent.
    #[serde(default)]
    pub merge_workspace_id: Option<WorkspaceId>,
    /// Child generation captured when planning began.
    pub source_generation: GenerationId,
    /// Exact direct-parent workspace.
    pub target_workspace_id: WorkspaceId,
    /// Target head that must validate before the commit decision.
    pub target_generation: GenerationId,
    /// Three-way common ancestor authenticated by the per-root join plan.
    pub base_generation: GenerationId,
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

/// Opaque durable reservation proving that one target was validated and
/// fenced for an exact publication operation.
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
    #[serde(default)]
    pub published_generations: BTreeMap<WorkspaceRootId, GenerationId>,
    /// Per-root reservations acquired before the commit decision.
    pub fences: BTreeMap<WorkspaceRootId, MultiRootFence>,
    /// Immutable typed conflicts pinned during validation.
    #[serde(default)]
    pub conflicts: BTreeMap<WorkspaceRootId, crate::MergePlan>,
    /// Parent generations containing the durable conflict projection.
    #[serde(default)]
    pub projected_roots: BTreeMap<WorkspaceRootId, GenerationId>,
    /// Conflict keys explicitly accepted from the projected working copy.
    #[serde(default)]
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
    ) -> impl Future<Output = Result<Option<MultiRootPublication>, Self::Error>> + StorageFuture;

    /// Atomically replaces one expected revision; zero creates the record.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> impl Future<Output = Result<bool, Self::Error>> + StorageFuture;

    /// Lists durable operations that may require restart recovery.
    fn list_operations(
        &self,
    ) -> impl Future<Output = Result<Vec<OperationId>, Self::Error>> + StorageFuture;

    /// Removes a terminal record only when its exact revision still matches.
    /// Failed physical cleanup leaves the record intact for restart recovery.
    fn compare_and_delete(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
    ) -> impl Future<Output = Result<bool, Self::Error>> + StorageFuture;

    /// Atomically claims the one publication slot for a parent context.
    fn claim_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + StorageFuture;

    /// Releases the exact parent claim after terminal cleanup.
    fn release_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + StorageFuture;
}

/// Root-specific publisher backed by the ordinary join and materializer APIs.
pub trait MultiRootPublisher: Send + Sync {
    /// Publisher error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Atomically validates and reserves the exact pinned target. Repeating
    /// this call for one operation must return the same live reservation.
    fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> impl Future<Output = Result<MultiRootPrepare, Self::Error>> + StorageFuture;

    /// Releases a reservation when validation fails before a commit decision.
    fn release(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> impl Future<Output = Result<(), Self::Error>> + StorageFuture;

    /// Projects one root into its durable conflicted working state.
    fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: Option<&crate::MergePlan>,
    ) -> impl Future<Output = Result<GenerationId, Self::Error>> + StorageFuture;

    /// Restores one projected root to its exact pre-merge generation.
    fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> impl Future<Output = Result<GenerationId, Self::Error>> + StorageFuture;

    /// Validates explicitly staged conflict paths and returns their exact keys.
    fn declare_conflicts(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> impl Future<Output = Result<BTreeSet<ConflictKey>, Self::Error>> + StorageFuture;

    /// Captures the current resolved working generation during continue.
    fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> impl Future<Output = Result<MultiRootConflictFinish, Self::Error>> + StorageFuture;

    /// Idempotently rolls one root forward after the durable commit decision.
    fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> impl Future<Output = Result<MultiRootPublishRoot, Self::Error>> + StorageFuture;

    /// Reclaims terminal per-root recovery state after the all-roots Applied
    /// decision is durable. It must be idempotent because restart recovery
    /// retries cleanup independently of logical publication.
    fn finalize(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
    ) -> impl Future<Output = Result<(), Self::Error>> + StorageFuture {
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
    ) -> impl Future<Output = Result<Workspace<A, O>, Self::Error>> + StorageFuture;
}

/// Applies one logically published root generation to its host checkout.
///
/// Implementations are injected capabilities: the coordinator owns retry and
/// ordering, while a native host may use the core journaled materializer and
/// a non-native host may deliberately provide no physical checkout.
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
    ) -> impl Future<Output = Result<(), Self::Error>> + StorageFuture;

    /// Reclaims terminal physical recovery state. Logical publication is
    /// already durable when this runs, so failures are retried on recovery.
    fn finalize(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
    ) -> impl Future<Output = Result<(), Self::Error>> + StorageFuture {
        async { Ok(()) }
    }
}

/// SDK-owned publisher over ordinary immutable workspace joins.
///
/// Preparation validates pinned inputs and resolutions against a disposable
/// SDK workspace. It never changes the target. Publication then repeats the
/// same deterministic join through the target's fenced authority CAS.
pub struct WorkspaceMultiRootPublisher<A, O, R> {
    resolver: R,
    merge_drivers: Option<std::sync::Arc<MergeDriverRegistry>>,
    root_merge_drivers: BTreeMap<WorkspaceRootId, std::sync::Arc<MergeDriverRegistry>>,
    resolution_cache: Mutex<MemoryMergeResolutionCache>,
    marker: std::marker::PhantomData<fn() -> (A, O)>,
}

/// Workspace publisher composed with an injected physical materializer.
///
/// Logical publication happens first under the durable all-roots decision.
/// If materialization is interrupted, retry discovers the exact generation
/// from the workspace idempotency record and resumes the same materializer.
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

impl<A, O, R> WorkspaceMultiRootPublisher<A, O, R>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
{
    async fn reserve_operation(
        &self,
        target: &Workspace<A, O>,
        operation_id: OperationId,
        conflict: &'static str,
    ) -> Result<PublicationReservation, WorkspaceMultiRootPublisherError<R::Error>> {
        let authority_head = target
            .authority()
            .head(
                target.authority_id(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?
            .value;
        match target
            .authority()
            .reserve_publication(
                target.authority_id(),
                authority_head,
                operation_id,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?
            .value
        {
            ReservationOutcome::Reserved(reservation)
            | ReservationOutcome::AlreadyReserved(reservation) => Ok(reservation),
            ReservationOutcome::Conflict { .. } => Err(
                WorkspaceMultiRootPublisherError::Candidate(conflict.to_owned()),
            ),
        }
    }

    async fn release_operation_reservation(
        &self,
        target: &Workspace<A, O>,
        operation_id: OperationId,
    ) -> Result<(), WorkspaceMultiRootPublisherError<R::Error>> {
        let reservation = self
            .reserve_operation(
                target,
                operation_id,
                "publication reservation is owned by another operation",
            )
            .await?;
        target
            .authority()
            .release_publication(
                reservation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        Ok(())
    }

    async fn pinned_plan(
        &self,
        root: &MultiRootMergeRoot,
        target_override: Option<Workspace<A, O>>,
    ) -> Result<crate::JoinPlan<A, O>, WorkspaceMultiRootPublisherError<R::Error>> {
        let source = self
            .resolver
            .resolve(root.merge_workspace_id.unwrap_or(root.source_workspace_id))
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let target = match target_override {
            Some(target) => target,
            None => self
                .resolver
                .resolve(root.target_workspace_id)
                .await
                .map_err(WorkspaceMultiRootPublisherError::Resolver)?,
        };
        let source_generation = source.generation(root.source_generation).await?;
        let target_generation = target.generation(root.target_generation).await?;
        let plan = source
            .join_into(&target)
            .plan_pinned(source_generation, target_generation)
            .await?;
        if plan.common_ancestor() != root.base_generation {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "common ancestor changed".to_owned(),
            ));
        }
        Ok(plan)
    }

    async fn apply_plan(
        &self,
        root_id: WorkspaceRootId,
        plan: &crate::JoinPlan<A, O>,
        operation: IdempotencyKey,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        permit: PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceMultiRootPublisherError<R::Error>> {
        let options = ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: operation,
        };
        let initial = plan.apply_with_permit(options, permit).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let typed = plan.describe_conflicts(&conflicts, truncated).await?;
        let candidate = if resolutions.is_empty() && !typed.conflicts.is_empty() {
            let Some(registry) = self
                .root_merge_drivers
                .get(&root_id)
                .or(self.merge_drivers.as_ref())
            else {
                return Err(WorkspaceMultiRootPublisherError::Conflicted {
                    plan: Box::new(typed),
                });
            };
            let mut cache = self.resolution_cache.lock().map_err(|_| {
                WorkspaceMultiRootPublisherError::Candidate(
                    "merge resolution cache is unavailable".to_owned(),
                )
            })?;
            match crate::resolve_merge_plan(typed.clone(), registry, &mut *cache, false) {
                Ok(candidate) => candidate,
                Err(
                    MergePlanResolutionError::MissingDriver(_)
                    | MergePlanResolutionError::Driver(DriverError::Unsupported),
                ) => {
                    return Err(WorkspaceMultiRootPublisherError::Conflicted {
                        plan: Box::new(typed),
                    });
                }
                Err(error) => {
                    return Err(WorkspaceMultiRootPublisherError::Candidate(
                        error.to_string(),
                    ));
                }
            }
        } else {
            UnpublishedMergeCandidate {
                plan: typed,
                resolutions: resolutions.clone(),
            }
        };
        if resolutions.is_empty()
            && candidate.resolutions.values().any(|resolution| {
                matches!(resolution, MergeResolution::Text(text)
                    if crate::text_merge::has_conflict_markers(text))
            })
        {
            return Err(WorkspaceMultiRootPublisherError::Conflicted {
                plan: Box::new(candidate.plan.clone()),
            });
        }
        plan.apply_candidate_with_permit(options, &candidate, permit)
            .await
            .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))
    }

    async fn preview(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<bool, WorkspaceMultiRootPublisherError<R::Error>> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        if target.head().await?.id() != root.target_generation {
            return Ok(false);
        }
        let preview_name = preview_workspace_name(operation_id, root_id)?;
        let target_generation = target.generation(root.target_generation).await?;
        let preview = target
            .fork(
                preview_name.as_str(),
                ForkOptions::from_generation(
                    target_generation,
                    publisher_key(operation_id, root_id, b"preview-fork"),
                ),
            )
            .await?;
        let result = async {
            let preview_generation = preview.head().await?;
            let preview_root = MultiRootMergeRoot {
                target_workspace_id: preview.id(),
                target_generation: preview_generation.id(),
                ..root.clone()
            };
            let plan = self
                .pinned_plan(&preview_root, Some(preview.clone()))
                .await?;
            Ok(matches!(
                self.apply_plan(
                    root_id,
                    &plan,
                    publisher_key(operation_id, root_id, b"preview-apply"),
                    resolutions,
                    PublicationPermit::Unrestricted,
                )
                .await?,
                JoinOutcome::Applied(_)
                    | JoinOutcome::AlreadyApplied(_)
                    | JoinOutcome::NoChanges(_)
            ))
        }
        .await;
        let cleanup = preview
            .delete(publisher_key(operation_id, root_id, b"preview-delete"))
            .await?;
        if !matches!(
            cleanup,
            WorkspaceDelete::Deleted | WorkspaceDelete::AlreadyDeleted
        ) {
            return Err(WorkspaceMultiRootPublisherError::PreviewCleanup);
        }
        result
    }
}

impl<A, O, R> MultiRootPublisher for WorkspaceMultiRootPublisher<A, O, R>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
{
    type Error = WorkspaceMultiRootPublisherError<R::Error>;

    async fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<MultiRootPrepare, Self::Error> {
        match self.preview(operation_id, root_id, root, resolutions).await {
            Ok(true) => {}
            Ok(false) => return Ok(MultiRootPrepare::Stale),
            Err(WorkspaceMultiRootPublisherError::Conflicted { plan }) => {
                return Ok(MultiRootPrepare::Conflicted(plan));
            }
            Err(error) => return Err(error),
        }
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let authority_head = target
            .authority()
            .head(
                target.authority_id(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?
            .value;
        let reservation = match target
            .authority()
            .reserve_publication(
                target.authority_id(),
                authority_head,
                operation_id,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?
            .value
        {
            ReservationOutcome::Reserved(reservation)
            | ReservationOutcome::AlreadyReserved(reservation) => reservation,
            ReservationOutcome::Conflict { .. } => return Ok(MultiRootPrepare::Stale),
        };
        Ok(MultiRootPrepare::Ready(MultiRootFence {
            token: serde_json::to_vec(&WorkspacePublicationFence {
                target_workspace_id: target.id(),
                reservation,
            })
            .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))?,
        }))
    }

    async fn release(
        &self,
        operation_id: OperationId,
        _root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> Result<(), Self::Error> {
        let decoded = decode_reservation(operation_id, fence)?;
        let target = self
            .resolver
            .resolve(decoded.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        target
            .authority()
            .release_publication(
                decoded.reservation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        Ok(())
    }

    async fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: Option<&crate::MergePlan>,
    ) -> Result<GenerationId, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let projection_key = publisher_key(operation_id, root_id, b"conflict-project");
        let reservation_operation = conflict_projection_operation_id(operation_id, root_id);
        if let Some(generation) = target.operation_generation(projection_key).await? {
            self.release_operation_reservation(&target, reservation_operation)
                .await?;
            return Ok(generation.id());
        }
        if target.head().await?.id() != root.target_generation {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "conflict projection target changed".to_owned(),
            ));
        }
        let plan = self.pinned_plan(root, Some(target.clone())).await?;
        let mut resolutions = BTreeMap::new();
        if let Some(conflict) = conflict {
            if conflict.truncated {
                return Err(WorkspaceMultiRootPublisherError::Candidate(
                    "conflict projection plan changed".to_owned(),
                ));
            }
            for view in &conflict.conflicts {
                let resolution = if view.kind == crate::ConflictKind::Text {
                    DefaultTextMergeDriver
                        .resolve(view)
                        .unwrap_or(MergeResolution::Select(ConflictSide::Ours))
                } else {
                    MergeResolution::Select(ConflictSide::Ours)
                };
                resolutions.insert(view.key.clone(), resolution);
            }
        }
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "conflict projection reservation changed",
            )
            .await?;
        let outcome = self
            .apply_plan(
                root_id,
                &plan,
                projection_key,
                &resolutions,
                PublicationPermit::Reservation {
                    operation_id: reservation_operation.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: reservation.expected,
                },
            )
            .await;
        let release = target
            .authority()
            .release_publication(
                reservation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error));
        if let Err(error) = release {
            return Err(error.into());
        }
        let outcome = outcome?;
        match outcome {
            JoinOutcome::Applied(generation)
            | JoinOutcome::AlreadyApplied(generation)
            | JoinOutcome::NoChanges(generation) => Ok(generation.id()),
            _ => Err(WorkspaceMultiRootPublisherError::Candidate(
                "conflict projection did not produce a generation".to_owned(),
            )),
        }
    }

    async fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<GenerationId, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let key = publisher_key(operation_id, root_id, b"conflict-abort");
        let reservation_operation =
            conflict_abort_materialization_operation_id(operation_id, root_id);
        if let Some(generation) = target.operation_generation(key).await? {
            self.release_operation_reservation(&target, reservation_operation)
                .await?;
            return Ok(generation.id());
        }
        let current = target.head().await?;
        let original = target.generation(root.target_generation).await?;
        let projected = target.generation(projected_generation).await?;
        let changed = original
            .diff_to(&projected, u32::MAX)
            .await?
            .changed_paths(u32::MAX)
            .await?;
        let paths = changed
            .iter()
            .map(|change| {
                portable_namespace_path(&change.path).ok_or_else(|| {
                    WorkspaceMultiRootPublisherError::Candidate(
                        "conflict abort cannot represent a non-portable path".to_owned(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if paths.is_empty() {
            // The conflicted projection changed no path in the target (every
            // conflicting record was held back), so there is nothing to undo.
            return Ok(current.id());
        }
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "conflict abort target changed while being fenced",
            )
            .await?;
        let outcome = target
            .apply_paths_from_with_permit(
                Some(&projected),
                Some(&original),
                &paths,
                current.id(),
                key,
                PublicationPermit::Reservation {
                    operation_id: reservation_operation.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: reservation.expected,
                },
            )
            .await;
        let release = target
            .authority()
            .release_publication(
                reservation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error));
        if let Err(error) = release {
            return Err(error.into());
        }
        let outcome = outcome?;
        match outcome {
            crate::WorkspacePathApply::Applied(generation)
            | crate::WorkspacePathApply::AlreadyApplied(generation)
            | crate::WorkspacePathApply::NoChanges(generation) => Ok(generation.id()),
            crate::WorkspacePathApply::Conflicted(_)
            | crate::WorkspacePathApply::Stale(_)
            | crate::WorkspacePathApply::Fenced
            | crate::WorkspacePathApply::IdempotencyConflict => {
                Err(WorkspaceMultiRootPublisherError::Candidate(
                    "conflict abort overlaps later workspace changes".to_owned(),
                ))
            }
        }
    }

    async fn declare_conflicts(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let current = target.head().await?;
        let normalized = paths
            .iter()
            .map(|path| path.trim_start_matches('/').trim_end_matches('/'))
            .collect::<Vec<_>>();
        let declares_all = normalized
            .iter()
            .any(|candidate| *candidate == "." || candidate.is_empty());
        let mut declared = BTreeSet::new();
        for view in &conflict.conflicts {
            let Some(raw_path) = view.path.as_deref() else {
                if declares_all {
                    declared.insert(view.key.clone());
                }
                continue;
            };
            let path = raw_path.trim_start_matches('/');
            if !normalized.iter().any(|candidate| {
                *candidate == "."
                    || *candidate == path
                    || (!candidate.is_empty()
                        && path
                            .strip_prefix(*candidate)
                            .is_some_and(|suffix| suffix.starts_with('/')))
            }) {
                continue;
            }
            if view.kind == crate::ConflictKind::Text {
                match current.read(&format!("/{path}"), 64 * 1024 * 1024).await {
                    Ok(bytes) => {
                        let text = std::str::from_utf8(&bytes).map_err(|_| {
                            WorkspaceMultiRootPublisherError::Candidate(format!(
                                "text conflict {path} is no longer UTF-8"
                            ))
                        })?;
                        if crate::text_merge::has_conflict_markers(text) {
                            return Err(WorkspaceMultiRootPublisherError::Candidate(format!(
                                "text conflict {path} still contains conflict markers"
                            )));
                        }
                    }
                    Err(WorkspaceError::NotFound) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            declared.insert(view.key.clone());
        }
        Ok(declared)
    }

    async fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<MultiRootConflictFinish, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let current = target.head().await?;
        if !generation_descends_from(&target, current.id(), projected_generation).await? {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "resolved conflict generation does not descend from its projection".to_owned(),
            ));
        }
        let reservation_operation = conflict_finish_operation_id(operation_id, root_id);
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "resolved conflict generation changed while being fenced",
            )
            .await?;
        Ok(MultiRootConflictFinish {
            generation: current.id(),
            fence: MultiRootFence {
                token: serde_json::to_vec(&WorkspacePublicationFence {
                    target_workspace_id: target.id(),
                    reservation,
                })
                .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))?,
            },
        })
    }

    async fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> Result<MultiRootPublishRoot, Self::Error> {
        let decoded = decode_reservation(operation_id, fence)?;
        if decoded.target_workspace_id != root.target_workspace_id {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        if target.head().await?.id() != root.target_generation {
            if let Some(generation) = target
                .operation_generation(publisher_key(operation_id, root_id, b"publish"))
                .await?
            {
                target
                    .authority()
                    .release_publication(
                        decoded.reservation,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| WorkspaceError::engine(failure.error))?;
                return Ok(MultiRootPublishRoot::AlreadyPublished(generation.id()));
            }
            return Ok(MultiRootPublishRoot::UnexpectedTarget);
        }
        if decoded.reservation.authority_id != target.authority_id() {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
        let plan = self.pinned_plan(root, Some(target.clone())).await?;
        let outcome = match self
            .apply_plan(
                root_id,
                &plan,
                publisher_key(operation_id, root_id, b"publish"),
                resolutions,
                PublicationPermit::Reservation {
                    operation_id: operation_id.into_bytes(),
                    gate_tail: decoded.reservation.gate_tail,
                    expected: decoded.reservation.expected,
                },
            )
            .await?
        {
            JoinOutcome::Applied(generation) | JoinOutcome::NoChanges(generation) => {
                MultiRootPublishRoot::Published(generation.id())
            }
            JoinOutcome::AlreadyApplied(generation) => {
                MultiRootPublishRoot::AlreadyPublished(generation.id())
            }
            JoinOutcome::StaleTarget(_)
            | JoinOutcome::Fenced
            | JoinOutcome::IdempotencyConflict
            | JoinOutcome::Conflicted { .. } => MultiRootPublishRoot::UnexpectedTarget,
        };
        if matches!(
            outcome,
            MultiRootPublishRoot::Published(_) | MultiRootPublishRoot::AlreadyPublished(_)
        ) {
            target
                .authority()
                .release_publication(
                    decoded.reservation,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| WorkspaceError::engine(failure.error))?;
        }
        Ok(outcome)
    }
}

impl<A, O, R, M> MultiRootPublisher for MaterializingWorkspaceMultiRootPublisher<A, O, R, M>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
    M: MultiRootMaterializer<A, O>,
{
    type Error = MaterializingWorkspaceMultiRootPublisherError<R::Error, M::Error>;

    async fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<MultiRootPrepare, Self::Error> {
        self.publisher
            .prepare(operation_id, root_id, root, resolutions)
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Workspace)
    }

    async fn release(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> Result<(), Self::Error> {
        self.publisher
            .release(operation_id, root_id, fence)
            .await
            .map_err(Into::into)
    }

    async fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: Option<&crate::MergePlan>,
    ) -> Result<GenerationId, Self::Error> {
        let to = self
            .publisher
            .project_conflict(operation_id, root_id, root, conflict)
            .await?;
        let workspace = self
            .publisher
            .resolver()
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        self.materializer
            .materialize(
                materialization_operation_id(operation_id, root_id),
                root_id,
                &workspace,
                root.target_generation,
                to,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        Ok(to)
    }

    async fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<GenerationId, Self::Error> {
        let workspace = self
            .publisher
            .resolver()
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let from = workspace
            .head()
            .await
            .map_err(WorkspaceMultiRootPublisherError::Workspace)?
            .id();
        let to = self
            .publisher
            .abort_conflict(operation_id, root_id, root, projected_generation)
            .await?;
        self.materializer
            .materialize(
                conflict_abort_materialization_operation_id(operation_id, root_id),
                root_id,
                &workspace,
                from,
                to,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        self.materializer
            .finalize(
                conflict_abort_materialization_operation_id(operation_id, root_id),
                root_id,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        Ok(to)
    }

    async fn declare_conflicts(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
        self.publisher
            .declare_conflicts(operation_id, root_id, root, conflict, paths)
            .await
            .map_err(Into::into)
    }

    async fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<MultiRootConflictFinish, Self::Error> {
        self.publisher
            .finish_conflict(operation_id, root_id, root, projected_generation)
            .await
            .map_err(Into::into)
    }

    async fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> Result<MultiRootPublishRoot, Self::Error> {
        let published = self
            .publisher
            .publish(operation_id, root_id, root, resolutions, fence)
            .await?;
        if !matches!(
            published,
            MultiRootPublishRoot::Published(_) | MultiRootPublishRoot::AlreadyPublished(_)
        ) {
            return Ok(published);
        }
        let workspace = self
            .publisher
            .resolver()
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let key = publisher_key(operation_id, root_id, b"publish");
        let to = if let Some(generation) = workspace
            .operation_generation(key)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Workspace)?
        {
            generation.id()
        } else {
            let head = workspace
                .head()
                .await
                .map_err(WorkspaceMultiRootPublisherError::Workspace)?;
            if head.id() != root.target_generation {
                return Err(
                    MaterializingWorkspaceMultiRootPublisherError::MissingPublishedGeneration,
                );
            }
            head.id()
        };
        self.materializer
            .materialize(
                materialization_operation_id(operation_id, root_id),
                root_id,
                &workspace,
                root.target_generation,
                to,
            )
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)?;
        Ok(published)
    }

    async fn finalize(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
    ) -> Result<(), Self::Error> {
        self.materializer
            .finalize(materialization_operation_id(operation_id, root_id), root_id)
            .await
            .map_err(MaterializingWorkspaceMultiRootPublisherError::Materializer)
    }
}

fn publisher_key(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
    purpose: &[u8],
) -> IdempotencyKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.workspace-multi-root-publisher.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    hasher.update(purpose);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

fn materialization_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-materialization.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn conflict_projection_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-conflict-projection.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn conflict_abort_materialization_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-conflict-abort-materialization.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn portable_namespace_path(path: &crate::kernel::NamespacePath) -> Option<String> {
    let mut value = String::new();
    for component in path.components() {
        if component.encoding() != crate::kernel::NameEncoding::Utf8 {
            return None;
        }
        value.push('/');
        value.push_str(std::str::from_utf8(component.as_bytes()).ok()?);
    }
    if value.is_empty() {
        value.push('/');
    }
    Some(value)
}

fn conflict_finish_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-conflict-finish.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

async fn generation_descends_from<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    descendant: GenerationId,
    ancestor: GenerationId,
) -> Result<bool, WorkspaceError> {
    if descendant == ancestor {
        return Ok(true);
    }
    let mut pending = vec![descendant];
    let mut visited = BTreeSet::new();
    while let Some(candidate) = pending.pop() {
        if !visited.insert(candidate) || visited.len() > 65_536 {
            continue;
        }
        for parent in workspace.generation(candidate).await?.parents().await? {
            if parent == ancestor {
                return Ok(true);
            }
            pending.push(parent);
        }
    }
    Ok(false)
}

fn decode_reservation<E: std::error::Error + 'static>(
    operation_id: OperationId,
    fence: &MultiRootFence,
) -> Result<WorkspacePublicationFence, WorkspaceMultiRootPublisherError<E>> {
    let decoded: WorkspacePublicationFence = serde_json::from_slice(&fence.token)
        .map_err(|_| WorkspaceMultiRootPublisherError::InvalidFence)?;
    if decoded.reservation.operation_id != operation_id {
        return Err(WorkspaceMultiRootPublisherError::InvalidFence);
    }
    Ok(decoded)
}

fn preview_workspace_name(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> Result<WorkspaceName, WorkspaceError> {
    WorkspaceName::new(format!(
        "multi-root-preview-{}-{}",
        hex::encode(&operation_id.into_bytes()[..6]),
        hex::encode(&root_id.into_bytes()[..6])
    ))
    .map_err(Into::into)
}

/// Authorization boundary for direct-parent and root-binding validation.
pub trait MultiRootPublicationAuthorizer: Send + Sync {
    /// Authorizer error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verifies that the plan publishes the exact child's bound roots only to
    /// its exact direct parent.
    fn authorize(
        &self,
        plan: &MultiRootMergePlan,
    ) -> impl Future<Output = Result<bool, Self::Error>> + StorageFuture;

    /// Verifies that an interactive transition is being requested by the
    /// exact direct parent that owns the durable merge state.
    fn authorize_transition(
        &self,
        plan: &MultiRootMergePlan,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + StorageFuture {
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

impl<S: MultiRootPublicationStore, P: MultiRootPublisher, A: MultiRootPublicationAuthorizer>
    MultiRootPublicationCoordinator<S, P, A>
{
    /// Resumes one durable publication, returning `None` when no such
    /// operation exists. The stored immutable candidate remains authoritative.
    pub async fn resume(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Publication>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        self.resume_with_finalization(operation_id, true).await
    }

    /// Resumes publication while retaining an Applied journal until an
    /// external, idempotent compatibility projection has been recorded.
    pub async fn resume_retained(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Publication>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        self.resume_with_finalization(operation_id, false).await
    }

    async fn resume_with_finalization(
        &self,
        operation_id: OperationId,
        finalize: bool,
    ) -> Result<Option<Publication>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let Some(journal) = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        else {
            return Ok(None);
        };
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase == MultiRootPublicationPhase::Applied {
            if finalize {
                self.finalize(&journal).await?;
            }
            return Ok(Some(Publication::Applied(journal)));
        }
        if journal.phase == MultiRootPublicationPhase::Conflicted {
            return self
                .project_conflicted(journal)
                .await
                .map(|journal| Some(Publication::Conflicted(journal)));
        }
        if journal.phase == MultiRootPublicationPhase::Aborting {
            self.abort_journal(journal).await?;
            return Ok(None);
        }
        self.publish_with_finalization(journal.candidate, finalize)
            .await
            .map(Some)
    }

    /// Lists durable publication operations for restart recovery.
    pub async fn pending_operations(
        &self,
    ) -> Result<Vec<OperationId>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        self.store
            .list_operations()
            .await
            .map_err(MultiRootPublicationError::Store)
    }

    /// Validates, commits, and rolls forward one exact candidate.
    pub fn publish(
        &self,
        candidate: MultiRootMergeCandidate,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + StorageFuture
    + '_ {
        Box::pin(async move { self.publish_with_finalization(candidate, true).await })
    }

    /// Publishes while retaining the terminal journal for a caller-owned,
    /// idempotent compatibility projection before acknowledgement.
    pub fn publish_retained(
        &self,
        candidate: MultiRootMergeCandidate,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + StorageFuture
    + '_ {
        Box::pin(async move { self.publish_with_finalization(candidate, false).await })
    }

    async fn publish_with_finalization(
        &self,
        candidate: MultiRootMergeCandidate,
        finalize: bool,
    ) -> MultiRootResult<Publication, S::Error, P::Error, A::Error> {
        validate_candidate(&candidate).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if !self
            .authorizer
            .authorize(&candidate.plan)
            .await
            .map_err(MultiRootPublicationError::Authorizer)?
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        let mut journal = self.load_or_create(candidate).await?;
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase == MultiRootPublicationPhase::Applied {
            if finalize {
                self.finalize(&journal).await?;
            }
            return Ok(Publication::Applied(journal));
        }
        if journal.phase == MultiRootPublicationPhase::Conflicted {
            return self
                .project_conflicted(journal)
                .await
                .map(Publication::Conflicted);
        }
        if journal.phase == MultiRootPublicationPhase::Validating {
            journal = match self.validate_and_commit(journal).await? {
                MultiRootValidation::Committed(journal) => journal,
                MultiRootValidation::Stale(root_id, journal) => {
                    self.discard_uncommitted(&journal).await?;
                    return Ok(Publication::StaleBeforeCommit(root_id));
                }
                MultiRootValidation::Conflicted(journal) => {
                    return self
                        .project_conflicted(journal)
                        .await
                        .map(Publication::Conflicted);
                }
            };
        }
        self.roll_forward(journal, finalize).await
    }

    /// Accepts the ordinary edits in a fully projected conflicted working copy.
    pub fn continue_conflicted(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + StorageFuture
    + '_ {
        Box::pin(async move {
            self.continue_conflicted_with_finalization(operation_id, caller_context_id, true)
                .await
        })
    }

    /// Continues a conflict while retaining the terminal journal until the
    /// caller acknowledges its compatibility projection.
    pub fn continue_conflicted_retained(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + StorageFuture
    + '_ {
        Box::pin(async move {
            self.continue_conflicted_with_finalization(operation_id, caller_context_id, false)
                .await
        })
    }

    /// Declares the current projected contents of conflict paths as the
    /// caller-selected resolutions. Text paths still containing generated
    /// markers are rejected.
    pub async fn declare_conflicted_paths(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
        root_id: WorkspaceRootId,
        paths: &[String],
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let mut journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase != MultiRootPublicationPhase::Conflicted
            || !self
                .authorizer
                .authorize_transition(&journal.candidate.plan, caller_context_id)
                .await
                .map_err(MultiRootPublicationError::Authorizer)?
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        let root = journal
            .candidate
            .plan
            .roots
            .get(&root_id)
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        let conflict = journal
            .conflicts
            .get(&root_id)
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        let declared = self
            .publisher
            .declare_conflicts(operation_id, root_id, root, conflict, paths)
            .await
            .map_err(MultiRootPublicationError::Publisher)?;
        if declared.is_empty() {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        journal
            .declared_conflicts
            .entry(root_id)
            .or_default()
            .extend(declared);
        self.persist(journal).await?;
        Ok(())
    }

    async fn continue_conflicted_with_finalization(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
        finalize: bool,
    ) -> MultiRootResult<Publication, S::Error, P::Error, A::Error> {
        let journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if !self
            .authorizer
            .authorize_transition(&journal.candidate.plan, caller_context_id)
            .await
            .map_err(MultiRootPublicationError::Authorizer)?
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        if journal.phase != MultiRootPublicationPhase::Conflicted
            || journal
                .projected_roots
                .keys()
                .ne(journal.candidate.plan.roots.keys())
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        for (root_id, conflict) in &journal.conflicts {
            let expected = conflict
                .conflicts
                .iter()
                .map(|view| view.key.clone())
                .collect::<BTreeSet<_>>();
            if !journal
                .declared_conflicts
                .get(root_id)
                .is_some_and(|declared| expected.is_subset(declared))
            {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
        }
        let roots = journal.candidate.plan.roots.clone();
        let mut applied = journal;
        for (root_id, root) in &roots {
            if applied.fences.contains_key(root_id) {
                continue;
            }
            let projected = *applied
                .projected_roots
                .get(root_id)
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            let finished = self
                .publisher
                .finish_conflict(operation_id, *root_id, root, projected)
                .await
                .map_err(MultiRootPublicationError::Publisher)?;
            applied
                .projected_roots
                .insert(*root_id, finished.generation);
            applied
                .published_generations
                .insert(*root_id, finished.generation);
            applied.fences.insert(*root_id, finished.fence);
            applied = self.persist(applied).await?;
        }
        applied.phase = MultiRootPublicationPhase::Applied;
        applied.published_roots = applied.candidate.plan.roots.keys().copied().collect();
        applied.conflicts.clear();
        applied.projected_roots.clear();
        applied = self.persist(applied).await?;
        if finalize {
            self.finalize(&applied).await?;
        }
        Ok(Publication::Applied(applied))
    }

    /// Acknowledges a retained Applied journal after external compatibility
    /// state has been durably and idempotently projected.
    pub async fn acknowledge_applied(
        &self,
        journal: &MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        validate_journal(journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase != MultiRootPublicationPhase::Applied {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        self.finalize(journal).await
    }

    /// Restores every conflicted root to its exact pre-merge generation.
    pub fn abort_conflicted(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = MultiRootResult<(), S::Error, P::Error, A::Error>> + StorageFuture + '_
    {
        Box::pin(async move {
            let mut journal = self
                .store
                .load(operation_id)
                .await
                .map_err(MultiRootPublicationError::Store)?
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
            if !self
                .authorizer
                .authorize_transition(&journal.candidate.plan, caller_context_id)
                .await
                .map_err(MultiRootPublicationError::Authorizer)?
            {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
            if journal.phase == MultiRootPublicationPhase::Conflicted {
                for (root_id, fence) in &journal.fences {
                    self.publisher
                        .release(
                            conflict_finish_operation_id(operation_id, *root_id),
                            *root_id,
                            fence,
                        )
                        .await
                        .map_err(MultiRootPublicationError::Publisher)?;
                }
                journal.fences.clear();
                journal.declared_conflicts.clear();
                journal.phase = MultiRootPublicationPhase::Aborting;
                journal.published_roots.clear();
                journal = self.persist(journal).await?;
            }
            if journal.phase != MultiRootPublicationPhase::Aborting {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
            self.abort_journal(journal).await
        })
    }

    async fn project_conflicted(
        &self,
        mut journal: MultiRootPublication,
    ) -> Result<MultiRootPublication, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let operation_id = journal.candidate.plan.operation_id;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.projected_roots.contains_key(&root_id) {
                continue;
            }
            let generation = self
                .publisher
                .project_conflict(
                    operation_id,
                    root_id,
                    &root,
                    journal.conflicts.get(&root_id),
                )
                .await
                .map_err(MultiRootPublicationError::Publisher)?;
            journal.projected_roots.insert(root_id, generation);
            journal = self.persist(journal).await?;
        }
        Ok(journal)
    }

    async fn abort_journal(
        &self,
        mut journal: MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.published_roots.contains(&root_id) {
                continue;
            }
            if let Some(projected_generation) = journal.projected_roots.get(&root_id).copied() {
                self.publisher
                    .abort_conflict(operation_id, root_id, &root, projected_generation)
                    .await
                    .map_err(MultiRootPublicationError::Publisher)?;
            }
            journal.published_roots.insert(root_id);
            journal = self.persist(journal).await?;
        }
        if !self
            .store
            .release_parent(journal.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        if !self
            .store
            .compare_and_delete(operation_id, journal.revision)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        Ok(())
    }

    async fn discard_uncommitted(
        &self,
        journal: &MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        if !self
            .store
            .release_parent(journal.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            || !self
                .store
                .compare_and_delete(operation_id, journal.revision)
                .await
                .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        Ok(())
    }

    async fn load_or_create(
        &self,
        candidate: MultiRootMergeCandidate,
    ) -> Result<MultiRootPublication, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let operation_id = candidate.plan.operation_id;
        if let Some(existing) = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            if existing.candidate != candidate {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
            if !self
                .store
                .claim_parent(candidate.plan.parent_context_id, operation_id)
                .await
                .map_err(MultiRootPublicationError::Store)?
            {
                if is_pristine_unclaimed(&existing)
                    && !self
                        .store
                        .compare_and_delete(operation_id, existing.revision)
                        .await
                        .map_err(MultiRootPublicationError::Store)?
                {
                    return Err(MultiRootPublicationError::Contended);
                }
                return Err(MultiRootPublicationError::ParentBusy);
            }
            return Ok(existing);
        }
        let journal = MultiRootPublication {
            version: MULTI_ROOT_VERSION,
            revision: 1,
            candidate,
            phase: MultiRootPublicationPhase::Validating,
            published_roots: BTreeSet::new(),
            published_generations: BTreeMap::new(),
            fences: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            projected_roots: BTreeMap::new(),
            declared_conflicts: BTreeMap::new(),
            paused_root: None,
        };
        let created = match self
            .store
            .compare_and_swap(operation_id, 0, journal.clone())
            .await
        {
            Ok(created) => created,
            Err(error) => return Err(MultiRootPublicationError::Store(error)),
        };
        if created {
            let claimed = self
                .store
                .claim_parent(journal.candidate.plan.parent_context_id, operation_id)
                .await
                .map_err(MultiRootPublicationError::Store)?;
            if !claimed {
                if !self
                    .store
                    .compare_and_delete(operation_id, journal.revision)
                    .await
                    .map_err(MultiRootPublicationError::Store)?
                {
                    return Err(MultiRootPublicationError::Contended);
                }
                return Err(MultiRootPublicationError::ParentBusy);
            }
            return Ok(journal);
        }
        let existing = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            .ok_or(MultiRootPublicationError::Contended)?;
        if existing.candidate != journal.candidate {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        if !self
            .store
            .claim_parent(existing.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            if is_pristine_unclaimed(&existing)
                && !self
                    .store
                    .compare_and_delete(operation_id, existing.revision)
                    .await
                    .map_err(MultiRootPublicationError::Store)?
            {
                return Err(MultiRootPublicationError::Contended);
            }
            return Err(MultiRootPublicationError::ParentBusy);
        }
        Ok(existing)
    }

    async fn validate_and_commit(
        &self,
        mut journal: MultiRootPublication,
    ) -> Result<MultiRootValidation, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let operation_id = journal.candidate.plan.operation_id;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.fences.contains_key(&root_id) {
                continue;
            }
            let resolutions = journal
                .candidate
                .resolutions
                .get(&root_id)
                .cloned()
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            let prepared = self
                .publisher
                .prepare(operation_id, root_id, &root, &resolutions)
                .await;
            let fence = match prepared {
                Ok(MultiRootPrepare::Ready(fence)) => fence,
                Ok(MultiRootPrepare::Stale) => {
                    for (prepared_root, prepared_fence) in &journal.fences {
                        self.publisher
                            .release(operation_id, *prepared_root, prepared_fence)
                            .await
                            .map_err(MultiRootPublicationError::Publisher)?;
                    }
                    journal.fences.clear();
                    journal.conflicts.clear();
                    journal = self.persist(journal).await?;
                    return Ok(MultiRootValidation::Stale(root_id, journal));
                }
                Ok(MultiRootPrepare::Conflicted(plan)) => {
                    journal.conflicts.insert(root_id, *plan);
                    continue;
                }
                Err(error) => {
                    for (prepared_root, prepared_fence) in &journal.fences {
                        self.publisher
                            .release(operation_id, *prepared_root, prepared_fence)
                            .await
                            .map_err(MultiRootPublicationError::Publisher)?;
                    }
                    journal.fences.clear();
                    journal.conflicts.clear();
                    let _ = self.persist(journal).await?;
                    return Err(MultiRootPublicationError::Publisher(error));
                }
            };
            journal.fences.insert(root_id, fence);
            journal = self.persist(journal).await?;
        }
        if !journal.conflicts.is_empty() {
            for (prepared_root, prepared_fence) in &journal.fences {
                self.publisher
                    .release(operation_id, *prepared_root, prepared_fence)
                    .await
                    .map_err(MultiRootPublicationError::Publisher)?;
            }
            journal.fences.clear();
            journal.phase = MultiRootPublicationPhase::Conflicted;
            return self
                .persist(journal)
                .await
                .map(MultiRootValidation::Conflicted);
        }
        journal.phase = MultiRootPublicationPhase::Committed;
        journal.paused_root = None;
        self.persist(journal)
            .await
            .map(MultiRootValidation::Committed)
    }

    async fn roll_forward(
        &self,
        mut journal: MultiRootPublication,
        finalize: bool,
    ) -> MultiRootResult<Publication, S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        journal.phase = MultiRootPublicationPhase::Publishing;
        journal.paused_root = None;
        journal = self.persist(journal).await?;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.published_roots.contains(&root_id) {
                continue;
            }
            let resolutions = journal
                .candidate
                .resolutions
                .get(&root_id)
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            let fence = journal
                .fences
                .get(&root_id)
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            match self
                .publisher
                .publish(operation_id, root_id, &root, resolutions, fence)
                .await
                .map_err(MultiRootPublicationError::Publisher)?
            {
                MultiRootPublishRoot::Published(generation)
                | MultiRootPublishRoot::AlreadyPublished(generation) => {
                    journal.published_roots.insert(root_id);
                    journal.published_generations.insert(root_id, generation);
                    journal = self.persist(journal).await?;
                }
                MultiRootPublishRoot::UnexpectedTarget => {
                    journal.phase = MultiRootPublicationPhase::Paused;
                    journal.paused_root = Some(root_id);
                    journal = self.persist(journal).await?;
                    return Ok(Publication::Paused(journal));
                }
            }
        }
        journal.phase = MultiRootPublicationPhase::Applied;
        journal.paused_root = None;
        journal = self.persist(journal).await?;
        if finalize {
            self.finalize(&journal).await?;
        }
        Ok(Publication::Applied(journal))
    }

    async fn finalize(
        &self,
        journal: &MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        if !journal.declared_conflicts.is_empty() {
            for (root_id, fence) in &journal.fences {
                self.publisher
                    .release(
                        conflict_finish_operation_id(operation_id, *root_id),
                        *root_id,
                        fence,
                    )
                    .await
                    .map_err(MultiRootPublicationError::Publisher)?;
            }
        }
        for (root_id, root) in &journal.candidate.plan.roots {
            self.publisher
                .finalize(operation_id, *root_id, root)
                .await
                .map_err(MultiRootPublicationError::Publisher)?;
        }
        if !self
            .store
            .release_parent(journal.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        if !self
            .store
            .compare_and_delete(operation_id, journal.revision)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        Ok(())
    }

    async fn persist(
        &self,
        mut journal: MultiRootPublication,
    ) -> Result<MultiRootPublication, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let expected = journal.revision;
        journal.revision = expected.saturating_add(1);
        if self
            .store
            .compare_and_swap(
                journal.candidate.plan.operation_id,
                expected,
                journal.clone(),
            )
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            Ok(journal)
        } else {
            Err(MultiRootPublicationError::Contended)
        }
    }
}

fn validate_candidate(candidate: &MultiRootMergeCandidate) -> Result<(), ()> {
    if candidate.plan.roots.is_empty()
        || candidate.plan.parent_context_id == candidate.plan.child_context_id
        || candidate.plan.roots.keys().ne(candidate.resolutions.keys())
        || candidate
            .plan
            .roots
            .values()
            .any(|root| root.source_workspace_id == root.target_workspace_id)
    {
        return Err(());
    }
    Ok(())
}

fn is_pristine_unclaimed(journal: &MultiRootPublication) -> bool {
    journal.phase == MultiRootPublicationPhase::Validating
        && journal.fences.is_empty()
        && journal.conflicts.is_empty()
        && journal.projected_roots.is_empty()
        && journal.declared_conflicts.is_empty()
        && journal.published_roots.is_empty()
        && journal.published_generations.is_empty()
        && journal.paused_root.is_none()
}

fn validate_journal(journal: &MultiRootPublication) -> Result<(), ()> {
    let roots: BTreeSet<_> = journal.candidate.plan.roots.keys().copied().collect();
    let fence_roots: BTreeSet<_> = journal.fences.keys().copied().collect();
    let generation_roots: BTreeSet<_> = journal.published_generations.keys().copied().collect();
    let no_publication_progress =
        journal.published_roots.is_empty() && journal.published_generations.is_empty();
    let no_conflict_state = journal.conflicts.is_empty()
        && journal.projected_roots.is_empty()
        && journal.declared_conflicts.is_empty();
    let phase_valid = match journal.phase {
        MultiRootPublicationPhase::Validating => {
            no_publication_progress
                && journal.projected_roots.is_empty()
                && journal.declared_conflicts.is_empty()
                && fence_roots.is_subset(&roots)
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Conflicted => {
            no_publication_progress
                && journal.fences.is_empty()
                && !journal.conflicts.is_empty()
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Aborting => {
            journal.published_generations.is_empty()
                && journal.fences.is_empty()
                && !journal.conflicts.is_empty()
                && journal.declared_conflicts.is_empty()
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Committed => {
            no_publication_progress
                && no_conflict_state
                && fence_roots == roots
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Publishing => {
            no_conflict_state
                && fence_roots == roots
                && generation_roots == journal.published_roots
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Paused => {
            no_conflict_state
                && fence_roots == roots
                && generation_roots == journal.published_roots
                && journal.paused_root.is_some_and(|root| {
                    roots.contains(&root) && !journal.published_roots.contains(&root)
                })
        }
        MultiRootPublicationPhase::Applied => {
            journal.conflicts.is_empty()
                && journal.projected_roots.is_empty()
                && fence_roots == roots
                && journal.published_roots == roots
                && generation_roots == roots
                && journal.paused_root.is_none()
        }
    };
    if journal.version != MULTI_ROOT_VERSION
        || journal.revision == 0
        || !journal.published_roots.is_subset(&roots)
        || !journal.conflicts.keys().all(|root| roots.contains(root))
        || !journal
            .projected_roots
            .keys()
            .all(|root| roots.contains(root))
        || !journal
            .declared_conflicts
            .keys()
            .all(|root| roots.contains(root))
        || !phase_valid
    {
        return Err(());
    }
    validate_candidate(&journal.candidate)
}

/// Process-local durable-state substitute for deterministic tests.
#[derive(Default)]
pub struct MemoryMultiRootPublicationStore {
    records: Mutex<BTreeMap<OperationId, MultiRootPublication>>,
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
            .lock()
            .map_err(|_| MemoryMultiRootPublicationStoreError)
            .map(|records| records.get(&operation_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> Result<bool, Self::Error> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MemoryMultiRootPublicationStoreError)?;
        if records.get(&operation_id).map_or(0, |value| value.revision) != expected_revision {
            return Ok(false);
        }
        records.insert(operation_id, replacement);
        Ok(true)
    }

    async fn list_operations(&self) -> Result<Vec<OperationId>, Self::Error> {
        let records = self
            .records
            .lock()
            .map_err(|_| MemoryMultiRootPublicationStoreError)?;
        Ok(records.keys().copied().collect())
    }

    async fn compare_and_delete(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MemoryMultiRootPublicationStoreError)?;
        if records.get(&operation_id).map_or(0, |value| value.revision) != expected_revision {
            return Ok(false);
        }
        records.remove(&operation_id);
        Ok(true)
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::{
        Digest, Fs, LocalCoreStateStore, MemoryWorkspaceContextStore, TransactionCommit,
        WorkspaceContextRoot,
    };
    use std::path::PathBuf;

    #[derive(Debug, Error)]
    #[error("test publisher lock failed")]
    struct PublisherError;

    #[derive(Default)]
    struct Publisher {
        published: Mutex<BTreeSet<WorkspaceRootId>>,
        pause_once: Mutex<Option<WorkspaceRootId>>,
        finalize_fail_once: Mutex<Option<WorkspaceRootId>>,
        finalized: Mutex<Vec<WorkspaceRootId>>,
    }

    #[derive(Debug, Error)]
    #[error("test authorizer failed")]
    struct AuthorizerError;

    struct Allow;

    #[derive(Debug, Error)]
    #[error("test resolver failed")]
    struct ResolverError;

    struct Resolver<A, O> {
        workspaces: Mutex<BTreeMap<WorkspaceId, Workspace<A, O>>>,
    }

    impl<A: AsyncAuthorityStore, O: AsyncObjectStore> WorkspaceResolver<A, O> for Resolver<A, O> {
        type Error = ResolverError;

        async fn resolve(&self, workspace_id: WorkspaceId) -> Result<Workspace<A, O>, Self::Error> {
            self.workspaces
                .lock()
                .map_err(|_| ResolverError)?
                .get(&workspace_id)
                .cloned()
                .ok_or(ResolverError)
        }
    }

    impl MultiRootPublicationAuthorizer for Allow {
        type Error = AuthorizerError;

        async fn authorize(&self, _plan: &MultiRootMergePlan) -> Result<bool, Self::Error> {
            Ok(true)
        }
    }

    impl MultiRootPublisher for Publisher {
        type Error = PublisherError;

        async fn prepare(
            &self,
            operation_id: OperationId,
            _root_id: WorkspaceRootId,
            _root: &MultiRootMergeRoot,
            _resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        ) -> Result<MultiRootPrepare, Self::Error> {
            Ok(MultiRootPrepare::Ready(MultiRootFence {
                token: operation_id.into_bytes().to_vec(),
            }))
        }

        async fn release(
            &self,
            _operation_id: OperationId,
            _root_id: WorkspaceRootId,
            _fence: &MultiRootFence,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn project_conflict(
            &self,
            _operation_id: OperationId,
            _root_id: WorkspaceRootId,
            root: &MultiRootMergeRoot,
            _conflict: Option<&crate::MergePlan>,
        ) -> Result<GenerationId, Self::Error> {
            Ok(root.target_generation)
        }

        async fn abort_conflict(
            &self,
            _operation_id: OperationId,
            _root_id: WorkspaceRootId,
            root: &MultiRootMergeRoot,
            _projected_generation: GenerationId,
        ) -> Result<GenerationId, Self::Error> {
            Ok(root.target_generation)
        }

        async fn declare_conflicts(
            &self,
            _operation_id: OperationId,
            _root_id: WorkspaceRootId,
            _root: &MultiRootMergeRoot,
            conflict: &crate::MergePlan,
            _paths: &[String],
        ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
            Ok(conflict
                .conflicts
                .iter()
                .map(|view| view.key.clone())
                .collect())
        }

        async fn finish_conflict(
            &self,
            _operation_id: OperationId,
            _root_id: WorkspaceRootId,
            root: &MultiRootMergeRoot,
            _projected_generation: GenerationId,
        ) -> Result<MultiRootConflictFinish, Self::Error> {
            Ok(MultiRootConflictFinish {
                generation: root.target_generation,
                fence: MultiRootFence { token: Vec::new() },
            })
        }

        async fn publish(
            &self,
            _operation_id: OperationId,
            root_id: WorkspaceRootId,
            root: &MultiRootMergeRoot,
            _resolutions: &BTreeMap<ConflictKey, MergeResolution>,
            _fence: &MultiRootFence,
        ) -> Result<MultiRootPublishRoot, Self::Error> {
            let mut pause = self.pause_once.lock().map_err(|_| PublisherError)?;
            if *pause == Some(root_id) {
                *pause = None;
                return Ok(MultiRootPublishRoot::UnexpectedTarget);
            }
            let inserted = self
                .published
                .lock()
                .map_err(|_| PublisherError)?
                .insert(root_id);
            Ok(if inserted {
                MultiRootPublishRoot::Published(root.source_generation)
            } else {
                MultiRootPublishRoot::AlreadyPublished(root.source_generation)
            })
        }

        async fn finalize(
            &self,
            _operation_id: OperationId,
            root_id: WorkspaceRootId,
            _root: &MultiRootMergeRoot,
        ) -> Result<(), Self::Error> {
            self.finalized
                .lock()
                .map_err(|_| PublisherError)?
                .push(root_id);
            let mut fail = self.finalize_fail_once.lock().map_err(|_| PublisherError)?;
            if *fail == Some(root_id) {
                *fail = None;
                return Err(PublisherError);
            }
            Ok(())
        }
    }

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
    }

    fn candidate() -> MultiRootMergeCandidate {
        let roots = [1_u8, 2]
            .into_iter()
            .map(|byte| {
                (
                    WorkspaceRootId::from_bytes([byte; 16]),
                    MultiRootMergeRoot {
                        source_workspace_id: WorkspaceId::from_bytes([byte; 16]),
                        merge_workspace_id: None,
                        source_generation: generation(byte),
                        target_workspace_id: WorkspaceId::from_bytes([byte + 10; 16]),
                        target_generation: generation(byte + 10),
                        base_generation: generation(0),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        MultiRootMergeCandidate {
            plan: MultiRootMergePlan {
                operation_id: OperationId::from_bytes([7; 16]),
                parent_context_id: WorkspaceContextId::from_bytes([8; 16]),
                child_context_id: WorkspaceContextId::from_bytes([9; 16]),
                roots,
            },
            resolutions: [1_u8, 2]
                .into_iter()
                .map(|byte| (WorkspaceRootId::from_bytes([byte; 16]), BTreeMap::new()))
                .collect(),
        }
    }

    #[tokio::test]
    async fn parent_claim_allows_only_one_unacknowledged_publication() {
        let coordinator = MultiRootPublicationCoordinator::new(
            MemoryMultiRootPublicationStore::default(),
            Publisher::default(),
            Allow,
        );
        let first = candidate();
        let Publication::Applied(retained) = coordinator
            .publish_retained(first.clone())
            .await
            .expect("first retained publication")
        else {
            panic!("expected applied publication");
        };
        let mut second = first;
        second.plan.operation_id = OperationId::from_bytes([0x55; 16]);
        assert!(matches!(
            coordinator.publish_retained(second.clone()).await,
            Err(MultiRootPublicationError::ParentBusy)
        ));
        assert_eq!(
            coordinator
                .pending_operations()
                .await
                .expect("list retained publication"),
            [retained.candidate.plan.operation_id]
        );
        coordinator
            .acknowledge_applied(&retained)
            .await
            .expect("acknowledge first publication");
        assert!(matches!(
            coordinator
                .publish_retained(second)
                .await
                .expect("second publication after acknowledgement"),
            Publication::Applied(_)
        ));
    }

    #[tokio::test]
    async fn add_all_declares_pathless_typed_conflicts() -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let target = fs.create_workspace("pathless-conflict-target").await?;
        let head = target.head().await?;
        let publisher = WorkspaceMultiRootPublisher::new(Resolver {
            workspaces: Mutex::new(BTreeMap::from([(target.id(), target.clone())])),
        });
        let directory_id = crate::FileId::new();
        let key = ConflictKey::Binding {
            directory_id,
            name: b"entry".to_vec(),
        };
        let plan = crate::MergePlan {
            base: head.id(),
            ours: head.id(),
            theirs: head.id(),
            conflicts: vec![crate::ConflictView {
                key: key.clone(),
                path: None,
                kind: crate::ConflictKind::Binding,
                base: crate::ConflictValue::Binding(None),
                ours: crate::ConflictValue::Binding(None),
                theirs: crate::ConflictValue::Binding(None),
            }],
            truncated: false,
        };
        let root = MultiRootMergeRoot {
            source_workspace_id: target.id(),
            merge_workspace_id: None,
            source_generation: head.id(),
            target_workspace_id: target.id(),
            target_generation: head.id(),
            base_generation: head.id(),
        };

        assert!(
            publisher
                .declare_conflicts(
                    OperationId::from_bytes([0x71; 16]),
                    WorkspaceRootId::from_bytes([0x72; 16]),
                    &root,
                    &plan,
                    &["specific/path".to_owned()],
                )
                .await?
                .is_empty()
        );
        assert_eq!(
            publisher
                .declare_conflicts(
                    OperationId::from_bytes([0x71; 16]),
                    WorkspaceRootId::from_bytes([0x72; 16]),
                    &root,
                    &plan,
                    &[".".to_owned()],
                )
                .await?,
            BTreeSet::from([key])
        );
        Ok(())
    }

    #[test]
    fn unchanged_child_generation_is_a_valid_noop_publication() {
        let mut candidate = candidate();
        for root in candidate.plan.roots.values_mut() {
            root.source_generation = root.target_generation;
        }
        assert!(validate_candidate(&candidate).is_ok());
    }

    #[tokio::test]
    async fn committed_publication_pauses_and_rolls_forward_on_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let publisher = Publisher::default();
        *publisher.pause_once.lock().expect("pause lock") =
            Some(WorkspaceRootId::from_bytes([2; 16]));
        let coordinator = MultiRootPublicationCoordinator::new(
            MemoryMultiRootPublicationStore::default(),
            publisher,
            Allow,
        );
        let candidate = candidate();
        let first = coordinator
            .publish(candidate.clone())
            .await
            .expect("first publication");
        let first = match first {
            Publication::Paused(publication) => publication,
            Publication::Applied(_)
            | Publication::Conflicted(_)
            | Publication::StaleBeforeCommit(_) => {
                return Err("publication should pause".into());
            }
        };
        assert_eq!(first.phase, MultiRootPublicationPhase::Paused);
        assert_eq!(first.published_roots.len(), 1);

        let second = coordinator
            .publish(candidate)
            .await
            .expect("recovered publication");
        let second = match second {
            Publication::Applied(publication) => publication,
            Publication::Paused(_)
            | Publication::Conflicted(_)
            | Publication::StaleBeforeCommit(_) => {
                return Err("publication should complete".into());
            }
        };
        assert_eq!(second.phase, MultiRootPublicationPhase::Applied);
        assert_eq!(second.published_roots.len(), 2);
        assert!(
            coordinator
                .pending_operations()
                .await
                .expect("terminal records are reclaimed")
                .is_empty()
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }

    #[tokio::test]
    async fn local_store_restart_resumes_committed_publication_without_republishing_roots()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let operation_id = candidate().plan.operation_id;
        let first_publisher = Publisher::default();
        *first_publisher.pause_once.lock().expect("pause lock") =
            Some(WorkspaceRootId::from_bytes([2; 16]));
        let first = MultiRootPublicationCoordinator::new(
            LocalCoreStateStore::new(directory.path()),
            first_publisher,
            Allow,
        );
        let Publication::Paused(paused) = first.publish(candidate()).await? else {
            return Err("publication should pause".into());
        };
        assert_eq!(
            paused.published_roots,
            [WorkspaceRootId::from_bytes([1; 16])].into()
        );
        drop(first);

        let reopened = MultiRootPublicationCoordinator::new(
            LocalCoreStateStore::new(directory.path()),
            Publisher::default(),
            Allow,
        );
        let resumed = reopened.resume(operation_id).await?;
        let Some(Publication::Applied(applied)) = resumed else {
            return Err("reopened publication should complete".into());
        };
        assert_eq!(applied.phase, MultiRootPublicationPhase::Applied);
        assert_eq!(applied.published_roots.len(), 2);
        assert_eq!(
            *reopened.publisher.published.lock().expect("published lock"),
            [WorkspaceRootId::from_bytes([2; 16])].into_iter().collect()
        );
        assert!(reopened.pending_operations().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn journal_created_before_parent_claim_survives_claim_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryMultiRootPublicationStore::default();
        store
            .fail_claim_once
            .store(true, std::sync::atomic::Ordering::Release);
        let coordinator = MultiRootPublicationCoordinator::new(store, Publisher::default(), Allow);
        let operation_id = candidate().plan.operation_id;

        assert!(matches!(
            coordinator.publish(candidate()).await,
            Err(MultiRootPublicationError::Store(_))
        ));
        assert_eq!(coordinator.pending_operations().await?, [operation_id]);

        assert!(matches!(
            coordinator.publish(candidate()).await?,
            Publication::Applied(_)
        ));
        assert!(coordinator.pending_operations().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn finalize_failure_retries_after_the_applied_decision()
    -> Result<(), Box<dyn std::error::Error>> {
        let publisher = Publisher::default();
        *publisher.finalize_fail_once.lock().expect("finalize lock") =
            Some(WorkspaceRootId::from_bytes([2; 16]));
        let coordinator = MultiRootPublicationCoordinator::new(
            MemoryMultiRootPublicationStore::default(),
            publisher,
            Allow,
        );
        let operation_id = candidate().plan.operation_id;
        assert!(matches!(
            coordinator.publish(candidate()).await,
            Err(MultiRootPublicationError::Publisher(_))
        ));
        let pending = coordinator.pending_operations().await?;
        assert_eq!(pending, vec![operation_id]);

        let resumed = coordinator.resume(operation_id).await?;
        assert!(matches!(resumed, Some(Publication::Applied(_))));
        assert!(coordinator.pending_operations().await?.is_empty());
        let finalized = coordinator
            .publisher
            .finalized
            .lock()
            .expect("finalized lock");
        assert_eq!(
            finalized.as_slice(),
            &[
                WorkspaceRootId::from_bytes([1; 16]),
                WorkspaceRootId::from_bytes([2; 16]),
                WorkspaceRootId::from_bytes([1; 16]),
                WorkspaceRootId::from_bytes([2; 16]),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn context_authorizer_rejects_forged_root_bindings()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
        let plan = candidate().plan;
        let parent_roots = plan
            .roots
            .iter()
            .map(|(root_id, root)| WorkspaceContextRoot {
                root_id: *root_id,
                source_path: PathBuf::from(if cfg!(windows) {
                    format!("C:\\roots\\{}", root_id.into_bytes()[0])
                } else {
                    format!("/roots/{}", root_id.into_bytes()[0])
                }),
                workspace_id: root.target_workspace_id,
                workspace_name: format!("parent-{}", root_id.into_bytes()[0]),
                parent_workspace_id: None,
                mount_path: None,
            })
            .collect::<Vec<_>>();
        registry
            .register_root(plan.parent_context_id, parent_roots.clone())
            .await?;
        let child_roots = plan
            .roots
            .iter()
            .zip(parent_roots)
            .map(|((root_id, root), parent)| WorkspaceContextRoot {
                root_id: *root_id,
                source_path: parent.source_path,
                workspace_id: root.source_workspace_id,
                workspace_name: format!("child-{}", root_id.into_bytes()[0]),
                parent_workspace_id: Some(root.target_workspace_id),
                mount_path: None,
            })
            .collect::<Vec<_>>();
        registry
            .register_child(plan.child_context_id, plan.parent_context_id, child_roots)
            .await?;
        assert!(registry.authorize(&plan).await?);

        let mut touched_only = plan.clone();
        touched_only
            .roots
            .remove(&WorkspaceRootId::from_bytes([2; 16]));
        assert!(registry.authorize(&touched_only).await?);

        let mut forged = plan;
        forged
            .roots
            .get_mut(&WorkspaceRootId::from_bytes([1; 16]))
            .expect("root")
            .target_workspace_id = WorkspaceId::from_bytes([99; 16]);
        assert!(!registry.authorize(&forged).await?);
        Ok(())
    }

    #[tokio::test]
    async fn workspace_publisher_previews_then_publishes_the_pinned_child_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let parent = fs.create_workspace("publisher-parent").await?;
        let mut base_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        base_tx.write_text("/base.txt", "base").await?;
        let TransactionCommit::Committed(base) = base_tx.commit().await? else {
            return Err("base commit did not publish".into());
        };
        let child = parent
            .fork(
                "publisher-child",
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        let mut child_tx = child.begin_transaction(IdempotencyKey::new()).await?;
        child_tx.write_text("/child.txt", "child").await?;
        let TransactionCommit::Committed(child_head) = child_tx.commit().await? else {
            return Err("child commit did not publish".into());
        };
        let mut parent_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        parent_tx.write_text("/parent.txt", "parent").await?;
        let TransactionCommit::Committed(parent_head) = parent_tx.commit().await? else {
            return Err("parent commit did not publish".into());
        };
        let resolver = Resolver {
            workspaces: Mutex::new(BTreeMap::new()),
        };
        resolver
            .workspaces
            .lock()
            .expect("resolver lock")
            .extend([(parent.id(), parent.clone()), (child.id(), child.clone())]);
        let publisher = WorkspaceMultiRootPublisher::new(resolver);
        let operation = OperationId::from_bytes([31; 16]);
        let root_id = WorkspaceRootId::from_bytes([32; 16]);
        let root = MultiRootMergeRoot {
            source_workspace_id: child.id(),
            merge_workspace_id: None,
            source_generation: child_head.id(),
            target_workspace_id: parent.id(),
            target_generation: parent_head.id(),
            base_generation: base.id(),
        };
        let resolutions = BTreeMap::new();
        let fence = match publisher
            .prepare(operation, root_id, &root, &resolutions)
            .await?
        {
            MultiRootPrepare::Ready(fence) => fence,
            MultiRootPrepare::Stale | MultiRootPrepare::Conflicted(_) => {
                return Err("preview rejected a clean join".into());
            }
        };
        assert_eq!(parent.head().await?.id(), parent_head.id());
        let published = publisher
            .publish(operation, root_id, &root, &resolutions, &fence)
            .await?;
        let published_generation = parent.head().await?.id();
        assert!(matches!(
            published,
            MultiRootPublishRoot::Published(generation)
                if generation == published_generation
        ));
        assert_eq!(parent.read("/child.txt", 16).await?.as_ref(), b"child");
        assert_eq!(parent.read("/parent.txt", 16).await?.as_ref(), b"parent");
        assert_eq!(
            publisher
                .publish(operation, root_id, &root, &resolutions, &fence)
                .await?,
            MultiRootPublishRoot::AlreadyPublished(published_generation)
        );
        Ok(())
    }

    #[tokio::test]
    async fn automatic_text_markers_are_reported_not_published()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let parent = fs.create_workspace("publisher-conflict-parent").await?;
        let mut base_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        base_tx.write_text("/conflicted.txt", "base\n").await?;
        let TransactionCommit::Committed(base) = base_tx.commit().await? else {
            return Err("base commit did not publish".into());
        };
        let child = parent
            .fork(
                "publisher-conflict-child",
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        let mut child_tx = child.begin_transaction(IdempotencyKey::new()).await?;
        child_tx.write_text("/conflicted.txt", "child\n").await?;
        let TransactionCommit::Committed(child_head) = child_tx.commit().await? else {
            return Err("child commit did not publish".into());
        };
        let mut parent_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        parent_tx.write_text("/conflicted.txt", "parent\n").await?;
        let TransactionCommit::Committed(parent_head) = parent_tx.commit().await? else {
            return Err("parent commit did not publish".into());
        };
        let resolver = Resolver {
            workspaces: Mutex::new(BTreeMap::from([
                (parent.id(), parent.clone()),
                (child.id(), child.clone()),
            ])),
        };
        let mut registry = MergeDriverRegistry::new();
        registry.register("text", std::sync::Arc::new(crate::DefaultTextMergeDriver))?;
        registry.set_default("text")?;
        let publisher = WorkspaceMultiRootPublisher::new(resolver)
            .with_merge_drivers(std::sync::Arc::new(registry));
        let root = MultiRootMergeRoot {
            source_workspace_id: child.id(),
            merge_workspace_id: None,
            source_generation: child_head.id(),
            target_workspace_id: parent.id(),
            target_generation: parent_head.id(),
            base_generation: base.id(),
        };
        assert!(matches!(
            publisher
                .prepare(
                    OperationId::from_bytes([41; 16]),
                    WorkspaceRootId::from_bytes([42; 16]),
                    &root,
                    &BTreeMap::new(),
                )
                .await,
            Ok(MultiRootPrepare::Conflicted(_))
        ));
        assert_eq!(parent.head().await?.id(), parent_head.id());
        assert_eq!(
            parent.read("/conflicted.txt", 32).await?.as_ref(),
            b"parent\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn conflicted_publication_projects_edits_and_continues_durably()
    -> Result<(), Box<dyn std::error::Error>> {
        Box::pin(conflicted_publication_projects_edits_and_continues_durably_case()).await
    }

    async fn conflicted_publication_projects_edits_and_continues_durably_case()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let parent = fs.create_workspace("conflict-parent").await?;
        let mut base_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        base_tx.write_text("/file.txt", "base\n").await?;
        let TransactionCommit::Committed(base) = base_tx.commit().await? else {
            return Err("base did not commit".into());
        };
        let child = parent
            .fork(
                "conflict-child",
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        let mut child_tx = child.begin_transaction(IdempotencyKey::new()).await?;
        child_tx.write_text("/file.txt", "child\n").await?;
        let TransactionCommit::Committed(child_head) = child_tx.commit().await? else {
            return Err("child did not commit".into());
        };
        let mut parent_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        parent_tx.write_text("/file.txt", "parent\n").await?;
        let TransactionCommit::Committed(parent_head) = parent_tx.commit().await? else {
            return Err("parent did not commit".into());
        };
        let resolver = Resolver {
            workspaces: Mutex::new(BTreeMap::from([
                (parent.id(), parent.clone()),
                (child.id(), child.clone()),
            ])),
        };
        let mut drivers = MergeDriverRegistry::new();
        drivers.register("text", std::sync::Arc::new(DefaultTextMergeDriver))?;
        drivers.set_default("text")?;
        let operation_id = OperationId::from_bytes([71; 16]);
        let root_id = WorkspaceRootId::from_bytes([72; 16]);
        let candidate = MultiRootMergeCandidate {
            plan: MultiRootMergePlan {
                operation_id,
                parent_context_id: WorkspaceContextId::from_bytes([73; 16]),
                child_context_id: WorkspaceContextId::from_bytes([74; 16]),
                roots: BTreeMap::from([(
                    root_id,
                    MultiRootMergeRoot {
                        source_workspace_id: child.id(),
                        merge_workspace_id: None,
                        source_generation: child_head.id(),
                        target_workspace_id: parent.id(),
                        target_generation: parent_head.id(),
                        base_generation: base.id(),
                    },
                )]),
            },
            resolutions: BTreeMap::from([(root_id, BTreeMap::new())]),
        };
        let coordinator = MultiRootPublicationCoordinator::new(
            MemoryMultiRootPublicationStore::default(),
            WorkspaceMultiRootPublisher::new(resolver)
                .with_merge_drivers(std::sync::Arc::new(drivers)),
            Allow,
        );
        let Publication::Conflicted(conflicted) = coordinator.publish(candidate).await? else {
            return Err("publication did not enter conflict state".into());
        };
        assert_eq!(conflicted.projected_roots.len(), 1);
        assert!(
            String::from_utf8(parent.read("/file.txt", 1_024).await?.to_vec())?
                .contains("<<<<<<< ours")
        );
        assert!(matches!(
            coordinator
                .continue_conflicted(operation_id, WorkspaceContextId::from_bytes([73; 16]))
                .await,
            Err(MultiRootPublicationError::InvalidCandidate)
        ));
        assert!(matches!(
            coordinator
                .declare_conflicted_paths(
                    operation_id,
                    WorkspaceContextId::from_bytes([73; 16]),
                    root_id,
                    &["file.txt".to_owned()],
                )
                .await,
            Err(MultiRootPublicationError::Publisher(
                WorkspaceMultiRootPublisherError::Candidate(_)
            ))
        ));
        let mut resolution = parent.begin_transaction(IdempotencyKey::new()).await?;
        resolution.write_text("/file.txt", "resolved\n").await?;
        resolution.commit().await?;
        coordinator
            .declare_conflicted_paths(
                operation_id,
                WorkspaceContextId::from_bytes([73; 16]),
                root_id,
                &["file.txt".to_owned()],
            )
            .await
            .expect("resolved conflict path declaration");
        let declared = coordinator
            .store
            .load(operation_id)
            .await?
            .expect("conflict journal");
        assert_eq!(
            declared.conflicts[&root_id]
                .conflicts
                .iter()
                .map(|view| view.key.clone())
                .collect::<BTreeSet<_>>(),
            declared.declared_conflicts[&root_id]
        );
        assert!(matches!(
            coordinator
                .continue_conflicted(operation_id, WorkspaceContextId::from_bytes([73; 16]))
                .await?,
            Publication::Applied(_)
        ));
        assert_eq!(parent.read("/file.txt", 32).await?.as_ref(), b"resolved\n");
        assert!(coordinator.pending_operations().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn conflicted_publication_abort_restores_premerge_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fs = Fs::memory();
        let parent = fs.create_workspace("abort-parent").await?;
        let mut base_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        base_tx.write_text("/file.txt", "base\n").await?;
        let TransactionCommit::Committed(base) = base_tx.commit().await? else {
            return Err("base did not commit".into());
        };
        let child = parent
            .fork(
                "abort-child",
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        let mut child_tx = child.begin_transaction(IdempotencyKey::new()).await?;
        child_tx
            .write_text("/file.txt", "discarded child\n")
            .await?;
        let TransactionCommit::Committed(child_head) = child_tx.commit().await? else {
            return Err("child did not commit".into());
        };
        let mut parent_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        parent_tx.write_text("/file.txt", "kept parent\n").await?;
        let TransactionCommit::Committed(parent_head) = parent_tx.commit().await? else {
            return Err("parent did not commit".into());
        };
        let resolver = Resolver {
            workspaces: Mutex::new(BTreeMap::from([
                (parent.id(), parent.clone()),
                (child.id(), child.clone()),
            ])),
        };
        let mut drivers = MergeDriverRegistry::new();
        drivers.register("text", std::sync::Arc::new(DefaultTextMergeDriver))?;
        drivers.set_default("text")?;
        let root_id = WorkspaceRootId::from_bytes([72; 16]);
        let abort_operation = OperationId::from_bytes([75; 16]);
        let abort_candidate = MultiRootMergeCandidate {
            plan: MultiRootMergePlan {
                operation_id: abort_operation,
                parent_context_id: WorkspaceContextId::from_bytes([76; 16]),
                child_context_id: WorkspaceContextId::from_bytes([77; 16]),
                roots: BTreeMap::from([(
                    root_id,
                    MultiRootMergeRoot {
                        source_workspace_id: child.id(),
                        merge_workspace_id: None,
                        source_generation: child_head.id(),
                        target_workspace_id: parent.id(),
                        target_generation: parent_head.id(),
                        base_generation: base.id(),
                    },
                )]),
            },
            resolutions: BTreeMap::from([(root_id, BTreeMap::new())]),
        };
        let aborting = MultiRootPublicationCoordinator::new(
            MemoryMultiRootPublicationStore::default(),
            WorkspaceMultiRootPublisher::new(resolver)
                .with_merge_drivers(std::sync::Arc::new(drivers)),
            Allow,
        );
        assert!(matches!(
            aborting.publish(abort_candidate).await?,
            Publication::Conflicted(_)
        ));
        let mut unrelated = parent.begin_transaction(IdempotencyKey::new()).await?;
        unrelated
            .write_text("/unrelated.txt", "preserved\n")
            .await?;
        let TransactionCommit::Committed(_) = unrelated.commit().await? else {
            return Err("unrelated edit did not commit".into());
        };
        aborting
            .abort_conflicted(abort_operation, WorkspaceContextId::from_bytes([76; 16]))
            .await?;
        assert_eq!(
            parent.read("/file.txt", 32).await?.as_ref(),
            b"kept parent\n"
        );
        assert_eq!(
            parent.read("/unrelated.txt", 32).await?.as_ref(),
            b"preserved\n"
        );
        assert!(aborting.pending_operations().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn aborting_a_conflict_that_changed_no_target_path_succeeds()
    -> Result<(), Box<dyn std::error::Error>> {
        // Both sides add the same path with different bytes and no merge
        // driver: the projection holds the record back, so it differs from the
        // pre-merge target in no path. Abort must still clear the operation.
        let fs = Fs::memory();
        let parent = fs.create_workspace("empty-abort-parent").await?;
        let mut base_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        base_tx.write_text("/keep.txt", "base\n").await?;
        let TransactionCommit::Committed(base) = base_tx.commit().await? else {
            return Err("base did not commit".into());
        };
        let child = parent
            .fork(
                "empty-abort-child",
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        let mut child_tx = child.begin_transaction(IdempotencyKey::new()).await?;
        child_tx.write_text("/added.txt", "from child\n").await?;
        let TransactionCommit::Committed(child_head) = child_tx.commit().await? else {
            return Err("child did not commit".into());
        };
        let mut parent_tx = parent.begin_transaction(IdempotencyKey::new()).await?;
        parent_tx.write_text("/added.txt", "from parent\n").await?;
        let TransactionCommit::Committed(parent_head) = parent_tx.commit().await? else {
            return Err("parent did not commit".into());
        };
        let resolver = Resolver {
            workspaces: Mutex::new(BTreeMap::from([
                (parent.id(), parent.clone()),
                (child.id(), child.clone()),
            ])),
        };
        let root_id = WorkspaceRootId::from_bytes([82; 16]);
        let operation = OperationId::from_bytes([85; 16]);
        let parent_context = WorkspaceContextId::from_bytes([86; 16]);
        let candidate = MultiRootMergeCandidate {
            plan: MultiRootMergePlan {
                operation_id: operation,
                parent_context_id: parent_context,
                child_context_id: WorkspaceContextId::from_bytes([87; 16]),
                roots: BTreeMap::from([(
                    root_id,
                    MultiRootMergeRoot {
                        source_workspace_id: child.id(),
                        merge_workspace_id: None,
                        source_generation: child_head.id(),
                        target_workspace_id: parent.id(),
                        target_generation: parent_head.id(),
                        base_generation: base.id(),
                    },
                )]),
            },
            resolutions: BTreeMap::from([(root_id, BTreeMap::new())]),
        };
        let coordinator = MultiRootPublicationCoordinator::new(
            MemoryMultiRootPublicationStore::default(),
            WorkspaceMultiRootPublisher::new(resolver),
            Allow,
        );
        assert!(matches!(
            coordinator.publish(candidate).await?,
            Publication::Conflicted(_)
        ));
        coordinator
            .abort_conflicted(operation, parent_context)
            .await?;
        assert_eq!(
            parent.read("/added.txt", 32).await?.as_ref(),
            b"from parent\n"
        );
        assert_eq!(parent.read("/keep.txt", 32).await?.as_ref(), b"base\n");
        assert!(coordinator.pending_operations().await?.is_empty());
        Ok(())
    }
}
