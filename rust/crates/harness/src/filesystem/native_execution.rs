//! Optional native command composition over existing task, effect and workspace owners.

use super::{
    FilesystemHost, NativeVolumeNamespace, NativeViewManifest, NativeViewOptions,
    NativeVolumeMapping,
};
use crate::{
    Error, Result,
    conversation::{ContentGrant, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    resources::GenerationRef,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, MaterializationJournal, MaterializationJournalStore,
    MaterializationPhase, NativeMaterializationWork, NativeWorkingSet, PublicationPermit,
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use std::{
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

/// Existing native namespace owner; this seam creates no view or authority.
pub trait NativeProcessView<P: StreamProvider>: Send + Sync {
    /// Immutable original request/approval selection.
    fn manifest(&self) -> &NativeViewManifest;
    /// The original preparation/lifetime token, never a replacement request token.
    fn operation_cancellation(&self) -> &acyclic_fs::CancellationToken;
    /// A genuine prepared caller family; legacy preparation carries no such proof.
    fn original_native_work(&self) -> Option<&NativeMaterializationWork>;
    /// Admit the dynamic frame before bounded physical checks.
    fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<crate::NativeFutureBundle<'_, std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)>>, (Error, acyclic_fs::WorkCounters)>;
    /// Observe an original held root and no-follow relative directory. This
    /// does not authorize a later ambient-path process launch as confinement.
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)>>, (Error, acyclic_fs::WorkCounters)>;
    /// Admit publication's dynamic frame; completed destinations remain final.
    fn publish<'a>(
        &'a self, owner: NativeViewOwner<'a, P>,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure>>, (Error, acyclic_fs::WorkCounters)>;
    /// Uses the caller's actual original ledger and the held view's cancellation.
    fn validate_for_dispatch_with_native_work<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Checks the same held root without constructing a replacement work span.
    fn validate_working_directory_with_native_work<'a>(
        &'a self, cwd: &'a Path, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Retains genuine mounted-namespace scopes through process quiescence.
    fn retain_native_namespace_scopes<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessNamespaceScopes>>;
    /// Retains actual completed destinations and original typed publication cause.
    fn publish_with_native_work<'a>(
        &'a self, owner: NativeViewOwner<'a, P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>>;
}

/// Opaque task freshness guard; no signing or journal capability is exposed.
pub struct NativeViewOwner<'a, P> {
    owner: &'a TaskJournalOwner<P>,
}

impl<P> Copy for NativeViewOwner<'_, P> {}

impl<P> Clone for NativeViewOwner<'_, P> {
    fn clone(&self) -> Self { *self }
}

impl<'a, P: StreamProvider> NativeViewOwner<'a, P> {
    pub(super) const fn new(owner: &'a TaskJournalOwner<P>) -> Self {
        Self { owner }
    }

    /// Check the original fresh owner and created WRITE custody on this same
    /// actual ledger, including bounded canonical history verification.
    pub async fn verify_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.owner.verify_with_native_work(false, work).await
    }

    /// Require the exact original normalized grant without exposing its signer.
    pub fn require_volume_grant_with_native_work(
        &self, volume: &VolumeRef, operation: VolumeOperation, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.owner.require_volume_grant_with_native_work(volume, operation, work)
    }

    /// Reserve a real SDK gate for the original qualified destination and
    /// immutable generation. Receipts already include all work on `work`.
    #[allow(clippy::too_many_arguments, reason = "original selection, work and cleanup tokens must not be substituted")]
    pub async fn reserve_publication<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self, workspace: acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<
        acyclic_fs::OperationReceipt<NativePublicationReservation<A, O>>,
        NativePublicationReservationFailure<A, O>,
    > {
        let receipt = self.acquire_publication(
            workspace, volume, original_generation, original_operation, work,
            cancellation, cleanup_cancellation, NativePublicationAcquisition::Reserve,
        ).await?;
        match receipt.value {
            Some(value) => Ok(acyclic_fs::OperationReceipt { value, work: receipt.work }),
            None => Err(NativePublicationReservationFailure {
                error: NativeReservationError::Selection("native reservation acknowledgement has no original proof"),
                cleanup_error: None,
                work: receipt.work, cleanup: None, unwitnessed_workspace: None,
            }),
        }
    }

    /// Observe only an already active gate for the original filesystem operation.
    /// Missing gates remain absent; neither acquisition nor failure cleanup writes.
    #[allow(clippy::too_many_arguments, reason = "recovery retains the same original selection and lifetime tokens")]
    pub async fn recover_publication<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self, workspace: acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<
        acyclic_fs::OperationReceipt<Option<NativePublicationReservation<A, O>>>,
        NativePublicationReservationFailure<A, O>,
    > {
        self.acquire_publication(
            workspace, volume, original_generation, original_operation, work,
            cancellation, cleanup_cancellation, NativePublicationAcquisition::ReadonlyExisting,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "one engine validates both original acquisition decisions")]
    async fn acquire_publication<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self, workspace: acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken, mode: NativePublicationAcquisition,
    ) -> std::result::Result<
        acyclic_fs::OperationReceipt<Option<NativePublicationReservation<A, O>>>,
        NativePublicationReservationFailure<A, O>,
    > {
        let mut reservation = None;
        let result = match crate::NativeFutureBundle::new(work, || async { Ok(async {
            self.owner.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
                .map_err(NativeReservationError::Owner)?;
            {
                let (encoded, _encoded_custody) = native_canonical(volume, work).map_err(NativeReservationError::Owner)?;
                work.charge(acyclic_fs::WorkCounters {
                    bytes_hashed: encoded.len() as u64, ..acyclic_fs::WorkCounters::default()
                }).map_err(NativeReservationError::Native)?;
                let (name, _name_custody) = work.display(format_args!(
                    "harness-{}", blake3::hash(&encoded).to_hex(),
                )).map_err(NativeReservationError::Native)?;
                if workspace.name().as_str() != name {
                    return Err(NativeReservationError::Selection("native reservation workspace differs from original volume"));
                }
            }
            cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(NativeReservationError::Native)?;
            let budget = work.remaining().map_err(NativeReservationError::Native)?;
            let acquired = match mode {
                NativePublicationAcquisition::Reserve => workspace
                    .reserve_publication_measured(original_operation, budget, cancellation).await
                    .map(|receipt| acyclic_fs::OperationReceipt { value: Some(receipt.value), work: receipt.work }),
                NativePublicationAcquisition::ReadonlyExisting => workspace
                    .publication_reservation_measured(original_operation, budget, cancellation).await,
            };
            match acquired {
                Ok(receipt) => {
                    // Own the real proof before any fallible accounting,
                    // cancellation, owner, or selected-generation check.
                    reservation = receipt.value;
                    work.spent(receipt.work).map_err(NativeReservationError::Native)?;
                }
                Err(failure) => {
                    let error = NativeReservationError::Sdk(failure.error);
                    work.spent(*failure.work).map_err(NativeReservationError::Native)?;
                    return Err(error);
                }
            }
            self.owner.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
                .map_err(NativeReservationError::Owner)?;
            if reservation.is_none() {
                return Ok(());
            }
            let head = match workspace.head_measured(
                work.remaining().map_err(NativeReservationError::Native)?, cancellation,
            ).await {
                Ok(receipt) => {
                    work.spent(receipt.work).map_err(NativeReservationError::Native)?;
                    receipt.value
                }
                Err(failure) => {
                    let error = NativeReservationError::Sdk(failure.error);
                    work.spent(*failure.work).map_err(NativeReservationError::Native)?;
                    return Err(error);
                }
            };
            if head.id() != original_generation {
                return Err(NativeReservationError::Selection("native reservation generation differs from original selection"));
            }
            drop(head);
            self.owner.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
                .map_err(NativeReservationError::Owner)?;
            cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(NativeReservationError::Native)?;
            work.remaining().map_err(NativeReservationError::Native)?;
            Ok::<(), NativeReservationError>(())
        }.await) }) {
            Ok(future) => match future.await {
                Ok(result) => result,
                Err(error) => Err(NativeReservationError::Owner(error)),
            },
            Err(error) => Err(NativeReservationError::Owner(error)),
        };
        let error = match result {
            Ok(()) => return Ok(acyclic_fs::OperationReceipt {
                value: reservation.map(|reservation| NativePublicationReservation {
                    cleanup: NativePublicationCleanup { workspace, reservation },
                }),
                work: work.receipt(),
            }),
            Err(error) => error,
        };
        if let Some(reservation) = reservation {
            let cleanup = NativePublicationCleanup { workspace, reservation };
            if matches!(mode, NativePublicationAcquisition::ReadonlyExisting) {
                return Err(NativePublicationReservationFailure {
                    error, cleanup_error: None,
                    work: work.receipt(), cleanup: Some(cleanup), unwitnessed_workspace: None,
                });
            }
            match cleanup.release(work, cleanup_cancellation).await {
                Ok(receipt) => Err(NativePublicationReservationFailure {
                    error, cleanup_error: None,
                    work: receipt.work, cleanup: None, unwitnessed_workspace: None,
                }),
                Err(failure) => Err(NativePublicationReservationFailure {
                    error, cleanup_error: Some(failure.error), work: failure.work,
                    cleanup: failure.cleanup, unwitnessed_workspace: failure.unwitnessed_workspace,
                }),
            }
        } else {
            Err(NativePublicationReservationFailure {
                error, cleanup_error: None,
                work: work.receipt(), cleanup: None, unwitnessed_workspace: Some(workspace),
            })
        }
    }
}

#[derive(Clone, Copy)]
enum NativePublicationAcquisition {
    Reserve,
    ReadonlyExisting,
}

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Borrow the original owner without exposing its signer or journal.
    /// This guard is not a grant, approval, or SDK publication reservation.
    pub fn native_view_owner(&self) -> NativeViewOwner<'_, P> { NativeViewOwner::new(self) }
}

/// An authenticated SDK reservation for an original native destination.
/// This does not replace task grants, approval, or current owner verification.
pub struct NativePublicationReservation<A, O> {
    cleanup: NativePublicationCleanup<A, O>,
}

/// Only settlement of an already acquired real SDK reservation is permitted.
/// In particular, an unsuccessful factory never exposes a publication permit.
pub struct NativePublicationCleanup<A, O> {
    workspace: acyclic_fs::Workspace<A, O>,
    reservation: acyclic_fs::PublicationReservation,
}

/// Allocation-free reservation failure classification with original causes.
#[derive(Debug, thiserror::Error)]
pub enum NativeReservationError {
    /// Original Root owner/grant/codec error, moved without formatting.
    #[error(transparent)]
    Owner(Error),
    /// Actual native ledger, cancellation, admission, or IO failure.
    #[error(transparent)]
    Native(acyclic_fs::NativeTreeMaterializationError),
    /// Actual SDK reservation/replay/release cause, including typed uncertainty.
    #[error(transparent)]
    Sdk(acyclic_fs::WorkspaceError),
    /// Fixed immutable selection mismatch, with no allocated diagnostic.
    #[error("{0}")]
    Selection(&'static str),
}


/// Physical settlement uncertainty retains the exact cleanup capability.
pub struct NativePublicationReservationFailure<A, O> {
    pub error: NativeReservationError,
    /// Additional actual release failure; `error` remains the factory's cause.
    pub cleanup_error: Option<NativeReservationError>,
    pub work: acyclic_fs::WorkCounters,
    pub cleanup: Option<NativePublicationCleanup<A, O>>,
    /// Original SDK workspace retained without any witnessed gate proof.
    /// This never yields a permit or claims physical settlement.
    pub unwitnessed_workspace: Option<acyclic_fs::Workspace<A, O>>,
}

impl<A, O> NativePublicationReservation<A, O> {
    /// Copies only the actual SDK gate identity and its authenticated head.
    pub const fn permit(&self) -> PublicationPermit {
        PublicationPermit::Reservation {
            operation_id: self.cleanup.reservation.operation_id.to_bytes(),
            gate_tail: self.cleanup.reservation.gate_tail,
            expected: self.cleanup.reservation.expected,
        }
    }

    /// Transfer the exact reservation into settlement-only custody.
    pub fn into_cleanup(self) -> NativePublicationCleanup<A, O> { self.cleanup }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> NativePublicationCleanup<A, O> {
    /// Release this actual gate using the caller's original cleanup token.
    /// Any failed or ambiguous release returns this same capability for retry.
    pub async fn release(
        self, work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<()>, NativePublicationReservationFailure<A, O>> {
        let result = match work.remaining().map_err(NativeReservationError::Native) {
            Ok(budget) => match crate::NativeFutureBundle::new(work,
                || async { Ok(self.workspace.release_publication_measured(self.reservation, budget, cancellation).await) },
            ) {
                Ok(future) => match future.await {
                    Ok(result) => result.map_err(|failure| {
                        let error = NativeReservationError::Sdk(failure.error);
                        match work.spent(*failure.work) {
                            Ok(()) => error,
                            Err(error) => NativeReservationError::Native(error),
                        }
                    }),
                    Err(error) => Err(NativeReservationError::Owner(error)),
                },
                Err(error) => Err(NativeReservationError::Owner(error)),
            },
            Err(error) => Err(error),
        };
        match result {
            Ok(receipt) => match work.spent(receipt.work).map_err(NativeReservationError::Native) {
                Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: work.receipt() }),
                // The gate was actually released, even if final accounting
                // rejects admission. Do not advertise a live cleanup owner.
                Err(error) => Err(NativePublicationReservationFailure {
                    error, cleanup_error: None,
                    work: work.receipt(), cleanup: None,
                    unwitnessed_workspace: None,
                }),
            },
            Err(error) => Err(NativePublicationReservationFailure {
                error, cleanup_error: None,
                work: work.receipt(), cleanup: Some(self),
                unwitnessed_workspace: None,
            }),
        }
    }
}

/// Host-only selection. Credentials and publication permits never enter a command.
pub struct BorrowedNativeVolumeBinding<'a, A, O> {
    /// Exact provider capability used for this volume; a consumer router may
    /// compose heterogeneous stores under the existing provider traits.
    pub host: &'a FilesystemHost<A, O>,
    /// Owner-bound selected volume.
    pub volume: &'a VolumeRef,
    /// Exact immutable input generation.
    pub generation: &'a GenerationRef,
    /// Declared mechanism; this preparer accepts materialized directories.
    /// Existing lazy mounts are composed by their live authenticated owner.
    pub namespace: &'a NativeVolumeNamespace,
    /// Relative native path below the prepared view root.
    pub path: &'a str,
    /// Whether captured edits may publish to this destination.
    pub writable: bool,
    /// Existing signed authority verifier.
    pub verifier: &'a AuthorityVerifier,
    /// Original read authority, and write authority for a destination.
    pub scope: &'a Scope,
    /// Existing Filesystem operation permit for destination publication.
    pub publication: PublicationPermit,
}

/// Explicit host allocation. Retained native paths never allocate or adopt a root.
#[derive(Clone, Debug)]
pub struct NativePreparationRoot {
    /// Existing physical namespace selected by the host.
    pub root: PathBuf,
    /// Same-volume service allocation outside the root, with empty target/backup directories.
    pub operation_directory: PathBuf,
    /// Actual root allocation witnessed by the host, not a provider result.
    pub expected_identity: acyclic_fs::NativeRootIdentity,
    /// Actual service allocation identities, witnessed before any staging writes.
    pub expected_preparation_identity: acyclic_fs::MaterializationPreparationIdentity,
}

impl NativePreparationRoot {
    /// Stable original task-derived preparation identity, including before an
    /// acknowledgement is available. Computing it grants no native authority.
    #[must_use]
    pub fn operation<P: StreamProvider>(owner: &TaskJournalOwner<P>) -> acyclic_fs::OperationId {
        native_preparation_operation(owner)
    }

    /// Witnesses existing host allocations before preparation. This constructor
    /// creates no directory, signs no grant, and refuses unrelated initial data.
    pub async fn witness(
        root: PathBuf,
        operation_directory: PathBuf,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Self>, (Error, acyclic_fs::WorkCounters)> {
        if !budget.has_finite_allowances() {
            return Err((Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation witness requires finite allowances" }, acyclic_fs::WorkCounters::default()));
        }
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, spent)| (Error::NativeConstruction { cause, work: spent }, spent))?;
        let result = Self::witness_with_native_work(&root, &operation_directory, &work)
            .map_err(|error| (error, work.receipt()))?.await;
        native_result(result.map(|(value, _custody)| value), &work)
    }

    /// Observes host allocations using the caller's original native operation.
    /// Borrowing the input paths does not re-adopt their already-owned buffers.
    pub fn witness_with_native_work<'a>(
        root: &'a std::path::Path,
        operation_directory: &'a std::path::Path,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Self, acyclic_fs::NativeAllocation)>> {
        use acyclic_fs::MaterializationBackend as _;
        if !work.remaining().map_err(native_root_error)?.has_finite_allowances() || !root.is_absolute() || !operation_directory.is_absolute() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation witness requires absolute paths and finite allowances" });
        }
        crate::NativeFutureBundle::new(work, || async {
        let (root, mut custody) = work.copy_path(root).map_err(native_root_error)?;
        let (operation_directory, mut operation_custody) = work.copy_path(operation_directory).map_err(native_root_error)?;
        custody.absorb(&mut operation_custody).map_err(native_root_error)?;
        let meter = work.clone();
        acyclic_native_runtime::run_blocking_io_measured(move || {
            let backend = acyclic_fs::NativeTreeMaterializationBackend::open_owned_with_work(
                root, operation_directory, custody, meter.clone(),
            ).map_err(native_root_error)?;
            backend.require_initial_empty().map_err(native_root_error)?;
            let expected_identity = acyclic_fs::NativeRootIdentity::from_bytes(
                backend.allocation_identity().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict, reason: "native root allocation is unwitnessed" })?,
            );
            let expected_preparation_identity = backend.preparation_identity()
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native staging allocation is unwitnessed" })?;
            let (root, mut custody) = meter.copy_path(backend.root_path()).map_err(native_root_error)?;
            let (operation_directory, mut operation_custody) = meter.copy_path(backend.operation_directory_path()).map_err(native_root_error)?;
            custody.absorb(&mut operation_custody).map_err(native_root_error)?;
            Ok::<_, Error>((Self { root, operation_directory, expected_identity, expected_preparation_identity }, custody))
        }, work.runtime_meter_owner())
            .map_err(|error| native_root_error(work.runtime_error(error)))?
            .await.map_err(|error| native_root_error(work.runtime_error(error)))?
        })
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NativePreparationIntent {
    task: crate::TaskId,
    original_fence: crate::scheduler::LeaseFence,
    operation_directory: PathBuf,
    manifest: NativeViewManifest,
}

/// Same-engine durable preparation progress; this evidence conveys no authority.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePreparationReceipt {
    /// Original task and creation fence retained before physical edits.
    pub task: crate::TaskId,
    /// Original task reservation, not permission to attach under a stale fence.
    pub original_fence: crate::scheduler::LeaseFence,
    /// Original selected namespace and generations.
    pub manifest: NativeViewManifest,
    /// Original materializer operation and actual allocation/preparation identities.
    pub journal: MaterializationJournal,
    /// Independently observed physical-root status.
    pub root: acyclic_fs::MaterializationRootObservation,
    /// Actual original staging-allocation status, including replacement.
    pub preparation: acyclic_fs::MaterializationPreparationObservation,
    /// Independently observed captured endpoints. Uncaptured edits remain unknown.
    pub observations: Vec<acyclic_fs::MaterializationObservation>,
    #[serde(skip)]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl PartialEq for NativePreparationReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.task == other.task && self.original_fence == other.original_fence
            && self.manifest == other.manifest && self.journal == other.journal
            && self.root == other.root && self.preparation == other.preparation
            && self.observations == other.observations
    }
}

/// A protected journal observation, never a physical-root or cleanup witness.
pub enum NativePreparationJournalObservation {
    /// The original record is absent. Allocation may have preceded its marker,
    /// so this never proves that the native namespace is absent or releasable.
    MissingJournalPhysicalStateUnknown { preparation_operation: acyclic_fs::OperationId },
    /// Original recorded intent, allocation IDs, decisions and spent work.
    Recorded(NativeRecordedPreparation),
}

/// Owned output from the existing preparation journal; no host paths are opened.
pub struct NativeRecordedPreparation {
    pub task: crate::TaskId,
    pub original_fence: crate::scheduler::LeaseFence,
    pub manifest: NativeViewManifest,
    pub operation_directory: PathBuf,
    pub journal: MaterializationJournal,
    _custody: acyclic_fs::NativeAllocation,
}

/// Read-only journal failures preserve actual causes without formatting them.
#[derive(Debug, thiserror::Error)]
pub enum NativePreparationJournalError {
    #[error(transparent)]
    Owner(#[from] Error),
    #[error(transparent)]
    Native(#[from] acyclic_fs::NativeTreeMaterializationError),
    #[error(transparent)]
    Backend(#[from] acyclic_stream::StreamError),
    #[error("{0}")]
    Record(&'static str),
}

impl From<acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>>
    for NativePreparationJournalError
{
    fn from(error: acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>) -> Self {
        Self::Owner(native_materialization_error(error))
    }
}


impl NativePreparationJournalError {
    // Only the existing ordinary materializer adapter requires Harness Error.
    // The new read-only query keeps this closed typed carrier throughout.
    fn into_owner_error(self) -> Error {
        match self {
            Self::Owner(error) => error,
            Self::Native(cause) => Error::NativeWork { cause, custody: None },
            Self::Backend(source) => Error::NativeStream { source, custody: None },
            Self::Record(reason) => Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason },
        }
    }
}

struct PreparedVolume<A, O> {
    view: NativeWorkingSet<A, O>,
    original_identity: acyclic_fs::NativeRootIdentity,
    writable: bool,
    publication: PublicationPermit,
    // Retains this volume's original resource gate through its payload lifetime.
    _work: NativeMaterializationWork,
}

/// Independent publication failure; completed destinations are not rolled back.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
#[error("native destination {destination:?}: {message}")]
pub struct NativePublicationFailure {
    /// Exact admitted mapping whose integrity/publication check failed.
    pub destination: Option<NativeVolumeMapping>,
    /// Destinations whose existing sync returned success before this failure.
    pub completed: Vec<NativeVolumeMapping>,
    /// Exact filesystem capture/publication work across mappings.
    pub work: acyclic_fs::WorkCounters,
    /// Original provider/fence/conflict diagnostic, not an inferred OS outcome.
    pub message: String,
}

/// An actual publication failure before any fallible diagnostic rendering.
/// The payload drops before its original allocation custody.
#[derive(Debug)]
pub struct NativeViewPublicationError {
    pub cause: Error,
    pub destination: Option<NativeVolumeMapping>,
    pub completed: Vec<NativeVolumeMapping>,
    /// Retained result already staged before a later destination failed.
    pub diagnostic: Option<crate::conversation::FileRef>,
    /// A denied diagnostic preserves both this cause and the actual denial.
    pub rendering_failure: Option<Error>,
    pub custody: acyclic_fs::NativeAllocation,
}

/// Actual successful publications remain actionable through caller postchecks.
pub struct NativeViewPublicationReceipt {
    pub completed: Vec<NativeVolumeMapping>,
    pub custody: acyclic_fs::NativeAllocation,
}

/// Actual mounted-source scopes; no process or SDK authority is created here.
/// Namespace contexts physically drop before their original vector custody.
pub struct NativeProcessNamespaceScopes {
    pub scopes: Vec<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope>,
    pub custody: acyclic_fs::NativeAllocation,
}

struct NativeMappingPayload {
    mapping: NativeVolumeMapping,
    allocation: acyclic_fs::NativeAllocation,
}

/// Provider-owned materialized namespace, with the canonical SDK capture owner
/// for every independently authorized volume. It carries no second workspace engine.
pub struct NativeVolumeView<A, O> {
    manifest: NativeViewManifest,
    volumes: Vec<PreparedVolume<A, O>>,
    preparation: NativePreparationReceipt,
    cancellation: acyclic_fs::CancellationToken,
    original_work: Option<NativeMaterializationWork>,
    _allocation: acyclic_fs::NativeAllocation,
}

impl<P, A, O> NativeProcessView<P> for NativeVolumeView<A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn manifest(&self) -> &NativeViewManifest {
        self.manifest()
    }
    fn operation_cancellation(&self) -> &acyclic_fs::CancellationToken {
        &self.cancellation
    }
    fn original_native_work(&self) -> Option<&NativeMaterializationWork> {
        self.original_work.as_ref()
    }
    fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<crate::NativeFutureBundle<'_, std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)>>, (Error, acyclic_fs::WorkCounters)> {
        let work = self.validation_work(budget)?;
        let meter = work.clone();
        crate::NativeFutureBundle::new(&work, || async move {
            let result = self.validate_dispatch_with_work(&meter).await;
            Ok(native_result(result, &meter))
        }).map_err(|error| (error, work.receipt()))
    }
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)>>, (Error, acyclic_fs::WorkCounters)> {
        let work = self.validation_work(budget)?;
        let meter = work.clone();
        crate::NativeFutureBundle::new(&work, || async move {
            Ok(native_result(self.validate_directory_with_work(cwd, &meter), &meter))
        }).map_err(|error| (error, work.receipt()))
    }
    fn publish<'a>(
        &'a self, owner: NativeViewOwner<'a, P>,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure>>, (Error, acyclic_fs::WorkCounters)> {
        let work = self.publication_work()?;
        let meter = work.clone();
        crate::NativeFutureBundle::new(&work, || async move {
            Ok(self.publish_with_work(owner.owner, &meter).await)
        }).map_err(|error| (error, work.receipt()))
    }
    fn validate_for_dispatch_with_native_work<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || self.validate_dispatch_with_work(work))
    }
    fn validate_working_directory_with_native_work<'a>(
        &'a self, cwd: &'a Path, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async {
            self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(native_root_error)?;
            self.validate_directory_with_work(cwd, work)
        })
    }
    fn retain_native_namespace_scopes<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessNamespaceScopes>> {
        crate::NativeFutureBundle::new(work, || async {
            self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(native_root_error)?;
            for mapping in &self.manifest.volumes {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: 1, ..acyclic_fs::WorkCounters::default()
                }).map_err(native_root_error)?;
                if !matches!(mapping.namespace, NativeVolumeNamespace::MaterializedDirectory) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "materialized native view cannot retain a mounted namespace",
                    });
                }
            }
            Ok(NativeProcessNamespaceScopes {
                scopes: Vec::new(),
                custody: work.retain_owned(0).map_err(native_root_error)?,
            })
        })
    }
    fn publish_with_native_work<'a>(
        &'a self, owner: NativeViewOwner<'a, P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>> {
        let custody = work.retain_owned(0).map_err(native_root_error)?;
        crate::NativeFutureBundle::new(work, || async move {
            Ok(self.publish_original(owner.owner, work, custody).await)
        })
    }
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Returns only the actual original operation retained by native preparation.
    #[must_use]
    pub fn original_native_work(&self) -> Option<&NativeMaterializationWork> {
        self.original_work.as_ref()
    }

    /// Transfers this view into an admitted Arc. Its custody remains inside the
    /// view until the final alias drops; opaque reference-count headers are not
    /// represented as logical payload.
    pub fn into_arc_with_native_work(
        mut self, work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self.original_work.as_ref().is_some_and(|original| original.shares_ledger(work))
            || !self._allocation.work().shares_ledger(work)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native view Arc requires its actual original operation" });
        }
        let mut custody = work.claim(std::mem::size_of::<Self>() as u64).map_err(native_root_error)?;
        self._allocation.absorb(&mut custody).map_err(native_root_error)?;
        Ok(Arc::new(self))
    }

    /// Creates or resumes the original task's journaled physical preparation.
    /// Reopening evidence never substitutes for current original grants/fences.
    #[inline(never)]
    pub async fn prepare<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>,
        allocation: NativePreparationRoot,
        options: NativeViewOptions,
        bindings: Vec<BorrowedNativeVolumeBinding<'a, A, O>>,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Self>, (Error, acyclic_fs::WorkCounters)> {
        let budget = options.budget_for_volume_count(bindings.len())
            .map_err(|error| (error, acyclic_fs::WorkCounters::default()))?;
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, spent)| (Error::NativeConstruction { cause, work: spent }, spent))?;
        let result = Self::prepare_with_native_work(
            owner, &allocation, &options, &bindings, &work,
        ).map_err(|error| (error, work.receipt()))?.await;
        native_result(result, &work)
    }

    /// Prepares and attaches the original selection through its original ledger.
    /// The host's allocation and all authority inputs remain borrowed.
    pub fn prepare_with_native_work<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>,
        allocation: &'a NativePreparationRoot,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        crate::NativeFutureBundle::new(work, || {
            Self::prepare_original(owner, allocation, options, bindings, work)
        })
    }

    #[allow(clippy::too_many_lines, reason = "one existing-engine preparation and attachment transaction")]
    async fn prepare_original<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>,
        allocation: &'a NativePreparationRoot,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        work: &'a NativeMaterializationWork,
    ) -> Result<Self> {
            let cancellation = work.cancellation();
            let result = async {
                owner.verify_with_native_work(false, work).await?;
                if options.maximum_volumes == 0 || bindings.is_empty()
                    || bindings.len() as u64 > u64::from(options.maximum_volumes)
                    || !options.root.is_absolute() || !allocation.root.is_absolute()
                    || !allocation.operation_directory.is_absolute()
                    || !options.work_per_volume.has_finite_allowances()
                {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "invalid native preparation allocation or allowances" });
                }
                if options.root != allocation.root {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native options root differs from witnessed host allocation" });
                }
                let (root, mut output_custody) = work.copy_path(&options.root).map_err(native_root_error)?;
                let owned_options = NativeViewOptions {
                    root, maximum_volumes: options.maximum_volumes,
                    work_per_volume: options.work_per_volume,
                };
                let (mut mappings, mut mappings_custody) = native_vec(bindings.len(), &work)?;
                output_custody.absorb(&mut mappings_custody).map_err(native_root_error)?;
                let (mut binding_work, binding_work_custody) = native_vec(bindings.len(), &work)?;
                for (index, binding) in bindings.iter().enumerate() {
                    work.charge(acyclic_fs::WorkCounters {
                        items_examined: 1, ..acyclic_fs::WorkCounters::default()
                    }).map_err(native_root_error)?;
                    if !matches!(binding.namespace, NativeVolumeNamespace::MaterializedDirectory) {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "an existing lazy mount must remain attached through its original native owner" });
                    }
                    let path = Path::new(binding.path);
                    if path.as_os_str().is_empty() {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native mapping path is empty" });
                    }
                    for part in path.components() {
                        work.charge(acyclic_fs::WorkCounters {
                            source_path_components: 1, ..acyclic_fs::WorkCounters::default()
                        }).map_err(native_root_error)?;
                        if !matches!(part, Component::Normal(_)) {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native volume paths overlap or escape their root" });
                        }
                    }
                    for prior in &bindings[..index] {
                        work.charge(acyclic_fs::WorkCounters {
                            items_examined: 1, ..acyclic_fs::WorkCounters::default()
                        }).map_err(native_root_error)?;
                        if native_paths_overlap(path, Path::new(prior.path)) {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native volume paths overlap or escape their root" });
                        }
                    }
                    let volume_work = work.native_volume_work(options.work_per_volume).map_err(native_root_error)?;
                    verify_binding(owner, binding, &volume_work)?;
                    binding_work.push(volume_work);
                    let (volume, mut volume_custody) = copy_native_volume_with_work(binding.volume, &work)?;
                    output_custody.absorb(&mut volume_custody).map_err(native_root_error)?;
                    let (generation, mut generation_custody) = copy_native_generation_with_work(binding.generation, &work)?;
                    output_custody.absorb(&mut generation_custody).map_err(native_root_error)?;
                    let (path, mut path_custody) = work.join_path(&options.root, path).map_err(native_root_error)?;
                    output_custody.absorb(&mut path_custody).map_err(native_root_error)?;
                    mappings.push(NativeVolumeMapping {
                        volume, generation, path, writable: binding.writable,
                        namespace: NativeVolumeNamespace::MaterializedDirectory,
                        authority_revision: {
                            let (bytes, _custody) = native_canonical(
                                &(binding.verifier.audience(), binding.scope), &work,
                            )?;
                            work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64,
                                ..acyclic_fs::WorkCounters::default() }).map_err(native_root_error)?;
                            *blake3::hash(&bytes).as_bytes()
                        },
                    });
                }
                let manifest = NativeViewManifest {
                    options: owned_options, volumes: mappings,
                };
                manifest.validate()?;
                let backend = native_backend(&allocation, &work, true).await?;
                let store = NativePreparationStore::new(owner, work.clone(), false);
                let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
                let operation = native_preparation_operation_with_work(owner, &work).map_err(native_root_error)?;
                let retained = materializer.load_owned(operation).await.map_err(native_materialization_error)?;
                // Qualify every source with its actual SDK volume identity before
                // durable intent; no host path or provider result supplies it.
                let (mut generations, generations_custody) = native_vec(bindings.len(), &work)?;
                let (mut selection, mut plan_custody) = native_vec(bindings.len(), &work)?;
                for (binding, volume_work) in bindings.iter().zip(&binding_work) {
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, volume_work)?;
                    let workspace = binding.host.open_volume_with_native_work(binding.volume, volume_work).await?;
                    let generation = binding.host.generation_with_native_work(
                        &workspace, binding.generation, volume_work,
                    ).await?;
                    if binding.writable {
                        let head = workspace.head_with_native_work(
                            &volume_work.native_mount_work(), cancellation,
                        ).await.map_err(acyclic_fs::MountLifecycleError::Workspace)?;
                        if head.id() != generation.id() {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native destination generation is stale" });
                        }
                    }
                    let (provider_identity, mut provider_custody) = native_canonical(binding.volume.provider(), &work)?;
                    plan_custody.absorb(&mut provider_custody).map_err(native_root_error)?;
                    selection.push(acyclic_fs::MaterializationPreparationSelection {
                        provider_identity, volume: workspace.id().volume_id(), generation: generation.id(),
                    });
                    generations.push((generation, volume_work.clone()));
                }
                drop(binding_work);
                drop(binding_work_custody);
                let mut intent_operation_custody = None;
                let intent = if let Some(journal) = &retained {
                    let (intent, mut intent_custody) = preparation_intent(journal, &work)?;
                    output_custody.absorb(&mut intent_custody).map_err(native_root_error)?;
                    if intent.task != owner.task_binding().0 || intent.manifest != manifest
                        || intent.operation_directory != allocation.operation_directory
                    {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation original selection changed" });
                    }
                    require_allocation(journal, &allocation)?;
                    intent
                } else {
                    materializer.backend().require_initial_empty().map_err(native_root_error)?;
                    let (task, original_fence) = owner.task_binding();
                    let (reservation_id, mut reservation_custody) = native_string(&original_fence.reservation_id, &work)?;
                    let (placement, mut placement_custody) = native_string(&original_fence.placement, &work)?;
                    output_custody.absorb(&mut reservation_custody).map_err(native_root_error)?;
                    output_custody.absorb(&mut placement_custody).map_err(native_root_error)?;
                    let original_fence = crate::scheduler::LeaseFence { reservation_id, placement };
                    let (operation_directory, operation_custody) = work.copy_path(&allocation.operation_directory)
                        .map_err(native_root_error)?;
                    let (manifest, mut manifest_custody) = native_manifest(&manifest, &work)?;
                    intent_operation_custody = Some(operation_custody);
                    output_custody.absorb(&mut manifest_custody).map_err(native_root_error)?;
                    NativePreparationIntent { task, original_fence, operation_directory, manifest }
                };
                let (mut top_level, top_custody) = native_vec(bindings.len(), &work)?;
                for binding in bindings {
                    let first = Path::new(binding.path).components().next()
                        .and_then(|part| part.as_os_str().to_str())
                        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native mapping name must be Unicode" })?;
                    top_level.push(first);
                }
                top_level.sort_unstable();
                top_level.dedup();
                let (mut edits, mut edits_custody) = native_vec(top_level.len(), &work)?;
                plan_custody.absorb(&mut edits_custody).map_err(native_root_error)?;
                for path in &top_level {
                    let (path, mut path_custody) = native_string(path, &work)?;
                    plan_custody.absorb(&mut path_custody).map_err(native_root_error)?;
                    let (image, mut image_custody) = native_bytes(acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX, &work)?;
                    plan_custody.absorb(&mut image_custody).map_err(native_root_error)?;
                    edits.push(acyclic_fs::MaterializationEdit::Install { path, image });
                }
                drop(top_level);
                drop(top_custody);
                let (original, mut original_custody) = native_canonical(&intent, &work)?;
                plan_custody.absorb(&mut original_custody).map_err(native_root_error)?;
                let plan = acyclic_fs::MaterializationPlan {
                    operation_id: operation,
                    intent: acyclic_fs::MaterializationIntent::NativePreparation {
                        selection, original,
                    },
                    edits,
                };
                if retained.as_ref().is_some_and(|journal| journal.plan != plan) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation operation was reused" });
                }
                if retained.is_none() {
                    work.set_original_work(acyclic_fs::WorkCounters::UNCHARGED);
                }
                drop(retained);
                let journal = materializer.prepare_owned(plan, plan_custody).await.map_err(native_materialization_error)?;
                require_allocation(&journal, &allocation)?;
                // No captured preimage or live edit exists in this state. Only the
                // original held, service-owned target may be cleared and restaged.
                let restage = journal.phase == MaterializationPhase::Preparing
                    && journal.preimages.is_empty();
                if restage {
                    owner.verify_with_native_work(false, work).await?;
                    materializer.backend().reset_target_directory("").await.map_err(native_root_error)?;
                    owner.verify_with_native_work(false, work).await?;
                    for (binding, (generation, volume_work)) in bindings.iter().zip(&generations) {
                        owner.verify_with_native_work(false, work).await?;
                        verify_binding(owner, binding, volume_work)?;
                        let identity = materializer.backend().reset_target_directory(binding.path)
                            .await.map_err(native_root_error)?;
                        owner.verify_with_native_work(false, work).await?;
                        verify_binding(owner, binding, volume_work)?;
                        let (path, path_custody) = materializer.backend().staging_path(binding.path)
                            .map_err(native_root_error)?;
                        let staged = (
                            acyclic_fs::MaterializeOptions::native(path).expected_root_identity(Some(identity)),
                            path_custody,
                        );
                        generation.materialize_with_native_work(
                            &staged.0, &volume_work.native_mount_work(), cancellation,
                        ).await?;
                        owner.verify_with_native_work(false, work).await?;
                        verify_binding(owner, binding, volume_work)?;
                    }
                }
                owner.verify_with_native_work(false, &work).await?;
                materializer.backend().require_selected_root(&journal.plan.edits).map_err(native_root_error)?;
                drop(journal);
                let journal = {
                    let selected = &generations;
                    let mut before_effect = |_write| async move {
                        owner.verify_with_native_work(false, work).await?;
                        for (binding, (_, volume_work)) in bindings.iter().zip(selected) {
                            verify_binding(owner, binding, volume_work)?;
                        }
                        Ok::<(), NativePreparationJournalError>(())
                    };
                    materializer.recover_owned_guarded(
                        operation, acyclic_fs::MaterializationRecovery::Complete, &mut before_effect,
                    ).await
                }.map_err(NativePreparationJournalError::into_owner_error)?
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation journal disappeared" })?;
                require_allocation(&journal, &allocation)?;
                materializer.backend().require_selected_root(&journal.plan.edits).map_err(native_root_error)?;
                let (mut volumes, mut volumes_custody) = native_vec(bindings.len(), &work)?;
                output_custody.absorb(&mut volumes_custody).map_err(native_root_error)?;
                let (mut identities, identities_custody) = native_vec(bindings.len(), &work)?;
                for ((binding, mapping), (generation, volume_work)) in bindings.iter()
                    .zip(&manifest.volumes).zip(generations)
                {
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, &volume_work)?;
                    let identity = materializer.backend().mapping_root_identity(binding.path).map_err(native_root_error)?;
                    if identities.contains(&identity) {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native volume directory aliases changed" });
                    }
                    identities.push(identity);
                    let mount = if binding.writable {
                        acyclic_fs::MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual)
                    } else { acyclic_fs::MountOptions::read_only() };
                    let (attachment_path, path_custody) = work.copy_path(&mapping.path).map_err(native_root_error)?;
                    let attachment = (acyclic_fs::MaterializeOptions::native(attachment_path), path_custody);
                    let view = generation.attach_native_working_set_with_work(
                        &mount, &attachment.0, identity, manifest.options.work_per_volume,
                        &volume_work.native_mount_work(), cancellation,
                    ).await?;
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, &volume_work)?;
                    volumes.push(PreparedVolume { view, original_identity: identity,
                        writable: binding.writable, publication: binding.publication,
                        _work: volume_work });
                }
                drop(generations_custody);
                drop(identities);
                drop(identities_custody);
                owner.verify_with_native_work(false, &work).await?;
                cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(native_root_error)?;
                checkpoint_preparation(materializer.store(), journal).await?;
                let inspection = materializer.inspect_owned(operation).await.map_err(native_materialization_error)?
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation journal disappeared" })?;
                owner.verify_with_native_work(false, work).await?;
                for (binding, volume) in bindings.iter().zip(&volumes) {
                    verify_binding(owner, binding, &volume._work)?;
                }
                let receipt = preparation_receipt(intent, inspection.0, inspection.1)?;
                drop(intent_operation_custody);
                Ok(Self { manifest, volumes, preparation: receipt, cancellation: cancellation.clone(),
                    original_work: Some(work.clone()), _allocation: output_custody })
            }.await;
            result
    }

    /// Original preparation evidence retained by this attachment.
    #[must_use]
    pub fn preparation_receipts(&self) -> &[NativePreparationReceipt] {
        std::slice::from_ref(&self.preparation)
    }

    /// Recovers only the original journaled allocation after lost acknowledgement.
    /// Existing directories without that task's original record are never
    /// adopted. The returned observation confers no mount/write authority.
    pub async fn recover_allocation<P: StreamProvider>(
        owner: &TaskJournalOwner<P>, options: &NativeViewOptions, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Option<NativePreparationRoot>>, (Error, acyclic_fs::WorkCounters)> {
        if !budget.has_finite_allowances() {
            return Err((Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation recovery requires finite allowances" },
                acyclic_fs::WorkCounters::default()));
        }
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, work)| (Error::NativeConstruction { cause, work }, work))?;
        let result = Self::recover_allocation_with_native_work(owner, options, &work)
            .map_err(|error| (error, work.receipt()))?.await
            .map(|(allocation, _custody)| allocation);
        native_result(result, &work)
    }

    /// Observes an original allocation using only the supplied operation ledger.
    /// A missing journal is not evidence that its physical namespace is absent.
    pub fn recover_allocation_with_native_work<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, options: &'a NativeViewOptions, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<NativePreparationRoot>, acyclic_fs::NativeAllocation)>> {
        if !work.remaining().map_err(native_root_error)?.has_finite_allowances() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation recovery requires finite original work" });
        }
        crate::NativeFutureBundle::new(work, || async {
            let store = NativePreparationStore::new(owner, work.clone(), true);
            let operation = native_preparation_operation_with_work(owner, work).map_err(native_root_error)?;
            let Some(journal) = store.load_record(operation, false).await
                .map_err(NativePreparationJournalError::into_owner_error)?
            else { return Ok((None, work.retain_owned(0).map_err(native_root_error)?)); };
            if journal.0.preparation_progress.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Indeterminate,
                    reason: "native original staging allocation is incomplete or indeterminate" });
            }
            let intent = preparation_intent(&journal.0, work)?;
            if intent.0.task != owner.task_binding().0 || &intent.0.manifest.options != options {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native allocation differs from original task or preparation options" });
            }
            let expected_root = journal.0.root_identity.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Indeterminate, reason: "native original root allocation is unwitnessed",
            })?;
            let expected_preparation_identity = journal.0.preparation_identity.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Indeterminate, reason: "native original staging allocation is unwitnessed",
            })?;
            let _owner_custody = store.verify_read_owner().await?;
            let expected_identity = work.root_identity(&intent.0.manifest.options.root).map_err(native_root_error)?;
            if expected_identity.to_bytes() != expected_root {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native original root allocation was replaced" });
            }
            let NativePreparationIntent { original_fence, manifest, operation_directory, .. } = intent.0;
            let NativeViewManifest { options, volumes } = manifest;
            // Release discarded decoded fields only after their physical drop.
            drop((volumes, original_fence));
            let mut allocation = (NativePreparationRoot {
                root: options.root, operation_directory, expected_identity, expected_preparation_identity,
            }, intent.1);
            let retained = allocation.0.root.capacity().checked_add(allocation.0.operation_directory.capacity())
                .ok_or_else(|| native_root_error(acyclic_fs::WorkError::Overflow.into()))?;
            allocation.1.substitute(retained as u64).map_err(native_root_error)?;
            // The same held backend verifies every recorded endpoint and creates
            // no directory; observing it grants neither WRITE nor disposal.
            let _backend = native_backend(&allocation.0, work, true).await?;
            let _final_owner_custody = store.verify_read_owner().await?;
            work.remaining().map_err(native_root_error)?;
            Ok((Some(allocation.0), allocation.1))
        })
    }

    /// Query only the original protected journal. Terminal task READ, or the
    /// original current settlement fence, is sufficient; no WRITE, reservation,
    /// physical path probe, replacement lease or cleanup authority is acquired.
    /// `original_admission` is the SDK task admission operation, not a child
    /// native command. The caller separately authenticates that command through
    /// its sealed original task input; preparation has its own task-derived ID.
    /// The supplied creator proof must be the protected original private scratch
    /// producer's proof. Recorded data never authorizes original-root disposal.
    pub async fn observe_preparation_journal<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        scratch: &crate::resource_preparation::ChildScratchCreationProof,
        original_admission: crate::OperationId,
        work: &NativeMaterializationWork,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<NativePreparationJournalObservation>, NativePreparationJournalError> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(NativePreparationJournalError::Record("native journal query requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, || async { Ok(async {
            if owner.task_binding().0 != scratch.original_task() || owner.operation_id() != original_admission {
                return Err(NativePreparationJournalError::Record("native journal query differs from original child or task admission operation"));
            }
            let (outcome, _outcome_custody) = owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(scratch.volume(), VolumeOperation::Read, work)?;
            let operation = native_preparation_operation_with_work(owner, work)?;
            let store = NativePreparationStore::new(owner, work.clone(), true);
            let value = native_preparation_observation(
                owner.task_binding().0, Some(scratch), operation, store.load_record(operation, false).await?, work,
            )?;
            // Reauthenticate after the read, keeping both actual outcome copies
            // and their custody live until their respective checks finish.
            let (final_outcome, _final_outcome_custody) = owner.outcome_with_native_work(work).await?;
            if final_outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(scratch.volume(), VolumeOperation::Read, work)?;
            work.remaining()?;
            Ok(acyclic_fs::OperationReceipt { value, work: work.receipt() })
        }.await) }).map_err(NativePreparationJournalError::Owner)?
            .await.map_err(NativePreparationJournalError::Owner)?
    }

    /// Queries the same materializer journal without adopting or repairing paths.
    pub async fn inspect_preparation<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        allocation: &NativePreparationRoot,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, (Error, acyclic_fs::WorkCounters)> {
        inspect_native_preparation(owner, allocation, budget, cancellation, false).await
    }

    /// Cancels the original preparation through the materializer's exact rollback.
    /// A cancelled task may settle under its original current fence; it cannot attach.
    pub async fn cleanup_preparation<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        allocation: &NativePreparationRoot,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, (Error, acyclic_fs::WorkCounters)> {
        inspect_native_preparation(owner, allocation, budget, cancellation, true).await
    }

    /// Observes the original journal and physical endpoints on the caller's ledger.
    pub fn inspect_preparation_with_native_work<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, allocation: &'a NativePreparationRoot,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<NativePreparationReceipt>>> {
        inspect_native_preparation_with_work(owner, allocation, work, false)
    }

    /// Settles only this witnessed preparation through the existing rollback engine.
    pub fn cleanup_preparation_with_native_work<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, allocation: &'a NativePreparationRoot,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<NativePreparationReceipt>>> {
        inspect_native_preparation_with_work(owner, allocation, work, true)
    }

    /// Exact request/approval manifest. Changing it requires new admission.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.manifest
    }

    /// Reads an entire mounted Project/agent-private file under this view's
    /// original task, exact mapping and current READ grant. The caller retains
    /// original SDK tool-call admission/effect ownership; no journal is minted.
    ///
    /// # Errors
    /// Rejects unmounted locators, stale owners, replacement roots, cancellation
    /// and over-cap files without truncating the returned bytes.
    pub async fn read_builtin_file<P: StreamProvider>(
        &self, owner: NativeViewOwner<'_, P>, _original_operation: crate::OperationId,
        relative: &Path, maximum_bytes: usize, work: &NativeMaterializationWork,
    ) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
        let (mapping, volume, file) = self.builtin_volume(relative, VolumeOperation::Read, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        work.read_working_file_guarded(&mapping.path, volume.original_identity, file, maximum_bytes,
            || self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work)).await
    }

    /// Writes bounded UTF-8 only through an original mounted writable mapping.
    /// An edit's SHA-256 compares the exact current held-leaf preimage before
    /// mutation. This neither grants user-project Root writes nor publishes.
    ///
    /// # Errors
    /// Preserves original owner/native causes and actual partial IO work.
    #[allow(clippy::too_many_arguments, reason = "original command, mapping and edit preimage are explicit")]
    pub async fn write_builtin_file<P: StreamProvider>(
        &self, owner: NativeViewOwner<'_, P>, _original_operation: crate::OperationId,
        relative: &Path, content: &str, create_parents: bool,
        expected_sha256: Option<[u8; 32]>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        let (mapping, volume, file) = self.builtin_volume(relative, VolumeOperation::Write, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        work.write_working_file_guarded(&mapping.path, volume.original_identity, file, content,
            create_parents, expected_sha256,
            || async {
                self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work).await?;
                if expected_sha256.is_some() {
                    owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                }
                Ok::<(), Error>(())
            }).await
    }

    fn builtin_volume<'a>(
        &'a self, relative: &'a Path, operation: VolumeOperation, work: &NativeMaterializationWork,
    ) -> Result<(&'a NativeVolumeMapping, &'a PreparedVolume<A, O>, &'a Path)> {
        let invalid = || native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout);
        let text = relative.to_str().ok_or_else(invalid)?;
        if text.is_empty() || text.contains(['\\', '\0'])
            || text.split('/').any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(invalid());
        }
        for component in relative.components() {
            work.charge(acyclic_fs::WorkCounters { source_path_components: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
            if !matches!(component, Component::Normal(_)) { return Err(invalid()); }
        }
        if self.volumes.len() != self.manifest.volumes.len() { return Err(invalid()); }
        for (mapping, volume) in self.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
            let mount = mapping.path.strip_prefix(&self.manifest.options.root).map_err(|_| invalid())?;
            let Ok(file) = relative.strip_prefix(mount) else { continue; };
            if file.as_os_str().is_empty() || volume.view.path() != mapping.path
                || !matches!(mapping.volume.class(), crate::conversation::VolumeClass::Project | crate::conversation::VolumeClass::AgentPrivate)
                || (operation == VolumeOperation::Write && (!mapping.writable || !volume.writable))
            {
                return Err(invalid());
            }
            return Ok((mapping, volume, file));
        }
        Err(invalid())
    }

    async fn authenticate_builtin_view<P: StreamProvider>(
        &self, owner: NativeViewOwner<'_, P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        owner.verify_with_native_work(work).await?;
        let store = NativePreparationStore::new(owner.owner, work.clone(), false);
        // The existing original journal, not a matching task UUID or a newly
        // observed path, authenticates this immutable task/view selection.
        let record = store.load_record(self.preparation.journal.plan.operation_id, false).await
            .map_err(NativePreparationJournalError::into_owner_error)?
            .ok_or_else(|| native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: record.0.plan.edits.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_root_error)?;
        let valid = record.0.phase == MaterializationPhase::Applied
            && record.0.root_identity == self.preparation.journal.root_identity
            && record.0.preparation_identity == self.preparation.journal.preparation_identity
            && record.0.plan == self.preparation.journal.plan;
        drop(record);
        if !valid {
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        owner.verify_with_native_work(work).await
    }

    async fn verify_builtin_owner<P: StreamProvider>(
        &self, owner: NativeViewOwner<'_, P>, mapping: &NativeVolumeMapping,
        operation: VolumeOperation, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.owner.task_binding().0 != self.preparation.task {
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(native_root_error)?;
        owner.verify_with_native_work(work).await?;
        owner.require_volume_grant_with_native_work(&mapping.volume, operation, work)?;
        let expected = self.preparation.journal.root_identity
            .map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or_else(|| native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
        let actual = work.root_identity(&self.manifest.options.root).map_err(native_root_error)?;
        if expected != actual {
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual }));
        }
        Ok(())
    }

    /// Rechecks the admitted views in one caller-supplied remaining span.
    pub async fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)> {
        let work = self.validation_work(budget)?;
        native_result(self.validate_dispatch_with_work(&work).await, &work)
    }

    /// Observes directory admission only. `Command::current_dir` may reopen a
    /// lexical path later; this view makes no sandbox or held-launch guarantee.
    pub async fn validate_working_directory(
        &self, cwd: &Path, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)> {
        let work = self.validation_work(budget)?;
        native_result(self.validate_directory_with_work(cwd, &work), &work)
    }

    fn validation_work(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<NativeMaterializationWork, (Error, acyclic_fs::WorkCounters)> {
        if !budget.has_finite_allowances() {
            return Err((Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native validation requires finite remaining allowances" },
                acyclic_fs::WorkCounters::default()));
        }
        let original = self.manifest.options.budget_for_volume_count(self.manifest.volumes.len())
            .map_err(|error| (error, acyclic_fs::WorkCounters::default()))?;
        NativeMaterializationWork::new(original.intersect_budget(budget), &self.cancellation)
            .map_err(|(cause, work)| (Error::NativeConstruction { cause, work }, work))
    }

    fn original_root_identity(&self) -> Result<acyclic_fs::NativeRootIdentity> {
        self.preparation.journal.root_identity.map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation has no original root witness" })
    }

    fn validate_directory_with_work(&self, cwd: &Path, work: &NativeMaterializationWork) -> Result<()> {
        // A prefix forms a locator, not authority. The actual held root,
        // witnessed identity and no-follow directory walk admit the locator.
        let relative = cwd.strip_prefix(&self.manifest.options.root)
            .map_err(|_| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native working directory is outside its selected locator" })?;
        work.validate_working_directory(&self.manifest.options.root, self.original_root_identity()?, relative)
            .map_err(native_root_error)
    }

    async fn validate_dispatch_with_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        let expected = self.original_root_identity()?;
        self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(native_root_error)?;
        if work.root_identity(&self.manifest.options.root).map_err(native_root_error)? != expected {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation root was replaced" });
        }
        let sdk_work = work.native_mount_work();
        for volume in &self.volumes {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::default() }).map_err(native_root_error)?;
            volume.view.validate_for_presentation_with_work(&sdk_work, &self.cancellation).await?;
            volume.view.verify_unchanged_with_work(&sdk_work, &self.cancellation).await?;
        }
        if work.root_identity(&self.manifest.options.root).map_err(native_root_error)? != expected {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation root changed during validation" });
        }
        self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(native_root_error)?;
        Ok(())
    }

    /// Publishes only authorized destinations after source integrity passes.
    /// Every volume uses its existing permit/conflict lifecycle independently.
    pub async fn publish<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
    ) -> std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure> {
        let work = self.publication_work().map_err(publication_admission_failure)?;
        self.publish_with_work(owner, &work).await
    }

    fn publication_work(&self) -> std::result::Result<NativeMaterializationWork, (Error, acyclic_fs::WorkCounters)> {
        let budget = self.manifest.validation_budget()
            .map_err(|error| (error, acyclic_fs::WorkCounters::default()))?;
        NativeMaterializationWork::new(budget, &self.cancellation)
            .map_err(|(cause, work)| (Error::NativeConstruction { cause, work }, work))
    }

    async fn publish_with_work<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure> {
        let custody = work.retain_owned(0).map_err(|error| NativePublicationFailure {
            destination: None, completed: Vec::new(), work: work.receipt(), message: error.to_string(),
        })?;
        match self.publish_original(owner, work, custody).await {
            Ok(receipt) => { drop(receipt); Ok(work.receipt()) },
            Err(failure) => Err(NativePublicationFailure {
                message: failure.cause.to_string(), destination: failure.destination,
                completed: failure.completed, work: work.receipt(),
            }),
        }
    }

    async fn publish_original<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
        mut custody: acyclic_fs::NativeAllocation,
    ) -> std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError> {
        let mut completed = Vec::new();
        let mut mappings = Vec::new();
        let mut mapping_storage = None;
        let mut failed_destination = None;
        let result: Result<()> = async {
            let (output, mut output_custody) = native_vec(self.manifest.volumes.len(), work)?;
            custody.absorb(&mut output_custody).map_err(native_root_error)?;
            completed = output;
            let (copies, copies_custody) = native_vec(self.manifest.volumes.len(), work)?;
            mapping_storage = Some(copies_custody);
            mappings = copies;
            // These concrete records are copied before the first publication.
            // Failure paths move them; they never allocate a diagnostic context.
            for mapping in &self.manifest.volumes {
                let (mapping, allocation) = copy_native_mapping_with_work(mapping, work)?;
                mappings.push(Some(NativeMappingPayload { mapping, allocation }));
            }
            let sdk_work = work.native_mount_work();
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                failed_destination = Some(index);
                self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(native_root_error)?;
                owner.verify_with_native_work(false, work).await?;
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                if !volume.writable {
                    volume.view.verify_unchanged_with_work(&sdk_work, &self.cancellation).await?;
                }
            }
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                if !volume.writable { continue; }
                failed_destination = Some(index);
                volume.view.sync_with_permit_with_work(
                    volume.publication, &sdk_work, &self.cancellation,
                    |actual| async move {
                        let actual = actual.native_materialization_work();
                        owner.verify_with_native_work(false, &actual).await?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, &actual)?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, &actual)?;
                        Ok::<(), Error>(())
                    },
                ).await?;
                // Physical publication is final before any later fallible check.
                let NativeMappingPayload { mapping, mut allocation } = mappings[index].take()
                    .expect("original preadmitted destination");
                completed.push(mapping);
                custody.absorb(&mut allocation).map_err(native_root_error)?;
                work.remaining().map_err(native_root_error)?;
            }
            failed_destination = None;
            owner.verify_with_native_work(false, work).await?;
            Ok(())
        }.await;
        match result {
            Ok(()) => {
                drop(mappings);
                drop(mapping_storage);
                Ok(NativeViewPublicationReceipt { completed, custody })
            }
            Err(cause) => {
                let destination = failed_destination.and_then(|index| mappings.get_mut(index))
                    .and_then(Option::take).map(|payload| {
                        let NativeMappingPayload { mapping, mut allocation } = payload;
                        custody.absorb(&mut allocation).expect("same original ledger and preadmitted total");
                        mapping
                    });
                drop(mappings);
                drop(mapping_storage);
                Err(NativeViewPublicationError { cause, destination, completed,
                    diagnostic: None, rendering_failure: None, custody })
            }
        }
    }
}

/// Reads only through the authenticated original SDK admission witness.
/// Its recorded fence is a historical selector, never new task authority.
pub(crate) fn observe_native_preparation_admission<'a, P: StreamProvider>(
    witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, NativePreparationJournalObservation>> {
    if !work.remaining().map_err(native_root_error)?.has_finite_allowances() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native admission query requires a finite original work allowance" });
    }
    crate::NativeFutureBundle::new(work, || async {
        witness.verify_with_native_work(work).await?;
        let scratch = witness.scratch_proof();
        if scratch.is_some_and(|scratch| witness.task() != scratch.original_task()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "native admission witness differs from its original scratch task" });
        }
        let operation = native_preparation_operation_for_task_with_work(witness.task(), work)
            .map_err(native_root_error)?;
        let store = NativePreparationStore::admission(witness, work.clone());
        let record = store.load_record(operation, false).await
            .map_err(NativePreparationJournalError::into_owner_error)?;
        let value = native_preparation_observation(
            witness.task(), scratch, operation, record, work,
        ).map_err(NativePreparationJournalError::into_owner_error)?;
        witness.verify_with_native_work(work).await?;
        work.remaining().map_err(native_root_error)?;
        Ok(value)
    })
}



fn native_preparation_operation<P: StreamProvider>(owner: &TaskJournalOwner<P>) -> acyclic_fs::OperationId {
    native_preparation_operation_for_task(owner.task_binding().0)
}

fn native_preparation_operation_for_task(task: crate::TaskId) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/native-preparation/v1\0");
    hash.update(&task.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
}

fn native_preparation_operation_with_work<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
) -> std::result::Result<acyclic_fs::OperationId, acyclic_fs::NativeTreeMaterializationError> {
    native_preparation_operation_for_task_with_work(owner.task_binding().0, work)
}

fn native_preparation_operation_for_task_with_work(
    task: crate::TaskId, work: &NativeMaterializationWork,
) -> std::result::Result<acyclic_fs::OperationId, acyclic_fs::NativeTreeMaterializationError> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: (b"harness/native-preparation/v1\0".len() + 16) as u64,
        ..acyclic_fs::WorkCounters::default()
    })?;
    Ok(native_preparation_operation_for_task(task))
}


fn native_materialization_error(
    error: acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>,
) -> Error {
    use acyclic_fs::MaterializationError as E;
    let (code, reason) = match error {
        E::Store(error) => return error,
        E::Backend(error) => return native_root_error(error),
        E::PlanConflict => (crate::wire::ErrorCode::Conflict, "materialization retry conflicts with its durable plan"),
        E::IncompatibleJournal => (crate::wire::ErrorCode::Storage, "materialization journal is incompatible"),
        E::Contended => (crate::wire::ErrorCode::Conflict, "materialization journal remained contended"),
        E::ExternalMutation => (crate::wire::ErrorCode::Conflict, "materialization paused because an external path mutation was observed"),
    };
    Error::NativeBoundary { code, reason }
}

fn charge_native_read(work: &NativeMaterializationWork) -> Result<()> {
    work.charge(acyclic_fs::WorkCounters {
        backend_read_operations: 1, ..acyclic_fs::WorkCounters::default()
    }).map_err(native_root_error)
}



async fn checkpoint_preparation<P: StreamProvider>(
    store: &NativePreparationStore<'_, P>, mut journal: acyclic_fs::OwnedMaterializationJournal,
) -> Result<()> {
    let expected = journal.revision;
    journal.revision = expected.checked_add(1)
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native preparation revision overflow" })?;
    let operation = journal.plan.operation_id;
    let (committed, _) = store.compare_and_swap(operation, expected, journal).await?;
    if !committed {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation changed before attachment checkpoint" });
    }
    Ok(())
}

fn native_paths_overlap(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        let mut left = left.components();
        let mut right = right.components();
        loop {
            match (left.next(), right.next()) {
                (Some(left), Some(right)) => {
                    let Some(left) = left.as_os_str().to_str() else { return false; };
                    let Some(right) = right.as_os_str().to_str() else { return false; };
                    if !left.chars().flat_map(char::to_uppercase)
                        .eq(right.chars().flat_map(char::to_uppercase)) { return false; }
                }
                _ => return true,
            }
        }
    }
    #[cfg(not(windows))]
    {
        left.starts_with(right) || right.starts_with(left)
    }
}

fn verify_binding<P: StreamProvider, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    owner: &TaskJournalOwner<P>, binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        if operation == VolumeOperation::Write && !binding.writable { continue; }
        owner.require_volume_grant_with_native_work(binding.volume, operation, work)?;
        // Tuple drop order keeps the authenticated owned grant before custody.
        let _original = ContentGrant::verify_original_volume_with_native_work(
            binding.verifier, binding.scope, binding.volume, operation, work,
        )?;
    }
    if binding.volume.provider() != &binding.host.provider
        || binding.generation.as_resource().provider() != &binding.host.provider
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "resource belongs to another Filesystem provider" });
    }
    Ok(())
}


fn require_allocation(journal: &MaterializationJournal, allocation: &NativePreparationRoot) -> Result<()> {
    if journal.root_identity != Some(allocation.expected_identity.to_bytes())
        || journal.preparation_identity.as_ref() != Some(&allocation.expected_preparation_identity)
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation allocation differs from original journal" });
    }
    Ok(())
}

async fn native_backend(
    allocation: &NativePreparationRoot, work: &NativeMaterializationWork, require_original: bool,
) -> Result<acyclic_fs::NativeTreeMaterializationBackend> {
    use acyclic_fs::MaterializationBackend as _;
    let (root, mut custody) = work.copy_path(&allocation.root).map_err(native_root_error)?;
    let (operation_directory, mut operation_custody) = work.copy_path(&allocation.operation_directory).map_err(native_root_error)?;
    custody.absorb(&mut operation_custody).map_err(native_root_error)?;
    let meter = work.clone();
    let backend = acyclic_native_runtime::run_blocking_io_measured(move || {
        acyclic_fs::NativeTreeMaterializationBackend::open_owned_with_work(root, operation_directory, custody, meter)
    }, work.runtime_meter_owner())
        .map_err(|error| native_root_error(work.runtime_error(error)))?
        .await.map_err(|error| native_root_error(work.runtime_error(error)))?
        .map_err(native_root_error)?;
    if require_original && (backend.allocation_identity() != Some(allocation.expected_identity.to_bytes())
        || backend.preparation_identity().as_ref() != Some(&allocation.expected_preparation_identity))
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "native allocation was physically replaced" });
    }
    Ok(backend)
}

fn preparation_intent(
    journal: &MaterializationJournal, work: &NativeMaterializationWork,
) -> Result<(NativePreparationIntent, acyclic_fs::NativeAllocation)> {
    preparation_intent_typed(journal, work).map_err(NativePreparationJournalError::into_owner_error)
}

fn native_preparation_observation(
    task: crate::TaskId,
    scratch: Option<&crate::resource_preparation::ChildScratchCreationProof>,
    operation: acyclic_fs::OperationId,
    record: Option<(MaterializationJournal, acyclic_fs::NativeAllocation)>,
    work: &NativeMaterializationWork,
) -> std::result::Result<NativePreparationJournalObservation, NativePreparationJournalError> {
    let Some(mut record) = record else {
        return Ok(NativePreparationJournalObservation::MissingJournalPhysicalStateUnknown {
            preparation_operation: operation,
        });
    };
    let mut intent = preparation_intent_typed(&record.0, work)?;
    if intent.0.task != task {
        return Err(NativePreparationJournalError::Record(
            "native journal original task differs from authenticated admission",
        ));
    }
    if let Some(scratch) = scratch {
        if task != scratch.original_task()
            || !intent.0.manifest.volumes.iter().any(|mapping| mapping.writable
                && &mapping.volume == scratch.volume())
        {
            return Err(NativePreparationJournalError::Record(
                "native journal does not retain the admitted original private scratch selection",
            ));
        }
    }
    record.1.absorb(&mut intent.1)?;
    Ok(NativePreparationJournalObservation::Recorded(NativeRecordedPreparation {
        task: intent.0.task, original_fence: intent.0.original_fence,
        manifest: intent.0.manifest, operation_directory: intent.0.operation_directory,
        journal: record.0, _custody: record.1,
    }))
}

fn preparation_intent_typed(
    journal: &MaterializationJournal, work: &NativeMaterializationWork,
) -> std::result::Result<(NativePreparationIntent, acyclic_fs::NativeAllocation), NativePreparationJournalError> {
    let acyclic_fs::MaterializationIntent::NativePreparation { selection, original } = &journal.plan.intent else {
        return Err(NativePreparationJournalError::Record("native preparation original qualified intent is missing"));
    };
    let intent: (NativePreparationIntent, _) = native_decoded_record_typed(
        original, NativeRecordSchema::PreparationIntent, work,
        |intent| native_intent_control_bytes(intent).map_err(NativePreparationJournalError::Owner),
    )?;
    intent.0.manifest.validate()?;
    if selection.len() != intent.0.manifest.volumes.len()
        || journal.plan.edits.iter().any(|edit| !matches!(edit,
            acyclic_fs::MaterializationEdit::Install { image, .. }
                if image.as_slice() == acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX
        ))
    {
        return Err(NativePreparationJournalError::Record("native preparation original selections or edit intents disagree"));
    }
    for (selected, mapping) in selection.iter().zip(&intent.0.manifest.volumes) {
        let identity = native_canonical(mapping.volume.provider(), work)?;
        if identity.0 != selected.provider_identity
            || selected.generation.digest().as_bytes().as_slice() != mapping.generation.as_resource().key()
            || !matches!(mapping.namespace, NativeVolumeNamespace::MaterializedDirectory)
        {
            return Err(NativePreparationJournalError::Record("native preparation original qualified volume differs"));
        }
    }
    Ok(intent)
}

fn preparation_receipt(
    intent: NativePreparationIntent, inspection: acyclic_fs::MaterializationInspection,
    custody: Option<acyclic_fs::NativeAllocation>,
) -> Result<NativePreparationReceipt> {
    let acyclic_fs::OwnedMaterializationJournal { journal, custody: mut journal_custody } = inspection.journal;
    let mut receipt = NativePreparationReceipt {
        task: intent.task, original_fence: intent.original_fence, manifest: intent.manifest,
        journal, root: inspection.root, preparation: inspection.preparation,
        observations: inspection.observations, _custody: custody,
    };
    if let Some(custody) = receipt._custody.as_mut() {
        if let Some(journal_custody) = journal_custody.as_mut() {
            custody.absorb(journal_custody).map_err(native_root_error)?;
        }
    } else {
        receipt._custody = journal_custody.take();
    }
    if receipt._custody.is_none() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
            reason: "native preparation inspection lost its original journal custody" });
    }
    Ok(receipt)
}

async fn inspect_native_preparation<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, allocation: &NativePreparationRoot,
    budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken, cleanup: bool,
) -> std::result::Result<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, (Error, acyclic_fs::WorkCounters)> {
    if !budget.has_finite_allowances() {
        return Err((Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native query requires finite allowances" }, acyclic_fs::WorkCounters::default()));
    }
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, spent)| (Error::NativeConstruction { cause, work: spent }, spent))?;
    let result = inspect_native_preparation_with_work(owner, allocation, &work, cleanup)
        .map_err(|error| (error, work.receipt()))?.await;
    native_result(result, &work)
}

fn inspect_native_preparation_with_work<'a, P: StreamProvider>(
    owner: &'a TaskJournalOwner<P>, allocation: &'a NativePreparationRoot,
    work: &'a NativeMaterializationWork, cleanup: bool,
) -> Result<crate::NativeFutureBundle<'a, Option<NativePreparationReceipt>>> {
    if !work.remaining().map_err(native_root_error)?.has_finite_allowances() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native query requires finite original work" });
    }
    crate::NativeFutureBundle::new(work, || async {
        let store = NativePreparationStore::new(owner, work.clone(), true);
        let operation = native_preparation_operation_with_work(owner, work).map_err(native_root_error)?;
        let Some(mut journal) = store.load_record(operation, false).await
            .map_err(NativePreparationJournalError::into_owner_error)? else { return Ok(None); };
        let mut intent = preparation_intent(&journal.0, work)?;
        if intent.0.task != owner.task_binding().0 || intent.0.manifest.options.root != allocation.root
            || intent.0.operation_directory != allocation.operation_directory
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native query differs from original preparation" });
        }
        require_allocation(&journal.0, allocation)?;
        let _owner_custody = store.verify_read_owner().await?;
        let backend = match native_backend(allocation, work, cleanup).await {
            Ok(backend) => backend,
            Err(error @ Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(_)
                    | acyclic_fs::NativeTreeMaterializationError::Cancelled
                    | acyclic_fs::NativeTreeMaterializationError::AllocationFailed(_)
                    | acyclic_fs::NativeTreeMaterializationError::Allocation(_),
                ..
            }) => return Err(error),
            Err(error) if !cleanup => {
                let mut root_message = work.display(&error).map_err(native_root_error)?;
                let mut preparation_message = native_string(&root_message.0, work)?;
                intent.1.absorb(&mut journal.1).map_err(native_root_error)?;
                intent.1.absorb(&mut root_message.1).map_err(native_root_error)?;
                intent.1.absorb(&mut preparation_message.1).map_err(native_root_error)?;
                let receipt = NativePreparationReceipt {
                    task: intent.0.task, original_fence: intent.0.original_fence, manifest: intent.0.manifest,
                    journal: journal.0, root: acyclic_fs::MaterializationRootObservation::Unavailable(root_message.0),
                    preparation: acyclic_fs::MaterializationPreparationObservation::Unavailable(preparation_message.0),
                    observations: Vec::new(), _custody: Some(intent.1),
                };
                let _final_owner_custody = store.verify_read_owner().await?;
                return Ok(Some(receipt));
            }
            Err(error) => return Err(error),
        };
        // The first loaded journal is no longer owned after the engine takes over;
        // retain the independently decoded intent until the final receipt drops.
        drop(journal);
        let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
        if cleanup {
            owner.verify_with_native_work(true, work).await?;
            let rolled_back = {
                let mut before_effect = |_write| async move {
                    owner.verify_with_native_work(true, work).await?;
                    Ok::<(), NativePreparationJournalError>(())
                };
                materializer.recover_owned_guarded(
                    operation, acyclic_fs::MaterializationRecovery::RollBack, &mut before_effect,
                ).await
            }.map_err(NativePreparationJournalError::into_owner_error)?
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native preparation journal disappeared" })?;
            owner.verify_with_native_work(true, work).await?;
            materializer.backend().reset_target_directory("").await.map_err(native_root_error)?;
            owner.verify_with_native_work(true, work).await?;
            checkpoint_preparation(materializer.store(), rolled_back).await?;
        }
        let mut inspection = materializer.inspect_owned(operation).await.map_err(native_materialization_error)?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation journal disappeared" })?;
        if let Some(custody) = inspection.1.as_mut() {
            intent.1.absorb(custody).map_err(native_root_error)?;
        }
        let receipt = preparation_receipt(intent.0, inspection.0, Some(intent.1))?;
        let _final_owner_custody = materializer.store().verify_read_owner().await?;
        Ok(Some(receipt))
    })
}

enum NativePreparationAuthority<'a, P: StreamProvider> {
    Owner { owner: &'a TaskJournalOwner<P>, settlement: bool },
    Admission(&'a crate::durable_host::TaskAdmissionReadonlyWitness<P>),
}

/// Fenced Stream adapter for the existing materializer journal, not a second log.
struct NativePreparationStore<'a, P: StreamProvider> {
    authority: NativePreparationAuthority<'a, P>,
    work: NativeMaterializationWork,
    loaded_original: AtomicBool,
}

impl<'a, P: StreamProvider> NativePreparationStore<'a, P> {
    fn new(owner: &'a TaskJournalOwner<P>, work: NativeMaterializationWork, settlement: bool) -> Self {
        Self { authority: NativePreparationAuthority::Owner { owner, settlement },
            work, loaded_original: AtomicBool::new(false) }
    }

    fn admission(
        witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
        work: NativeMaterializationWork,
    ) -> Self {
        Self { authority: NativePreparationAuthority::Admission(witness),
            work, loaded_original: AtomicBool::new(false) }
    }

    fn task(&self) -> crate::TaskId {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, .. } => owner.task_binding().0,
            NativePreparationAuthority::Admission(witness) => witness.task(),
        }
    }

    fn read_client(&self) -> &acyclic_stream::StreamClient<P> {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, .. } => owner.stream(),
            NativePreparationAuthority::Admission(witness) => witness.prepared_stream(),
        }
    }

    fn path(&self, operation: acyclic_fs::OperationId) -> Result<(acyclic_stream::StreamPath, acyclic_fs::NativeAllocation)> {
        self.path_typed(operation).map_err(NativePreparationJournalError::into_owner_error)
    }

    fn path_typed(
        &self, operation: acyclic_fs::OperationId,
    ) -> std::result::Result<(acyclic_stream::StreamPath, acyclic_fs::NativeAllocation), NativePreparationJournalError> {
        if operation != native_preparation_operation_for_task_with_work(self.task(), &self.work)? {
            return Err(NativePreparationJournalError::Record("native materializer operation differs from original task"));
        }
        let (path, custody) = self.work.display(&format_args!(
            "harness/v2/materialization/{}/{operation}", self.task(),
        ))?;
        Ok((acyclic_stream::StreamPath::new(path)?, custody))
    }

    async fn verify_read_owner(
        &self,
    ) -> Result<Option<(Option<crate::contract::Outcome<crate::conversation::FileRef>>, acyclic_fs::NativeAllocation)>> {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement } => {
                let (outcome, custody) = owner.outcome_with_native_work(&self.work).await?;
                if outcome.is_none() {
                    owner.verify_with_native_work(*settlement, &self.work).await?;
                }
                Ok(Some((outcome, custody)))
            }
            NativePreparationAuthority::Admission(witness) => {
                witness.verify_with_native_work(&self.work).await?;
                Ok(None)
            }
        }
    }

    async fn load_record(
        &self, operation: acyclic_fs::OperationId, adopt_original_work: bool,
    ) -> std::result::Result<Option<(MaterializationJournal, acyclic_fs::NativeAllocation)>, NativePreparationJournalError> {
        use futures::TryStreamExt as _;
        let (path, _path_custody) = self.path_typed(operation)?;
        let stream = self.read_client().stream(path.as_str())?;
        let _owner_custody = self.verify_read_owner().await?;
        self.work.charge(acyclic_fs::WorkCounters {
            backend_read_operations: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let sequence = match bounds.tail.checked_sub(1) {
            Some(sequence) => sequence,
            // Preserve the canonical stage engine's empty-record failure. A
            // read-only observer reports the marker's absence, never absence
            // of the physical namespace, and does not restart preparation.
            None if !adopt_original_work => return Ok(None),
            None => return Err(NativePreparationJournalError::Record("empty materializer journal")),
        };
        let _read_owner_custody = self.verify_read_owner().await?;
        acyclic_fs::WorkCounters::default().admit(&acyclic_fs::WorkCounters {
            authority_bytes_read: acyclic_stream::MAX_RECORD_BYTES as u64,
            ..acyclic_fs::WorkCounters::default()
        }, &self.work.remaining()?).map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        // Acquire the original logical quota before the provider can construct
        // an owned record. This is not an invented allocator operation.
        let mut record_custody = self.work.retain_owned(acyclic_stream::MAX_RECORD_BYTES as u64)?;
        self.work.charge(acyclic_fs::WorkCounters {
            backend_read_operations: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let mut records = stream.read(sequence, 1).await?;
        let record = records.try_next().await?
            .ok_or(NativePreparationJournalError::Record("missing materializer journal"))?;
        // Preserve completed records and bytes before a late cancellation or
        // current-owner check can fail.
        self.work.spent(acyclic_fs::WorkCounters {
            authority_bytes_read: record.value.len() as u64, authority_records_read: 1,
            ..acyclic_fs::WorkCounters::default()
        })?;
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(NativePreparationJournalError::Record("materializer journal exceeds Stream record limit"));
        }
        record_custody.substitute(record.value.len() as u64)?;
        let _final_owner_custody = self.verify_read_owner().await?;
        let (journal, custody): (MaterializationJournal, _) = native_decoded_record_typed(
            &record.value, NativeRecordSchema::Journal, &self.work,
            |journal| NativeMaterializationWork::journal_control_bytes(journal).map_err(Into::into),
        )?;
        if record.sequence != sequence || journal.revision != bounds.tail || journal.plan.operation_id != operation {
            return Err(NativePreparationJournalError::Record("materializer journal revision or operation changed"));
        }
        if !journal.is_valid_for(operation) {
            return Err(NativePreparationJournalError::Record("materializer journal has incompatible structural progress"));
        }
        if adopt_original_work && !self.loaded_original.swap(true, Ordering::AcqRel) {
            self.work.set_original_work(journal.work);
        }
        Ok(Some((journal, custody)))
    }
}

impl<P: StreamProvider> MaterializationJournalStore for NativePreparationStore<'_, P> {
    type Error = Error;

    async fn load(&self, operation: acyclic_fs::OperationId) -> Result<Option<acyclic_fs::OwnedMaterializationJournal>> {
        self.load_record(operation, true).await
            .map(|record| record.map(|(journal, custody)| acyclic_fs::OwnedMaterializationJournal {
                journal, custody: Some(custody),
            }))
            .map_err(NativePreparationJournalError::into_owner_error)
    }

    async fn compare_and_swap(
        &self, operation: acyclic_fs::OperationId, expected_revision: u64,
        mut replacement: acyclic_fs::OwnedMaterializationJournal,
    ) -> Result<(bool, acyclic_fs::OwnedMaterializationJournal)> {
        let (owner, settlement) = match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement } => (*owner, *settlement),
            NativePreparationAuthority::Admission(_) => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "read-only native admission cannot append a materializer journal",
            }),
        };
        if !replacement.custody.as_ref().is_some_and(|custody| custody.work().shares_ledger(&self.work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native journal requires its original buffer custody" });
        }
        if replacement.plan.operation_id != operation
            || replacement.revision != expected_revision.checked_add(1).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "materializer revision overflow",
            })?
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "invalid native materializer replacement",
            });
        }
        // This is the observed prefix before serialization/commit. The exact
        // later byte work is retained in this invocation's receipt, then the
        // next same-journal checkpoint; never invent a lost acknowledgement.
        replacement.work = self.work.journal_work().map_err(native_root_error)?;
        let (encoded, _encoded_custody) = native_canonical(&replacement.journal, &self.work)?;
        let bytes = bytes::Bytes::from(encoded);
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "materializer journal exceeds Stream record limit",
            });
        }
        self.work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64, ..acyclic_fs::WorkCounters::default()
        }).map_err(native_root_error)?;
        let (key_bytes, _key_custody) = native_bytes(blake3::hash(&bytes).as_bytes(), &self.work)?;
        let key = acyclic_stream::IdempotencyKey::new(bytes::Bytes::from(key_bytes))?;
        let (path, _path_custody) = self.path(operation)?;
        let committed = owner.append_with_native_work(
            path, expected_revision, &key, bytes,
            if settlement { crate::distributed::JournalWrite::Settlement }
            else { crate::distributed::JournalWrite::Fresh },
            &self.work,
        ).await?;
        Ok((committed, replacement))
    }
}


fn native_intent_control_bytes(intent: &NativePreparationIntent) -> Result<u64> {
    let mut bytes = 0_usize;
    macro_rules! retain {
        ($bytes:expr) => {
            bytes = bytes.checked_add($bytes)
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native intent payload overflow" })?;
        };
    }
    retain!(intent.operation_directory.capacity());
    retain!(intent.original_fence.reservation_id.capacity());
    retain!(intent.original_fence.placement.capacity());
    retain!(intent.manifest.options.root.capacity());
    retain!(intent.manifest.volumes.capacity().checked_mul(std::mem::size_of::<NativeVolumeMapping>())
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native intent mapping capacity overflow" })?);
    for mapping in &intent.manifest.volumes {
        retain!(mapping.path.capacity());
        let volume = &mapping.volume;
        retain!(volume.provider().namespace().len());
        retain!(volume.provider().family().len());
        retain!(volume.provider().version().len());
        retain!(volume.id().len());
        if let crate::conversation::VolumeOwner::Project(owner)
            | crate::conversation::VolumeOwner::Session(owner) = volume.owner()
        {
            retain!(owner.len());
        }
        let generation = mapping.generation.as_resource();
        retain!(generation.provider().namespace().len());
        retain!(generation.provider().family().len());
        retain!(generation.provider().version().len());
        retain!(generation.key().len());
        if let Some(version) = generation.version() { retain!(version.len()); }
    }
    // Private reference fields are logical decoded payload, not guesses at
    // opaque allocator capacity. Public path/vector/fence capacities are known.
    Ok(bytes as u64)
}

/// Only the two audited, closed native record schemas use this helper.
/// The audited logical quota is held through trusted serde and atomically
/// substituted by known retained data; it is not allocator/RSS accounting.
enum NativeRecordSchema {
    Journal,
    PreparationIntent,
}

fn native_decoded_record_typed<T: serde::de::DeserializeOwned>(
    bytes: &[u8], schema: NativeRecordSchema, work: &NativeMaterializationWork,
    retained_bytes: impl FnOnce(&T) -> std::result::Result<u64, NativePreparationJournalError>,
) -> std::result::Result<(T, acyclic_fs::NativeAllocation), NativePreparationJournalError> {
    let maximum_slot = match schema {
        // These are every dynamic Vec element type in MaterializationJournal:
        // edits, qualified selections, preimages, and opaque byte images.
        // Preparation progress owns only inline optional IDs and an enum.
        NativeRecordSchema::Journal => [
            std::mem::size_of::<acyclic_fs::MaterializationEdit>(),
            std::mem::size_of::<acyclic_fs::MaterializationPreparationSelection>(),
            std::mem::size_of::<acyclic_fs::MaterializationPreimage>(),
            std::mem::size_of::<u8>(),
        ].into_iter().max().expect("nonempty concrete journal schema"),
        // Manifest mappings and resource-key bytes are the only vectors.
        // Provider, owner, resource-version and fence values are Strings;
        // LazySnapshotRef and all identifiers are inline Copy records.
        NativeRecordSchema::PreparationIntent => std::mem::size_of::<NativeVolumeMapping>()
            .max(std::mem::size_of::<u8>()),
    } as u64;
    let remaining = work.remaining()?;
    let bounds = crate::contract::JsonBounds {
        // Borrowed bytes already admitted at the authority/read or original
        // journal boundary; scanning is not a second physical read.
        maximum_bytes: bytes.len() as u64,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let mut items = 0_u64;
    let mut input_bytes = 0_u64;
    let mut custody = work.retain_owned(0)?;
    let value = crate::contract::json_from_slice_bounded_native_trusted(bytes, bounds, 0, crate::wire::ErrorCode::Storage, crate::contract::JsonValueSchema::None, &mut |charge| {
        let result = (|| -> Result<()> {
            match charge {
                crate::contract::JsonCodecCharge::Work { items: examined, .. } => {
                    items = items.checked_add(examined)
                        .ok_or(acyclic_fs::WorkError::Overflow)
                        .map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native_root_error)?;
                }
                crate::contract::JsonCodecCharge::Read { bytes } => {
                    input_bytes = input_bytes.checked_add(bytes)
                        .ok_or(acyclic_fs::WorkError::Overflow)
                        .map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native_root_error)?;
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes: scratch, .. } => {
                    // The lexical count includes every element/container/key.
                    // Nonempty-vector count and total element count are each at
                    // most items. Vec growth doubles; tiny vectors may initially
                    // reserve up to eight slots, including the u8 minimum.
                    let owned = items.checked_mul(10).and_then(|slots| slots.checked_mul(maximum_slot))
                        .and_then(|slots| slots.checked_add(input_bytes))
                        .and_then(|owned| owned.checked_add(scratch))
                        .ok_or(acyclic_fs::WorkError::Overflow)
                        .map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native_root_error)?;
                    // Audited serde JSON strings use visit_str/borrowed_str; the
                    // Rust Unix byte and Windows WTF-8 PathBuf backends copy/move
                    // UTF-8, not a simultaneous UTF-16 buffer. Escapes never grow
                    // total decoded UTF-8 past the consumed JSON input.
                    // This is an acquired quota, not a prediction reported as
                    // physical allocation work. It covers partial serde outputs.
                    let mut reservation = work.retain_owned(owned).map_err(native_root_error)?;
                    custody.absorb(&mut reservation).map_err(native_root_error)?;
                    return Ok(());
                }
                _ => {}
            }
            native_codec_charge_typed(charge, work, &mut custody).map_err(native_root_error)
        })();
        result
    });
    let value = match value {
        Ok(value) => value,
        Err(error) => return Err(NativePreparationJournalError::Owner(native_codec_error_custody(error, custody))),
    };
    custody.substitute(retained_bytes(&value)?)?;
    Ok((value, custody))
}

fn native_root_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

fn native_codec_error_custody(error: Error, custody: acyclic_fs::NativeAllocation) -> Error {
    match error {
        Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(custody) },
        Error::NativeDecode { code, source, custody: None } => Error::NativeDecode { code, source, custody: Some(custody) },
        // Preserve an already owned Root cause/custody whole. Codec buffers
        // have physically dropped before this function can drop local custody.
        error => error,
    }
}

fn native_canonical<T: serde::Serialize + ?Sized>(
    value: &T, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let remaining = work.remaining().map_err(native_root_error)?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: remaining.bytes_encoded,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let mut custody = work.retain_owned(0).map_err(native_root_error)?;
    let mut raw_custody: Option<acyclic_fs::NativeAllocation> = None;
    let bytes = crate::contract::canonical_json_bytes_bounded(value, bounds, &mut |charge| {
        match charge {
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                let mut reservation = work.retain_owned(maximum_owned_bytes).map_err(native_root_error)?;
                if let Some(existing) = raw_custody.as_mut() {
                    existing.absorb(&mut reservation).map_err(native_root_error)?;
                } else {
                    raw_custody = Some(reservation);
                }
                Ok(())
            }
            crate::contract::JsonCodecCharge::Retain { bytes } if raw_custody.is_some() => {
                let retained = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| native_root_error(acyclic_fs::WorkError::Overflow.into()))?;
                let mut reservation = raw_custody.take().expect("checked opaque quota");
                custody.absorb(&mut reservation).map_err(native_root_error)?;
                // The original RawValue parser and its scratch have dropped;
                // substitute its observed tree within the already held quota.
                custody.substitute(retained).map_err(native_root_error)
            }
            charge => native_codec_charge_typed(charge, work, &mut custody).map_err(native_root_error),
        }
    });
    match bytes {
        Ok(bytes) => Ok((bytes, custody)),
        Err(error) => Err(native_codec_error_custody(error, custody)),
    }
}


fn native_codec_charge_typed(
    charge: crate::contract::JsonCodecCharge, work: &NativeMaterializationWork,
    custody: &mut acyclic_fs::NativeAllocation,
) -> std::result::Result<(), acyclic_fs::NativeTreeMaterializationError> {
    use crate::contract::JsonCodecCharge;
    match charge {
        JsonCodecCharge::Work { bytes, items } => work.charge(acyclic_fs::WorkCounters {
            bytes_encoded: bytes, items_examined: items, ..acyclic_fs::WorkCounters::default()
        }),
        JsonCodecCharge::Inspect { items } => work.charge(acyclic_fs::WorkCounters {
            items_examined: items, ..acyclic_fs::WorkCounters::default()
        }),
        JsonCodecCharge::CopyAdmission { bytes } => work.admit(acyclic_fs::WorkCounters {
            bytes_copied: bytes, ..acyclic_fs::WorkCounters::default()
        }),
        JsonCodecCharge::Copy { bytes } => work.charge(acyclic_fs::WorkCounters {
            bytes_copied: bytes, ..acyclic_fs::WorkCounters::default()
        }),
        JsonCodecCharge::Read { .. } => work.remaining().map(|_| ()),
        JsonCodecCharge::AllocationOperation { .. } => work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
        }),
        JsonCodecCharge::Retain { bytes } => {
            let retained = custody.bytes().checked_add(bytes)
                .ok_or(acyclic_fs::WorkError::Overflow)?;
            custody.resize_retained(retained)
        }
        JsonCodecCharge::Release { bytes } => custody.release(bytes),
        JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
            Ok(acyclic_fs::WorkCounters::default().admit(&acyclic_fs::WorkCounters {
                peak_allocation_bytes: maximum_owned_bytes, ..acyclic_fs::WorkCounters::default()
            }, &work.remaining()?)?)
        }
    }
}

// Each guard owns actual buffer capacity. Moves between preparation stages
// transfer guards; nested SDK work sees all still-live caller storage.
fn native_vec<T>(capacity: usize, work: &NativeMaterializationWork) -> Result<(Vec<T>, acyclic_fs::NativeAllocation)> {
    let requested = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_root_error(acyclic_fs::WorkError::Overflow.into()))?;
    let mut custody = if requested == 0 {
        work.retain_owned(0)
    } else {
        work.claim(requested as u64)
    }.map_err(native_root_error)?;
    let mut value = Vec::new();
    value.try_reserve_exact(capacity).map_err(|error|
        native_root_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(error)))?;
    let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_root_error(acyclic_fs::WorkError::Overflow.into()))?;
    if let Err(cause) = custody.resize_retained(actual as u64) {
        drop(value);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok((value, custody))
}

fn native_bytes(value: &[u8], work: &NativeMaterializationWork) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let (copied, custody) = native_vec(value.len(), work)?;
    let custody = custody;
    let mut copied = copied;
    work.charge(acyclic_fs::WorkCounters {
        bytes_copied: value.len() as u64, ..acyclic_fs::WorkCounters::default()
    }).map_err(native_root_error)?;
    copied.extend_from_slice(value);
    Ok((copied, custody))
}

fn native_string(value: &str, work: &NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
    work.display(&value).map_err(native_root_error)
}

fn native_provider(
    provider: &crate::resources::ProviderRef, work: &NativeMaterializationWork,
) -> Result<(crate::resources::ProviderRef, acyclic_fs::NativeAllocation)> {
    let (namespace, custody) = native_string(provider.namespace(), work)?;
    let mut custody = custody;
    let namespace = namespace;
    let (family, mut family_custody) = native_string(provider.family(), work)?;
    custody.absorb(&mut family_custody).map_err(native_root_error)?;
    let (version, mut version_custody) = native_string(provider.version(), work)?;
    custody.absorb(&mut version_custody).map_err(native_root_error)?;
    Ok((crate::resources::ProviderRef::new(namespace, family, version)?, custody))
}

/// Copies the original qualified volume without substituting its owner.
pub fn copy_native_volume_with_work(
    volume: &VolumeRef, work: &NativeMaterializationWork,
) -> Result<(VolumeRef, acyclic_fs::NativeAllocation)> {
    use crate::conversation::VolumeOwner;
    let (provider, custody) = native_provider(volume.provider(), work)?;
    let mut custody = custody;
    let provider = provider;
    let (id, mut id_custody) = native_string(volume.id(), work)?;
    custody.absorb(&mut id_custody).map_err(native_root_error)?;
    let owner = match volume.owner() {
        VolumeOwner::Agent(agent) => VolumeOwner::Agent(*agent),
        VolumeOwner::Project(owner) => {
            let (owner, mut owner_custody) = native_string(owner, work)?;
            custody.absorb(&mut owner_custody).map_err(native_root_error)?;
            VolumeOwner::Project(owner)
        }
        VolumeOwner::Session(owner) => {
            let (owner, mut owner_custody) = native_string(owner, work)?;
            custody.absorb(&mut owner_custody).map_err(native_root_error)?;
            VolumeOwner::Session(owner)
        }
    };
    Ok((VolumeRef::new(provider, id, volume.class(), owner)?, custody))
}

/// Copies the original immutable generation with same-ledger custody.
pub fn copy_native_generation_with_work(
    generation: &GenerationRef, work: &NativeMaterializationWork,
) -> Result<(GenerationRef, acyclic_fs::NativeAllocation)> {
    let resource = generation.as_resource();
    let (provider, custody) = native_provider(resource.provider(), work)?;
    let mut custody = custody;
    let provider = provider;
    let (key, mut key_custody) = native_bytes(resource.key(), work)?;
    custody.absorb(&mut key_custody).map_err(native_root_error)?;
    let version = resource.version().map(|version| {
        let (version, mut version_custody) = native_string(version, work)?;
        custody.absorb(&mut version_custody).map_err(native_root_error)?;
        Ok::<_, Error>(version)
    }).transpose()?;
    Ok((GenerationRef::new(provider, key, version)?, custody))
}

/// Copies one admitted mapping while retaining its actual original-ledger storage.
pub fn copy_native_mapping_with_work(
    mapping: &NativeVolumeMapping, work: &NativeMaterializationWork,
) -> Result<(NativeVolumeMapping, acyclic_fs::NativeAllocation)> {
    let (volume, custody) = copy_native_volume_with_work(&mapping.volume, work)?;
    let mut custody = custody;
    let volume = volume;
    let (generation, mut generation_custody) = copy_native_generation_with_work(&mapping.generation, work)?;
    custody.absorb(&mut generation_custody).map_err(native_root_error)?;
    let (path, mut path_custody) = work.copy_path(&mapping.path).map_err(native_root_error)?;
    custody.absorb(&mut path_custody).map_err(native_root_error)?;
    Ok((NativeVolumeMapping { volume, generation, path, writable: mapping.writable,
        namespace: mapping.namespace.clone(),
        authority_revision: mapping.authority_revision }, custody))
}

fn native_manifest(
    manifest: &NativeViewManifest, work: &NativeMaterializationWork,
) -> Result<(NativeViewManifest, acyclic_fs::NativeAllocation)> {
    let (root, custody) = work.copy_path(&manifest.options.root).map_err(native_root_error)?;
    let mut custody = custody;
    let root = root;
    let options = NativeViewOptions {
        root, maximum_volumes: manifest.options.maximum_volumes,
        work_per_volume: manifest.options.work_per_volume,
    };
    let (mut volumes, mut volumes_custody) = native_vec(manifest.volumes.len(), work)?;
    custody.absorb(&mut volumes_custody).map_err(native_root_error)?;
    for mapping in &manifest.volumes {
        if !matches!(mapping.namespace, NativeVolumeNamespace::MaterializedDirectory) {
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        let (mapping, mut mapping_custody) = copy_native_mapping_with_work(mapping, work)?;
        custody.absorb(&mut mapping_custody).map_err(native_root_error)?;
        volumes.push(mapping);
    }
    Ok((NativeViewManifest { options, volumes }, custody))
}


fn native_result<T>(
    result: Result<T>, work: &NativeMaterializationWork,
)-> std::result::Result<acyclic_fs::OperationReceipt<T>, (Error, acyclic_fs::WorkCounters)> {
    result.map(|value| acyclic_fs::OperationReceipt { value, work: work.receipt() })
        .map_err(|error| (error, work.receipt()))
}

fn publication_admission_failure((error, work): (Error, acyclic_fs::WorkCounters)) -> NativePublicationFailure {
    NativePublicationFailure {
        destination: None, completed: Vec::new(), work, message: error.to_string(),
    }
}
