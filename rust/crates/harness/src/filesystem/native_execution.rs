//! Optional native command composition over existing task, effect and workspace owners.

use super::{
    FilesystemHost, NativeVolumeNamespace, NativeViewManifest, NativeViewOptions,
    NativeVolumeMapping, workspace_ref,
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
    /// Admit the dynamic frame before bounded physical checks.
    fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<BoxProviderFuture<'_, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>, Error>;
    /// Observe an original held root and no-follow relative directory. This
    /// does not authorize a later ambient-path process launch as confinement.
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>, Error>;
    /// Admit publication's dynamic frame; completed destinations remain final.
    fn publish<'a>(
        &'a self, owner: NativeViewOwner<'a, P>,
    ) -> std::result::Result<BoxProviderFuture<'a, std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure>>, NativePublicationFailure>;
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
        let result = match native_future_typed(|| async {
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
            Ok(())
        }, work) {
            Ok(future) => future.await,
            Err(error) => Err(NativeReservationError::Native(error)),
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
            Ok(budget) => match native_future_typed(
                || self.workspace.release_publication_measured(self.reservation, budget, cancellation), work,
            ) {
                Ok(future) => future.await.map_err(|failure| {
                    let error = NativeReservationError::Sdk(failure.error);
                    match work.spent(*failure.work) {
                        Ok(()) => error,
                        Err(error) => NativeReservationError::Native(error),
                    }
                }),
                Err(error) => Err(NativeReservationError::Native(error)),
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
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Self>, Error> {
        use acyclic_fs::MaterializationBackend as _;
        if !budget.has_finite_allowances() || !root.is_absolute() || !operation_directory.is_absolute() {
            return Err(acyclic_fs::OperationFailure::before_work(Error::Invalid("native allocation witness requires absolute paths and finite allowances".into())));
        }
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), materializer_error))?;
        let result = async {
            let meter = work.clone();
            acyclic_native_runtime::run_blocking_io(move || {
                let backend = acyclic_fs::NativeTreeMaterializationBackend::open_with_work(
                    root, operation_directory, meter.clone(),
                ).map_err(materializer_error)?;
                backend.require_initial_empty().map_err(materializer_error)?;
                let expected_identity = acyclic_fs::NativeRootIdentity::from_bytes(
                    backend.allocation_identity().ok_or_else(|| Error::Conflict("native root allocation is unwitnessed".into()))?,
                );
                let expected_preparation_identity = backend.preparation_identity()
                    .ok_or_else(|| Error::Conflict("native staging allocation is unwitnessed".into()))?;
                let (root, root_custody) = meter.copy_path(backend.root_path()).map_err(materializer_error)?;
                let (operation_directory, operation_custody) = meter.copy_path(backend.operation_directory_path()).map_err(materializer_error)?;
                Ok::<_, Error>((Self { root, operation_directory, expected_identity, expected_preparation_identity }, root_custody, operation_custody))
            }).await.map_err(materializer_error)?
        }.await.map(|(value, _root_custody, _operation_custody)| value);
        result.map(|value| acyclic_fs::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| acyclic_fs::OperationFailure::new(error, work.receipt()))
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
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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


impl NativePreparationJournalError {
    // Only the existing ordinary materializer adapter requires Harness Error.
    // The new read-only query keeps this closed typed carrier throughout.
    fn into_owner_error(self) -> Error {
        match self {
            Self::Owner(error) => error,
            Self::Native(cause) => Error::NativeWork { cause, custody: None },
            Self::Backend(error) => error.into(),
            Self::Record(message) => Error::Storage(message.into()),
        }
    }
}

struct PreparedVolume<A, O> {
    view: NativeWorkingSet<A, O>,
    writable: bool,
    publication: PublicationPermit,
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

/// Provider-owned materialized namespace, with the canonical SDK capture owner
/// for every independently authorized volume. It carries no second workspace engine.
pub struct NativeVolumeView<A, O> {
    manifest: NativeViewManifest,
    volumes: Vec<PreparedVolume<A, O>>,
    preparation: NativePreparationReceipt,
    cancellation: acyclic_fs::CancellationToken,
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
    fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<BoxProviderFuture<'_, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>, Error> {
        let work = self.validation_work(budget)?;
        let meter = work.clone();
        native_future(|| async move {
            let result = self.validate_dispatch_with_work(&meter).await;
            native_result(result, &meter)
        }, &work)
    }
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>, Error> {
        let work = self.validation_work(budget)?;
        let meter = work.clone();
        native_future(|| async move {
            native_result(self.validate_directory_with_work(cwd, &meter), &meter)
        }, &work)
    }
    fn publish<'a>(
        &'a self, owner: NativeViewOwner<'a, P>,
    ) -> std::result::Result<BoxProviderFuture<'a, std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure>>, NativePublicationFailure> {
        let work = self.publication_work()?;
        let meter = work.clone();
        native_future(|| async move { self.publish_with_work(owner.owner, &meter).await }, &work)
            .map_err(publication_admission_failure)
    }
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Creates or resumes the original task's journaled physical preparation.
    /// Reopening evidence never substitutes for current original grants/fences.
    #[inline(never)]
    pub async fn prepare<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>,
        allocation: NativePreparationRoot,
        options: NativeViewOptions,
        bindings: Vec<BorrowedNativeVolumeBinding<'a, A, O>>,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Self>, Error> {
            let budget = match options.budget_for_volume_count(bindings.len()) {
                Ok(budget) => budget,
                Err(error) => return Err(acyclic_fs::OperationFailure::before_work(error)),
            };
            let work = NativeMaterializationWork::new(budget, cancellation)
                .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), materializer_error))?;
            let meter = work.clone();
            native_future(|| Self::prepare_with_work(
                owner, allocation, options, bindings, cancellation, meter,
            ), &work)?.await
    }

    #[allow(clippy::too_many_lines, reason = "one existing-engine preparation and attachment transaction")]
    async fn prepare_with_work<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>,
        allocation: NativePreparationRoot,
        options: NativeViewOptions,
        bindings: Vec<BorrowedNativeVolumeBinding<'a, A, O>>,
        cancellation: &'a acyclic_fs::CancellationToken,
        work: NativeMaterializationWork,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Self>, Error> {
            let result = async {
                let input_bytes = bindings.capacity().checked_mul(std::mem::size_of::<BorrowedNativeVolumeBinding<'_, A, O>>())
                    .ok_or_else(|| Error::Storage("native preparation input capacity overflow".into()))?;
                let bindings_custody = work.retain_owned(input_bytes as u64).map_err(materializer_error)?;
                let allocation_bytes = allocation.root.capacity().checked_add(allocation.operation_directory.capacity())
                    .ok_or_else(|| Error::Storage("native allocation input capacity overflow".into()))?;
                let _allocation_custody = work.retain_owned(allocation_bytes as u64).map_err(materializer_error)?;
                let mut output_custody = work.retain_owned(options.root.capacity() as u64).map_err(materializer_error)?;
                owner.verify(false).await?;
                if options.maximum_volumes == 0 || bindings.is_empty()
                    || bindings.len() as u64 > u64::from(options.maximum_volumes)
                    || !options.root.is_absolute() || !allocation.root.is_absolute()
                    || !allocation.operation_directory.is_absolute()
                    || !options.work_per_volume.has_finite_allowances()
                {
                    return Err(Error::Invalid("invalid native preparation allocation or allowances".into()));
                }
                if options.root != allocation.root {
                    return Err(Error::Conflict("native options root differs from witnessed host allocation".into()));
                }
                let (mut mappings, mappings_custody) = native_vec(bindings.len(), &work)?;
                output_custody.absorb(mappings_custody).map_err(materializer_error)?;
                let (mut binding_work, binding_work_custody) = native_vec(bindings.len(), &work)?;
                for (index, binding) in bindings.iter().enumerate() {
                    work.charge(acyclic_fs::WorkCounters {
                        items_examined: 1, ..acyclic_fs::WorkCounters::default()
                    }).map_err(materializer_error)?;
                    if !matches!(binding.namespace, NativeVolumeNamespace::MaterializedDirectory) {
                        return Err(Error::Invalid("an existing lazy mount must remain attached through its original native owner".into()));
                    }
                    let path = Path::new(binding.path);
                    if path.as_os_str().is_empty() {
                        return Err(Error::Invalid("native mapping path is empty".into()));
                    }
                    for part in path.components() {
                        work.charge(acyclic_fs::WorkCounters {
                            source_path_components: 1, ..acyclic_fs::WorkCounters::default()
                        }).map_err(materializer_error)?;
                        if !matches!(part, Component::Normal(_)) {
                            return Err(Error::Invalid("native volume paths overlap or escape their root".into()));
                        }
                    }
                    for prior in &bindings[..index] {
                        work.charge(acyclic_fs::WorkCounters {
                            items_examined: 1, ..acyclic_fs::WorkCounters::default()
                        }).map_err(materializer_error)?;
                        if native_paths_overlap(path, Path::new(prior.path)) {
                            return Err(Error::Invalid("native volume paths overlap or escape their root".into()));
                        }
                    }
                    let mut volume_work = acyclic_fs::WorkCounters::default();
                    verify_binding(owner, binding, &work, &mut volume_work, options.work_per_volume, cancellation)?;
                    binding_work.push(volume_work);
                    let (volume, volume_custody) = native_volume(binding.volume, &work)?;
                    output_custody.absorb(volume_custody).map_err(materializer_error)?;
                    let (generation, generation_custody) = native_generation(binding.generation, &work)?;
                    output_custody.absorb(generation_custody).map_err(materializer_error)?;
                    let (path, path_custody) = work.join_path(&options.root, path).map_err(materializer_error)?;
                    output_custody.absorb(path_custody).map_err(materializer_error)?;
                    mappings.push(NativeVolumeMapping {
                        volume, generation, path, writable: binding.writable,
                        namespace: NativeVolumeNamespace::MaterializedDirectory,
                        authority_revision: {
                            let (bytes, _custody) = native_canonical(
                                &(binding.verifier.audience(), binding.scope), &work,
                            )?;
                            *blake3::hash(&bytes).as_bytes()
                        },
                    });
                }
                let manifest = NativeViewManifest {
                    options, volumes: mappings,
                };
                manifest.validate()?;
                let backend = native_backend(&allocation, &work, true).await?;
                let store = NativePreparationStore::new(owner, work.clone(), false);
                let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
                let operation = native_preparation_operation(owner);
                let retained = materializer.load_owned(operation).await.map_err(materializer_error)?;
                // Qualify every source with its actual SDK volume identity before
                // durable intent; no host path or provider result supplies it.
                let (mut generations, generations_custody) = native_vec(bindings.len(), &work)?;
                let (mut selection, mut plan_custody) = native_vec(bindings.len(), &work)?;
                for (binding, volume_work) in bindings.iter().zip(&mut binding_work) {
                    owner.verify(false).await?;
                    verify_binding(owner, binding, &work, volume_work, manifest.options.work_per_volume, cancellation)?;
                    let reference = workspace_ref(binding.host.provider.clone(), &binding.volume.storage_name()?)?;
                    let opened = binding.host.open_measured(
                        &reference, native_allowance(*volume_work, manifest.options.work_per_volume, &work)?, cancellation,
                    ).await;
                    let workspace = account_preparation(opened, &work, volume_work, |error| error)?;
                    let selected = binding.host.generation_measured(
                        &workspace, &binding.generation,
                        native_allowance(*volume_work, manifest.options.work_per_volume, &work)?, cancellation,
                    ).await;
                    let generation = account_preparation(selected, &work, volume_work, |error| error)?;
                    if binding.writable {
                        let head = workspace.head_measured(
                            native_allowance(*volume_work, manifest.options.work_per_volume, &work)?, cancellation,
                        ).await;
                        let head = account_preparation(head, &work, volume_work, |error| Error::Conflict(error.to_string()))?;
                        if head.id() != generation.id() {
                            return Err(Error::Conflict("native destination generation is stale".into()));
                        }
                    }
                    let (provider_identity, provider_custody) = native_canonical(binding.volume.provider(), &work)?;
                    plan_custody.absorb(provider_custody).map_err(materializer_error)?;
                    selection.push(acyclic_fs::MaterializationPreparationSelection {
                        provider_identity, volume: workspace.id().volume_id(), generation: generation.id(),
                    });
                    generations.push((generation, *volume_work));
                }
                drop(binding_work);
                drop(binding_work_custody);
                let mut intent_operation_custody = None;
                let intent = if let Some((journal, _journal_custody)) = &retained {
                    let (intent, intent_custody) = preparation_intent(journal, &work)?;
                    output_custody.absorb(intent_custody).map_err(materializer_error)?;
                    if intent.task != owner.task_binding().0 || intent.manifest != manifest
                        || intent.operation_directory != allocation.operation_directory
                    {
                        return Err(Error::Conflict("native preparation original selection changed".into()));
                    }
                    require_allocation(journal, &allocation)?;
                    intent
                } else {
                    materializer.backend().require_initial_empty().map_err(materializer_error)?;
                    let (task, original_fence) = owner.task_binding();
                    let (reservation_id, reservation_custody) = native_string(&original_fence.reservation_id, &work)?;
                    let (placement, placement_custody) = native_string(&original_fence.placement, &work)?;
                    output_custody.absorb(reservation_custody).map_err(materializer_error)?;
                    output_custody.absorb(placement_custody).map_err(materializer_error)?;
                    let original_fence = crate::scheduler::LeaseFence { reservation_id, placement };
                    let (operation_directory, operation_custody) = work.copy_path(&allocation.operation_directory)
                        .map_err(materializer_error)?;
                    let (manifest, manifest_custody) = native_manifest(&manifest, &work)?;
                    intent_operation_custody = Some(operation_custody);
                    output_custody.absorb(manifest_custody).map_err(materializer_error)?;
                    NativePreparationIntent { task, original_fence, operation_directory, manifest }
                };
                let (mut top_level, top_custody) = native_vec(bindings.len(), &work)?;
                for binding in &bindings {
                    let first = Path::new(binding.path).components().next()
                        .and_then(|part| part.as_os_str().to_str())
                        .ok_or_else(|| Error::Invalid("native mapping name must be Unicode".into()))?;
                    top_level.push(first);
                }
                top_level.sort_unstable();
                top_level.dedup();
                let (mut edits, edits_custody) = native_vec(top_level.len(), &work)?;
                plan_custody.absorb(edits_custody).map_err(materializer_error)?;
                for path in &top_level {
                    let (path, path_custody) = native_string(path, &work)?;
                    plan_custody.absorb(path_custody).map_err(materializer_error)?;
                    let (image, image_custody) = native_bytes(acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX, &work)?;
                    plan_custody.absorb(image_custody).map_err(materializer_error)?;
                    edits.push(acyclic_fs::MaterializationEdit::Install { path, image });
                }
                drop(top_level);
                drop(top_custody);
                let (original, original_custody) = native_canonical(&intent, &work)?;
                plan_custody.absorb(original_custody).map_err(materializer_error)?;
                let plan = acyclic_fs::MaterializationPlan {
                    operation_id: operation,
                    intent: acyclic_fs::MaterializationIntent::NativePreparation {
                        selection, original,
                    },
                    edits,
                };
                if retained.as_ref().is_some_and(|(journal, _journal_custody)| journal.plan != plan) {
                    return Err(Error::Conflict("native preparation operation was reused".into()));
                }
                drop(retained);
                let (journal, journal_custody) = materializer.prepare_owned(plan, plan_custody).await.map_err(materializer_error)?;
                require_allocation(&journal, &allocation)?;
                // No captured preimage or live edit exists in this state. Only the
                // original held, service-owned target may be cleared and restaged.
                let restage = journal.phase == MaterializationPhase::Preparing
                    && journal.preimages.is_empty();
                if restage {
                    materializer.backend().reset_target_directory("").await.map_err(materializer_error)?;
                    for (binding, (generation, volume_work)) in bindings.iter().zip(&mut generations) {
                        owner.verify(false).await?;
                        verify_binding(owner, binding, &work, volume_work, manifest.options.work_per_volume, cancellation)?;
                        let identity = materializer.backend().reset_target_directory(&binding.path)
                            .await.map_err(materializer_error)?;
                        let (path, _path_custody) = materializer.backend().staging_path(binding.path).map_err(materializer_error)?;
                        let staged = generation.materialize(
                            &acyclic_fs::MaterializeOptions::native(path).expected_root_identity(Some(identity)),
                            native_allowance(*volume_work, manifest.options.work_per_volume, &work)?, cancellation,
                        ).await;
                        account_preparation(staged, &work, volume_work, |error| Error::Storage(error.to_string()))?;
                    }
                }
                owner.verify(false).await?;
                materializer.backend().require_selected_root(&journal.plan.edits).map_err(materializer_error)?;
                drop((journal, journal_custody));
                let (journal, journal_custody) = materializer.recover_owned(operation, acyclic_fs::MaterializationRecovery::Complete)
                    .await.map_err(materializer_error)?
                    .ok_or_else(|| Error::Conflict("native preparation journal disappeared".into()))?;
                require_allocation(&journal, &allocation)?;
                materializer.backend().require_selected_root(&journal.plan.edits).map_err(materializer_error)?;
                let (mut volumes, volumes_custody) = native_vec(bindings.len(), &work)?;
                output_custody.absorb(volumes_custody).map_err(materializer_error)?;
                let (mut identities, identities_custody) = native_vec(bindings.len(), &work)?;
                for ((binding, mapping), (generation, mut volume_work)) in bindings.into_iter()
                    .zip(&manifest.volumes).zip(generations)
                {
                    owner.verify(false).await?;
                    verify_binding(owner, &binding, &work, &mut volume_work, manifest.options.work_per_volume, cancellation)?;
                    let identity = materializer.backend().mapping_root_identity(binding.path).map_err(materializer_error)?;
                    if identities.contains(&identity) {
                        return Err(Error::Conflict("native volume directory aliases changed".into()));
                    }
                    identities.push(identity);
                    let mount = if binding.writable {
                        acyclic_fs::MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual)
                    } else { acyclic_fs::MountOptions::read_only() };
                    let (attachment_path, _path_custody) = work.copy_path(&mapping.path).map_err(materializer_error)?;
                    let attached = generation.attach_native_working_set_with_budgets(
                        &mount, &acyclic_fs::MaterializeOptions::native(attachment_path), identity,
                        native_allowance(volume_work, manifest.options.work_per_volume, &work)?,
                        manifest.options.work_per_volume, cancellation,
                    ).await;
                    let view = account_preparation(attached, &work, &mut volume_work, |error| Error::Conflict(error.to_string()))?;
                    let retained_bytes = view.preparation_control_bytes()
                        .checked_add(view.preparation_captured_data_bytes())
                        .ok_or_else(|| Error::Storage("native prepared output custody overflow".into()))?;
                    output_custody.absorb(work.retain_owned(retained_bytes).map_err(materializer_error)?)
                        .map_err(materializer_error)?;
                    volumes.push(PreparedVolume { view, writable: binding.writable, publication: binding.publication });
                }
                drop(bindings_custody);
                drop(generations_custody);
                drop(identities);
                drop(identities_custody);
                owner.verify(false).await?;
                cancellation.check().map_err(materializer_error)?;
                checkpoint_preparation(materializer.store(), journal, journal_custody).await?;
                let (inspection, inspection_custody) = materializer.inspect_owned(operation).await.map_err(materializer_error)?
                    .ok_or_else(|| Error::Conflict("native preparation journal disappeared".into()))?;
                output_custody.absorb(inspection_custody).map_err(materializer_error)?;
                let receipt = preparation_receipt(intent, inspection);
                drop(intent_operation_custody);
                Ok(Self { manifest, volumes, preparation: receipt, cancellation: cancellation.clone(), _allocation: output_custody })
            }.await;
            result.map(|value| acyclic_fs::OperationReceipt { value, work: work.receipt() })
                .map_err(|error| acyclic_fs::OperationFailure::new(error, work.receipt()))
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
        owner: &TaskJournalOwner<P>, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<NativePreparationRoot>>, Error> {
        if !budget.has_finite_allowances() {
            return Err(acyclic_fs::OperationFailure::before_work(
                Error::Invalid("native allocation recovery requires finite allowances".into()),
            ));
        }
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), materializer_error))?;
        let meter = work.clone();
        native_future(|| async move {
            let result = async {
                owner.verify(true).await?;
                let store = NativePreparationStore::new(owner, meter.clone(), true);
                let operation = native_preparation_operation(owner);
                let Some(journal) = store.load(operation).await? else { return Ok(None); };
                if journal.preparation_progress.is_some() {
                    return Err(Error::Conflict("native original staging allocation is incomplete or indeterminate".into()));
                }
                let _journal_custody = meter.retain_journal(&journal).map_err(materializer_error)?;
                let (intent, _intent_custody) = preparation_intent(&journal, &meter)?;
                if intent.task != owner.task_binding().0 {
                    return Err(Error::Conflict("native allocation differs from original task".into()));
                }
                let expected_root = journal.root_identity
                    .ok_or_else(|| Error::Conflict("native original root allocation is unwitnessed".into()))?;
                let expected_preparation_identity = journal.preparation_identity
                    .ok_or_else(|| Error::Conflict("native original staging allocation is unwitnessed".into()))?;
                let expected_identity = meter.root_identity(&intent.manifest.options.root).map_err(materializer_error)?;
                if expected_identity.to_bytes() != expected_root {
                    return Err(Error::Conflict("native original root allocation was replaced".into()));
                }
                let allocation = NativePreparationRoot {
                    root: intent.manifest.options.root,
                    operation_directory: intent.operation_directory,
                    expected_identity, expected_preparation_identity,
                };
                // The existing held backend also compares every original
                // operation/target/backup identity. It creates no directories.
                let _backend = native_backend(&allocation, &meter, true).await?;
                owner.verify(true).await?;
                meter.remaining().map_err(materializer_error)?;
                Ok(Some(allocation))
            }.await;
            native_result(result, &meter)
        }, &work)?.await
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
        native_future_typed(|| async {
            if owner.task_binding().0 != scratch.original_task() || owner.operation_id() != original_admission {
                return Err(NativePreparationJournalError::Record("native journal query differs from original child or task admission operation"));
            }
            let (outcome, _outcome_custody) = owner.outcome_with_native_work(work).await?;
            if outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(scratch.volume(), VolumeOperation::Read, work)?;
            let operation = native_preparation_operation_with_work(owner, work)?;
            let store = NativePreparationStore::new(owner, work.clone(), true);
            let value = match store.load_record(operation, false).await? {
                None => NativePreparationJournalObservation::MissingJournalPhysicalStateUnknown {
                    preparation_operation: operation,
                },
                Some((journal, mut custody)) => {
                    let (intent, intent_custody) = preparation_intent_typed(&journal, work)?;
                    if intent.task != scratch.original_task()
                        || !intent.manifest.volumes.iter().any(|mapping| mapping.writable
                            && &mapping.volume == scratch.volume())
                    {
                        return Err(NativePreparationJournalError::Record("native journal does not retain the admitted original private scratch selection"));
                    }
                    custody.absorb(intent_custody)?;
                    NativePreparationJournalObservation::Recorded(NativeRecordedPreparation {
                        task: intent.task, original_fence: intent.original_fence,
                        manifest: intent.manifest, operation_directory: intent.operation_directory,
                        journal, _custody: custody,
                    })
                },
            };
            // Reauthenticate after the read, keeping both actual outcome copies
            // and their custody live until their respective checks finish.
            let (final_outcome, _final_outcome_custody) = owner.outcome_with_native_work(work).await?;
            if final_outcome.is_none() { owner.verify_with_native_work(true, work).await?; }
            owner.require_volume_grant_with_native_work(scratch.volume(), VolumeOperation::Read, work)?;
            work.remaining()?;
            Ok(acyclic_fs::OperationReceipt { value, work: work.receipt() })
        }, work)?.await
    }

    /// Queries the same materializer journal without adopting or repairing paths.
    pub async fn inspect_preparation<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        allocation: &NativePreparationRoot,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, Error> {
        inspect_native_preparation(owner, allocation, budget, cancellation, false).await
    }

    /// Cancels the original preparation through the materializer's exact rollback.
    /// A cancelled task may settle under its original current fence; it cannot attach.
    pub async fn cleanup_preparation<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        allocation: &NativePreparationRoot,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, Error> {
        inspect_native_preparation(owner, allocation, budget, cancellation, true).await
    }

    /// Exact request/approval manifest. Changing it requires new admission.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.manifest
    }

    /// Rechecks the admitted views in one caller-supplied remaining span.
    pub async fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let work = self.validation_work(budget)?;
        native_result(self.validate_dispatch_with_work(&work).await, &work)
    }

    /// Observes directory admission only. `Command::current_dir` may reopen a
    /// lexical path later; this view makes no sandbox or held-launch guarantee.
    pub async fn validate_working_directory(
        &self, cwd: &Path, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let work = self.validation_work(budget)?;
        native_result(self.validate_directory_with_work(cwd, &work), &work)
    }

    fn validation_work(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> acyclic_fs::MeasuredResult<NativeMaterializationWork, Error> {
        if !budget.has_finite_allowances() {
            return Err(acyclic_fs::OperationFailure::before_work(Error::Invalid("native validation requires finite remaining allowances".into())));
        }
        let original = self.manifest.options.budget_for_volume_count(self.manifest.volumes.len())
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        NativeMaterializationWork::new(original.intersect_budget(budget), &self.cancellation)
            .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), materializer_error))
    }

    fn original_root_identity(&self) -> Result<acyclic_fs::NativeRootIdentity> {
        self.preparation.journal.root_identity.map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or_else(|| Error::Conflict("native preparation has no original root witness".into()))
    }

    fn validate_directory_with_work(&self, cwd: &Path, work: &NativeMaterializationWork) -> Result<()> {
        // A prefix forms a locator, not authority. The actual held root,
        // witnessed identity and no-follow directory walk admit the locator.
        let relative = cwd.strip_prefix(&self.manifest.options.root)
            .map_err(|_| Error::Invalid("native working directory is outside its selected locator".into()))?;
        work.validate_working_directory(&self.manifest.options.root, self.original_root_identity()?, relative)
            .map_err(materializer_error)
    }

    async fn validate_dispatch_with_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        let expected = self.original_root_identity()?;
        if work.root_identity(&self.manifest.options.root).map_err(materializer_error)? != expected {
            return Err(Error::Conflict("native preparation root was replaced".into()));
        }
        for volume in &self.volumes {
            let mut volume_work = acyclic_fs::WorkCounters::default();
            let presented = volume.view.validate_for_presentation_measured(
                native_allowance(volume_work, self.manifest.options.work_per_volume, work)?, &self.cancellation,
            ).await;
            account_preparation(presented, work, &mut volume_work, |error| Error::Conflict(error.to_string()))?;
            let verified = volume.view.verify_unchanged_measured(
                native_allowance(volume_work, self.manifest.options.work_per_volume, work)?, &self.cancellation,
            ).await;
            account_preparation(verified, work, &mut volume_work, |error| Error::Conflict(error.to_string()))?;
        }
        if work.root_identity(&self.manifest.options.root).map_err(materializer_error)? != expected {
            return Err(Error::Conflict("native preparation root changed during validation".into()));
        }
        self.cancellation.check().map_err(materializer_error)?;
        Ok(())
    }

    /// Publishes only authorized destinations after source integrity passes.
    /// Every volume uses its existing permit/conflict lifecycle independently.
    pub async fn publish<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
    ) -> std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure> {
        let work = self.publication_work()?;
        self.publish_with_work(owner, &work).await
    }

    fn publication_work(&self) -> std::result::Result<NativeMaterializationWork, NativePublicationFailure> {
        let budget = self.manifest.validation_budget()
            .map_err(acyclic_fs::OperationFailure::before_work)
            .map_err(publication_admission_failure)?;
        NativeMaterializationWork::new(budget, &self.cancellation)
            .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), materializer_error))
            .map_err(publication_admission_failure)
    }

    async fn publish_with_work<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure> {
        let mut completed = 0_usize;
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest.volumes) {
            if !volume.writable {
                let mut volume_work = acyclic_fs::WorkCounters::default();
                let grant_budget = native_allowance(volume_work, self.manifest.options.work_per_volume, work)
                    .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                account_preparation(owner.require_volume_grant_measured(
                    &mapping.volume, VolumeOperation::Read, grant_budget, &self.cancellation,
                ), work, &mut volume_work, |error| error)
                    .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                let budget = native_allowance(volume_work, self.manifest.options.work_per_volume, work)
                    .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                let verified = volume.view.verify_unchanged_measured(budget, &self.cancellation).await;
                match verified {
                    Ok(receipt) => work.spent(receipt.work)
                        .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?,
                    Err(failure) => {
                        work.spent(*failure.work)
                            .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                        return Err(self.publication_failure(mapping, completed, work, failure.error.to_string()));
                    }
                }
                work.remaining().map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
            }
        }
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest.volumes) {
            if volume.writable {
                owner.verify(false).await
                    .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                let mut volume_work = acyclic_fs::WorkCounters::default();
                for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                    let grant_budget = native_allowance(volume_work, self.manifest.options.work_per_volume, work)
                        .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                    account_preparation(owner.require_volume_grant_measured(
                        &mapping.volume, operation, grant_budget, &self.cancellation,
                    ), work, &mut volume_work, |error| error)
                        .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                }
                let budget = native_allowance(volume_work, self.manifest.options.work_per_volume, work)
                    .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                match volume.view.sync_with_permit_measured(volume.publication, budget, &self.cancellation).await {
                    Ok(receipt) => {
                        // Record the actual successful destination before any
                        // later accounting or independent destination failure.
                        completed += 1;
                        work.spent(receipt.work)
                            .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                    }
                    Err(failure) => {
                        work.spent(*failure.work)
                            .map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
                        return Err(self.publication_failure(mapping, completed, work, failure.error.to_string()));
                    }
                }
                work.remaining().map_err(|error| self.publication_failure(mapping, completed, work, error.to_string()))?;
            }
        }
        Ok(work.receipt())
    }

    fn publication_failure(
        &self, mapping: &NativeVolumeMapping, completed: usize,
        work: &NativeMaterializationWork, message: String,
    ) -> NativePublicationFailure {
        NativePublicationFailure {
            destination: Some(mapping.clone()),
            completed: self.manifest.volumes.iter().filter(|mapping| mapping.writable)
                .take(completed).cloned().collect(),
            work: work.receipt(), message,
        }
    }
}

fn native_allowance(
    volume: acyclic_fs::WorkCounters, budget: acyclic_fs::WorkBudget,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::WorkBudget> {
    let volume = volume.remaining(budget).map_err(materializer_error)?;
    Ok(volume.intersect_budget(work.remaining().map_err(materializer_error)?))
}


fn native_preparation_operation<P: StreamProvider>(owner: &TaskJournalOwner<P>) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/native-preparation/v1\0");
    hash.update(&owner.task_binding().0.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
}

fn native_preparation_operation_with_work<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
) -> std::result::Result<acyclic_fs::OperationId, acyclic_fs::NativeTreeMaterializationError> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: (b"harness/native-preparation/v1\0".len() + 16) as u64,
        ..acyclic_fs::WorkCounters::default()
    })?;
    Ok(native_preparation_operation(owner))
}

fn materializer_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn charge_native_read(work: &NativeMaterializationWork) -> Result<()> {
    work.charge(acyclic_fs::WorkCounters {
        backend_read_operations: 1, ..acyclic_fs::WorkCounters::default()
    }).map_err(materializer_error)
}



async fn checkpoint_preparation<P: StreamProvider>(
    store: &NativePreparationStore<'_, P>, mut journal: MaterializationJournal,
    _custody: acyclic_fs::NativeAllocation,
) -> Result<()> {
    let expected = journal.revision;
    journal.revision = expected.checked_add(1)
        .ok_or_else(|| Error::Storage("native preparation revision overflow".into()))?;
    let operation = journal.plan.operation_id;
    let (committed, _) = store.compare_and_swap(operation, expected, journal).await?;
    if !committed {
        return Err(Error::Conflict("native preparation changed before attachment checkpoint".into()));
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

#[allow(clippy::too_many_arguments, reason = "same original grant checks share one cumulative volume allowance")]
fn verify_binding<P: StreamProvider, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    owner: &TaskJournalOwner<P>, binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    work: &NativeMaterializationWork, volume_work: &mut acyclic_fs::WorkCounters,
    budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
) -> Result<()> {
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        if operation == VolumeOperation::Write && !binding.writable { continue; }
        let task = owner.require_volume_grant_measured(
            binding.volume, operation, native_allowance(*volume_work, budget, work)?, cancellation,
        );
        account_preparation(task, work, volume_work, |error| error)?;
        let original = ContentGrant::verify_original_volume_measured(
            binding.verifier, binding.scope, binding.volume, operation,
            native_allowance(*volume_work, budget, work)?, cancellation,
        );
        account_preparation(original, work, volume_work, |error| error)?;
    }
    binding.host.validate_provider(binding.volume.provider())?;
    binding.host.validate_provider(binding.generation.as_resource().provider())
}

fn account_preparation<T, E>(
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, E>,
    work: &NativeMaterializationWork,
    volume: &mut acyclic_fs::WorkCounters,
    map: impl FnOnce(E) -> Error,
) -> Result<T> {
    match result {
        Ok(receipt) => {
            work.spent(receipt.work).map_err(materializer_error)?;
            *volume = volume.checked_add(receipt.work).map_err(materializer_error)?;
            work.remaining().map_err(materializer_error)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            work.spent(*failure.work).map_err(materializer_error)?;
            Err(map(failure.error))
        }
    }
}

fn require_allocation(journal: &MaterializationJournal, allocation: &NativePreparationRoot) -> Result<()> {
    if journal.root_identity != Some(allocation.expected_identity.to_bytes())
        || journal.preparation_identity.as_ref() != Some(&allocation.expected_preparation_identity)
    {
        return Err(Error::Conflict("native preparation allocation differs from original journal".into()));
    }
    Ok(())
}

async fn native_backend(
    allocation: &NativePreparationRoot, work: &NativeMaterializationWork, require_original: bool,
) -> Result<acyclic_fs::NativeTreeMaterializationBackend> {
    use acyclic_fs::MaterializationBackend as _;
    let (root, mut custody) = work.copy_path(&allocation.root).map_err(materializer_error)?;
    let (operation_directory, operation_custody) = work.copy_path(&allocation.operation_directory).map_err(materializer_error)?;
    custody.absorb(operation_custody).map_err(materializer_error)?;
    let meter = work.clone();
    let backend = acyclic_native_runtime::run_blocking_io(move || {
        acyclic_fs::NativeTreeMaterializationBackend::open_owned_with_work(root, operation_directory, custody, meter)
    }).await.map_err(materializer_error)?.map_err(materializer_error)?;
    if require_original && (backend.allocation_identity() != Some(allocation.expected_identity.to_bytes())
        || backend.preparation_identity().as_ref() != Some(&allocation.expected_preparation_identity))
    {
        return Err(Error::Conflict("native allocation was physically replaced".into()));
    }
    Ok(backend)
}

fn preparation_intent(
    journal: &MaterializationJournal, work: &NativeMaterializationWork,
) -> Result<(NativePreparationIntent, acyclic_fs::NativeAllocation)> {
    preparation_intent_typed(journal, work).map_err(NativePreparationJournalError::into_owner_error)
}

fn preparation_intent_typed(
    journal: &MaterializationJournal, work: &NativeMaterializationWork,
) -> std::result::Result<(NativePreparationIntent, acyclic_fs::NativeAllocation), NativePreparationJournalError> {
    let acyclic_fs::MaterializationIntent::NativePreparation { selection, original } = &journal.plan.intent else {
        return Err(NativePreparationJournalError::Record("native preparation original qualified intent is missing"));
    };
    let (intent, custody): (NativePreparationIntent, _) = native_decoded_record_typed(
        original, NativeRecordSchema::PreparationIntent, work,
        |intent| native_intent_control_bytes(intent).map_err(NativePreparationJournalError::Owner),
    )?;
    intent.manifest.validate()?;
    if selection.len() != intent.manifest.volumes.len()
        || journal.plan.edits.iter().any(|edit| !matches!(edit,
            acyclic_fs::MaterializationEdit::Install { image, .. }
                if image.as_slice() == acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX
        ))
    {
        return Err(NativePreparationJournalError::Record("native preparation original selections or edit intents disagree"));
    }
    for (selected, mapping) in selection.iter().zip(&intent.manifest.volumes) {
        let (identity, _identity_custody) = native_canonical(mapping.volume.provider(), work)?;
        if identity != selected.provider_identity
            || selected.generation.digest().as_bytes().as_slice() != mapping.generation.as_resource().key()
            || !matches!(mapping.namespace, NativeVolumeNamespace::MaterializedDirectory)
        {
            return Err(NativePreparationJournalError::Record("native preparation original qualified volume differs"));
        }
    }
    Ok((intent, custody))
}

fn preparation_receipt(
    intent: NativePreparationIntent, inspection: acyclic_fs::MaterializationInspection,
) -> NativePreparationReceipt {
    NativePreparationReceipt {
        task: intent.task, original_fence: intent.original_fence, manifest: intent.manifest,
        journal: inspection.journal, root: inspection.root, preparation: inspection.preparation,
        observations: inspection.observations,
    }
}

async fn inspect_native_preparation<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, allocation: &NativePreparationRoot,
    budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken, cleanup: bool,
) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, Error> {
    if !budget.has_finite_allowances() {
        return Err(acyclic_fs::OperationFailure::before_work(Error::Invalid("native query requires finite allowances".into())));
    }
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), materializer_error))?;
    let result = async {
        owner.verify(true).await?;
        let store = NativePreparationStore::new(owner, work.clone(), true);
        let operation = native_preparation_operation(owner);
        let Some(journal) = store.load(operation).await? else { return Ok(None); };
        let journal_custody = work.retain_journal(&journal).map_err(materializer_error)?;
        let (intent, _intent_custody) = preparation_intent(&journal, &work)?;
        if intent.task != owner.task_binding().0 || intent.manifest.options.root != allocation.root
            || intent.operation_directory != allocation.operation_directory
        {
            return Err(Error::Conflict("native query differs from original preparation".into()));
        }
        require_allocation(&journal, allocation)?;
        let backend = match native_backend(allocation, &work, cleanup).await {
            Ok(backend) => backend,
            Err(error) if !cleanup => {
                let (root_message, _root_message_custody) = work.display(&error).map_err(materializer_error)?;
                let (preparation_message, _preparation_message_custody) = native_string(&root_message, &work)?;
                return Ok(Some(NativePreparationReceipt {
                    task: intent.task, original_fence: intent.original_fence, manifest: intent.manifest,
                    journal, root: acyclic_fs::MaterializationRootObservation::Unavailable(root_message),
                    preparation: acyclic_fs::MaterializationPreparationObservation::Unavailable(preparation_message),
                    observations: Vec::new(),
                }));
            }
            Err(error) => return Err(error),
        };
        drop((journal, journal_custody));
        let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
        if cleanup {
            let (rolled_back, rollback_custody) = materializer.recover_owned(operation, acyclic_fs::MaterializationRecovery::RollBack)
                .await.map_err(materializer_error)?
                .ok_or_else(|| Error::Conflict("native preparation journal disappeared".into()))?;
            materializer.backend().reset_target_directory("").await.map_err(materializer_error)?;
            checkpoint_preparation(materializer.store(), rolled_back, rollback_custody).await?;
        }
        let (inspection, _inspection_custody) = materializer.inspect_owned(operation).await.map_err(materializer_error)?
            .ok_or_else(|| Error::Conflict("native preparation journal disappeared".into()))?;
        Ok(Some(preparation_receipt(intent, inspection)))
    }.await;
    result.map(|value| acyclic_fs::OperationReceipt { value, work: work.receipt() })
        .map_err(|error| acyclic_fs::OperationFailure::new(error, work.receipt()))
}

/// Fenced Stream adapter for the existing materializer journal, not a second log.
struct NativePreparationStore<'a, P> {
    owner: &'a TaskJournalOwner<P>,
    work: NativeMaterializationWork,
    loaded_original: AtomicBool,
    settlement: bool,
}

impl<'a, P: StreamProvider> NativePreparationStore<'a, P> {
    fn new(owner: &'a TaskJournalOwner<P>, work: NativeMaterializationWork, settlement: bool) -> Self {
        Self { owner, work, loaded_original: AtomicBool::new(false), settlement }
    }

    fn path(&self, operation: acyclic_fs::OperationId) -> Result<(acyclic_stream::StreamPath, acyclic_fs::NativeAllocation)> {
        self.path_typed(operation).map_err(NativePreparationJournalError::into_owner_error)
    }

    fn path_typed(
        &self, operation: acyclic_fs::OperationId,
    ) -> std::result::Result<(acyclic_stream::StreamPath, acyclic_fs::NativeAllocation), NativePreparationJournalError> {
        if operation != native_preparation_operation_with_work(self.owner, &self.work)? {
            return Err(NativePreparationJournalError::Record("native materializer operation differs from original task"));
        }
        let (path, custody) = self.work.display(&format_args!(
            "harness/v2/materialization/{}/{operation}", self.owner.task_binding().0,
        ))?;
        Ok((acyclic_stream::StreamPath::new(path)?, custody))
    }

    async fn load_record(
        &self, operation: acyclic_fs::OperationId, adopt_original_work: bool,
    ) -> std::result::Result<Option<(MaterializationJournal, acyclic_fs::NativeAllocation)>, NativePreparationJournalError> {
        use futures::TryStreamExt as _;
        self.work.charge(acyclic_fs::WorkCounters {
            backend_read_operations: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let (path, _path_custody) = self.path_typed(operation)?;
        let stream = self.owner.stream().stream(path.as_str())?;
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
        // The same finite protocol admission applies before the actual fetch.
        acyclic_fs::WorkCounters::default().admit(&acyclic_fs::WorkCounters {
            authority_bytes_read: acyclic_stream::MAX_RECORD_BYTES as u64,
            peak_allocation_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            ..acyclic_fs::WorkCounters::default()
        }, &self.work.remaining()?).map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        self.work.charge(acyclic_fs::WorkCounters {
            backend_read_operations: 1, authority_records_read: 1,
            ..acyclic_fs::WorkCounters::default()
        })?;
        let mut records = stream.read(sequence, 1).await?;
        let record = records.try_next().await?
            .ok_or(NativePreparationJournalError::Record("missing materializer journal"))?;
        self.work.spent(acyclic_fs::WorkCounters {
            authority_bytes_read: record.value.len() as u64,
            peak_allocation_bytes: record.value.len() as u64,
            ..acyclic_fs::WorkCounters::default()
        })?;
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(NativePreparationJournalError::Record("materializer journal exceeds Stream record limit"));
        }
        let _record_custody = self.work.retain_owned(record.value.len() as u64)?;
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

    async fn load(&self, operation: acyclic_fs::OperationId) -> Result<Option<MaterializationJournal>> {
        self.load_record(operation, true).await
            .map(|record| record.map(|(journal, _custody)| journal))
            .map_err(NativePreparationJournalError::into_owner_error)
    }

    async fn compare_and_swap(
        &self, operation: acyclic_fs::OperationId, expected_revision: u64,
        mut replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal)> {
        if replacement.plan.operation_id != operation
            || replacement.revision != expected_revision.checked_add(1).ok_or_else(|| Error::Storage("materializer revision overflow".into()))?
        {
            return Err(Error::Storage("invalid native materializer replacement".into()));
        }
        // This is the observed prefix before serialization/commit. The exact
        // later byte work is retained in this invocation's receipt, then the
        // next same-journal checkpoint; never invent a lost acknowledgement.
        replacement.work = self.work.journal_work().map_err(materializer_error)?;
        let (encoded, _encoded_custody) = native_canonical(&replacement, &self.work)?;
        let bytes = bytes::Bytes::from(encoded);
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Storage("materializer journal exceeds Stream record limit".into()));
        }
        self.work.remaining().map_err(materializer_error)?;
        let (key_bytes, _key_custody) = native_bytes(blake3::hash(&bytes).as_bytes(), &self.work)?;
        let key = acyclic_stream::IdempotencyKey::new(bytes::Bytes::from(key_bytes))?;
        let (path, _path_custody) = self.path(operation)?;
        self.work.charge(acyclic_fs::WorkCounters {
            backend_write_operations: 1, authority_records_appended: 1,
            authority_bytes_written: bytes.len() as u64,
            ..acyclic_fs::WorkCounters::default()
        }).map_err(materializer_error)?;
        let committed = self.owner.append(
            path, expected_revision, &key, bytes,
            if self.settlement { crate::distributed::JournalWrite::Settlement }
            else { crate::distributed::JournalWrite::Fresh },
        ).await?;
        Ok((committed, replacement))
    }
}


fn native_intent_control_bytes(intent: &NativePreparationIntent) -> Result<u64> {
    let mut bytes = 0_usize;
    macro_rules! retain {
        ($bytes:expr) => {
            bytes = bytes.checked_add($bytes)
                .ok_or_else(|| Error::Storage("native intent payload overflow".into()))?;
        };
    }
    retain!(intent.operation_directory.capacity());
    retain!(intent.original_fence.reservation_id.capacity());
    retain!(intent.original_fence.placement.capacity());
    retain!(intent.manifest.options.root.capacity());
    retain!(intent.manifest.volumes.capacity().checked_mul(std::mem::size_of::<NativeVolumeMapping>())
        .ok_or_else(|| Error::Storage("native intent mapping capacity overflow".into()))?);
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
    let value = crate::contract::json_from_slice_bounded_trusted(bytes, bounds, 0, &mut |charge| {
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
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes: scratch } => {
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
                    custody.absorb(work.retain_owned(owned).map_err(native_root_error)?).map_err(native_root_error)?;
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
    let bytes = crate::contract::canonical_json_bytes_bounded(value, bounds, &mut |charge| {
        native_codec_charge_typed(charge, work, &mut custody).map_err(native_root_error)
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
        JsonCodecCharge::Copy { bytes } => work.charge(acyclic_fs::WorkCounters {
            bytes_copied: bytes, ..acyclic_fs::WorkCounters::default()
        }),
        JsonCodecCharge::Read { .. } => work.remaining().map(|_| ()),
        JsonCodecCharge::Retain { bytes } => {
            if bytes != 0 {
                custody.absorb(work.claim(bytes)?)?;
            }
            Ok(())
        }
        JsonCodecCharge::Release { bytes } => custody.release(bytes),
        JsonCodecCharge::Admission { maximum_owned_bytes } => {
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
        .ok_or_else(|| Error::Storage("native vector capacity overflow".into()))?;
    let mut custody = work.claim(requested as u64).map_err(materializer_error)?;
    let mut value = Vec::new();
    value.try_reserve_exact(capacity).map_err(materializer_error)?;
    let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Storage("native vector capacity overflow".into()))?;
    if actual > requested {
        let extra = actual - requested;
        match work.retain_owned(extra as u64) {
            Ok(extra) => custody.absorb(extra).map_err(materializer_error)?,
            Err(error) => {
                work.spent(acyclic_fs::WorkCounters {
                    peak_allocation_bytes: extra as u64, ..acyclic_fs::WorkCounters::default()
                }).map_err(materializer_error)?;
                return Err(materializer_error(error));
            }
        }
    }
    Ok((value, custody))
}

fn native_bytes(value: &[u8], work: &NativeMaterializationWork) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let (mut copied, custody) = native_vec(value.len(), work)?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_copied: value.len() as u64, ..acyclic_fs::WorkCounters::default()
    }).map_err(materializer_error)?;
    copied.extend_from_slice(value);
    Ok((copied, custody))
}

fn native_string(value: &str, work: &NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let (bytes, custody) = native_bytes(value.as_bytes(), work)?;
    // The source is a str, so this reuses the same allocation without decoding.
    let value = String::from_utf8(bytes).map_err(materializer_error)?;
    Ok((value, custody))
}

fn native_provider(
    provider: &crate::resources::ProviderRef, work: &NativeMaterializationWork,
) -> Result<(crate::resources::ProviderRef, acyclic_fs::NativeAllocation)> {
    let (namespace, mut custody) = native_string(provider.namespace(), work)?;
    let (family, family_custody) = native_string(provider.family(), work)?;
    custody.absorb(family_custody).map_err(materializer_error)?;
    let (version, version_custody) = native_string(provider.version(), work)?;
    custody.absorb(version_custody).map_err(materializer_error)?;
    Ok((crate::resources::ProviderRef::new(namespace, family, version)?, custody))
}

fn native_volume(
    volume: &VolumeRef, work: &NativeMaterializationWork,
) -> Result<(VolumeRef, acyclic_fs::NativeAllocation)> {
    use crate::conversation::VolumeOwner;
    let (provider, mut custody) = native_provider(volume.provider(), work)?;
    let (id, id_custody) = native_string(volume.id(), work)?;
    custody.absorb(id_custody).map_err(materializer_error)?;
    let owner = match volume.owner() {
        VolumeOwner::Agent(agent) => VolumeOwner::Agent(*agent),
        VolumeOwner::Project(owner) => {
            let (owner, owner_custody) = native_string(owner, work)?;
            custody.absorb(owner_custody).map_err(materializer_error)?;
            VolumeOwner::Project(owner)
        }
        VolumeOwner::Session(owner) => {
            let (owner, owner_custody) = native_string(owner, work)?;
            custody.absorb(owner_custody).map_err(materializer_error)?;
            VolumeOwner::Session(owner)
        }
    };
    Ok((VolumeRef::new(provider, id, volume.class(), owner)?, custody))
}

fn native_generation(
    generation: &GenerationRef, work: &NativeMaterializationWork,
) -> Result<(GenerationRef, acyclic_fs::NativeAllocation)> {
    let resource = generation.as_resource();
    let (provider, mut custody) = native_provider(resource.provider(), work)?;
    let (key, key_custody) = native_bytes(resource.key(), work)?;
    custody.absorb(key_custody).map_err(materializer_error)?;
    let version = resource.version().map(|version| {
        let (version, version_custody) = native_string(version, work)?;
        custody.absorb(version_custody).map_err(materializer_error)?;
        Ok::<_, Error>(version)
    }).transpose()?;
    Ok((GenerationRef::new(provider, key, version)?, custody))
}

fn native_manifest(
    manifest: &NativeViewManifest, work: &NativeMaterializationWork,
) -> Result<(NativeViewManifest, acyclic_fs::NativeAllocation)> {
    let (root, mut custody) = work.copy_path(&manifest.options.root).map_err(materializer_error)?;
    let options = NativeViewOptions {
        root, maximum_volumes: manifest.options.maximum_volumes,
        work_per_volume: manifest.options.work_per_volume,
    };
    let (mut volumes, volumes_custody) = native_vec(manifest.volumes.len(), work)?;
    custody.absorb(volumes_custody).map_err(materializer_error)?;
    for mapping in &manifest.volumes {
        if !matches!(mapping.namespace, NativeVolumeNamespace::MaterializedDirectory) {
            return Err(Error::Invalid("a materialized preparation cannot replace an existing lazy mount".into()));
        }
        let (volume, volume_custody) = native_volume(&mapping.volume, work)?;
        custody.absorb(volume_custody).map_err(materializer_error)?;
        let (generation, generation_custody) = native_generation(&mapping.generation, work)?;
        custody.absorb(generation_custody).map_err(materializer_error)?;
        let (path, path_custody) = work.copy_path(&mapping.path).map_err(materializer_error)?;
        custody.absorb(path_custody).map_err(materializer_error)?;
        volumes.push(NativeVolumeMapping {
            volume, generation, path, writable: mapping.writable,
            namespace: NativeVolumeNamespace::MaterializedDirectory,
            authority_revision: mapping.authority_revision,
        });
    }
    Ok((NativeViewManifest { options, volumes }, custody))
}

struct NativeOwnedFuture<F> {
    future: F,
    _allocation: acyclic_fs::NativeAllocation,
}

impl<F: Future> Future for NativeOwnedFuture<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: pinning this wrapper pins `future`; it is never moved out,
        // and the allocation guard is released only after the future drops.
        unsafe { self.map_unchecked_mut(|owned| &mut owned.future) }.poll(context)
    }
}

#[inline(never)]
fn native_future<'a, T, F, B>(
    build: B, work: &NativeMaterializationWork,
) -> acyclic_fs::MeasuredResult<BoxProviderFuture<'a, T>, Error>
where
    F: Future<Output = T> + Send + 'a,
    B: FnOnce() -> F,
{
    native_future_typed(build, work)
        .map_err(|error| acyclic_fs::OperationFailure::new(materializer_error(error), work.receipt()))
}

#[inline(never)]
pub(super) fn native_future_typed<'a, T, F, B>(
    build: B, work: &NativeMaterializationWork,
) -> std::result::Result<BoxProviderFuture<'a, T>, acyclic_fs::NativeTreeMaterializationError>
where
    F: Future<Output = T> + Send + 'a,
    B: FnOnce() -> F,
{
    // Admission precedes constructing the potentially deep preparation frame.
    // Keep the claim live across every nested child until that frame drops.
    let allocation = work.claim(std::mem::size_of::<NativeOwnedFuture<F>>() as u64)?;
    Ok(Box::pin(NativeOwnedFuture { future: build(), _allocation: allocation }))
}

fn native_result<T>(
    result: Result<T>, work: &NativeMaterializationWork,
) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error> {
    result.map(|value| acyclic_fs::OperationReceipt { value, work: work.receipt() })
        .map_err(|error| acyclic_fs::OperationFailure::new(error, work.receipt()))
}

fn publication_admission_failure(failure: acyclic_fs::OperationFailure<Error>) -> NativePublicationFailure {
    NativePublicationFailure {
        destination: None, completed: Vec::new(), work: *failure.work, message: failure.error.to_string(),
    }
}
