//! Optional native command composition over existing task, effect and workspace owners.

use super::{
    FilesystemHost, NativeVolumeNamespace, NativeViewManifest, NativeViewOptions,
    NativeVolumeMapping,
};
use crate::{
    Error, Result,
    conversation::{Attachment, ContentGrant, ContentPublisher, ContentResidencyVerifier, FileRef,
        VolumeOperation, VolumeRef},
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
    sync::Arc,
};

/// Authenticated, unmounted original ROOT result storage.
pub struct NativeRootOutputSource {
    binding: crate::runtime::ContentBindings,
    owner: Arc<dyn NativeRootOutputOwner>,
    bind_operation: crate::OperationId,
    bind_intent: [u8; 32],
    creation_operation: acyclic_fs::OperationId,
    workspace: acyclic_fs::WorkspaceId,
    generation: acyclic_fs::GenerationId,
    custody: acyclic_fs::NativeAllocation,
}

trait NativeRootOutputOwner: std::any::Any + Send + Sync {
    fn same_owner(&self, other: &dyn NativeRootOutputOwner) -> bool;
    fn authorize<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn verify<'a>(
        &'a self, context: &'a crate::runtime::TaskContext, volume: &'a VolumeRef,
        bind_operation: crate::OperationId, bind_intent: [u8; 32],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}

impl<P: StreamProvider + 'static> NativeRootOutputOwner for TaskJournalOwner<P> {
    fn same_owner(&self, other: &dyn NativeRootOutputOwner) -> bool {
        (other as &dyn std::any::Any).downcast_ref::<Self>()
            .is_some_and(|other| self.same_original_root_output_owner(other))
    }

    fn authorize<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.require_volume_grant_with_native_work(source.original_writer().volume(), operation, work)?;
            let proof = self.authenticated_root_binding_with_native_work(work).await?;
            if proof.bind_operation() != source.bind_operation || proof.intent_digest() != source.bind_intent {
                return Err(native_root_output_denial("ROOT content differs from its original protected binding"));
            }
            Ok(())
        })
    }

    fn verify<'a>(
        &'a self, context: &'a crate::runtime::TaskContext, volume: &'a VolumeRef,
        bind_operation: crate::OperationId, bind_intent: [u8; 32],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_task_context_binding_with_native_work(context, work)?;
        self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let proof = self.authenticated_root_binding_with_native_work(work).await?;
            if proof.bind_operation() != bind_operation || proof.intent_digest() != bind_intent {
                return Err(native_root_output_denial("original ROOT output binding changed"));
            }
            Ok(())
        })
    }
}

impl NativeRootOutputSource {
    /// Provider aliases retain this whole source, including its actual owner and metadata custody.
    pub(crate) fn retained_content_binding(self: &Arc<Self>) -> crate::runtime::ContentBindings {
        crate::runtime::ContentBindings {
            reader: Arc::clone(self) as Arc<dyn crate::conversation::ContentResidencyVerifier>,
            writer: Some(Arc::clone(self) as Arc<dyn crate::conversation::ContentPublisher>),
        }
    }

    fn original_writer(&self) -> &dyn crate::conversation::ContentPublisher {
        self.binding.writer.as_deref().expect("private ROOT output constructor always retains its writer")
    }

    fn require_content_work(&self, volume: &VolumeRef, work: &NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) || volume != self.original_writer().volume() {
            return Err(native_root_output_denial("ROOT content differs from its original volume or native work"));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(Error::from)
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.owner.same_owner(other.owner.as_ref())
            && self.bind_operation == other.bind_operation
            && self.bind_intent == other.bind_intent
            && self.creation_operation == other.creation_operation
            && self.workspace == other.workspace
            && self.generation == other.generation
            && self.binding.writer.as_ref().map(|writer| writer.volume())
                == other.binding.writer.as_ref().map(|writer| writer.volume())
    }

    pub(crate) async fn verify_context_with_native_work(
        &self, context: &crate::runtime::TaskContext, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.custody.matches_work(work) {
            return Err(native_root_output_denial("ROOT output differs from its original native work"));
        }
        let writer = self.binding.writer.as_ref()
            .ok_or_else(|| native_root_output_denial("ROOT output lost its original writer"))?;
        self.owner.verify(context, writer.volume(), self.bind_operation, self.bind_intent, work)?.await
    }

    pub(crate) fn into_native_shared(self, work: &NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.custody.matches_work(work)
            || self.custody.bytes() < std::mem::size_of::<Self>() as u64
        {
            return Err(native_root_output_denial("ROOT output sharing requires its original metadata custody"));
        }
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        Ok(Arc::new(self))
    }
}

impl ContentResidencyVerifier for NativeRootOutputSource {
    fn verify<'a>(&'a self, _reference: &'a FileRef) -> BoxProviderFuture<'a, Result<()>> {
        native_root_output_ordinary_denial()
    }

    fn supports_native_read(&self, volume: &VolumeRef, work: &NativeMaterializationWork) -> Result<bool> {
        self.require_content_work(volume, work)?;
        self.binding.reader.supports_native_read(volume, work)
    }

    fn read_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        self.require_content_work(reference.volume(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.owner.authorize(self, VolumeOperation::Read, work)?.await?;
            self.binding.reader.read_with_native_work(reference, maximum, work)?.await
        })
    }

    fn verify_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_content_work(reference.volume(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.owner.authorize(self, VolumeOperation::Read, work)?.await?;
            self.binding.reader.verify_with_native_work(reference, maximum, work)?.await
        })
    }

    fn load_manifest_with_native_work<'a>(
        &'a self, reference: &'a FileRef, count: u32, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(reference.volume(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.owner.authorize(self, VolumeOperation::Read, work)?.await?;
            self.binding.reader.load_manifest_with_native_work(reference, count, work)?.await
        })
    }
}

impl ContentPublisher for NativeRootOutputSource {
    fn volume(&self) -> &VolumeRef { self.original_writer().volume() }

    fn stage<'a>(
        &'a self, _operation: crate::OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        native_root_output_ordinary_denial()
    }

    fn supports_native_stage(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_content_work(self.volume(), work)?;
        self.original_writer().supports_native_stage(work)
    }

    fn stage_once_with_native_work<'a>(
        &'a self, operation: crate::OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(self.volume(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.owner.authorize(self, VolumeOperation::Write, work)?.await?;
            self.original_writer().stage_once_with_native_work(operation, path, bytes, media_type, display_name, work)?.await
        })
    }

    fn stage_at_with_native_work<'a>(
        &'a self, operation: crate::OperationId, reference: &'a FileRef, bytes: &'a [u8],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(reference.volume(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.owner.authorize(self, VolumeOperation::Write, work)?.await?;
            self.original_writer().stage_at_with_native_work(operation, reference, bytes, work)?.await
        })
    }

    fn observe_staged_once_with_native_work<'a>(
        &'a self, operation: crate::OperationId, maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(self.volume(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.owner.authorize(self, VolumeOperation::Write, work)?.await?;
            self.original_writer().observe_staged_once_with_native_work(operation, maximum, work)?.await
        })
    }
}

fn native_root_output_ordinary_denial<'a, T: Send + 'a>() -> BoxProviderFuture<'a, Result<T>> {
    Box::pin(std::future::ready(Err(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unsupported,
        reason: "original ROOT output requires its authenticated native-work provider boundary",
    })))
}

fn native_root_output_denial(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason }
}

/// Canonical native ROOT bind identity; this derives an operation, not authority.
pub fn native_root_output_binding_operation(
    audience: &crate::core::Authority, volume: &VolumeRef,
    creation: acyclic_fs::OperationId, work: &NativeMaterializationWork,
) -> Result<crate::OperationId> {
    let (encoded, _custody) = encode_record(
        &("harness/native-root-output-bind/v2", audience, volume,
            crate::OperationId::from_bytes(creation.into_bytes())), work,
    )?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&blake3::hash(&encoded).as_bytes()[..16]);
    Ok(crate::OperationId::from_bytes(bytes))
}

/// Original process custody and the exact physically retired mount prefix.
pub struct NativeTaskViewDrainReceipt {
    process: super::native_process::NativeProcessDrainProof,
    mounts: Vec<acyclic_fs::native_mount::NativeMountDrainProof>,
    custody: acyclic_fs::NativeAllocation,
}

impl NativeTaskViewDrainReceipt {
    /// Borrows the original process proof without discarding mounted custody.
    pub fn process_drain_proof(&self) -> &super::native_process::NativeProcessDrainProof {
        &self.process
    }
}

trait NativeTaskWorkingViewSeal {}

/// An SDK-issued working view bound to its original owner and ledger.
pub trait NativeTaskWorkingView: NativeTaskWorkingViewSeal + std::any::Any + Send + Sync {
    /// Compares the exact held view and owner.
    fn same_identity(&self, other: &dyn NativeTaskWorkingView) -> bool;
    /// Selects an authorized mounted volume.
    fn volume_for_builtin_path<'a>(&'a self, path: &'a Path, operation: VolumeOperation,
        work: &NativeMaterializationWork) -> Result<&'a VolumeRef>;
    /// Returns the original task and creation fence.
    fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence);
    /// Returns its immutable namespace declaration.
    fn manifest(&self) -> &NativeViewManifest;
    /// Returns its original ledger.
    fn original_native_work(&self) -> &NativeMaterializationWork;
    /// Authenticates the view on that ledger.
    fn verify_with_native_work<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Reads verified physical bytes.
    fn read_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path, maximum: usize,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    /// Writes an authorized mounted path.
    fn write_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path, content: &'a str,
        parents: bool, expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>>;
}

struct HeldNativeTaskWorkingView<P, A, O> {
    view: Arc<NativeVolumeView<A, O>>,
    owner: Arc<TaskJournalOwner<P>>,
    custody: acyclic_fs::NativeAllocation,
}

impl<P, A, O> HeldNativeTaskWorkingView<P, A, O>
where P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn require_original(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) || !self.view._allocation.matches_work(work)
            || !self.owner.native_shared_work_matches(work)
            || self.owner.task_binding() != (self.view.preparation.task, &self.view.preparation.original_fence)
        {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(Error::from)
    }
    async fn authenticate(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_original(work)?;
        self.view.authenticate_builtin_view(&self.owner, work).await?;
        self.view.validate_dispatch_with_work(work).await?;
        self.owner.verify_with_native_work(false, work).await
    }
}

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + Send + Sync + 'static, O: AsyncObjectStore + Send + Sync + 'static>
    NativeTaskWorkingViewSeal for HeldNativeTaskWorkingView<P, A, O> {}

impl<P, A, O> NativeTaskWorkingView for HeldNativeTaskWorkingView<P, A, O>
where P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn same_identity(&self, other: &dyn NativeTaskWorkingView) -> bool {
        (other as &dyn std::any::Any).downcast_ref::<Self>().is_some_and(|other|
            Arc::ptr_eq(&self.view, &other.view) && Arc::ptr_eq(&self.owner, &other.owner))
    }
    fn volume_for_builtin_path<'a>(&'a self, path: &'a Path, operation: VolumeOperation,
        work: &NativeMaterializationWork) -> Result<&'a VolumeRef> {
        self.require_original(work)?;
        self.view.builtin_volume(path, operation, work).map(|(mapping, _, _)| &mapping.volume)
    }
    fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence) {
        (self.view.preparation.task, &self.view.preparation.original_fence)
    }
    fn manifest(&self) -> &NativeViewManifest { &self.view.preparation.manifest }
    fn original_native_work(&self) -> &NativeMaterializationWork {
        self.view.work()
    }
    fn verify_with_native_work<'a>(&'a self, work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || self.authenticate(work))
    }
    fn read_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path, maximum: usize,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.view.read_builtin_file(&self.owner, operation, path, maximum, work).await
        })
    }
    fn write_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path, content: &'a str,
        parents: bool, expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.view.write_builtin_file(&self.owner, operation, path, content, parents, expected, work).await
        })
    }
}

impl<A, O> NativeVolumeView<A, O>
where A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Issues only an original funded, protected Applied preparation.
    pub fn task_working_view_with_native_work<'a, P: StreamProvider + 'static>(
        self: &'a Arc<Self>, owner: &'a Arc<TaskJournalOwner<P>>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Arc<dyn NativeTaskWorkingView>>> {
        crate::NativeFutureBundle::new(work, || async move {
            let custody = work.retain_owned(std::mem::size_of::<HeldNativeTaskWorkingView<P, A, O>>() as u64)?;
            let held = HeldNativeTaskWorkingView { view: Arc::clone(self), owner: Arc::clone(owner), custody };
            held.authenticate(work).await?;
            work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            let resource: Arc<dyn NativeTaskWorkingView> = Arc::new(held);
            owner.verify_with_native_work(false, work).await?;
            Ok(resource)
        })
    }
}

/// Existing native namespace owner; this seam creates no view or authority.
pub trait NativeProcessView<P: StreamProvider>: Send + Sync {
    /// Immutable original request/approval selection.
    fn manifest(&self) -> &NativeViewManifest;
    /// The original preparation/lifetime token, never a replacement request token.
    fn operation_cancellation(&self) -> &acyclic_fs::CancellationToken;
    /// A genuine prepared caller family; legacy preparation carries no such proof.
    fn original_native_work(&self) -> Option<&NativeMaterializationWork>;
    /// Uses the caller's actual original ledger and the held view's cancellation.
    fn validate_for_dispatch<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Checks the same held root without constructing a replacement work span.
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Retains genuine mounted-namespace scopes through process quiescence.
    fn retain_native_namespace_scopes<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessNamespaceScopes>>;
    /// Retains actual completed destinations and original typed publication cause.
    fn publish<'a>(
        &'a self, owner: &'a TaskJournalOwner<P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>>;
}

impl<P: StreamProvider> TaskJournalOwner<P> {
    #[allow(clippy::too_many_arguments, reason = "one engine validates both original acquisition decisions")]
    /// Reserves or observes the original qualified publication gate.
    pub async fn publication<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        &self, workspace: &acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken, recover: bool,
    ) -> Result<Option<NativePublicationReservation<A, O>>> {
        let mut failure_slot = Some(NativeReservationFailureSlot::new(work)?);
        let workspace = workspace.clone();
        let mut reservation = None;
        let mut cleanup_binding = None;
        let result = match crate::NativeFutureBundle::new(work, || async {
            self.verify_with_native_work(false, work).await?;
            self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
            cleanup_binding = Some(NativePublicationCleanupBinding {
                task: self.task_binding().0,
                fence: native_publication_fence_digest(self, work)?,
                volume: allocate(work, |io| volume.clone_with_io(io))?,
            });
            {
                let (encoded, _encoded_custody) = encode_record(volume, work)?;
                work.charge(acyclic_fs::WorkCounters {
                    bytes_hashed: encoded.len() as u64, ..acyclic_fs::WorkCounters::default()
                })?;
                let (name, _name_custody) = work.display(&format_args!(
                    "harness-{}", blake3::hash(&encoded).to_hex(),
                ))?;
                if workspace.name().as_str() != name {
                    return Err(reservation_selection("native reservation workspace differs from original volume"));
                }
            }
            cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            let acquired = if recover {
                workspace.publication_reservation_with_native_work(original_operation, work, cancellation).await
            } else {
                workspace.reserve_publication_with_native_work(original_operation, work, cancellation).await.map(Some)
            };
            reservation = acquired.map_err(|(cause, custody)| Error::NativeWorkspace { source: cause, custody })?;
            self.verify_with_native_work(false, work).await?;
            self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
            if reservation.is_none() {
                return Ok(());
            }
            let head = workspace.head_with_native_work(work, cancellation)
                .await.map_err(|(cause, custody)| Error::NativeWorkspace { source: cause, custody })?;
            if head.id() != original_generation {
                return Err(reservation_selection("native reservation generation differs from original selection"));
            }
            drop(head);
            self.verify_with_native_work(false, work).await?;
            self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
            cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            work.remaining()?;
            Ok::<(), Error>(())
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        let error = match result {
            Ok(()) => return Ok(reservation.map(|reservation| NativePublicationReservation {
                cleanup: NativePublicationCleanup { workspace, reservation,
                    binding: cleanup_binding.expect("reservation follows original binding"), failure_slot },
            })),
            Err(error) => error,
        };
        let failure = if let Some(reservation) = reservation {
            let cleanup = NativePublicationCleanup { workspace, reservation,
                binding: cleanup_binding.expect("reservation follows original binding"), failure_slot: None };
            if recover {
                NativePublicationReservationFailure { error, cleanup_error: None,
                    work: work.receipt(), cleanup: Some(cleanup), unwitnessed_workspace: None }
            } else {
                match cleanup.release_original(self, work, cleanup_cancellation).await {
                    Ok(()) => NativePublicationReservationFailure { error, cleanup_error: None,
                        work: work.receipt(), cleanup: None, unwitnessed_workspace: None },
                    Err(failure) => NativePublicationReservationFailure {
                        error, cleanup_error: Some(failure.error), work: failure.work,
                        cleanup: failure.cleanup, unwitnessed_workspace: failure.unwitnessed_workspace,
                    },
                }
            }
        } else {
            NativePublicationReservationFailure { error, cleanup_error: None,
                work: work.receipt(), cleanup: None, unwitnessed_workspace: Some(workspace) }
        };
        Err(failure_slot.take().expect("prepaid original reservation failure slot").retain(failure))
    }
}

/// An authenticated SDK reservation for an original native destination.
pub struct NativePublicationReservation<A, O> {
    cleanup: NativePublicationCleanup<A, O>,
}

/// Only settlement of an already acquired real SDK reservation is permitted.
pub struct NativePublicationCleanup<A, O> {
    workspace: acyclic_fs::Workspace<A, O>,
    reservation: acyclic_fs::PublicationReservation,
    binding: NativePublicationCleanupBinding,
    failure_slot: Option<NativeReservationFailureSlot<A, O>>,
}

struct NativePublicationCleanupBinding {
    task: crate::TaskId,
    fence: [u8; 32],
    volume: (VolumeRef, acyclic_fs::NativeAllocation),
}

fn native_publication_fence_digest<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
) -> Result<[u8; 32]> {
    let encoded = encode_record(owner.task_binding().1, work)?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.0.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    Ok(*blake3::hash(&encoded.0).as_bytes())
}

impl NativePublicationCleanupBinding {
    fn verify<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.task_binding().0 != self.task || native_publication_fence_digest(owner, work)? != self.fence {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native publication cleanup differs from the original task and fence" });
        }
        owner.require_volume_grant_with_native_work(&self.volume.0, VolumeOperation::Write, work)
    }
}

fn reservation_selection(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason }
}

/// Physical settlement uncertainty retains the exact cleanup capability.
pub struct NativePublicationReservationFailure<A, O> {
    pub error: Error,
    /// Additional actual release failure; `error` remains the factory's cause.
    pub cleanup_error: Option<Error>,
    pub work: acyclic_fs::WorkCounters,
    pub cleanup: Option<NativePublicationCleanup<A, O>>,
    /// Original SDK workspace retained without any witnessed gate proof.
    pub unwitnessed_workspace: Option<acyclic_fs::Workspace<A, O>>,
}

impl<A, O> std::fmt::Debug for NativePublicationReservationFailure<A, O> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativePublicationReservationFailure")
            .field("error", &self.error).field("cleanup_error", &self.cleanup_error)
            .field("work", &self.work).field("cleanup", &self.cleanup.is_some())
            .field("unwitnessed_workspace", &self.unwitnessed_workspace.is_some()).finish()
    }
}

impl<A, O> std::fmt::Display for NativePublicationReservationFailure<A, O> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, formatter)
    }
}

impl<A: 'static, O: 'static> std::error::Error for NativePublicationReservationFailure<A, O> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&self.error) }
}

struct NativeReservationFailureSlot<A, O> {
    value: Box<std::mem::MaybeUninit<NativePublicationReservationFailure<A, O>>>,
    custody: acyclic_fs::NativeAllocation,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> NativeReservationFailureSlot<A, O> {
    fn new(work: &NativeMaterializationWork) -> Result<Self> {
        let custody = work.claim_allocation(
            std::mem::size_of::<NativePublicationReservationFailure<A, O>>() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        Ok(Self { value: Box::new_uninit(), custody })
    }

    fn retain(self, failure: NativePublicationReservationFailure<A, O>) -> Error {
        let code = failure.error.code();
        Error::NativeReservation { code, source: Box::write(self.value, failure), custody: Some(self.custody) }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> NativePublicationReservationFailure<A, O> {
    /// Borrows the actual retained failure without extracting its owner or custody.
    pub fn from_error(error: &Error) -> Option<&Self> {
        let mut source: &(dyn std::error::Error + 'static) = error;
        loop {
            if let Some(failure) = source.downcast_ref::<Self>() { return Some(failure); }
            source = source.source()?;
        }
    }

    /// Retries only the retained actual gate while keeping the original Root error.
    pub async fn retry_cleanup_with_native_work<P: StreamProvider>(
        error: &mut Error, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<bool, Error> {
        let Error::NativeReservation { source, custody, .. } = error else { return Ok(false); };
        if !custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(reservation_selection(
                "native cleanup differs from the original operation and resource",
            ));
        }
        let Some(failure) = source.downcast_mut::<Self>() else { return Ok(false); };
        let Some(cleanup) = failure.cleanup.as_ref() else { return Ok(false); };
        let mut released = false;
        let result = match crate::NativeFutureBundle::new(work, || async {
            cleanup.binding.verify(owner, work)?;
            owner.verify_with_native_work(true, work).await?;
            cleanup.workspace.release_publication_with_native_work(cleanup.reservation, work, cancellation)
                .await.map_err(|(cause, custody)| Error::NativeWorkspace { source: cause, custody })?;
            released = true;
            owner.verify_with_native_work(true, work).await?;
            cleanup.binding.verify(owner, work)
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        if released { failure.cleanup = None; }
        result?;
        Ok(true)
    }
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

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> NativePublicationCleanup<A, O> {
    /// Releases the actual gate without admitting a new failure carrier.
    pub async fn release<P: StreamProvider>(
        mut self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        let slot = self.failure_slot.take().expect("successful reservation retains its prepaid failure slot");
        if !slot.custody.matches_work(work) {
            return Err(slot.retain(NativePublicationReservationFailure {
                error: reservation_selection("native cleanup differs from the original operation and resource"),
                cleanup_error: None, work: work.receipt(), cleanup: Some(self), unwitnessed_workspace: None,
            }));
        }
        self.release_original(owner, work, cancellation).await.map_err(|failure| slot.retain(failure))
    }

    async fn release_original<P: StreamProvider>(
        self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<(), NativePublicationReservationFailure<A, O>> {
        let mut released = false;
        let result = match crate::NativeFutureBundle::new(work, || async {
            self.binding.verify(owner, work)?;
            owner.verify_with_native_work(true, work).await?;
            self.workspace.release_publication_with_native_work(self.reservation, work, cancellation)
                .await.map_err(|(cause, custody)| Error::NativeWorkspace { source: cause, custody })?;
            released = true;
            owner.verify_with_native_work(true, work).await?;
            self.binding.verify(owner, work)
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(NativePublicationReservationFailure {
                error, cleanup_error: None,
                work: work.receipt(), cleanup: if released { None } else { Some(self) },
                unwitnessed_workspace: None,
            }),
        }
    }
}

/// A sealed reference to an existing native lazy mount, not permission to mount or append a namespace to an already prepared operation.
pub struct NativeExistingLazyVolume {
    held: Arc<dyn NativeHeldLazyVolume>,
    custody: acyclic_fs::NativeAllocation,
}

trait NativeHeldLazyVolume: Send + Sync {
    fn matches_owner(&self, owner: &dyn std::any::Any) -> bool;
    fn matches_binding(&self, volume: &VolumeRef, generation: &GenerationRef, writable: bool,
        publication: PublicationPermit, authority_revision: &[u8; 32]) -> bool;
    fn path(&self) -> &Path;
    fn identity(&self) -> acyclic_fs::NativeRootIdentity;
    fn original_work(&self) -> &NativeMaterializationWork;
    fn snapshot<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>>;
    fn publication_snapshot<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativePublicationSyncFailure>>>;
    fn begin_scope(&self, work: &NativeMaterializationWork)
        -> Result<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope>;
    fn read<'a>(&'a self, relative: &'a Path, maximum: usize, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    fn write<'a>(&'a self, relative: &'a Path, content: &'a str, parents: bool,
        expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn sync<'a>(&'a self, permit: PublicationPermit, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativePublicationSyncFailure>>>;
    fn retire(&self, wait: &acyclic_fs::CancellationToken)
        -> Result<acyclic_fs::native_mount::NativeMountDrainProof>;
}

struct NativePublicationSyncFailure {
    cause: Error,
    accounting_failure: Option<Error>,
    source_acknowledged: bool,
    publication: Option<acyclic_fs::NativePublicationSnapshot>,
}

struct HeldLazyVolume<P: StreamProvider, A, O, D, S> {
    mount: Arc<acyclic_fs::LazyMount<A, O, D, S>>,
    owner: Arc<TaskJournalOwner<P>>,
    callback: Arc<dyn crate::runtime::NativeVolumePublicationGuard>,
    identity: acyclic_fs::NativeRootIdentity,
    volume: VolumeRef,
    generation: GenerationRef,
    writable: bool,
    publication: PublicationPermit,
    authority_revision: [u8; 32],
    custody: acyclic_fs::NativeAllocation,
}

impl NativeExistingLazyVolume {
    /// Captures only the actual existing mount and its immutable declared snapshot.
    pub fn capture_with_native_work<'a, P, A, O, D, S>(
        mount: &'a Arc<acyclic_fs::LazyMount<A, O, D, S>>,
        owner: &'a Arc<TaskJournalOwner<P>>, binding: &'a BorrowedNativeVolumeBinding<'a, A, O>,
        guard: &'a crate::runtime::NativeVolumeCallbackGuard<P>,
        metadata_work: &'a NativeMaterializationWork, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>>
    where P: StreamProvider + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
    {
        crate::NativeFutureBundle::new(work, || async move {
            if !owner.native_shared_work_matches(metadata_work) || !work.shares_ledger(metadata_work) {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            owner.verify_with_native_work(false, metadata_work).await?;
            verify_binding(owner, binding, work)?;
            guard.require_volume_with_native_work(owner, binding.volume, binding.writable, work)?;
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            };
            let identity = mount.native_root_identity()
                .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            let workspace = binding.host.open_volume_with_native_work(binding.volume, work).await?;
            let generation = binding.host.generation_with_native_work(&workspace, binding.generation, work).await?;
            let actual = mount.snapshot_with_work(&work, work.cancellation()).await?;
            if &actual != snapshot || snapshot.workspace_id != workspace.id()
                || snapshot.authored_generation != generation.id()
            {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let callback = guard.for_volume(owner, binding.volume, binding.writable, work)?.publication;
            let custody = work.retain_owned(std::mem::size_of::<Self>() as u64)?;
            let mut holder_custody = work.retain_owned(std::mem::size_of::<HeldLazyVolume<P, A, O, D, S>>() as u64)?;
            let (volume, mut volume_custody) = allocate(work, |io| binding.volume.clone_with_io(io))?;
            holder_custody.absorb(&mut volume_custody)?;
            let (generation, mut generation_custody) = allocate(work, |io| binding.generation.clone_with_io(io))?;
            holder_custody.absorb(&mut generation_custody)?;
            let (canonical, _canonical_custody) = encode_record(&(binding.verifier.audience(), binding.scope), work)?;
            work.charge(acyclic_fs::WorkCounters { bytes_hashed: canonical.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            let authority_revision = *blake3::hash(&canonical).as_bytes();
            work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            let held = Arc::new(HeldLazyVolume { mount: Arc::clone(mount), owner: Arc::clone(owner),
                callback, identity, volume, generation, writable: binding.writable, publication: binding.publication,
                authority_revision, custody: holder_custody });
            owner.verify_with_native_work(false, metadata_work).await?;
            Ok(Self { held, custody })
        })
    }
}

impl<P, A, O, D, S> HeldLazyVolume<P, A, O, D, S>
where P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
{
    fn require_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(Error::from)
    }
    async fn check(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_work(work)?;
        self.callback.check(work)?.await
    }
}

impl<P, A, O, D, S> NativeHeldLazyVolume for HeldLazyVolume<P, A, O, D, S>
where P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: acyclic_fs::DemandSource + 'static, S: acyclic_fs::LazyWorkspaceStore + 'static,
{
    fn matches_owner(&self, owner: &dyn std::any::Any) -> bool {
        owner.downcast_ref::<TaskJournalOwner<P>>()
            .is_some_and(|owner| std::ptr::eq(owner, self.owner.as_ref()))
    }
    fn matches_binding(&self, volume: &VolumeRef, generation: &GenerationRef, writable: bool,
        publication: PublicationPermit, authority_revision: &[u8; 32]) -> bool {
        &self.volume == volume && &self.generation == generation && self.writable == writable
            && self.publication == publication && &self.authority_revision == authority_revision
    }
    fn path(&self) -> &Path { self.mount.path() }
    fn identity(&self) -> acyclic_fs::NativeRootIdentity { self.identity }
    fn original_work(&self) -> &NativeMaterializationWork { self.custody.work() }
    fn snapshot<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let snapshot = self.mount.snapshot_with_work(&work, work.cancellation())
                .await?;
            self.check(work).await?;
            Ok(snapshot)
        })
    }
    fn publication_snapshot<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativePublicationSyncFailure>>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let snapshot = match self.mount.publication_snapshot_with_work(
                &work, work.cancellation(),
            ).await {
                Ok(snapshot) => snapshot,
                Err((cause, accounting)) => return Ok(Err(NativePublicationSyncFailure {
                    cause: Error::from(cause), accounting_failure: accounting.map(Error::from),
                    source_acknowledged: false, publication: None,
                })),
            };
            if let Err(cause) = self.check(work).await {
                return Ok(Err(NativePublicationSyncFailure {
                    cause, accounting_failure: None, source_acknowledged: false,
                    publication: Some(snapshot),
                }));
            }
            Ok(Ok(snapshot))
        })
    }
    fn begin_scope(&self, work: &NativeMaterializationWork)
        -> Result<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope> {
        self.require_work(work)?;
        self.mount.begin_original_work(&work).map_err(Error::from)
    }
    fn read<'a>(&'a self, relative: &'a Path, maximum: usize, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        if !work.shares_ledger(self.custody.work()) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let lease = self.mount.native_source_io_lease()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
            let physical = work.with_source_io_lease(lease)?;
            self.check(&physical).await?;
            self.mount.with_original_work(&physical, |scope| {
                let scope = scope.try_clone_for_io();
                let physical = &physical;
                async move {
                    let scope = scope.map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
                    let result = physical.read_working_file(self.path(), self.identity, relative, maximum,
                        || self.check(physical)).await;
                    drop(scope);
                    result
                }
            }).await?
        })
    }
    fn write<'a>(&'a self, relative: &'a Path, content: &'a str, parents: bool,
        expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        if !work.shares_ledger(self.custody.work()) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let lease = self.mount.native_source_io_lease()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
            let physical = work.with_source_io_lease(lease)?;
            self.check(&physical).await?;
            self.mount.with_original_work(&physical, |scope| {
                let scope = scope.try_clone_for_io();
                let physical = &physical;
                async move {
                    let _scope = scope.map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
                    physical.write_working_file( self.path(), self.identity, relative,
                        content, parents, expected, || self.check(physical)).await
                }
            }).await?
        })
    }
    fn sync<'a>(&'a self, permit: PublicationPermit, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativePublicationSyncFailure>>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let callback = Arc::clone(&self.callback);
            let result = self.mount.sync_with_permit_work_guarded(
                permit, &work, work.cancellation(),
                move |actual: &NativeMaterializationWork| {
                    let callback = Arc::clone(&callback);
                    let actual = actual.clone();
                    async move { callback.check(&actual)?.await }
                },
            ).await;
            Ok(result.map_err(|failure| NativePublicationSyncFailure {
                cause: failure.cause, source_acknowledged: failure.source_acknowledged,
                accounting_failure: failure.accounting_failure.map(Error::from),
                publication: failure.publication,
            }))
        })
    }
    fn retire(&self, wait: &acyclic_fs::CancellationToken)
        -> Result<acyclic_fs::native_mount::NativeMountDrainProof> {
        self.mount.retire_with_work(wait).map_err(Error::from)
    }
}

/// Host-only selection. Credentials and publication permits never enter a command.
pub struct BorrowedNativeVolumeBinding<'a, A, O> {
    /// Exact provider capability used for this volume; a consumer router may compose heterogeneous stores under the existing provider traits.
    pub host: &'a FilesystemHost<A, O>,
    /// Owner-bound selected volume.
    pub volume: &'a VolumeRef,
    /// Exact immutable input generation.
    pub generation: &'a GenerationRef,
    /// Declared mechanism.
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
    /// Stable original task-derived preparation identity, including before an acknowledgement is available.
    #[must_use]
    pub fn operation<P: StreamProvider>(owner: &TaskJournalOwner<P>) -> acyclic_fs::OperationId {
        native_preparation_operation_for_task(owner.task_binding().0)
    }

    /// Observes host allocations using the caller's original native operation.
    pub fn witness<'a>(
        root: &'a std::path::Path,
        operation_directory: &'a std::path::Path,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Self, acyclic_fs::NativeAllocation)>> {
        use acyclic_fs::MaterializationBackend as _;
        if !work.remaining()?.has_finite_allowances() || !root.is_absolute() || !operation_directory.is_absolute() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation witness requires absolute paths and finite allowances" });
        }
        crate::NativeFutureBundle::new(work, || async {
        let (root, mut custody) = work.path(root, None)?;
        let (operation_directory, mut operation_custody) = work.path(operation_directory, None)?;
        custody.absorb(&mut operation_custody)?;
        let meter = work.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            let backend = acyclic_fs::NativeTreeMaterializationBackend::new(root, operation_directory, Some(custody), meter.clone(), false)?;
            backend.require_initial_empty()?;
            let expected_identity = acyclic_fs::NativeRootIdentity::from_bytes(
                backend.allocation_identity().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict, reason: "native root allocation is unwitnessed" })?,
            );
            let expected_preparation_identity = backend.preparation_identity()
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native staging allocation is unwitnessed" })?;
            let (root, mut custody) = meter.path(backend.root_path(), None)?;
            let (operation_directory, mut operation_custody) = meter.path(backend.operation_directory_path(), None)?;
            custody.absorb(&mut operation_custody)?;
            Ok::<_, Error>((Self { root, operation_directory, expected_identity, expected_preparation_identity }, custody))
        }, Some((work.runtime_meter_owner(), false)))
            .map_err(|error| Error::from(work.runtime_error(error)))?
            .await.map_err(|error| Error::from(work.runtime_error(error)))?
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
    /// The original record is absent.
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

impl From<acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>> for Error {
    fn from(error: acyclic_fs::MaterializationError<Error, acyclic_fs::NativeTreeMaterializationError>) -> Self {
        use acyclic_fs::MaterializationError as E;
        let (code, reason) = match error {
            E::Store(error) => return error,
            E::Backend(error) => return Error::from(error),
            E::PlanConflict => (crate::wire::ErrorCode::Conflict, "materialization retry conflicts with its durable plan"),
            E::IncompatibleJournal => (crate::wire::ErrorCode::Storage, "materialization journal is incompatible"),
            E::Contended => (crate::wire::ErrorCode::Conflict, "materialization journal remained contended"),
            E::ExternalMutation => (crate::wire::ErrorCode::Conflict, "materialization paused because an external path mutation was observed"),
        };
        Error::NativeBoundary { code, reason }
    }
}

fn preparation_error(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason }
}

fn preparation_backend(source: acyclic_stream::StreamError) -> Error {
    Error::NativeStream {
        code: if matches!(source, acyclic_stream::StreamError::IdempotencyMismatch) {
            crate::wire::ErrorCode::Conflict
        } else { crate::wire::ErrorCode::Storage },
        source, custody: None,
    }
}

struct PreparedVolume<A, O> {
    view: NativePreparedVolumeBackend<A, O>,
    original_identity: acyclic_fs::NativeRootIdentity,
    publication: PublicationPermit,
    _work: NativeMaterializationWork,
}

enum NativePreparedVolumeBackend<A, O> {
    Materialized { view: NativeWorkingSet<A, O>, callback: Arc<dyn crate::runtime::NativeVolumePublicationGuard> },
    HeldLazy(Arc<dyn NativeHeldLazyVolume>),
}

impl<A: AsyncAuthorityStore + Send + Sync + 'static, O: AsyncObjectStore + Send + Sync + 'static>
    NativePreparedVolumeBackend<A, O> {
    fn path(&self) -> &Path {
        match self {
            Self::Materialized { view, .. } => view.path(),
            Self::HeldLazy(view) => view.path(),
        }
    }
}

/// Serialized diagnostic data; the runtime failure retains its typed causes and custody separately.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug)]
pub struct NativeViewPublicationError {
    pub cause: Error,
    /// Actual reconciliation failure retained beside the original publication cause.
    pub accounting_failure: Option<Error>,
    /// Actual cumulative original work at the publication boundary.
    pub work: acyclic_fs::WorkCounters,
    pub destination: Option<NativeVolumeMapping>,
    pub completed: Vec<NativeVolumeMapping>,
    /// SDK-qualified final snapshots, including unchanged results and acknowledged prefixes.
    pub published: Vec<super::native_process::NativeProcessPublishedVolume>,
    /// Retained result already staged before a later destination failed.
    pub diagnostic: Option<crate::conversation::FileRef>,
    /// Absent only when admission failed before SDK effects and no custody was issued.
    pub custody: Option<acyclic_fs::NativeAllocation>,
}

/// Actual successful publications remain actionable through caller postchecks.
pub struct NativeViewPublicationReceipt {
    /// Actual cumulative original work at the publication boundary.
    pub work: acyclic_fs::WorkCounters,
    pub completed: Vec<NativeVolumeMapping>,
    /// These immutable results are data, never permission to inspect or mutate a project.
    pub published: Vec<super::native_process::NativeProcessPublishedVolume>,
    pub custody: acyclic_fs::NativeAllocation,
}

/// Actual mounted-source scopes; no process or SDK authority is created here.
pub struct NativeProcessNamespaceScopes {
    pub scopes: Vec<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope>,
    pub custody: acyclic_fs::NativeAllocation,
}

struct NativeMappingPayload {
    mapping: NativeVolumeMapping,
    readonly_snapshot: Option<acyclic_fs::NativePublicationSnapshot>,
    allocation: acyclic_fs::NativeAllocation,
}

/// Provider-owned materialized namespace, with the canonical SDK capture owner for every independently authorized volume.
pub struct NativeVolumeView<A, O> {
    volumes: Vec<PreparedVolume<A, O>>,
    preparation: NativePreparationReceipt,
    _allocation: acyclic_fs::NativeAllocation,
}

impl<P, A, O> NativeProcessView<P> for NativeVolumeView<A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }
    fn operation_cancellation(&self) -> &acyclic_fs::CancellationToken {
        self.work().cancellation()
    }
    fn original_native_work(&self) -> Option<&NativeMaterializationWork> {
        Some(self.work())
    }
    fn validate_for_dispatch<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || self.validate_dispatch_with_work(work))
    }
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async {
            self.work().cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            self.validate_directory_with_work(cwd, work)
        })
    }
    fn retain_native_namespace_scopes<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessNamespaceScopes>> {
        crate::NativeFutureBundle::new(work, || async {
            self.work().cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            work.charge(acyclic_fs::WorkCounters { items_examined: self.volumes.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            let count = self.volumes.iter().filter(|volume|
                matches!(volume.view, NativePreparedVolumeBackend::HeldLazy(_))).count();
            let mut custody = work.retain_owned(std::mem::size_of::<NativeProcessNamespaceScopes>() as u64)?;
            let (scopes, mut vector_custody) = allocate(work, |io| super::fork_preparer_io::vector(count, Some(io)))?;
            custody.absorb(&mut vector_custody)?;
            let mut held = NativeProcessNamespaceScopes { scopes, custody };
            for volume in &self.volumes {
                work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                    ..acyclic_fs::WorkCounters::UNCHARGED })?;
                if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                    lazy.snapshot(lazy.original_work())?.await?;
                    held.scopes.push(lazy.begin_scope(lazy.original_work())?);
                }
            }
            Ok(held)
        })
    }
    fn publish<'a>(
        &'a self, owner: &'a TaskJournalOwner<P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>> {
        let custody = work.retain_owned(0)?;
        crate::NativeFutureBundle::new(work, || async move {
            Ok(self.publish_original(owner, work, custody).await)
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
    pub fn work(&self) -> &NativeMaterializationWork { self._allocation.work() }

    /// Retires only this held view's SDK mounts on their accepted birth credit.
    pub fn cleanup_mounts_with_native_work<'a, P: StreamProvider + 'static>(
        &'a self, owner: &'a TaskJournalOwner<P>,
        process: &'a mut Option<super::native_process::NativeProcessDrainProof>,
        partial: &'a mut Option<NativeTaskViewDrainReceipt>,
        original: &'a NativeMaterializationWork,
        work: &'a NativeMaterializationWork,
        wait: &'a acyclic_fs::CancellationToken,
    ) -> Result<crate::NativeFutureBundle<'a, NativeTaskViewDrainReceipt>> {
        crate::NativeFutureBundle::new(work, || async move {
            wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            if !work.remaining()?.has_finite_allowances()
                || !self.work().shares_allocation_context(original)
                || !self._allocation.matches_work(original)
                || !owner.native_shared_work_matches(original)
                || self.preparation.manifest.volumes.len() != self.volumes.len()
                || process.is_some() == partial.is_some()
            {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            work.charge(acyclic_fs::WorkCounters {
                items_examined: self.volumes.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED
            })?;
            let proof = if let Some(receipt) = partial.as_ref() {
                if receipt.mounts.len() > self.volumes.len()
                    || receipt.mounts.capacity() < self.volumes.len()
                {
                    return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                }
                &receipt.process
            } else {
                process.as_ref().ok_or_else(||
                    Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?
            };
            proof.require_view_binding(self, owner, original)?;
            for volume in &self.volumes {
                if !volume._work.shares_ledger(original) {
                    return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                }
                if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                    if !lazy.matches_owner(owner)
                        || !lazy.original_work().shares_allocation_context(&volume._work)
                    {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                }
            }
            self.authenticate_mount_retirement(owner, work).await?;
            if partial.is_none() {
                let mut custody = work.retain_owned(std::mem::size_of::<NativeTaskViewDrainReceipt>() as u64)?;
                let mut vector = allocate(work, |io| super::fork_preparer_io::vector(self.volumes.len(), Some(io)))?;
                custody.absorb(&mut vector.1)?;
                wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                work.remaining()?;
                let process = process.take().ok_or_else(||
                    Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
                let (mounts, _empty_vector_custody) = vector;
                *partial = Some(NativeTaskViewDrainReceipt { process, mounts, custody });
            }
            let receipt = partial.as_mut().expect("accepted cleanup retains its partial receipt");
            while receipt.mounts.len() < self.volumes.len() {
                wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                self.authenticate_mount_retirement(owner, work).await?;
                let volume = &self.volumes[receipt.mounts.len()];
                let proof = match &volume.view {
                    NativePreparedVolumeBackend::Materialized { view, .. } =>
                        view.retire_with_work(wait)?,
                    NativePreparedVolumeBackend::HeldLazy(lazy) => lazy.retire(wait)?,
                };
                receipt.mounts.push(proof);
                if let Some(cause) = receipt.mounts.last_mut()
                    .and_then(acyclic_fs::NativeMountDrainProof::take_retained_capture_failure)
                {
                    return Err(Error::from(cause));
                }
                self.authenticate_mount_retirement(owner, work).await?;
            }
            wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            work.remaining()?;
            self.authenticate_mount_retirement(owner, work).await?;
            Ok(partial.take().expect("completed cleanup retains its receipt"))
        })
    }

    async fn authenticate_mount_retirement<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        let (outcome, custody) = owner.outcome_with_native_work(work).await?;
        if outcome.is_none() {
            owner.verify_with_native_work(true, work).await?;
        }
        drop(outcome);
        drop(custody);
        for mapping in &self.preparation.manifest.volumes {
            owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
            if mapping.writable {
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, work)?;
            }
        }
        Ok(())
    }

    /// Transfers this view into an admitted Arc.
    pub fn into_arc_with_native_work(
        mut self, work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self.work().shares_ledger(work)
            || !self._allocation.work().shares_ledger(work)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native view Arc requires its actual original operation" });
        }
        let mut custody = work.claim(std::mem::size_of::<Self>() as u64)?;
        self._allocation.absorb(&mut custody)?;
        Ok(Arc::new(self))
    }

    /// Prepares one immutable mixed declaration before any Scratch staging.
    pub fn prepare<'a, P: StreamProvider + 'static>(
        owner: &'a TaskJournalOwner<P>, guards: &'a [crate::runtime::NativeVolumeCallbackGuard<P>],
        allocation: &'a NativePreparationRoot, options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>], work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        if guards.len() != bindings.len() || existing_lazy.len() != bindings.len() {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        for ((guard, binding), lazy) in guards.iter().zip(bindings).zip(existing_lazy) {
            guard.require_volume_with_native_work(owner, binding.volume, binding.writable, work)?;
            match (binding.namespace, lazy) {
                (NativeVolumeNamespace::MaterializedDirectory, None) => {},
                (NativeVolumeNamespace::LazyMount { .. }, Some(lazy))
                    if lazy.custody.matches_work(lazy.held.original_work())
                        && lazy.held.original_work().shares_ledger(work)
                        && lazy.held.original_work().budget() == options.work_per_volume
                        && lazy.held.matches_owner(owner) => {},
                _ => return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)),
            }
        }
        crate::NativeFutureBundle::new(work, || async move {
            let cancellation = work.cancellation();
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
            let (root, mut output_custody) = work.path(&options.root, None)?;
            let owned_options = NativeViewOptions {
                root, maximum_volumes: options.maximum_volumes,
                work_per_volume: options.work_per_volume,
            };
            let (mut mappings, mut mappings_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            output_custody.absorb(&mut mappings_custody)?;
            let (mut binding_work, binding_work_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for (index, binding) in bindings.iter().enumerate() {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: 1, ..acyclic_fs::WorkCounters::default()
                })?;
                let lazy = existing_lazy.get(index).copied().flatten();
                if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) != lazy.is_some() {
                    return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                }
                let path = Path::new(binding.path);
                if path.as_os_str().is_empty() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native mapping path is empty" });
                }
                for part in path.components() {
                    work.charge(acyclic_fs::WorkCounters {
                        source_path_components: 1, ..acyclic_fs::WorkCounters::default()
                    })?;
                    if !matches!(part, Component::Normal(_)) {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native volume paths overlap or escape their root" });
                    }
                }
                for prior in &bindings[..index] {
                    work.charge(acyclic_fs::WorkCounters {
                        items_examined: 1, ..acyclic_fs::WorkCounters::default()
                    })?;
                    if native_paths_overlap(path, Path::new(prior.path)) {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native volume paths overlap or escape their root" });
                    }
                }
                let volume_work = match lazy {
                    Some(lazy) => lazy.held.original_work().clone(),
                    None => work.native_volume_work(options.work_per_volume)?,
                };
                verify_binding(owner, binding, &volume_work)?;
                let (volume, mut volume_custody) = allocate(&work, |io| binding.volume.clone_with_io(io))?;
                output_custody.absorb(&mut volume_custody)?;
                let (generation, mut generation_custody) = allocate(&work, |io| binding.generation.clone_with_io(io))?;
                output_custody.absorb(&mut generation_custody)?;
                let (path, mut path_custody) = work.path(&options.root, Some(path))?;
                output_custody.absorb(&mut path_custody)?;
                if let Some(lazy) = lazy {
                    let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else { unreachable!() };
                    if lazy.held.path() != path
                        || lazy.held.snapshot(&volume_work)?.await? != *snapshot
                    {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                }
                binding_work.push(volume_work);
                let authority_revision = {
                    let (bytes, _custody) = encode_record(&(binding.verifier.audience(), binding.scope), work)?;
                    work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64,
                        ..acyclic_fs::WorkCounters::UNCHARGED })?;
                    *blake3::hash(&bytes).as_bytes()
                };
                if lazy.is_some_and(|lazy| !lazy.held.matches_binding(&volume, &generation,
                    binding.writable, binding.publication, &authority_revision))
                {
                    return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                }
                mappings.push(NativeVolumeMapping {
                    volume, generation, path, writable: binding.writable,
                    namespace: binding.namespace.clone(),
                    authority_revision,
                });
            }
            let manifest = NativeViewManifest {
                options: owned_options, volumes: mappings,
            };
            manifest.validate()?;
            let lazy_count = existing_lazy.iter().filter(|lazy| lazy.is_some()).count();
            let mut preserved_custody = work.retain_owned(
                std::mem::size_of::<Vec<&str>>() as u64)?;
            let (mut preserved, mut preserved_slots) = allocate(work, |io| super::fork_preparer_io::vector(lazy_count, Some(io)))?;
            preserved_custody.absorb(&mut preserved_slots)?;
            for (index, binding) in bindings.iter().enumerate() {
                if existing_lazy.get(index).copied().flatten().is_some() {
                    preserved.push(binding.path);
                }
            }
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            let backend = native_backend(&allocation, &work, true).await?;
            let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: false }, work: work.clone() };
            let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
            let operation = native_preparation_operation_for_task_with_work(owner.task_binding().0, &work)?;
            let retained = materializer.load_owned(operation).await.map_err(Error::from)?;
            let (mut generations, generations_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            let (mut selection, mut plan_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for (binding, volume_work) in bindings.iter().zip(&binding_work) {
                owner.verify_with_native_work(false, work).await?;
                verify_binding(owner, binding, volume_work)?;
                let workspace = binding.host.open_volume_with_native_work(binding.volume, volume_work).await?;
                let generation = binding.host.generation_with_native_work(
                    &workspace, binding.generation, volume_work,
                ).await?;
                if binding.writable {
                    let head = workspace.head_with_native_work(
                        volume_work, cancellation,
                    ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                    if head.id() != generation.id() {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native destination generation is stale" });
                    }
                }
                let (provider_identity, mut provider_custody) = encode_record(binding.volume.provider(), &work)?;
                plan_custody.absorb(&mut provider_custody)?;
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
                if (intent.task, &intent.original_fence) != owner.task_binding() || intent.manifest != manifest
                    || intent.operation_directory != allocation.operation_directory
                {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation original selection changed" });
                }
                require_allocation(journal, &allocation)?;
                drop(manifest);
                output_custody.substitute(0)?;
                output_custody.absorb(&mut intent_custody)?;
                intent
            } else {
                materializer.backend().require_initial_empty()?;
                let (task, original_fence) = owner.task_binding();
                let (reservation_id, mut reservation_custody) = work.display(&original_fence.reservation_id)?;
                let (placement, mut placement_custody) = work.display(&original_fence.placement)?;
                output_custody.absorb(&mut reservation_custody)?;
                output_custody.absorb(&mut placement_custody)?;
                let original_fence = crate::scheduler::LeaseFence { reservation_id, placement };
                let (operation_directory, operation_custody) = work.path(&allocation.operation_directory, None)?;
                intent_operation_custody = Some(operation_custody);
                NativePreparationIntent { task, original_fence, operation_directory, manifest }
            };
            let manifest = &intent.manifest;
            let (mut top_level, top_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for binding in bindings {
                if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) { continue; }
                let first = Path::new(binding.path).components().next()
                    .and_then(|part| part.as_os_str().to_str())
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native mapping name must be Unicode" })?;
                top_level.push(first);
            }
            top_level.sort_unstable();
            top_level.dedup();
            let (mut edits, mut edits_custody) = allocate(&work, |io| super::fork_preparer_io::vector(top_level.len(), Some(io)))?;
            plan_custody.absorb(&mut edits_custody)?;
            for path in &top_level {
                let (path, mut path_custody) = work.display(path)?;
                plan_custody.absorb(&mut path_custody)?;
                let (image, mut image_custody) = work.copy_bytes(acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)?;
                plan_custody.absorb(&mut image_custody)?;
                edits.push(acyclic_fs::MaterializationEdit::Install { path, image });
            }
            drop(top_level);
            drop(top_custody);
            let (original, mut original_custody) = encode_record(&intent, &work)?;
            plan_custody.absorb(&mut original_custody)?;
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
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            materializer.backend().require_selected_root(&plan.edits, &preserved)?;
            let journal = materializer.prepare_owned(plan, plan_custody).await.map_err(Error::from)?;
            require_allocation(&journal, &allocation)?;
            let restage = journal.phase == MaterializationPhase::Preparing
                && journal.preimages.is_empty();
            if restage {
                owner.verify_with_native_work(false, work).await?;
                materializer.backend().reset_target_directory("").await?;
                owner.verify_with_native_work(false, work).await?;
                for (binding, (generation, volume_work)) in bindings.iter().zip(&generations) {
                    if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) { continue; }
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, volume_work)?;
                    let identity = materializer.backend().reset_target_directory(binding.path)
                        .await?;
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, volume_work)?;
                    let (path, path_custody) = materializer.backend().staging_path(binding.path)?;
                    let staged = (
                        acyclic_fs::MaterializeOptions::native(path).expected_root_identity(Some(identity)),
                        path_custody,
                    );
                    generation.materialize_with_native_work(
                        &staged.0, &volume_work, cancellation,
                    ).await?;
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, volume_work)?;
                }
            }
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            materializer.backend().require_selected_root(&journal.plan.edits, &preserved)?;
            drop(journal);
            let journal = {
                let selected = &generations;
                let namespace_root = &manifest.options.root;
                let mut before_effect = |_write| async move {
                    owner.verify_with_native_work(false, work).await?;
                    for (binding, (_, volume_work)) in bindings.iter().zip(selected) {
                        verify_binding(owner, binding, volume_work)?;
                    }
                    Self::authenticate_lazy_roots(owner, bindings, existing_lazy, namespace_root, work)
                        .await?;
                    Ok::<(), Error>(())
                };
                materializer.recover_owned_guarded(
                    operation, acyclic_fs::MaterializationRecovery::Complete, &mut before_effect,
                ).await
            }?
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation journal disappeared" })?;
            require_allocation(&journal, &allocation)?;
            Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
            materializer.backend().require_selected_root(&journal.plan.edits, &preserved)?;
            let (mut volumes, mut volumes_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            output_custody.absorb(&mut volumes_custody)?;
            let (mut identities, identities_custody) = allocate(&work, |io| super::fork_preparer_io::vector(bindings.len(), Some(io)))?;
            for (index, ((binding, mapping), (generation, volume_work))) in bindings.iter()
                .zip(&manifest.volumes).zip(generations).enumerate()
            {
                owner.verify_with_native_work(false, work).await?;
                verify_binding(owner, binding, &volume_work)?;
                if let Some(lazy) = existing_lazy.get(index).copied().flatten() {
                    let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else { unreachable!() };
                    if lazy.held.path() != mapping.path || lazy.held.snapshot(&volume_work)?.await? != *snapshot {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                    let identity = lazy.held.identity();
                    if identities.contains(&identity) {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                    identities.push(identity);
                    volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::HeldLazy(Arc::clone(&lazy.held)),
                        original_identity: identity,
                        publication: binding.publication, _work: volume_work });
                    continue;
                }
                let callback_guard = guards[index]
                    .for_volume(owner, binding.volume, binding.writable, &volume_work)?;
                let crate::runtime::NativeVolumeCallbackPorts { sdk, publication } = callback_guard;
                let identity = materializer.backend().mapping_root_identity(binding.path)?;
                if identities.contains(&identity) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native volume directory aliases changed" });
                }
                identities.push(identity);
                let mount = if binding.writable {
                    acyclic_fs::MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual)
                } else { acyclic_fs::MountOptions::read_only() };
                let (attachment_path, path_custody) = work.path(&mapping.path, None)?;
                let attachment = (acyclic_fs::MaterializeOptions::native(attachment_path), path_custody);
                let native_original = Some(volume_work.clone());
                let view = generation.attach_native_working_set_with_work(
                    &mount, &attachment.0, identity, manifest.options.work_per_volume,
                    &volume_work, cancellation, Some(sdk),
                    native_original,
                ).await?;
                owner.verify_with_native_work(false, work).await?;
                verify_binding(owner, binding, &volume_work)?;
                volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::Materialized { view, callback: publication },
                    original_identity: identity,
                    publication: binding.publication, _work: volume_work });
            }
            drop(generations_custody);
            drop(identities);
            drop(identities_custody);
            owner.verify_with_native_work(false, &work).await?;
            cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            checkpoint_preparation(materializer.store(), journal).await?;
            let inspection = materializer.inspect_owned(operation).await.map_err(Error::from)?
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation journal disappeared" })?;
            owner.verify_with_native_work(false, work).await?;
            for (binding, volume) in bindings.iter().zip(&volumes) {
                verify_binding(owner, binding, &volume._work)?;
            }
            let receipt = preparation_receipt(intent, inspection.0, inspection.1)?;
            drop(intent_operation_custody);
            Ok(Self { volumes, preparation: receipt,
                _allocation: output_custody })
        })
    }

    async fn authenticate_lazy_roots<P: StreamProvider>(
        owner: &TaskJournalOwner<P>, bindings: &[BorrowedNativeVolumeBinding<'_, A, O>],
        existing_lazy: &[Option<&NativeExistingLazyVolume>], root: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        owner.verify_with_native_work(false, work).await?;
        if existing_lazy.is_empty() { return Ok(()); }
        for (index, binding) in bindings.iter().enumerate() {
            let Some(lazy) = existing_lazy.get(index).copied().flatten() else { continue; };
            verify_binding(owner, binding, work)?;
            let relative = lazy.held.path().strip_prefix(root)
                .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            if relative.as_os_str().is_empty() || relative != Path::new(binding.path) {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let expected = lazy.held.identity();
            let actual = lazy.held.original_work().root_identity(lazy.held.path())?;
            if actual != expected {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual }));
            }
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            };
            if lazy.held.snapshot(lazy.held.original_work())?.await? != *snapshot {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        owner.verify_with_native_work(false, work).await
    }



    /// Returns this attachment's original preparation.
    pub fn preparation(&self) -> &NativePreparationReceipt { &self.preparation }

    /// Recovers the recorded host allocation on the caller's original ledger.
    pub fn recover_allocation<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, options: &'a NativeViewOptions, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<NativePreparationRoot>, acyclic_fs::NativeAllocation)>> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation recovery requires finite original work" });
        }
        crate::NativeFutureBundle::new(work, || async {
            let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: true }, work: work.clone() };
            let operation = native_preparation_operation_for_task_with_work(owner.task_binding().0, work)?;
            let Some(journal) = store.load_record(operation, false).await
                ?
            else { return Ok((None, work.retain_owned(0)?)); };
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
            store.verify_read_owner().await?;
            let expected_identity = work.root_identity(&intent.0.manifest.options.root)?;
            if expected_identity.to_bytes() != expected_root {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native original root allocation was replaced" });
            }
            let NativePreparationIntent { original_fence, manifest, operation_directory, .. } = intent.0;
            let NativeViewManifest { options, volumes } = manifest;
            drop((volumes, original_fence));
            let mut allocation = (NativePreparationRoot {
                root: options.root, operation_directory, expected_identity, expected_preparation_identity,
            }, intent.1);
            let retained = allocation.0.root.capacity().checked_add(allocation.0.operation_directory.capacity())
                .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
            allocation.1.substitute(retained as u64)?;
            let _backend = native_backend(&allocation.0, work, true).await?;
            store.verify_read_owner().await?;
            work.remaining()?;
            Ok((Some(allocation.0), allocation.1))
        })
    }

    /// Inspects the original preparation and optionally rolls it back.
    pub fn inspect_preparation<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, allocation: &'a NativePreparationRoot,
        work: &'a NativeMaterializationWork, cleanup: bool,
    ) -> Result<crate::NativeFutureBundle<'a, Option<NativePreparationReceipt>>> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native query requires finite original work" });
        }
        crate::NativeFutureBundle::new(work, || async {
            let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: true }, work: work.clone() };
            let operation = native_preparation_operation_for_task_with_work(owner.task_binding().0, work)?;
            let Some(mut journal) = store.load_record(operation, false).await
                ? else { return Ok(None); };
            let mut intent = preparation_intent(&journal.0, work)?;
            if intent.0.task != owner.task_binding().0 || intent.0.manifest.options.root != allocation.root
                || intent.0.operation_directory != allocation.operation_directory
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native query differs from original preparation" });
            }
            require_allocation(&journal.0, allocation)?;
            store.verify_read_owner().await?;
            let backend = match native_backend(allocation, work, cleanup).await {
                Ok(backend) => backend,
                Err(error @ Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Work(_)
                        | acyclic_fs::NativeTreeMaterializationError::Cancelled
                        | acyclic_fs::NativeTreeMaterializationError::AllocationFailed(_),
                    ..
                }) => return Err(error),
                Err(error) if !cleanup => {
                    let mut root_message = work.display(&error)?;
                    let mut preparation_message = work.display(&root_message.0)?;
                    intent.1.absorb(&mut journal.1)?;
                    intent.1.absorb(&mut root_message.1)?;
                    intent.1.absorb(&mut preparation_message.1)?;
                    let receipt = NativePreparationReceipt {
                        task: intent.0.task, original_fence: intent.0.original_fence, manifest: intent.0.manifest,
                        journal: journal.0, root: acyclic_fs::MaterializationRootObservation::Unavailable(root_message.0),
                        preparation: acyclic_fs::MaterializationPreparationObservation::Unavailable(preparation_message.0),
                        observations: Vec::new(), _custody: Some(intent.1),
                    };
                    store.verify_read_owner().await?;
                    return Ok(Some(receipt));
                }
                Err(error) => return Err(error),
            };
            drop(journal);
            let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
            if cleanup {
                owner.verify_with_native_work(true, work).await?;
                let rolled_back = {
                    let mut before_effect = |_write| async move {
                        owner.verify_with_native_work(true, work).await?;
                        Ok::<(), Error>(())
                    };
                    materializer.recover_owned_guarded(
                        operation, acyclic_fs::MaterializationRecovery::RollBack, &mut before_effect,
                    ).await
                }?
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                        reason: "native preparation journal disappeared" })?;
                owner.verify_with_native_work(true, work).await?;
                materializer.backend().reset_target_directory("").await?;
                owner.verify_with_native_work(true, work).await?;
                checkpoint_preparation(materializer.store(), rolled_back).await?;
            }
            let mut inspection = materializer.inspect_owned(operation).await.map_err(Error::from)?
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native preparation journal disappeared" })?;
            if let Some(custody) = inspection.1.as_mut() {
                intent.1.absorb(custody)?;
            }
            let receipt = preparation_receipt(intent.0, inspection.0, Some(intent.1))?;
            materializer.store().verify_read_owner().await?;
            Ok(Some(receipt))
        })
    }

    /// Exact request/approval manifest. Changing it requires new admission.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }

    /// Reads an entire mounted Project/agent-private file under this view's original task, exact mapping and current READ grant.
    pub async fn read_builtin_file<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, _original_operation: crate::OperationId,
        relative: &Path, maximum_bytes: usize, work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let (mapping, volume, file) = self.builtin_volume(relative, VolumeOperation::Read, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work).await?;
        match &volume.view {
            NativePreparedVolumeBackend::HeldLazy(lazy) =>
                lazy.read(file, maximum_bytes, work)?.await,
            NativePreparedVolumeBackend::Materialized { view, .. } => {
                let lease = view.native_source_io_lease()
                    .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
                let physical = work.with_source_io_lease(lease)?;
                physical.read_working_file(&mapping.path, volume.original_identity, file, maximum_bytes,
                    || self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work)).await
            }
        }
    }

    /// Writes bounded UTF-8 only through an original mounted writable mapping.
    #[allow(clippy::too_many_arguments, reason = "original command, mapping and edit preimage are explicit")]
    pub async fn write_builtin_file<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, _original_operation: crate::OperationId,
        relative: &Path, content: &str, create_parents: bool,
        expected_sha256: Option<[u8; 32]>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        let (mapping, volume, file) = self.builtin_volume(relative, VolumeOperation::Write, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work).await?;
        if expected_sha256.is_some() {
            owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
        }
        match &volume.view {
            NativePreparedVolumeBackend::HeldLazy(lazy) =>
                lazy.write(file, content, create_parents, expected_sha256, work)?.await,
            NativePreparedVolumeBackend::Materialized { view, .. } => {
                let lease = view.native_source_io_lease()
                    .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
                let physical = work.with_source_io_lease(lease)?;
                physical.write_working_file(&mapping.path, volume.original_identity, file, content,
                    create_parents, expected_sha256,
                    || async {
                        self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work).await?;
                        if expected_sha256.is_some() {
                            owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                        }
                        Ok::<(), Error>(())
                    }).await
            }
        }
    }

    fn builtin_volume<'a>(
        &'a self, relative: &'a Path, operation: VolumeOperation, work: &NativeMaterializationWork,
    ) -> Result<(&'a NativeVolumeMapping, &'a PreparedVolume<A, O>, &'a Path)> {
        let invalid = || Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout);
        let text = relative.to_str().ok_or_else(invalid)?;
        if text.is_empty() || text.contains(['\\', '\0'])
            || text.split('/').any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(invalid());
        }
        for component in relative.components() {
            work.charge(acyclic_fs::WorkCounters { source_path_components: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            if !matches!(component, Component::Normal(_)) { return Err(invalid()); }
        }
        if self.volumes.len() != self.preparation.manifest.volumes.len() { return Err(invalid()); }
        for (mapping, volume) in self.preparation.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            let mount = mapping.path.strip_prefix(&self.preparation.manifest.options.root).map_err(|_| invalid())?;
            let Ok(file) = relative.strip_prefix(mount) else { continue; };
            if file.as_os_str().is_empty() || volume.view.path() != mapping.path
                || !matches!(mapping.volume.class(), crate::conversation::VolumeClass::Project | crate::conversation::VolumeClass::AgentPrivate)
                || (operation == VolumeOperation::Write && !mapping.writable)
            {
                return Err(invalid());
            }
            return Ok((mapping, volume, file));
        }
        Err(invalid())
    }

    async fn authenticate_builtin_view<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        owner.verify_with_native_work(false, work).await?;
        let store = NativePreparationStore { authority: NativePreparationAuthority::Owner { owner, settlement: false }, work: work.clone() };
        let record = store.load_record(self.preparation.journal.plan.operation_id, false).await
            ?
            .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: record.0.plan.edits.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        let valid = record.0.phase == MaterializationPhase::Applied
            && record.0.root_identity == self.preparation.journal.root_identity
            && record.0.preparation_identity == self.preparation.journal.preparation_identity
            && record.0.plan == self.preparation.journal.plan;
        drop(record);
        if !valid {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        owner.verify_with_native_work(false, work).await
    }

    async fn verify_builtin_owner<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, mapping: &NativeVolumeMapping,
        operation: VolumeOperation, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.task_binding() != (self.preparation.task, &self.preparation.original_fence) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        self.work().cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        owner.verify_with_native_work(false, work).await?;
        owner.require_volume_grant_with_native_work(&mapping.volume, operation, work)?;
        let expected = self.preparation.journal.root_identity
            .map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
        let actual = work.root_identity(&self.preparation.manifest.options.root)?;
        if expected != actual {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual }));
        }
        Ok(())
    }

    fn original_root_identity(&self) -> Result<acyclic_fs::NativeRootIdentity> {
        self.preparation.journal.root_identity.map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation has no original root witness" })
    }

    fn validate_directory_with_work(&self, cwd: &Path, work: &NativeMaterializationWork) -> Result<()> {
        if cwd == self.preparation.manifest.options.root {
            return work.open_working_directory(&self.preparation.manifest.options.root,
                self.original_root_identity()?, Path::new("")).map(drop).map_err(Error::from);
        }
        for (mapping, volume) in self.preparation.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED })?;
            if let Ok(relative) = cwd.strip_prefix(&mapping.path) {
                let physical_work = match &volume.view {
                    NativePreparedVolumeBackend::Materialized { .. } => work,
                    NativePreparedVolumeBackend::HeldLazy(lazy) => lazy.original_work(),
                };
                return physical_work.open_working_directory(&mapping.path, volume.original_identity, relative).map(drop)
                    .map_err(Error::from);
            }
        }
        Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))
    }

    async fn validate_dispatch_with_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        let expected = self.original_root_identity()?;
        self.work().cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        if work.root_identity(&self.preparation.manifest.options.root)? != expected {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation root was replaced" });
        }
        for (volume, mapping) in self.volumes.iter().zip(&self.preparation.manifest.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::default() })?;
            match &volume.view {
                NativePreparedVolumeBackend::Materialized { view, .. } => {
                    let original = volume._work.clone();
                    view.validate_for_presentation_with_work(&original, volume._work.cancellation()).await?;
                    view.verify_unchanged_with_work(&original, volume._work.cancellation()).await?;
                },
                NativePreparedVolumeBackend::HeldLazy(lazy) => {
                    let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    };
                    let actual = lazy.snapshot(lazy.original_work())?.await?;
                    if actual.workspace_id != snapshot.workspace_id || actual.source != snapshot.source
                        || lazy.path() != mapping.path
                        || lazy.original_work().root_identity(&mapping.path)? != volume.original_identity
                    {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                },
            }
        }
        if work.root_identity(&self.preparation.manifest.options.root)? != expected {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation root changed during validation" });
        }
        self.work().cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        Ok(())
    }

    async fn publish_original<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
        mut custody: acyclic_fs::NativeAllocation,
    ) -> std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError> {
        let mut completed = Vec::new();
        let mut published = Vec::new();
        let mut mappings = Vec::new();
        let mut mapping_storage = None;
        let mut failed_destination = None;
        let mut accounting_failure = None;
        let result: Result<()> = async {
            let (output, mut output_custody) = allocate(work, |io| super::fork_preparer_io::vector(self.preparation.manifest.volumes.len(), Some(io)))?;
            custody.absorb(&mut output_custody)?;
            completed = output;
            let (proofs, mut proofs_custody) = allocate(work, |io| super::fork_preparer_io::vector(self.preparation.manifest.volumes.len(), Some(io)))?;
            custody.absorb(&mut proofs_custody)?;
            published = proofs;
            let (copies, copies_custody) = allocate(work, |io| super::fork_preparer_io::vector(self.preparation.manifest.volumes.len(), Some(io)))?;
            mapping_storage = Some(copies_custody);
            mappings = copies;
            for mapping in &self.preparation.manifest.volumes {
                let (mapping, allocation) = copy_native_mapping_with_work(mapping, work)?;
                mappings.push(Some(NativeMappingPayload { mapping, readonly_snapshot: None, allocation }));
            }
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.preparation.manifest.volumes).enumerate() {
                failed_destination = Some(index);
                self.work().cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                owner.verify_with_native_work(false, work).await?;
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                if !mapping.writable {
                    let snapshot = match &volume.view {
                        NativePreparedVolumeBackend::Materialized { view, .. } =>
                            view.verify_unchanged_with_work(&volume._work, volume._work.cancellation()).await?,
                        NativePreparedVolumeBackend::HeldLazy(lazy) => {
                            let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                            };
                            let qualified = match lazy.publication_snapshot(lazy.original_work())?.await? {
                                Ok(qualified) => qualified,
                                Err(failure) => {
                                    mappings[index].as_mut().expect("original readonly destination")
                                        .readonly_snapshot = failure.publication;
                                    accounting_failure = failure.accounting_failure;
                                    return Err(failure.cause);
                                },
                            };
                            let actual = qualified.snapshot().lazy_snapshot_with_native_work(
                                lazy.original_work(), lazy.original_work().cancellation(),
                            )?.ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
                            if actual.workspace_id != snapshot.workspace_id || actual.source != snapshot.source
                                || actual.authored_generation != snapshot.authored_generation
                            {
                                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                            }
                            qualified
                        },
                    };
                    mappings[index].as_mut().expect("original readonly destination")
                        .readonly_snapshot = Some(snapshot);
                    owner.verify_with_native_work(false, work).await?;
                    owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                }
            }
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.preparation.manifest.volumes).enumerate() {
                if !mapping.writable {
                    let snapshot = mappings[index].as_mut().expect("original readonly destination")
                        .readonly_snapshot.take().expect("verified original readonly SDK cut");
                    published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
                    continue;
                }
                failed_destination = Some(index);
                let result = match &volume.view {
                    NativePreparedVolumeBackend::Materialized { view, callback } => {
                        let callback = Arc::clone(callback);
                        view.sync_with_permit_with_work(
                            volume.publication, &volume._work, volume._work.cancellation(),
                            move |actual: &NativeMaterializationWork| {
                                let callback = Arc::clone(&callback);
                                let actual = actual.clone();
                                async move { callback.check(&actual)?.await }
                            },
                        ).await.map_err(|failure| NativePublicationSyncFailure {
                            cause: failure.cause, accounting_failure: failure.accounting_failure.map(Error::from),
                            source_acknowledged: failure.source_acknowledged, publication: failure.publication,
                        })
                    }
                    NativePreparedVolumeBackend::HeldLazy(lazy) => {
                        owner.verify_with_native_work(false, work).await?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, work)?;
                        lazy.sync(volume.publication, lazy.original_work())?.await?
                    }
                };
                let snapshot = match result {
                    Ok(snapshot) => snapshot,
                    Err(failure) => {
                        if let Some(snapshot) = failure.publication {
                            published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
                        }
                        if failure.source_acknowledged {
                            let NativeMappingPayload { mapping, mut allocation, .. } = mappings[index].take()
                                .expect("original preadmitted acknowledged destination");
                            completed.push(mapping);
                            custody.absorb(&mut allocation)
                                .expect("same original publication ledger and prepaid mapping");
                            failed_destination = None;
                        }
                        accounting_failure = failure.accounting_failure;
                        return Err(failure.cause);
                    }
                };
                published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });                let NativeMappingPayload { mapping, mut allocation, .. } = mappings[index].take()
                    .expect("original preadmitted destination");
                completed.push(mapping);
                custody.absorb(&mut allocation)?;
                work.remaining()?;
            }
            failed_destination = None;
            owner.verify_with_native_work(false, work).await?;
            Ok(())
        }.await;
        match result {
            Ok(()) => {
                drop(mappings);
                drop(mapping_storage);
                Ok(NativeViewPublicationReceipt { work: work.receipt(), completed, published, custody })
            }
            Err(cause) => {
                for (index, payload) in mappings.iter_mut().enumerate() {
                    if let Some(snapshot) = payload.as_mut().and_then(|payload| payload.readonly_snapshot.take()) {
                        published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
                    }
                }
                let destination = failed_destination.and_then(|index| mappings.get_mut(index))
                    .and_then(Option::take).map(|payload| {
                        let NativeMappingPayload { mapping, mut allocation, .. } = payload;
                        custody.absorb(&mut allocation).expect("same original ledger and preadmitted total");
                        mapping
                    });
                drop(mappings);
                drop(mapping_storage);
                Err(NativeViewPublicationError { cause, accounting_failure, work: work.receipt(),
                    destination, completed, published, diagnostic: None, custody: Some(custody) })
            }
        }
    }
}

/// Reads only through the authenticated original SDK admission witness.
pub(crate) fn observe_native_preparation_admission<'a, P: StreamProvider>(
    witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, NativePreparationJournalObservation>> {
    if !work.remaining()?.has_finite_allowances() {
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
        let operation = native_preparation_operation_for_task_with_work(witness.task(), work)?;
        let store = NativePreparationStore { authority: NativePreparationAuthority::Admission(witness), work: work.clone() };
        let record = store.load_record(operation, false).await?;
        let value = native_preparation_observation(
            witness.task(), scratch, operation, record, work,
        )?;
        witness.verify_with_native_work(work).await?;
        work.remaining()?;
        Ok(value)
    })
}

fn native_preparation_operation_for_task(task: crate::TaskId) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/native-preparation/v1\0");
    hash.update(&task.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
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



async fn checkpoint_preparation<P: StreamProvider>(
    store: &NativePreparationStore<'_, P>, mut journal: acyclic_fs::MaterializationJournal,
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
    let mut left = left.components();
    let mut right = right.components();
    loop {
        let (Some(left), Some(right)) = (left.next(), right.next()) else { return true; };
        #[cfg(windows)]
        let equal = match (left.as_os_str().to_str(), right.as_os_str().to_str()) {
            (Some(left), Some(right)) => left.chars().flat_map(char::to_uppercase)
                .eq(right.chars().flat_map(char::to_uppercase)),
            _ => false,
        };
        #[cfg(not(windows))]
        let equal = left == right;
        if !equal { return false; }
    }
}

fn verify_binding<P: StreamProvider, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    owner: &TaskJournalOwner<P>, binding: &BorrowedNativeVolumeBinding<'_, A, O>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        if operation == VolumeOperation::Write && !binding.writable { continue; }
        owner.require_volume_grant_with_native_work(binding.volume, operation, work)?;
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
    let (root, mut custody) = work.path(&allocation.root, None)?;
    let (operation_directory, mut operation_custody) = work.path(&allocation.operation_directory, None)?;
    custody.absorb(&mut operation_custody)?;
    let meter = work.clone();
    let backend = acyclic_native_runtime::run_blocking_io(move || {
        acyclic_fs::NativeTreeMaterializationBackend::new(root, operation_directory, Some(custody), meter, false)
    }, Some((work.runtime_meter_owner(), false)))
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .await.map_err(|error| Error::from(work.runtime_error(error)))??;
    if require_original && (backend.allocation_identity() != Some(allocation.expected_identity.to_bytes())
        || backend.preparation_identity().as_ref() != Some(&allocation.expected_preparation_identity))
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "native allocation was physically replaced" });
    }
    Ok(backend)
}

fn native_preparation_observation(
    task: crate::TaskId,
    scratch: Option<&crate::resource_preparation::ChildScratchCreationProof>,
    operation: acyclic_fs::OperationId,
    record: Option<(MaterializationJournal, acyclic_fs::NativeAllocation)>,
    work: &NativeMaterializationWork,
) -> Result<NativePreparationJournalObservation> {
    let Some(mut record) = record else {
        return Ok(NativePreparationJournalObservation::MissingJournalPhysicalStateUnknown {
            preparation_operation: operation,
        });
    };
    let mut intent = preparation_intent(&record.0, work)?;
    if intent.0.task != task {
        return Err(preparation_error(
            "native journal original task differs from authenticated admission",
        ));
    }
    if let Some(scratch) = scratch {
        if task != scratch.original_task()
            || !intent.0.manifest.volumes.iter().any(|mapping| mapping.writable
                && &mapping.volume == scratch.volume())
        {
            return Err(preparation_error(
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

fn preparation_intent(
    journal: &MaterializationJournal, work: &NativeMaterializationWork,
) -> std::result::Result<(NativePreparationIntent, acyclic_fs::NativeAllocation), Error> {
    let acyclic_fs::MaterializationIntent::NativePreparation { selection, original } = &journal.plan.intent else {
        return Err(preparation_error("native preparation original qualified intent is missing"));
    };
    let intent: (NativePreparationIntent, _) = decode_record(
        original, std::mem::size_of::<NativeVolumeMapping>().max(std::mem::size_of::<u8>()), work,
        |intent| native_intent_control_bytes(intent),
    )?;
    intent.0.manifest.validate()?;
    if selection.len() != intent.0.manifest.volumes.len()
        || journal.plan.edits.iter().any(|edit| !matches!(edit,
            acyclic_fs::MaterializationEdit::Install { image, .. }
                if image.as_slice() == acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX
        ))
    {
        return Err(preparation_error("native preparation original selections or edit intents disagree"));
    }
    for (selected, mapping) in selection.iter().zip(&intent.0.manifest.volumes) {
        let identity = encode_record(mapping.volume.provider(), work)?;
        if identity.0 != selected.provider_identity
            || selected.generation.digest().as_bytes().as_slice() != mapping.generation.as_resource().key()
        {
            return Err(preparation_error("native preparation original qualified volume differs"));
        }
    }
    Ok(intent)
}

fn preparation_receipt(
    intent: NativePreparationIntent, inspection: acyclic_fs::MaterializationInspection,
    custody: Option<acyclic_fs::NativeAllocation>,
) -> Result<NativePreparationReceipt> {
    let mut journal = inspection.journal; let mut journal_custody = journal.custody.take();
    let mut receipt = NativePreparationReceipt {
        task: intent.task, original_fence: intent.original_fence, manifest: intent.manifest,
        journal, root: inspection.root, preparation: inspection.preparation,
        observations: inspection.observations, _custody: custody,
    };
    if let Some(custody) = receipt._custody.as_mut() {
        if let Some(journal_custody) = journal_custody.as_mut() {
            custody.absorb(journal_custody)?;
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

enum NativePreparationAuthority<'a, P: StreamProvider> {
    Owner { owner: &'a TaskJournalOwner<P>, settlement: bool },
    Admission(&'a crate::durable_host::TaskAdmissionReadonlyWitness<P>),
}

/// Authenticated pre-action packet for one accepted task-preparation completion.
pub(crate) struct NativePreparationCompletionTemplate {
    journal: acyclic_fs::MaterializationJournal,
    intent: NativePreparationIntent,
    path: acyclic_stream::StreamPath,
    key: acyclic_stream::IdempotencyKey,
    value: serde_json::Value,
    output: acyclic_stream::PreparedOwnedBytes<acyclic_fs::NativeAllocation>,
    maximum_bytes: u64,
    custody: acyclic_fs::NativeAllocation,
}

impl NativePreparationCompletionTemplate {
    pub(crate) fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence) {
        (self.intent.task, &self.intent.original_fence)
    }
    pub(crate) fn operation(&self) -> acyclic_fs::OperationId { self.journal.plan.operation_id }
    pub(crate) fn expected_revision(&self) -> u64 { self.journal.revision }
    pub(crate) fn path(&self) -> &acyclic_stream::StreamPath { &self.path }
    pub(crate) fn key(&self) -> &acyclic_stream::IdempotencyKey { &self.key }
    pub(crate) fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.custody.matches_work(work)
            && self.journal.custody.as_ref().is_some_and(|guard| guard.matches_work(work))
    }
    /// Checks sealed packet associations only.
    pub(crate) fn verify_owner<P: StreamProvider>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.matches_work(work) || self.task_binding() != owner.task_binding()
            || self.journal.phase != MaterializationPhase::Preparing
            || !self.journal.is_valid_for(self.operation())
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native completion differs from its original protected preparation" });
        }
        for mapping in &self.intent.manifest.volumes {
            owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
            if mapping.writable {
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, work)?;
            }
        }
        Ok(())
    }

    /// Only the revision and existing unsigned work slots may change after the action.
    pub(crate) fn set_completed_work(&mut self, counters: acyclic_fs::WorkCounters) -> Result<()> {
        Self::set_value_work(&mut self.value, self.journal.revision, counters)?;
        self.journal.work = counters;
        Ok(())
    }

    fn set_value_work(value: &mut serde_json::Value, expected_revision: u64, counters: acyclic_fs::WorkCounters) -> Result<()> {
        let revision = expected_revision.checked_add(1).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native completion revision overflow",
        })?;
        let root = value.as_object_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native completion object is missing",
        })?;
        let revision_slot = root.get_mut("revision").ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native completion revision slot is missing",
        })?;
        *revision_slot = serde_json::Value::Number(revision.into());
        let work = root.get_mut("work").and_then(serde_json::Value::as_object_mut)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "native completion work slots are missing" })?;
        macro_rules! replace {
            ($field:ident, $key:literal) => {
                *work.get_mut($key).ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage, reason: "native completion scalar slot is missing",
                })? = serde_json::Value::Number(counters.$field.into());
            };
        }
        replace!(authority_records_read, "authorityRecordsRead");
        replace!(authority_records_appended, "authorityRecordsAppended");
        replace!(authority_bytes_read, "authorityBytesRead");
        replace!(authority_bytes_written, "authorityBytesWritten");
        replace!(object_probes, "objectProbes");
        replace!(backend_read_operations, "backendReadOperations");
        replace!(backend_write_operations, "backendWriteOperations");
        replace!(durability_operations, "durabilityOperations");
        replace!(page_reads, "pageReads");
        replace!(page_writes, "pageWrites");
        replace!(object_bytes_read, "objectBytesRead");
        replace!(object_bytes_written, "objectBytesWritten");
        replace!(bytes_hashed, "bytesHashed");
        replace!(bytes_copied, "bytesCopied");
        replace!(bytes_encoded, "bytesEncoded");
        replace!(source_bytes_read, "sourceBytesRead");
        replace!(source_path_components, "sourcePathComponents");
        replace!(source_entries_visited, "sourceEntriesVisited");
        replace!(output_bytes, "outputBytes");
        replace!(items_examined, "itemsExamined");
        replace!(items_returned, "itemsReturned");
        replace!(allocation_operations, "allocationOperations");
        replace!(peak_allocation_bytes, "peakAllocationBytes");
        replace!(materializations, "materializations");
        Ok(())
    }

    /// Borrows the fixed source and prepaid output; the prepared History writer owns its accepted accounting observer and must never reserve here.
    pub(crate) fn codec_parts(&mut self) -> (&serde_json::Value, &mut [u8], u64) {
        (&self.value, self.output.writable(), self.maximum_bytes)
    }
    pub(crate) fn into_completed_parts(self)
        -> (acyclic_fs::MaterializationJournal, acyclic_stream::PreparedOwnedBytes<acyclic_fs::NativeAllocation>, acyclic_fs::NativeAllocation)
    {
        let Self { mut journal, intent, path, key, value, output, maximum_bytes: _, custody } = self;
        journal.revision += 1;
        drop((intent, path, key, value));
        (journal, output, custody)
    }
}

/// Authenticates or publishes the original protected ROOT conversation Bind and returns that exact SDK command's canonical bytes.
#[allow(clippy::too_many_arguments, reason = "original creator, owner and paid authority are distinct")]
pub async fn bind_original_root_conversation_with_native_work<P: StreamProvider + 'static>(
    stream: &acyclic_stream::StreamClient<P>,
    authority: (crate::core::Authority, crate::core::AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation,
    owner: &TaskJournalOwner<P>,
    private_volume: &VolumeRef,
    original_creation: acyclic_fs::OperationId,
    limits: crate::conversation::Limits,
    work: &NativeMaterializationWork,
) -> Result<acyclic_stream::OwnedBytes> {
    let _authority_custody = authority_custody;
    let (audience, issuer, scope) = authority;
    let invalid = |reason| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason,
    };
    if audience.kind != crate::core::AggregateKind::Conversation
        || scope.agent().is_none()
        || !scope.capabilities().contains("conversation:bind")
        || limits.file_bytes == 0
    {
        return Err(invalid("ROOT binding requires its original conversation authority and finite event allowance"));
    }
    owner.verify_with_native_work(false, work).await?;
    let verifier = issuer.verifier_with_native_work(work)?;
    let operation = native_root_output_binding_operation(
        &audience, private_volume, original_creation, work,
    )?;
    let reader = crate::store::HistoryReader::new_with_native_work(
        stream, &audience, &verifier.0, work,
    )?;
    let event = reader.0.operation_event_bounded_with_native_work(
        operation, limits.file_bytes, work,
    ).await?;
    let original_revision = match event.0.as_ref() {
        None => None,
        Some(event) => match &event.payload {
            crate::core::EventPayload::ConversationBound { agent }
                if Some(*agent) == scope.agent()
                    && event.scope.agent() == scope.agent()
                    && event.scope.id() == scope.id()
                    && event.scope.capabilities() == scope.capabilities()
                    && event.causal_parent.is_none() =>
            {
                Some(event.revision.checked_sub(1)
                    .ok_or_else(|| invalid("original ROOT binding has no command revision"))?)
            }
            _ => return Err(invalid("ROOT binding differs from the protected original creation")),
        },
    };
    drop(event);
    drop(reader);
    let aggregate = crate::store::StreamAggregate::open_with_native_work(
        stream, audience,
        verifier.0, crate::core::SchemaRegistry::new(), work,
    ).await?;
    let mut aggregate = (aggregate.0.with_limits(limits)?, aggregate.1);
    if original_revision.is_none()
        && aggregate.0.reducer().conversation().and_then(|state| state.agent).is_some()
    {
        return Err(invalid("ROOT conversation changed during original binding admission"));
    }
    let key = work.display(&format_args!("harness/native-root-output-bind/v2/{operation}"))?;
    let key = (crate::IdempotencyKey::new(key.0)?, key.1);
    let agent = scope.agent().expect("validated ROOT binding actor");
    let command = crate::core::Command {
        operation_id: operation, idempotency_key: key.0,
        expected_revision: original_revision.unwrap_or_else(|| aggregate.0.reducer().revision()),
        causal_parent: None, action: crate::core::Action::BindConversation { agent }, scope,
    };
    let mut io = crate::store::history::native_work_io(work)?;
    let encoded = crate::store::history::canonical_bytes_with_work(
        &command, crate::store::default_projection_read_limits().maximum_bytes,
        &mut Some(&mut io),
    )?;
    let encoded_custody = crate::store::history::HistoryIo::take_native_custody(
        &mut io, encoded.capacity() as u64,
    )?;
    let encoded = acyclic_stream::OwnedBytes::from_vec_with_guard(
        encoded, encoded_custody, work.runtime_meter_owner(),
    ).map_err(|failure| Error::from(work.runtime_error(failure.into_error())))?;
    let applied = aggregate.0.execute_task_command_with_native_work(command, owner, work).await?;
    drop(applied);
    Ok(encoded)
}

impl acyclic_fs::MaterializationActionCompletion for crate::NativeFutureBundle<'_,
    (acyclic_fs::MaterializationJournal, acyclic_fs::NativeAllocation)> {
    type Error = Error;

    async fn complete(self) -> Result<acyclic_fs::MaterializationJournal> {
        let (mut journal, mut output_custody) = self.await?;
        match journal.custody.as_mut() {
            Some(custody) => {
                if let Err(cause) = custody.absorb(&mut output_custody) {
                    drop(journal);
                    return Err(Error::NativeWork { cause, custody: Some(output_custody) });
                }
            }
            None => journal.custody = Some(output_custody),
        }
        Ok(journal)
    }
}

/// Fenced Stream adapter for the existing materializer journal, not a second log.
struct NativePreparationStore<'a, P: StreamProvider> {
    authority: NativePreparationAuthority<'a, P>,
    work: NativeMaterializationWork,
}

impl<'a, P: StreamProvider> NativePreparationStore<'a, P> {
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

    fn path(
        &self, operation: acyclic_fs::OperationId,
    ) -> Result<acyclic_stream::StreamPath> {
        if operation != native_preparation_operation_for_task_with_work(self.task(), &self.work)? {
            return Err(preparation_error("native materializer operation differs from original task"));
        }
        const PREFIX: &[u8] = b"harness/v2/materialization/";
        let mut bytes = [0_u8; PREFIX.len() + 36 + 1 + 36];
        let completed = acyclic_fs::WorkCounters {
            bytes_copied: PREFIX.len() as u64 + 1,
            bytes_encoded: 72,
            items_examined: bytes.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        };
        self.work.admit(completed)?;
        bytes[..PREFIX.len()].copy_from_slice(PREFIX);
        let separator = PREFIX.len() + 36;
        uuid::Uuid::from_bytes(self.task().into_bytes()).hyphenated()
            .encode_lower(&mut bytes[PREFIX.len()..separator]);
        bytes[separator] = b'/';
        uuid::Uuid::from_bytes(operation.into_bytes()).hyphenated()
            .encode_lower(&mut bytes[separator + 1..]);
        let path = std::str::from_utf8(&bytes)
            .map_err(|_| preparation_error("native preparation UUID encoding is invalid"))?;
        self.work.spent(completed)?;
        acyclic_stream::StreamPath::new_native(path, &self.work.runtime_meter_owner())
            .map_err(|error| Error::from(self.work.runtime_error(error)))?
            .map_err(preparation_backend)
    }

    /// Authenticates the actual protected Preparing record before accepting one physical action.
    async fn completion_template(
        &self, journal: acyclic_fs::MaterializationJournal,
    ) -> Result<NativePreparationCompletionTemplate> {
        let operation = journal.plan.operation_id;
        let owner = match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement: false } => *owner,
            _ => return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native completion requires the original running preparation owner" }),
        };
        owner.verify_with_native_work(false, &self.work).await?;
        let (loaded, loaded_custody) = self.load_record(operation, false).await
            ?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "native completion original preparation is missing" })?;
        if loaded != journal {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native completion original packet differs from its durable Preparing record" });
        }
        drop(loaded);
        drop(loaded_custody);
        if journal.phase != MaterializationPhase::Preparing || journal.revision == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native completion source is not Preparing" });
        }
        let (intent, mut intent_custody) = preparation_intent(&journal, &self.work)?;
        if (intent.task, &intent.original_fence) != owner.task_binding() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native completion source belongs to another original task fence" });
        }
        for mapping in &intent.manifest.volumes {
            owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, &self.work)?;
            if mapping.writable {
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, &self.work)?;
            }
        }
        let (encoded, encoded_custody) = encode_record(&journal, &self.work)?;
        self.work.charge(acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64,
            ..acyclic_fs::WorkCounters::default() })?;
        let association = *blake3::hash(&encoded).as_bytes();
        drop(encoded);
        drop(encoded_custody);
        let (key_bytes, key_custody) = self.work.copy_bytes(&association)?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes, key_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
        let io = super::PreparationIo::from_native_work(&self.work)?;
        let mut value = io.value_json(&journal, self.work.remaining()?.bytes_encoded)?;
        let mut custody = self.work.retain_owned(std::mem::size_of::<NativePreparationCompletionTemplate>() as u64)?;
        custody.absorb(&mut intent_custody)?;
        let held = io.owned_bytes()?;
        custody.absorb(&mut io.into_native_custody(held)?)?;
        NativePreparationCompletionTemplate::set_value_work(
            &mut value, journal.revision, acyclic_fs::WorkCounters::UNBOUNDED,
        )?;
        let (largest, largest_custody) = encode_record(&value, &self.work)?;
        let maximum_bytes = largest.len();
        drop(largest);
        drop(largest_custody);
        NativePreparationCompletionTemplate::set_value_work(&mut value, journal.revision, journal.work)?;
        if maximum_bytes > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "native completion scalar envelope exceeds record limit" });
        }
        let (mut output, output_custody) = allocate(&self.work, |io| super::fork_preparer_io::vector::<u8>(maximum_bytes, Some(io)))?;
        let initialized = acyclic_fs::WorkCounters {
            bytes_copied: output.capacity() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        };
        self.work.admit(initialized)?;
        output.resize(output.capacity(), 0);
        self.work.spent(initialized)?;
        let output = acyclic_stream::PreparedOwnedBytes::new(
            output, output_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let template = NativePreparationCompletionTemplate {
            journal, intent, path, key, value, output,
            maximum_bytes: maximum_bytes as u64, custody,
        };
        owner.verify_with_native_work(false, &self.work).await?;
        template.verify_owner(owner, &self.work)?;
        Ok(template)
    }

    async fn verify_read_owner(&self) -> Result<()> {
        match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement } => {
                let (outcome, custody) = owner.outcome_with_native_work(&self.work).await?;
                if outcome.is_none() {
                    owner.verify_with_native_work(*settlement, &self.work).await?;
                }
                drop(outcome);
                drop(custody);
                Ok(())
            }
            NativePreparationAuthority::Admission(witness) => {
                witness.verify_with_native_work(&self.work).await?;
                Ok(())
            }
        }
    }

    async fn load_record(
        &self, operation: acyclic_fs::OperationId, adopt_original_work: bool,
    ) -> Result<Option<(MaterializationJournal, acyclic_fs::NativeAllocation)>> {
        let path = self.path(operation)?;
        self.verify_read_owner().await?;
        let mut native_io = crate::store::history::native_work_io(&self.work)?;
        let mut observer = Some(&mut native_io as &mut dyn crate::store::history::HistoryIo);
        let bounds = match crate::store::operations::bounds_with_work(
            self.read_client(), &path, &mut observer,
        ).await? {
            Ok(bounds) => bounds,
            Err(error) if matches!(error.source(), acyclic_stream::StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into_error(&observer)),
        };
        let sequence = match bounds.tail.checked_sub(1) {
            Some(sequence) => sequence,
            None if !adopt_original_work => return Ok(None),
            None => return Err(preparation_error("empty materializer journal")),
        };
        self.verify_read_owner().await?;
        let stream = crate::store::operations::stream_with_work(self.read_client(), &path, &mut observer)?;
        let mut records = crate::store::operations::read_records_with_work(
            self.read_client(), &stream.stream, sequence, 1, &mut observer,
        ).await?.map_err(|error| error.into_error(&observer))?;
        let record = records.next(&mut observer).await?
            .ok_or(preparation_error("missing materializer journal"))?;
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(preparation_error("materializer journal exceeds Stream record limit"));
        }
        self.verify_read_owner().await?;
        let (journal, custody): (MaterializationJournal, _) = decode_record(
            &record.value, [std::mem::size_of::<acyclic_fs::MaterializationEdit>(),
                std::mem::size_of::<acyclic_fs::MaterializationPreparationSelection>(),
                std::mem::size_of::<acyclic_fs::MaterializationPreimage>(), std::mem::size_of::<u8>()]
                .into_iter().max().expect("nonempty concrete journal schema"), &self.work,
            |journal| acyclic_fs::materializer::journal_owned_bytes(journal).map_err(Into::into),
        )?;
        if record.sequence != sequence || journal.revision != bounds.tail || journal.plan.operation_id != operation {
            return Err(preparation_error("materializer journal revision or operation changed"));
        }
        if !journal.is_valid_for(operation) {
            return Err(preparation_error("materializer journal has incompatible structural progress"));
        }
        if adopt_original_work {
            self.work.set_original_work(journal.work);
        }
        Ok(Some((journal, custody)))
    }
}

impl<P: StreamProvider> MaterializationJournalStore for NativePreparationStore<'_, P> {
    type Error = Error;

    type ActionCompletion<'a> = crate::NativeFutureBundle<'a,
        (acyclic_fs::MaterializationJournal, acyclic_fs::NativeAllocation)> where Self: 'a;

    async fn prepare_action<'a>(
        &'a self, journal: acyclic_fs::MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>> {
        let owner = match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement: false } => *owner,
            _ => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native action acceptance requires the original running owner",
            }),
        };
        let mut template = self.completion_template(journal).await?;
        let maximum = template.codec_parts().2;
        let items = maximum.checked_add(24).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "native completion inspection allowance overflows",
        })?;
        let reservation = self.work.reserve_work(acyclic_fs::WorkCounters {
            backend_read_operations: 1,
            backend_write_operations: 1,
            authority_records_read: 1,
            authority_records_appended: 1,
            authority_bytes_read: maximum,
            authority_bytes_written: maximum,
            bytes_encoded: maximum,
            bytes_copied: maximum,
            items_examined: items,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        let mut native_io = crate::store::history::native_work_io(&self.work)
            .map_err(Error::from)?;
        let prepared = crate::distributed::PreparedMaterializationCommit::prepare(
            owner, template, reservation, &self.work, &mut Some(&mut native_io),
        ).await?;
        let future = crate::NativeFutureBundle::new(&self.work, || async move {
            let counters = prepared.completion_work()?;
            prepared.finish(&owner.stream(), counters, None).await
        })?;
        Ok(future)
    }

    async fn load(&self, operation: acyclic_fs::OperationId) -> Result<Option<acyclic_fs::MaterializationJournal>> {
        self.load_record(operation, true).await
            .map(|record| record.map(|(journal, custody)| { let mut journal = journal; journal.custody = Some(custody); journal }))
            .map_err(Error::from)
    }

    async fn compare_and_swap(
        &self, operation: acyclic_fs::OperationId, expected_revision: u64,
        mut replacement: acyclic_fs::MaterializationJournal,
    ) -> Result<(bool, acyclic_fs::MaterializationJournal)> {
        let (owner, settlement) = match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement } => (*owner, *settlement),
            NativePreparationAuthority::Admission(_) => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "read-only native admission cannot append a materializer journal",
            }),
        };
        if !replacement.custody.as_ref().is_some_and(|custody| custody.matches_work(&self.work)) {
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
        replacement.work = self.work.journal_work()?;
        let (encoded, encoded_custody) = encode_record(&replacement, &self.work)?;
        let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            encoded, encoded_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "materializer journal exceeds Stream record limit",
            });
        }
        self.work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64, ..acyclic_fs::WorkCounters::default()
        })?;
        let (key_bytes, key_custody) = self.work.copy_bytes(blake3::hash(&bytes).as_bytes())?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes, key_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
        let mut io = crate::store::history::native_work_io(&self.work)?;
        let committed = owner.append(
            path, expected_revision, &key, bytes,
            if settlement { crate::distributed::JournalWrite::Settlement }
            else { crate::distributed::JournalWrite::Fresh },
            &mut Some(&mut io),
        ).await?;
        Ok((committed, replacement))
    }
}

fn native_intent_control_bytes(intent: &NativePreparationIntent) -> Result<u64> {
    let overflow = || preparation_error("native intent payload overflow");
    let fixed = intent.operation_directory.capacity()
        .checked_add(intent.original_fence.reservation_id.capacity())
        .and_then(|n| n.checked_add(intent.original_fence.placement.capacity()))
        .and_then(|n| n.checked_add(intent.manifest.options.root.capacity()))
        .and_then(|n| intent.manifest.volumes.capacity().checked_mul(std::mem::size_of::<NativeVolumeMapping>())
            .and_then(|slots| n.checked_add(slots))).ok_or_else(overflow)? as u64;
    intent.manifest.volumes.iter().try_fold(fixed, |bytes, mapping| {
        let volume = mapping.volume.logical_owned_bytes()?;
        let generation = mapping.generation.logical_owned_bytes()?;
        bytes.checked_add(mapping.path.capacity() as u64)
            .and_then(|n| n.checked_add(volume))
            .and_then(|n| n.checked_add(generation))
            .ok_or_else(overflow)
    })
}


fn decode_record<T: serde::de::DeserializeOwned>(
    bytes: &[u8], maximum_slot: usize, work: &NativeMaterializationWork,
    retained_bytes: impl FnOnce(&T) -> Result<u64>,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let remaining = work.remaining()?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let io = super::PreparationIo::from_native_work(work)?;
    let value = crate::contract::json_from_slice_bounded_native_trusted(bytes, bounds, 0,
        crate::wire::ErrorCode::Storage, crate::contract::JsonValueSchema::None, &mut |charge| {
            if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes: scratch, value_shape } = charge {
                let owned = value_shape.lexical_items().checked_mul(10)
                    .and_then(|slots| slots.checked_mul(maximum_slot as u64))
                    .and_then(|slots| slots.checked_add(value_shape.input_bytes()))
                    .and_then(|owned| owned.checked_add(scratch))
                    .ok_or(acyclic_fs::WorkError::Overflow)
                    .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                return io.retain_owned(owned);
            }
            crate::store::history::HistoryIo::codec_charge(&mut io.history_io(), charge)
        });
    match value {
        Ok(value) => {
            let custody = io.into_native_custody(retained_bytes(&value)?)?;
            Ok((value, custody))
        }
        Err(error) => {
            let held = io.owned_bytes()?;
            Err(native_codec_error_custody(error, io.into_native_custody(held)?))
        }
    }
}

fn native_codec_error_custody(error: Error, custody: acyclic_fs::NativeAllocation) -> Error {
    match error {
        Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(custody) },
        Error::NativeDecode { code, source, custody: None } => Error::NativeDecode { code, source, custody: Some(custody) },
        Error::NativeEncode { code, source, custody: None } => Error::NativeEncode { code, source, custody: Some(custody) },
        error => error,
    }
}

fn encode_record<T: serde::Serialize + ?Sized>(
    value: &T, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let io = super::PreparationIo::from_native_work(work)?;
    io.retain_owned(crate::contract::sdk_json_diagnostic_bytes())?;
    let remaining = work.remaining()?;
    let bytes = crate::store::history::canonical_bytes_for_bounds_with_work(value,
        crate::contract::JsonBounds { maximum_bytes: remaining.bytes_encoded,
            maximum_items: remaining.items_examined, maximum_depth: 256 },
        &mut Some(&mut io.history_io()));
    match bytes {
        Ok(bytes) => {
            let custody = io.into_native_custody(bytes.capacity() as u64)?;
            Ok((bytes, custody))
        }
        Err(error) => {
            let held = io.owned_bytes()?;
            Err(native_codec_error_custody(error, io.into_native_custody(held)?))
        }
    }
}



pub(super) fn native_owned_bytes_failure(
    failure: acyclic_stream::PreparedOwnedBytesFailure<acyclic_fs::NativeAllocation>,
    work: &NativeMaterializationWork,
) -> Error {
    match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => Error::from(work.runtime_error(error)),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
        => Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native owned byte buffer violated its fixed initialized ownership invariant",
        },
    }
}



fn allocate<T>(work: &NativeMaterializationWork,
    copy: impl FnOnce(&super::PreparationIo) -> Result<T>,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let io = super::PreparationIo::from_native_work(work)?;
    let copied = copy(&io)?;
    let retained = io.owned_bytes()?;
    Ok((copied, io.into_native_custody(retained)?))
}

/// Copies one admitted mapping while retaining its actual original-ledger storage.
pub fn copy_native_mapping_with_work(
    mapping: &NativeVolumeMapping, work: &NativeMaterializationWork,
) -> Result<(NativeVolumeMapping, acyclic_fs::NativeAllocation)> {
    allocate(work, |io| {
        let volume = mapping.volume.clone_with_io(io)?;
        let generation = mapping.generation.clone_with_io(io)?;
        let (path, custody) = work.path(&mapping.path, None)?;
        io.absorb_native_custody(custody)?;
        Ok(NativeVolumeMapping { volume, generation, path, writable: mapping.writable,
            namespace: mapping.namespace.clone(), authority_revision: mapping.authority_revision })
    })
}
