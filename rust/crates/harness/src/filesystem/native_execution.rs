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
    sync::{Arc, atomic::{AtomicBool, Ordering}},
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

trait NativeRootOutputOwner: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn same_owner(&self, other: &dyn NativeRootOutputOwner) -> bool;
    fn read<'a>(
        &'a self, source: &'a NativeRootOutputSource, reference: &'a FileRef, maximum: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    fn verify_content<'a>(
        &'a self, source: &'a NativeRootOutputSource, reference: &'a FileRef, maximum: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn load_manifest<'a>(
        &'a self, source: &'a NativeRootOutputSource, reference: &'a FileRef, count: u32,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>>;
    fn stage_once<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: crate::OperationId,
        path: &'a str, bytes: &'a [u8], media_type: &'a str, display_name: &'a str,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>>;
    fn stage_at<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: crate::OperationId,
        reference: &'a FileRef, bytes: &'a [u8], work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>>;
    fn observe_staged_once<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: crate::OperationId,
        maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>>;
    fn verify<'a>(
        &'a self, context: &'a crate::runtime::TaskContext, volume: &'a VolumeRef,
        bind_operation: crate::OperationId, bind_intent: [u8; 32],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}

impl<P: StreamProvider + 'static> NativeRootOutputOwner for TaskJournalOwner<P> {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn same_owner(&self, other: &dyn NativeRootOutputOwner) -> bool {
        other.as_any().downcast_ref::<Self>()
            .is_some_and(|other| self.same_original_root_output_owner(other))
    }

    fn read<'a>(
        &'a self, source: &'a NativeRootOutputSource, reference: &'a FileRef, maximum: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        crate::NativeFutureBundle::new(work, || async move {
            authorize_native_root_output(self, source, VolumeOperation::Read, work).await?;
            source.binding.reader.read_with_native_work(reference, maximum, work)?.await
        })
    }

    fn verify_content<'a>(
        &'a self, source: &'a NativeRootOutputSource, reference: &'a FileRef, maximum: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            authorize_native_root_output(self, source, VolumeOperation::Read, work).await?;
            source.binding.reader.verify_with_native_work(reference, maximum, work)?.await
        })
    }

    fn load_manifest<'a>(
        &'a self, source: &'a NativeRootOutputSource, reference: &'a FileRef, count: u32,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            authorize_native_root_output(self, source, VolumeOperation::Read, work).await?;
            source.binding.reader.load_manifest_with_native_work(reference, count, work)?.await
        })
    }

    fn stage_once<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: crate::OperationId,
        path: &'a str, bytes: &'a [u8], media_type: &'a str, display_name: &'a str,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            authorize_native_root_output(self, source, VolumeOperation::Write, work).await?;
            source.original_writer().stage_once_with_native_work(
                operation, path, bytes, media_type, display_name, work,
            )?.await
        })
    }

    fn stage_at<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: crate::OperationId,
        reference: &'a FileRef, bytes: &'a [u8], work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            authorize_native_root_output(self, source, VolumeOperation::Write, work).await?;
            source.original_writer().stage_at_with_native_work(operation, reference, bytes, work)?.await
        })
    }

    fn observe_staged_once<'a>(
        &'a self, source: &'a NativeRootOutputSource, operation: crate::OperationId,
        maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            authorize_native_root_output(self, source, VolumeOperation::Write, work).await?;
            source.original_writer().observe_staged_once_with_native_work(operation, maximum, work)?.await
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

async fn authorize_native_root_output<P: StreamProvider>(
    owner: &TaskJournalOwner<P>, source: &NativeRootOutputSource,
    operation: VolumeOperation, work: &NativeMaterializationWork,
) -> Result<()> {
    owner.require_volume_grant_with_native_work(source.original_writer().volume(), operation, work)?;
    let proof = owner.authenticated_root_binding_with_native_work(work).await?;
    if proof.bind_operation() != source.bind_operation || proof.intent_digest() != source.bind_intent {
        return Err(native_root_output_denial("ROOT content differs from its original protected binding"));
    }
    Ok(())
}

impl NativeRootOutputSource {
    pub(crate) fn content_binding(&self) -> &crate::runtime::ContentBindings {
        &self.binding
    }

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
        }).map_err(Error::from)?;
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
        self.owner.read(self, reference, maximum, work)
    }

    fn verify_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_content_work(reference.volume(), work)?;
        self.owner.verify_content(self, reference, maximum, work)
    }

    fn load_manifest_with_native_work<'a>(
        &'a self, reference: &'a FileRef, count: u32, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(reference.volume(), work)?;
        self.owner.load_manifest(self, reference, count, work)
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
        self.owner.stage_once(self, operation, path, bytes, media_type, display_name, work)
    }

    fn stage_at_with_native_work<'a>(
        &'a self, operation: crate::OperationId, reference: &'a FileRef, bytes: &'a [u8],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(reference.volume(), work)?;
        self.owner.stage_at(self, operation, reference, bytes, work)
    }

    fn observe_staged_once_with_native_work<'a>(
        &'a self, operation: crate::OperationId, maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        self.require_content_work(self.volume(), work)?;
        self.owner.observe_staged_once(self, operation, maximum, work)
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
    let (encoded, _custody) = native_canonical(
        &("harness/native-root-output-bind/v2", audience, volume,
            crate::OperationId::from_bytes(creation.into_bytes())), work,
    )?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
    }).map_err(Error::from)?;
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&blake3::hash(&encoded).as_bytes()[..16]);
    Ok(crate::OperationId::from_bytes(bytes))
}

/// Issued immutable working view, retaining its actual prepared view and owner.
pub struct NativeTaskWorkingView {
    held: Arc<dyn NativeTaskWorkingViewHolder>,
    custody: acyclic_fs::NativeAllocation,
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

/// Keeps the genuine process proof and every completed mount on failure.
pub struct NativeTaskViewDrainFailure {
    pub cause: Error,
    pub partial_receipt: NativeTaskViewDrainReceipt,
}

struct NativeTaskViewDrainReturn<'a> {
    receipt: Option<NativeTaskViewDrainReceipt>,
    target: &'a mut Option<NativeTaskViewDrainReceipt>,
}

impl Drop for NativeTaskViewDrainReturn<'_> {
    fn drop(&mut self) {
        if let Some(receipt) = self.receipt.take() {
            *self.target = Some(receipt);
        }
    }
}

trait NativeTaskWorkingViewHolder: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn same_identity(&self, other: &dyn NativeTaskWorkingViewHolder) -> bool;
    fn volume_for_builtin_path<'a>(&'a self, path: &'a Path, operation: VolumeOperation,
        work: &NativeMaterializationWork) -> Result<&'a VolumeRef>;
    fn original_task(&self) -> crate::TaskId;
    fn original_fence(&self) -> &crate::scheduler::LeaseFence;
    fn manifest(&self) -> &NativeViewManifest;
    fn original_native_work(&self) -> &NativeMaterializationWork;
    fn verify<'a>(&'a self, work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn read<'a>(&'a self, operation: crate::OperationId, path: &'a Path, maximum: usize,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    fn write<'a>(&'a self, operation: crate::OperationId, path: &'a Path, content: &'a str,
        parents: bool, expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>>;
}

struct HeldNativeTaskWorkingView<P, A, O> {
    view: Arc<NativeVolumeView<A, O>>,
    owner: Arc<TaskJournalOwner<P>>,
}

impl NativeTaskWorkingView {
    /// Original admitted task.
    pub fn original_task(&self) -> crate::TaskId { self.held.original_task() }
    pub(crate) fn original_fence(&self) -> &crate::scheduler::LeaseFence { self.held.original_fence() }
    /// Pinned namespace declaration.
    pub fn manifest(&self) -> &NativeViewManifest { self.held.manifest() }
    /// Original materialization ledger.
    pub fn original_native_work(&self) -> &NativeMaterializationWork { self.held.original_native_work() }
    /// Exact held view and original owner identity; this does not grant authority.
    pub fn same_identity(&self, other: &Self) -> bool {
        self.held.same_identity(other.held.as_ref())
    }
    /// Pure selection through the same bounded parser used by physical IO.
    pub fn volume_for_builtin_path<'a>(&'a self, path: &'a Path, operation: VolumeOperation,
        work: &NativeMaterializationWork) -> Result<&'a VolumeRef> {
        self.require_work(work)?;
        self.held.volume_for_builtin_path(path, operation, work)
    }
    fn require_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(Error::from)
    }
    /// Authenticates the held view on its original ledger.
    pub fn verify_with_native_work<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_work(work)?;
        self.held.verify(work)
    }
    /// Forwards intrinsically funded physical bytes without rewrapping them.
    pub fn read_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path,
        maximum: usize, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        self.require_work(work)?;
        self.held.read(operation, path, maximum, work)
    }
    /// Writes an authorized path on the held view.
    pub fn write_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path,
        content: &'a str, parents: bool, expected: Option<[u8; 32]>,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_work(work)?;
        self.held.write(operation, path, content, parents, expected, work)
    }
}

impl<P, A, O> HeldNativeTaskWorkingView<P, A, O>
where P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn require_original(&self, work: &NativeMaterializationWork) -> Result<()> {
        if self.view.original_work.is_none() || !self.view._allocation.matches_work(work)
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

impl<P, A, O> NativeTaskWorkingViewHolder for HeldNativeTaskWorkingView<P, A, O>
where P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn same_identity(&self, other: &dyn NativeTaskWorkingViewHolder) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other|
            Arc::ptr_eq(&self.view, &other.view) && Arc::ptr_eq(&self.owner, &other.owner))
    }
    fn volume_for_builtin_path<'a>(&'a self, path: &'a Path, operation: VolumeOperation,
        work: &NativeMaterializationWork) -> Result<&'a VolumeRef> {
        self.require_original(work)?;
        self.view.builtin_volume(path, operation, work).map(|(mapping, _, _)| &mapping.volume)
    }
    fn original_task(&self) -> crate::TaskId { self.view.preparation.task }
    fn original_fence(&self) -> &crate::scheduler::LeaseFence { &self.view.preparation.original_fence }
    fn manifest(&self) -> &NativeViewManifest { &self.view.manifest }
    fn original_native_work(&self) -> &NativeMaterializationWork {
        self.view.original_work.as_ref().expect("issued view retains original work")
    }
    fn verify<'a>(&'a self, work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || self.authenticate(work))
    }
    fn read<'a>(&'a self, operation: crate::OperationId, path: &'a Path, maximum: usize,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.view.read_builtin_file(&self.owner, operation, path, maximum, work).await
        })
    }
    fn write<'a>(&'a self, operation: crate::OperationId, path: &'a Path, content: &'a str,
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
    ) -> Result<crate::NativeFutureBundle<'a, NativeTaskWorkingView>> {
        crate::NativeFutureBundle::new(work, || async move {
            let custody = work.retain_owned((std::mem::size_of::<NativeTaskWorkingView>()
                + std::mem::size_of::<HeldNativeTaskWorkingView<P, A, O>>()) as u64)
                .map_err(Error::from)?;
            let held = HeldNativeTaskWorkingView { view: Arc::clone(self), owner: Arc::clone(owner) };
            held.authenticate(work).await?;
            work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
            let resource = NativeTaskWorkingView { held: Arc::new(held), custody };
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
    /// Admit the dynamic frame before bounded physical checks.
    fn validate_for_dispatch(
        &self, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<crate::NativeFutureBundle<'_, std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)>>, (Error, acyclic_fs::WorkCounters)>;
    /// Observe an original held root and no-follow relative directory.
    fn validate_working_directory<'a>(
        &'a self, cwd: &'a Path, budget: acyclic_fs::WorkBudget,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::OperationReceipt<()>, (Error, acyclic_fs::WorkCounters)>>, (Error, acyclic_fs::WorkCounters)>;
    /// Admit publication's dynamic frame; completed destinations remain final.
    fn publish<'a>(
        &'a self, owner: &'a TaskJournalOwner<P>,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>, (Error, acyclic_fs::WorkCounters)>;
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
        &'a self, owner: &'a TaskJournalOwner<P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>>;
}

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Reserve a real SDK gate for the original qualified destination and immutable generation.
    #[allow(clippy::too_many_arguments, reason = "original selection, work and cleanup tokens must not be substituted")]
    pub async fn reserve_publication<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        &self, workspace: &acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<NativePublicationReservation<A, O>> {
        self.acquire_publication(
            workspace, volume, original_generation, original_operation, work,
            cancellation, cleanup_cancellation, NativePublicationAcquisition::Reserve,
        ).await?.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
            reason: "native reservation acknowledgement has no original proof" })
    }

    /// Observe only an already active gate for the original filesystem operation.
    #[allow(clippy::too_many_arguments, reason = "recovery retains the same original selection and lifetime tokens")]
    pub async fn recover_publication<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        &self, workspace: &acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<Option<NativePublicationReservation<A, O>>> {
        self.acquire_publication(
            workspace, volume, original_generation, original_operation, work,
            cancellation, cleanup_cancellation, NativePublicationAcquisition::ReadonlyExisting,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "one engine validates both original acquisition decisions")]
    async fn acquire_publication<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        &self, workspace: &acyclic_fs::Workspace<A, O>, volume: &VolumeRef,
        original_generation: acyclic_fs::GenerationId, original_operation: acyclic_fs::OperationId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken,
        cleanup_cancellation: &acyclic_fs::CancellationToken, mode: NativePublicationAcquisition,
    ) -> Result<Option<NativePublicationReservation<A, O>>> {
        let mut failure_slot = Some(NativeReservationFailureSlot::new(work)?);
        let workspace = workspace.clone();
        let mut reservation = None;
        let mut cleanup_binding = None;
        let result = match crate::NativeFutureBundle::new(work, || async { Ok(async {
            self.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
                .map_err(NativeReservationError::Owner)?;
            cleanup_binding = Some(NativePublicationCleanupBinding {
                task: self.task_binding().0,
                fence: native_publication_fence_digest(self, work).map_err(NativeReservationError::Owner)?,
                volume: volume.clone_with_native_work(work).map_err(NativeReservationError::Owner)?,
            });
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
            let acquired = match mode {
                NativePublicationAcquisition::Reserve => workspace
                    .reserve_publication_with_native_work(original_operation, work, cancellation).await
                    .map(Some),
                NativePublicationAcquisition::ReadonlyExisting => workspace
                    .publication_reservation_with_native_work(original_operation, work, cancellation).await,
            };
            reservation = acquired.map_err(|(cause, custody)| NativeReservationError::Sdk { cause, custody })?;
            self.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
                .map_err(NativeReservationError::Owner)?;
            if reservation.is_none() {
                return Ok(());
            }
            let head = workspace.head_with_native_work(work, cancellation)
                .await.map_err(|(cause, custody)| NativeReservationError::Sdk { cause, custody })?;
            if head.id() != original_generation {
                return Err(NativeReservationError::Selection("native reservation generation differs from original selection"));
            }
            drop(head);
            self.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
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
            Ok(()) => return Ok(reservation.map(|reservation| NativePublicationReservation {
                cleanup: NativePublicationCleanup { workspace, reservation,
                    binding: cleanup_binding.expect("reservation follows original binding"), failure_slot },
            })),
            Err(error) => error,
        };
        let failure = if let Some(reservation) = reservation {
            let cleanup = NativePublicationCleanup { workspace, reservation,
                binding: cleanup_binding.expect("reservation follows original binding"), failure_slot: None };
            if matches!(mode, NativePublicationAcquisition::ReadonlyExisting) {
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

#[derive(Clone, Copy)]
enum NativePublicationAcquisition {
    Reserve,
    ReadonlyExisting,
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
    let encoded = native_canonical(owner.task_binding().1, work)?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.0.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
    }).map_err(Error::from)?;
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

/// Allocation-free reservation failure classification with original causes.
#[derive(Debug, thiserror::Error)]
pub enum NativeReservationError {
    /// Original Root owner/grant/codec error, moved without formatting.
    #[error(transparent)]
    Owner(Error),
    /// Actual native ledger, cancellation, admission, or IO failure.
    #[error(transparent)]
    Native(acyclic_fs::NativeTreeMaterializationError),
    /// Actual SDK cause drops before its original native allocation custody.
    #[error("{cause}")]
    Sdk {
        #[source]
        cause: acyclic_fs::WorkspaceError,
        custody: Option<acyclic_fs::NativeAllocation>,
    },
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
            std::mem::size_of::<NativePublicationReservationFailure<A, O>>() as u64, 1,
        ).map_err(Error::from)?;
        Ok(Self { value: Box::new_uninit(), custody })
    }

    fn retain(self, failure: NativePublicationReservationFailure<A, O>) -> Error {
        let code = match &failure.error {
            NativeReservationError::Owner(error) => error.code(),
            NativeReservationError::Native(cause) => crate::contract::native_work_error_code(cause),
            NativeReservationError::Sdk { .. } => crate::wire::ErrorCode::Storage,
            NativeReservationError::Selection(_) => crate::wire::ErrorCode::Invalid,
        };
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
    ) -> std::result::Result<bool, NativeReservationError> {
        let Error::NativeReservation { source, custody, .. } = error else { return Ok(false); };
        if !custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(NativeReservationError::Selection(
                "native cleanup differs from the original operation and resource",
            ));
        }
        let Some(failure) = source.downcast_mut::<Self>() else { return Ok(false); };
        let Some(cleanup) = failure.cleanup.as_ref() else { return Ok(false); };
        let mut released = false;
        let result = match crate::NativeFutureBundle::new(work, || async { Ok(async {
            cleanup.binding.verify(owner, work).map_err(NativeReservationError::Owner)?;
            owner.verify_with_native_work(true, work).await.map_err(NativeReservationError::Owner)?;
            cleanup.workspace.release_publication_with_native_work(cleanup.reservation, work, cancellation)
                .await.map_err(|(cause, custody)| NativeReservationError::Sdk { cause, custody })?;
            released = true;
            owner.verify_with_native_work(true, work).await.map_err(NativeReservationError::Owner)?;
            cleanup.binding.verify(owner, work).map_err(NativeReservationError::Owner)
        }.await) }) {
            Ok(future) => match future.await {
                Ok(result) => result,
                Err(error) => Err(NativeReservationError::Owner(error)),
            },
            Err(error) => Err(NativeReservationError::Owner(error)),
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
                error: NativeReservationError::Selection("native cleanup differs from the original operation and resource"),
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
        let result = match crate::NativeFutureBundle::new(work, || async { Ok(async {
            self.binding.verify(owner, work).map_err(NativeReservationError::Owner)?;
            owner.verify_with_native_work(true, work).await.map_err(NativeReservationError::Owner)?;
            self.workspace.release_publication_with_native_work(self.reservation, work, cancellation)
                .await.map_err(|(cause, custody)| NativeReservationError::Sdk { cause, custody })?;
            released = true;
            owner.verify_with_native_work(true, work).await.map_err(NativeReservationError::Owner)?;
            self.binding.verify(owner, work).map_err(NativeReservationError::Owner)
        }.await) }) {
            Ok(future) => match future.await {
                Ok(result) => result,
                Err(error) => Err(NativeReservationError::Owner(error)),
            },
            Err(error) => Err(NativeReservationError::Owner(error)),
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
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativeLazyPublicationFailure>>>;
    fn validate_selected_snapshot<'a>(&'a self, frozen: acyclic_fs::LazySnapshotRef,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn begin_scope(&self, work: &NativeMaterializationWork)
        -> Result<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope>;
    fn read<'a>(&'a self, relative: &'a Path, maximum: usize, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    fn write<'a>(&'a self, relative: &'a Path, content: &'a str, parents: bool,
        expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn sync<'a>(&'a self, permit: PublicationPermit, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativeLazyPublicationFailure>>>;
    fn retire(&self, wait: &acyclic_fs::CancellationToken)
        -> Result<acyclic_fs::native_mount::NativeMountDrainProof>;
}

struct NativeLazyPublicationFailure {
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
    work: NativeMaterializationWork,
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
            let actual = mount.snapshot_with_work(&work.native_mount_work(), work.cancellation()).await.map_err(Error::from)?;
            if &actual != snapshot || snapshot.workspace_id != workspace.id()
                || snapshot.authored_generation != generation.id()
            {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let callback = guard.for_volume(owner, binding.volume, binding.writable, work)?.publication;
            let custody = work.retain_owned(std::mem::size_of::<Self>() as u64).map_err(Error::from)?;
            let mut holder_custody = work.retain_owned(std::mem::size_of::<HeldLazyVolume<P, A, O, D, S>>() as u64)
                .map_err(Error::from)?;
            let (volume, mut volume_custody) = binding.volume.clone_with_native_work(work)?;
            holder_custody.absorb(&mut volume_custody).map_err(Error::from)?;
            let (generation, mut generation_custody) = crate::resources::GenerationRef::new_with_native_work(
            binding.generation.as_resource().provider(), binding.generation.as_resource().key(),
            binding.generation.as_resource().version(), work,
        )?;
            holder_custody.absorb(&mut generation_custody).map_err(Error::from)?;
            let (canonical, _canonical_custody) = native_canonical(&(binding.verifier.audience(), binding.scope), work)?;
            work.charge(acyclic_fs::WorkCounters { bytes_hashed: canonical.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
            let authority_revision = *blake3::hash(&canonical).as_bytes();
            work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
            let held = Arc::new(HeldLazyVolume { mount: Arc::clone(mount), owner: Arc::clone(owner),
                callback, identity, volume, generation, writable: binding.writable, publication: binding.publication,
                authority_revision, work: work.clone(), custody: holder_custody });
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
    fn original_work(&self) -> &NativeMaterializationWork { &self.work }
    fn snapshot<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let snapshot = self.mount.snapshot_with_work(&work.native_mount_work(), work.cancellation())
                .await.map_err(Error::from)?;
            self.check(work).await?;
            Ok(snapshot)
        })
    }
    fn publication_snapshot<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativeLazyPublicationFailure>>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let snapshot = match self.mount.publication_snapshot_with_work(
                &work.native_mount_work(), work.cancellation(),
            ).await {
                Ok(snapshot) => snapshot,
                Err((cause, accounting)) => return Ok(Err(NativeLazyPublicationFailure {
                    cause: Error::from(cause), accounting_failure: accounting.map(Error::from),
                    source_acknowledged: false, publication: None,
                })),
            };
            if let Err(cause) = self.check(work).await {
                return Ok(Err(NativeLazyPublicationFailure {
                    cause, accounting_failure: None, source_acknowledged: false,
                    publication: Some(snapshot),
                }));
            }
            Ok(Ok(snapshot))
        })
    }
    fn validate_selected_snapshot<'a>(&'a self, frozen: acyclic_fs::LazySnapshotRef,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.mount.validate_snapshot_with_work(frozen, &work.native_mount_work())
                .await.map_err(Error::from)
        })
    }
    fn begin_scope(&self, work: &NativeMaterializationWork)
        -> Result<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope> {
        self.require_work(work)?;
        self.mount.begin_original_work(&work.native_mount_work()).map_err(Error::from)
    }
    fn read<'a>(&'a self, relative: &'a Path, maximum: usize, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        if !work.shares_ledger(&self.work) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let lease = self.mount.native_source_io_lease()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
            let physical = work.with_source_io_lease(lease).map_err(Error::from)?;
            self.check(&physical).await?;
            self.mount.with_original_work(&physical.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                let physical = &physical;
                async move {
                    let scope = scope.map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
                    physical.read_working_file_guarded_with_scope(scope, self.path(), self.identity, relative, maximum,
                        || self.check(physical)).await
                }
            }).await.map_err(Error::from)?
        })
    }
    fn write<'a>(&'a self, relative: &'a Path, content: &'a str, parents: bool,
        expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        if !work.shares_ledger(&self.work) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let lease = self.mount.native_source_io_lease()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
            let physical = work.with_source_io_lease(lease).map_err(Error::from)?;
            self.check(&physical).await?;
            self.mount.with_original_work(&physical.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                let physical = &physical;
                async move {
                    let scope = scope.map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
                    physical.write_working_file_guarded_with_scope(scope, self.path(), self.identity, relative,
                        content, parents, expected, || self.check(physical)).await
                }
            }).await.map_err(Error::from)?
        })
    }
    fn sync<'a>(&'a self, permit: PublicationPermit, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<acyclic_fs::NativePublicationSnapshot, NativeLazyPublicationFailure>>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let callback = Arc::clone(&self.callback);
            let result = self.mount.sync_with_permit_work_guarded(
                permit, &work.native_mount_work(), work.cancellation(),
                move |actual: &NativeMaterializationWork| {
                    let callback = Arc::clone(&callback);
                    let actual = actual.clone();
                    async move { callback.check(&actual)?.await }
                },
            ).await;
            Ok(result.map_err(|failure| NativeLazyPublicationFailure {
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
        native_preparation_operation(owner)
    }

    /// Witnesses existing host allocations before preparation.
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
    pub fn witness_with_native_work<'a>(
        root: &'a std::path::Path,
        operation_directory: &'a std::path::Path,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Self, acyclic_fs::NativeAllocation)>> {
        use acyclic_fs::MaterializationBackend as _;
        if !work.remaining().map_err(Error::from)?.has_finite_allowances() || !root.is_absolute() || !operation_directory.is_absolute() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation witness requires absolute paths and finite allowances" });
        }
        crate::NativeFutureBundle::new(work, || async {
        let (root, mut custody) = work.copy_path(root).map_err(Error::from)?;
        let (operation_directory, mut operation_custody) = work.copy_path(operation_directory).map_err(Error::from)?;
        custody.absorb(&mut operation_custody).map_err(Error::from)?;
        let meter = work.clone();
        acyclic_native_runtime::run_blocking_io_measured(move || {
            let backend = acyclic_fs::NativeTreeMaterializationBackend::open_owned_with_work(
                root, operation_directory, custody, meter.clone(),
            ).map_err(Error::from)?;
            backend.require_initial_empty().map_err(Error::from)?;
            let expected_identity = acyclic_fs::NativeRootIdentity::from_bytes(
                backend.allocation_identity().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict, reason: "native root allocation is unwitnessed" })?,
            );
            let expected_preparation_identity = backend.preparation_identity()
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native staging allocation is unwitnessed" })?;
            let (root, mut custody) = meter.copy_path(backend.root_path()).map_err(Error::from)?;
            let (operation_directory, mut operation_custody) = meter.copy_path(backend.operation_directory_path()).map_err(Error::from)?;
            custody.absorb(&mut operation_custody).map_err(Error::from)?;
            Ok::<_, Error>((Self { root, operation_directory, expected_identity, expected_preparation_identity }, custody))
        }, work.runtime_meter_owner())
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
    fn into_owner_error(self) -> Error {
        match self {
            Self::Owner(error) => error,
            Self::Native(cause) => Error::NativeWork { cause, custody: None },
            Self::Backend(source) => Error::NativeStream {
                code: if matches!(&source, acyclic_stream::StreamError::IdempotencyMismatch) {
                    crate::wire::ErrorCode::Conflict
                } else { crate::wire::ErrorCode::Storage },
                source, custody: None,
            },
            Self::Record(reason) => Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason },
        }
    }
}

struct PreparedVolume<A, O> {
    view: NativePreparedVolumeBackend<A, O>,
    callback: Option<Arc<dyn crate::runtime::NativeVolumePublicationGuard>>,
    original_identity: acyclic_fs::NativeRootIdentity,
    writable: bool,
    publication: PublicationPermit,
    _work: NativeMaterializationWork,
}

enum NativePreparedVolumeBackend<A, O> {
    Materialized(NativeWorkingSet<A, O>),
    HeldLazy(Arc<dyn NativeHeldLazyVolume>),
}

impl<A: AsyncAuthorityStore + Send + Sync + 'static, O: AsyncObjectStore + Send + Sync + 'static>
    NativePreparedVolumeBackend<A, O> {
    fn path(&self) -> &Path {
        match self {
            Self::Materialized(view) => view.path(),
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
    /// A denied diagnostic preserves both this cause and the actual denial.
    pub rendering_failure: Option<Error>,
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

enum NativePreparationCallbackGuards<'a, P: StreamProvider> {
    Ordinary(&'a Arc<TaskJournalOwner<P>>),
    Native(&'a [crate::runtime::NativeVolumeCallbackGuard<P>]),
}

struct OrdinaryVolumeCallback<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    volume: VolumeRef,
    writable: bool,
    work: NativeMaterializationWork,
    custody: acyclic_fs::NativeAllocation,
}

impl<P: StreamProvider + 'static> OrdinaryVolumeCallback<P> {
    async fn new(
        owner: &Arc<TaskJournalOwner<P>>, volume: &VolumeRef, writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<crate::runtime::NativeVolumeCallbackPorts> {
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, work)?;
        if writable {
            owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        }
        owner.verify_with_native_work(false, work).await?;
        let mut custody = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(Error::from)?;
        let (volume, mut volume_custody) = volume.clone_with_native_work(work)?;
        custody.absorb(&mut volume_custody).map_err(Error::from)?;
        work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
            ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
        let callback = Arc::new(Self { owner: Arc::clone(owner), volume, writable,
            work: work.clone(), custody });
        Ok(crate::runtime::NativeVolumeCallbackPorts {
            sdk: callback.clone(), publication: callback,
        })
    }

    async fn verify_with_work(
        &self, work: &NativeMaterializationWork, require_write: bool,
    ) -> Result<()> {
        if require_write && !self.writable {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "read-only original volume callback cannot authorize authored writes",
            });
        }
        if !self.work.shares_allocation_context(work) || !self.custody.matches_work(work) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Read, work)?;
        if require_write {
            self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Write, work)?;
        }
        self.owner.verify_with_native_work(false, work).await?;
        self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Read, work)?;
        if require_write {
            self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Write, work)?;
        }
        self.owner.verify_with_native_work(false, work).await
    }

    fn sdk_check<'a>(
        &'a self, native: &'a acyclic_fs::native_mount::NativeMountMeterWork,
        require_write: bool,
    ) -> std::result::Result<acyclic_fs::native_mount::NativeMountCallbackFuture<'a>,
        acyclic_fs::native_mount::NativeMountError> {
        acyclic_fs::native_mount::NativeMountCallbackFuture::new_guarded::<Error, _, _>(native, || async move {
            let work = native.native_materialization_work();
            self.verify_with_work(&work, require_write).await
        })
    }
}

impl<P: StreamProvider + 'static> acyclic_fs::native_mount::NativeMountCallbackGuard
    for OrdinaryVolumeCallback<P> {
    fn check<'a>(&'a self, native: &'a acyclic_fs::native_mount::NativeMountMeterWork)
        -> std::result::Result<acyclic_fs::native_mount::NativeMountCallbackFuture<'a>,
            acyclic_fs::native_mount::NativeMountError> {
        self.sdk_check(native, false)
    }

    fn check_write<'a>(&'a self, native: &'a acyclic_fs::native_mount::NativeMountMeterWork)
        -> std::result::Result<acyclic_fs::native_mount::NativeMountCallbackFuture<'a>,
            acyclic_fs::native_mount::NativeMountError> {
        self.sdk_check(native, true)
    }
}

impl<P: StreamProvider + 'static> crate::runtime::NativeVolumePublicationGuard for OrdinaryVolumeCallback<P> {
    fn check<'a>(&'a self, work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || self.verify_with_work(work, true))
    }
}

/// Provider-owned materialized namespace, with the canonical SDK capture owner for every independently authorized volume.
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
        &'a self, owner: &'a TaskJournalOwner<P>,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>, (Error, acyclic_fs::WorkCounters)> {
        NativeVolumeView::publish(self, owner)
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
                .map_err(Error::from)?;
            self.validate_directory_with_work(cwd, work)
        })
    }
    fn retain_native_namespace_scopes<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessNamespaceScopes>> {
        crate::NativeFutureBundle::new(work, || async {
            self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(Error::from)?;
            work.charge(acyclic_fs::WorkCounters { items_examined: self.volumes.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
            let count = self.volumes.iter().filter(|volume|
                matches!(volume.view, NativePreparedVolumeBackend::HeldLazy(_))).count();
            let mut custody = work.retain_owned(std::mem::size_of::<NativeProcessNamespaceScopes>() as u64)
                .map_err(Error::from)?;
            let (scopes, mut vector_custody) = native_vec(count, work)?;
            custody.absorb(&mut vector_custody).map_err(Error::from)?;
            let mut held = NativeProcessNamespaceScopes { scopes, custody };
            for volume in &self.volumes {
                work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                    ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
                if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                    lazy.snapshot(lazy.original_work())?.await?;
                    held.scopes.push(lazy.begin_scope(lazy.original_work())?);
                }
            }
            Ok(held)
        })
    }
    fn publish_with_native_work<'a>(
        &'a self, owner: &'a TaskJournalOwner<P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>> {
        let custody = work.retain_owned(0).map_err(Error::from)?;
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
    pub fn original_native_work(&self) -> Option<&NativeMaterializationWork> {
        self.original_work.as_ref()
    }

    /// Retires only this held view's SDK mounts on their accepted birth credit.
    pub fn cleanup_mounts_with_native_work<'a, P: StreamProvider + 'static>(
        &'a self, owner: &'a TaskJournalOwner<P>,
        process: &'a mut Option<super::native_process::NativeProcessDrainProof>,
        partial: &'a mut Option<NativeTaskViewDrainReceipt>,
        original: &'a NativeMaterializationWork,
        work: &'a NativeMaterializationWork,
        wait: &'a acyclic_fs::CancellationToken,
    ) -> Result<crate::NativeFutureBundle<'a,
        std::result::Result<NativeTaskViewDrainReceipt, NativeTaskViewDrainFailure>>> {
        crate::NativeFutureBundle::new(work, || async move {
            wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(Error::from)?;
            if !work.remaining()?.has_finite_allowances()
                || !self.original_work.as_ref()
                    .is_some_and(|birth| birth.shares_allocation_context(original))
                || !self._allocation.matches_work(original)
                || !owner.native_shared_work_matches(original)
                || self.manifest.volumes.len() != self.volumes.len()
                || process.is_some() == partial.is_some()
            {
                return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            work.charge(acyclic_fs::WorkCounters {
                items_examined: self.volumes.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED
            }).map_err(Error::from)?;
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
            let receipt = if partial.is_some() {
                wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(Error::from)?;
                work.remaining()?;
                partial.take().ok_or_else(||
                    Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?
            } else {
                let mut custody = work.retain_owned(
                    std::mem::size_of::<NativeTaskViewDrainFailure>() as u64,
                ).map_err(Error::from)?;
                let mut vector = native_vec(self.volumes.len(), work)?;
                custody.absorb(&mut vector.1).map_err(Error::from)?;
                wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(Error::from)?;
                work.remaining()?;
                let process = process.take().ok_or_else(||
                    Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
                let (mounts, _empty_vector_custody) = vector;
                NativeTaskViewDrainReceipt { process, mounts, custody }
            };
            let mut returned = NativeTaskViewDrainReturn { receipt: Some(receipt), target: partial };
            let result: Result<()> = async {
                let receipt = returned.receipt.as_mut()
                    .expect("the accepted drain receipt remains held until return");
                while receipt.mounts.len() < self.volumes.len() {
                    wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                        .map_err(Error::from)?;
                    self.authenticate_mount_retirement(owner, work).await?;
                    let volume = &self.volumes[receipt.mounts.len()];
                    let proof = match &volume.view {
                        NativePreparedVolumeBackend::Materialized(view) =>
                            view.retire_with_work(wait).map_err(Error::from)?,
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
                wait.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(Error::from)?;
                work.remaining()?;
                self.authenticate_mount_retirement(owner, work).await
            }.await;
            let receipt = returned.receipt.take()
                .expect("the accepted drain receipt remains held until return");
            Ok(match result {
                Ok(()) => Ok(receipt),
                Err(cause) => Err(NativeTaskViewDrainFailure { cause, partial_receipt: receipt }),
            })
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
        for mapping in &self.manifest.volumes {
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
        if !self.original_work.as_ref().is_some_and(|original| original.shares_ledger(work))
            || !self._allocation.work().shares_ledger(work)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native view Arc requires its actual original operation" });
        }
        let mut custody = work.claim(std::mem::size_of::<Self>() as u64).map_err(Error::from)?;
        self._allocation.absorb(&mut custody).map_err(Error::from)?;
        Ok(Arc::new(self))
    }

    /// Creates or resumes the original task's journaled physical preparation.
    #[inline(never)]
    pub async fn prepare<'a, P: StreamProvider + 'static>(
        owner: &'a Arc<TaskJournalOwner<P>>,
        allocation: NativePreparationRoot,
        options: NativeViewOptions,
        bindings: Vec<BorrowedNativeVolumeBinding<'a, A, O>>,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Self>, (Error, acyclic_fs::WorkCounters)> {
        let budget = options.budget_for_volume_count(bindings.len())
            .map_err(|error| (error, acyclic_fs::WorkCounters::default()))?;
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, spent)| (Error::NativeConstruction { cause, work: spent }, spent))?;
        let result = crate::NativeFutureBundle::new(&work, || Self::prepare_original(
            owner, NativePreparationCallbackGuards::Ordinary(owner),
            &allocation, &options, &bindings, &[], &work,
        )).map_err(|error| (error, work.receipt()))?.await;
        native_result(result, &work)
    }

    /// Prepares one immutable mixed declaration before any Scratch staging.
    pub fn prepare_with_native_work<'a, P: StreamProvider + 'static>(
        owner: &'a TaskJournalOwner<P>, guards: &'a [crate::runtime::NativeVolumeCallbackGuard<P>],
        allocation: &'a NativePreparationRoot, options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>], work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        if guards.len() != bindings.len() || (!existing_lazy.is_empty() && existing_lazy.len() != bindings.len()) {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        for (index, (guard, binding)) in guards.iter().zip(bindings).enumerate() {
            let lazy = existing_lazy.get(index).copied().flatten();
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
        crate::NativeFutureBundle::new(work, || Self::prepare_original(owner,
            NativePreparationCallbackGuards::Native(guards), allocation, options, bindings, existing_lazy, work))
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
            let actual = lazy.held.original_work().root_identity(lazy.held.path()).map_err(Error::from)?;
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

    #[allow(clippy::too_many_lines, reason = "one existing-engine preparation and attachment transaction")]
    async fn prepare_original<'a, P: StreamProvider + 'static>(
        owner: &'a TaskJournalOwner<P>,
        callback_guards: NativePreparationCallbackGuards<'a, P>,
        allocation: &'a NativePreparationRoot,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>],
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
                let (root, mut output_custody) = work.copy_path(&options.root).map_err(Error::from)?;
                let owned_options = NativeViewOptions {
                    root, maximum_volumes: options.maximum_volumes,
                    work_per_volume: options.work_per_volume,
                };
                let (mut mappings, mut mappings_custody) = native_vec(bindings.len(), &work)?;
                output_custody.absorb(&mut mappings_custody).map_err(Error::from)?;
                let (mut binding_work, binding_work_custody) = native_vec(bindings.len(), &work)?;
                for (index, binding) in bindings.iter().enumerate() {
                    work.charge(acyclic_fs::WorkCounters {
                        items_examined: 1, ..acyclic_fs::WorkCounters::default()
                    }).map_err(Error::from)?;
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
                        }).map_err(Error::from)?;
                        if !matches!(part, Component::Normal(_)) {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native volume paths overlap or escape their root" });
                        }
                    }
                    for prior in &bindings[..index] {
                        work.charge(acyclic_fs::WorkCounters {
                            items_examined: 1, ..acyclic_fs::WorkCounters::default()
                        }).map_err(Error::from)?;
                        if native_paths_overlap(path, Path::new(prior.path)) {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native volume paths overlap or escape their root" });
                        }
                    }
                    let volume_work = match lazy {
                        Some(lazy) => lazy.held.original_work().clone(),
                        None => work.native_volume_work(options.work_per_volume).map_err(Error::from)?,
                    };
                    verify_binding(owner, binding, &volume_work)?;
                    let (volume, mut volume_custody) = binding.volume.clone_with_native_work(&work)?;
                    output_custody.absorb(&mut volume_custody).map_err(Error::from)?;
                    let (generation, mut generation_custody) = crate::resources::GenerationRef::new_with_native_work(
            binding.generation.as_resource().provider(), binding.generation.as_resource().key(),
            binding.generation.as_resource().version(), &work,
        )?;
                    output_custody.absorb(&mut generation_custody).map_err(Error::from)?;
                    let (path, mut path_custody) = work.join_path(&options.root, path).map_err(Error::from)?;
                    output_custody.absorb(&mut path_custody).map_err(Error::from)?;
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
                        let (bytes, _custody) = native_canonical(&(binding.verifier.audience(), binding.scope), work)?;
                        work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64,
                            ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
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
                    std::mem::size_of::<Vec<&str>>() as u64).map_err(Error::from)?;
                let (mut preserved, mut preserved_slots) = native_vec(lazy_count, work)?;
                preserved_custody.absorb(&mut preserved_slots).map_err(Error::from)?;
                for (index, binding) in bindings.iter().enumerate() {
                    if existing_lazy.get(index).copied().flatten().is_some() {
                        preserved.push(binding.path);
                    }
                }
                Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
                let backend = native_backend(&allocation, &work, true).await?;
                let store = NativePreparationStore::new(owner, work.clone(), false);
                let materializer = acyclic_fs::JournaledMaterializer::new(store, backend);
                let operation = native_preparation_operation_with_work(owner, &work).map_err(Error::from)?;
                let retained = materializer.load_owned(operation).await.map_err(native_materialization_error)?;
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
                            volume_work, cancellation,
                        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                        if head.id() != generation.id() {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native destination generation is stale" });
                        }
                    }
                    let (provider_identity, mut provider_custody) = native_canonical(binding.volume.provider(), &work)?;
                    plan_custody.absorb(&mut provider_custody).map_err(Error::from)?;
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
                    output_custody.absorb(&mut intent_custody).map_err(Error::from)?;
                    if (intent.task, &intent.original_fence) != owner.task_binding() || intent.manifest != manifest
                        || intent.operation_directory != allocation.operation_directory
                    {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation original selection changed" });
                    }
                    require_allocation(journal, &allocation)?;
                    intent
                } else {
                    materializer.backend().require_initial_empty().map_err(Error::from)?;
                    let (task, original_fence) = owner.task_binding();
                    let (reservation_id, mut reservation_custody) = native_string(&original_fence.reservation_id, &work)?;
                    let (placement, mut placement_custody) = native_string(&original_fence.placement, &work)?;
                    output_custody.absorb(&mut reservation_custody).map_err(Error::from)?;
                    output_custody.absorb(&mut placement_custody).map_err(Error::from)?;
                    let original_fence = crate::scheduler::LeaseFence { reservation_id, placement };
                    let (operation_directory, operation_custody) = work.copy_path(&allocation.operation_directory)
                        .map_err(Error::from)?;
                    let (manifest, mut manifest_custody) = native_manifest(&manifest, &work)?;
                    intent_operation_custody = Some(operation_custody);
                    output_custody.absorb(&mut manifest_custody).map_err(Error::from)?;
                    NativePreparationIntent { task, original_fence, operation_directory, manifest }
                };
                let (mut top_level, top_custody) = native_vec(bindings.len(), &work)?;
                for binding in bindings {
                    if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) { continue; }
                    let first = Path::new(binding.path).components().next()
                        .and_then(|part| part.as_os_str().to_str())
                        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "native mapping name must be Unicode" })?;
                    top_level.push(first);
                }
                top_level.sort_unstable();
                top_level.dedup();
                let (mut edits, mut edits_custody) = native_vec(top_level.len(), &work)?;
                plan_custody.absorb(&mut edits_custody).map_err(Error::from)?;
                for path in &top_level {
                    let (path, mut path_custody) = native_string(path, &work)?;
                    plan_custody.absorb(&mut path_custody).map_err(Error::from)?;
                    let (image, mut image_custody) = native_bytes(acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX, &work)?;
                    plan_custody.absorb(&mut image_custody).map_err(Error::from)?;
                    edits.push(acyclic_fs::MaterializationEdit::Install { path, image });
                }
                drop(top_level);
                drop(top_custody);
                let (original, mut original_custody) = native_canonical(&intent, &work)?;
                plan_custody.absorb(&mut original_custody).map_err(Error::from)?;
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
                materializer.backend().require_selected_root(&plan.edits, &preserved).map_err(Error::from)?;
                let journal = materializer.prepare_owned(plan, plan_custody).await.map_err(native_materialization_error)?;
                require_allocation(&journal, &allocation)?;
                let restage = journal.phase == MaterializationPhase::Preparing
                    && journal.preimages.is_empty();
                if restage {
                    owner.verify_with_native_work(false, work).await?;
                    materializer.backend().reset_target_directory("").await.map_err(Error::from)?;
                    owner.verify_with_native_work(false, work).await?;
                    for (binding, (generation, volume_work)) in bindings.iter().zip(&generations) {
                        if matches!(binding.namespace, NativeVolumeNamespace::LazyMount { .. }) { continue; }
                        owner.verify_with_native_work(false, work).await?;
                        verify_binding(owner, binding, volume_work)?;
                        let identity = materializer.backend().reset_target_directory(binding.path)
                            .await.map_err(Error::from)?;
                        owner.verify_with_native_work(false, work).await?;
                        verify_binding(owner, binding, volume_work)?;
                        let (path, path_custody) = materializer.backend().staging_path(binding.path)
                            .map_err(Error::from)?;
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
                Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
                materializer.backend().require_selected_root(&journal.plan.edits, &preserved).map_err(Error::from)?;
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
                            .await.map_err(NativePreparationJournalError::Owner)?;
                        Ok::<(), NativePreparationJournalError>(())
                    };
                    materializer.recover_owned_guarded(
                        operation, acyclic_fs::MaterializationRecovery::Complete, &mut before_effect,
                    ).await
                }.map_err(NativePreparationJournalError::into_owner_error)?
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native preparation journal disappeared" })?;
                require_allocation(&journal, &allocation)?;
                Self::authenticate_lazy_roots(owner, bindings, existing_lazy, &manifest.options.root, work).await?;
                materializer.backend().require_selected_root(&journal.plan.edits, &preserved).map_err(Error::from)?;
                let (mut volumes, mut volumes_custody) = native_vec(bindings.len(), &work)?;
                output_custody.absorb(&mut volumes_custody).map_err(Error::from)?;
                let (mut identities, identities_custody) = native_vec(bindings.len(), &work)?;
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
                            callback: None, original_identity: identity, writable: binding.writable,
                            publication: binding.publication, _work: volume_work });
                        continue;
                    }
                    let callback_guard = match &callback_guards {
                        NativePreparationCallbackGuards::Ordinary(owner) =>
                            OrdinaryVolumeCallback::new(owner, binding.volume, binding.writable, &volume_work).await?,
                        NativePreparationCallbackGuards::Native(guards) =>
                            guards[index].for_volume(owner, binding.volume, binding.writable, &volume_work)?,
                    };
                    let crate::runtime::NativeVolumeCallbackPorts { sdk, publication } = callback_guard;
                    let identity = materializer.backend().mapping_root_identity(binding.path).map_err(Error::from)?;
                    if identities.contains(&identity) {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "native volume directory aliases changed" });
                    }
                    identities.push(identity);
                    let mount = if binding.writable {
                        acyclic_fs::MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual)
                    } else { acyclic_fs::MountOptions::read_only() };
                    let (attachment_path, path_custody) = work.copy_path(&mapping.path).map_err(Error::from)?;
                    let attachment = (acyclic_fs::MaterializeOptions::native(attachment_path), path_custody);
                    let native_original = match &callback_guards {
                        NativePreparationCallbackGuards::Ordinary(_) => None,
                        NativePreparationCallbackGuards::Native(_) => Some(&volume_work),
                    };
                    let view = generation.attach_native_working_set_with_work(
                        &mount, &attachment.0, identity, manifest.options.work_per_volume,
                        &volume_work.native_mount_work(), cancellation, Some(sdk),
                        native_original,
                    ).await?;
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, &volume_work)?;
                    volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::Materialized(view),
                        callback: Some(publication), original_identity: identity, writable: binding.writable,
                        publication: binding.publication, _work: volume_work });
                }
                drop(generations_custody);
                drop(identities);
                drop(identities_custody);
                owner.verify_with_native_work(false, &work).await?;
                cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(Error::from)?;
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
                    original_work: matches!(&callback_guards, NativePreparationCallbackGuards::Native(_))
                        .then(|| work.clone()), _allocation: output_custody })
            }.await;
            result
    }

    /// Original preparation evidence retained by this attachment.
    #[must_use]
    pub fn preparation_receipts(&self) -> &[NativePreparationReceipt] {
        std::slice::from_ref(&self.preparation)
    }

    /// Recovers only the original journaled allocation after lost acknowledgement.
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
    pub fn recover_allocation_with_native_work<'a, P: StreamProvider>(
        owner: &'a TaskJournalOwner<P>, options: &'a NativeViewOptions, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<NativePreparationRoot>, acyclic_fs::NativeAllocation)>> {
        if !work.remaining().map_err(Error::from)?.has_finite_allowances() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native allocation recovery requires finite original work" });
        }
        crate::NativeFutureBundle::new(work, || async {
            let store = NativePreparationStore::new(owner, work.clone(), true);
            let operation = native_preparation_operation_with_work(owner, work).map_err(Error::from)?;
            let Some(journal) = store.load_record(operation, false).await
                .map_err(NativePreparationJournalError::into_owner_error)?
            else { return Ok((None, work.retain_owned(0).map_err(Error::from)?)); };
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
            let expected_identity = work.root_identity(&intent.0.manifest.options.root).map_err(Error::from)?;
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
            allocation.1.substitute(retained as u64).map_err(Error::from)?;
            let _backend = native_backend(&allocation.0, work, true).await?;
            let _final_owner_custody = store.verify_read_owner().await?;
            work.remaining().map_err(Error::from)?;
            Ok((Some(allocation.0), allocation.1))
        })
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
    pub async fn cleanup_preparation<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        allocation: &NativePreparationRoot,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<acyclic_fs::OperationReceipt<Option<NativePreparationReceipt>>, (Error, acyclic_fs::WorkCounters)> {
        inspect_native_preparation(owner, allocation, budget, cancellation, true).await
    }

    /// Exact request/approval manifest. Changing it requires new admission.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.manifest
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
            NativePreparedVolumeBackend::Materialized(view) => {
                let lease = view.native_source_io_lease()
                    .map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
                let physical = work.with_source_io_lease(lease).map_err(Error::from)?;
                physical.read_working_file_guarded(&mapping.path, volume.original_identity, file, maximum_bytes,
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
            NativePreparedVolumeBackend::Materialized(view) => {
                let lease = view.native_source_io_lease()
                    .map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
                let physical = work.with_source_io_lease(lease).map_err(Error::from)?;
                physical.write_working_file_guarded(&mapping.path, volume.original_identity, file, content,
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

    #[cfg(test)]
    pub(crate) fn builtin_meter_for_test(
        &self, relative: &Path, work: &NativeMaterializationWork,
    ) -> Result<Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        let (_, volume, _) = self.builtin_volume(relative, VolumeOperation::Read, work)?;
        Ok(volume._work.runtime_meter_owner())
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
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
            if !matches!(component, Component::Normal(_)) { return Err(invalid()); }
        }
        if self.volumes.len() != self.manifest.volumes.len() { return Err(invalid()); }
        for (mapping, volume) in self.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
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
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        owner.verify_with_native_work(false, work).await?;
        let store = NativePreparationStore::new(owner, work.clone(), false);
        let record = store.load_record(self.preparation.journal.plan.operation_id, false).await
            .map_err(NativePreparationJournalError::into_owner_error)?
            .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: record.0.plan.edits.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(Error::from)?;
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
        self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(Error::from)?;
        owner.verify_with_native_work(false, work).await?;
        owner.require_volume_grant_with_native_work(&mapping.volume, operation, work)?;
        let expected = self.preparation.journal.root_identity
            .map(acyclic_fs::NativeRootIdentity::from_bytes)
            .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
        let actual = work.root_identity(&self.manifest.options.root).map_err(Error::from)?;
        if expected != actual {
            return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual }));
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

    /// Observes directory admission only.
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
        if cwd == self.manifest.options.root {
            return work.validate_working_directory(&self.manifest.options.root,
                self.original_root_identity()?, Path::new("")).map_err(Error::from);
        }
        for (mapping, volume) in self.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(Error::from)?;
            if let Ok(relative) = cwd.strip_prefix(&mapping.path) {
                let physical_work = match &volume.view {
                    NativePreparedVolumeBackend::Materialized(_) => work,
                    NativePreparedVolumeBackend::HeldLazy(lazy) => lazy.original_work(),
                };
                return physical_work.validate_working_directory(&mapping.path, volume.original_identity, relative)
                    .map_err(Error::from);
            }
        }
        Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))
    }

    async fn validate_dispatch_with_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        let expected = self.original_root_identity()?;
        self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(Error::from)?;
        if work.root_identity(&self.manifest.options.root).map_err(Error::from)? != expected {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation root was replaced" });
        }
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::default() }).map_err(Error::from)?;
            match &volume.view {
                NativePreparedVolumeBackend::Materialized(view) => {
                    let original = volume._work.native_mount_work();
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
                        || lazy.original_work().root_identity(&mapping.path).map_err(Error::from)? != volume.original_identity
                    {
                        return Err(Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                },
            }
        }
        if work.root_identity(&self.manifest.options.root).map_err(Error::from)? != expected {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation root changed during validation" });
        }
        self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(Error::from)?;
        Ok(())
    }

    /// Publishes only authorized destinations after source integrity passes.
    pub fn publish<'a, P: StreamProvider>(
        &'a self,
        owner: &'a TaskJournalOwner<P>,
    ) -> std::result::Result<crate::NativeFutureBundle<'a, std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>>, (Error, acyclic_fs::WorkCounters)> {
        let work = self.publication_work()?;
        let custody = work.retain_owned(0)
            .map_err(|cause| (Error::from(cause), work.receipt()))?;
        let meter = work.clone();
        crate::NativeFutureBundle::new(&work, || async move {
            Ok(self.publish_original(owner, &meter, custody).await)
        }).map_err(|error| (error, work.receipt()))
    }

    fn publication_work(&self) -> std::result::Result<NativeMaterializationWork, (Error, acyclic_fs::WorkCounters)> {
        if let Some(work) = &self.original_work {
            work.charge(acyclic_fs::WorkCounters::UNCHARGED)
                .map_err(|cause| (Error::from(cause), work.receipt()))?;
            return Ok(work.clone());
        }
        let budget = self.manifest.validation_budget()
            .map_err(|error| (error, acyclic_fs::WorkCounters::default()))?;
        NativeMaterializationWork::new(budget, &self.cancellation)
            .map_err(|(cause, work)| (Error::NativeConstruction { cause, work }, work))
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
            let (output, mut output_custody) = native_vec(self.manifest.volumes.len(), work)?;
            custody.absorb(&mut output_custody).map_err(Error::from)?;
            completed = output;
            let (proofs, mut proofs_custody) = native_vec(self.manifest.volumes.len(), work)?;
            custody.absorb(&mut proofs_custody).map_err(Error::from)?;
            published = proofs;
            let (copies, copies_custody) = native_vec(self.manifest.volumes.len(), work)?;
            mapping_storage = Some(copies_custody);
            mappings = copies;
            for mapping in &self.manifest.volumes {
                let (mapping, allocation) = copy_native_mapping_with_work(mapping, work)?;
                mappings.push(Some(NativeMappingPayload { mapping, readonly_snapshot: None, allocation }));
            }
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                failed_destination = Some(index);
                self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(Error::from)?;
                owner.verify_with_native_work(false, work).await?;
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                if !volume.writable {
                    let snapshot = match &volume.view {
                        NativePreparedVolumeBackend::Materialized(view) =>
                            view.verify_unchanged_with_work(&volume._work.native_mount_work(), volume._work.cancellation()).await?,
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
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                if !volume.writable {
                    let snapshot = mappings[index].as_mut().expect("original readonly destination")
                        .readonly_snapshot.take().expect("verified original readonly SDK cut");
                    published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
                    continue;
                }
                failed_destination = Some(index);
                match &volume.view {
                    NativePreparedVolumeBackend::Materialized(view) => {
                        let callback = Arc::clone(volume.callback.as_ref().ok_or_else(||
                            Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?);
                        let result = view.sync_with_permit_with_work(
                            volume.publication, &volume._work.native_mount_work(), volume._work.cancellation(),
                            move |actual: &NativeMaterializationWork| {
                                let callback = Arc::clone(&callback);
                                let actual = actual.clone();
                                async move { callback.check(&actual)?.await }
                            },
                        ).await;
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
                                accounting_failure = failure.accounting_failure.map(Error::from);
                                return Err(failure.cause);
                            }
                        };
                        published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
                    },
                    NativePreparedVolumeBackend::HeldLazy(lazy) => {
                        owner.verify_with_native_work(false, work).await?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, work)?;
                        let snapshot = match lazy.sync(volume.publication, lazy.original_work())?.await? {
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
                        published.push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
                    },
                }
                let NativeMappingPayload { mapping, mut allocation, .. } = mappings[index].take()
                    .expect("original preadmitted destination");
                completed.push(mapping);
                custody.absorb(&mut allocation).map_err(Error::from)?;
                work.remaining().map_err(Error::from)?;
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
                    destination, completed, published, diagnostic: None, rendering_failure: None, custody: Some(custody) })
            }
        }
    }
}

/// Reads only through the authenticated original SDK admission witness.
pub(crate) fn observe_native_preparation_admission<'a, P: StreamProvider>(
    witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, NativePreparationJournalObservation>> {
    if !work.remaining().map_err(Error::from)?.has_finite_allowances() {
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
            .map_err(Error::from)?;
        let store = NativePreparationStore::admission(witness, work.clone());
        let record = store.load_record(operation, false).await
            .map_err(NativePreparationJournalError::into_owner_error)?;
        let value = native_preparation_observation(
            witness.task(), scratch, operation, record, work,
        ).map_err(NativePreparationJournalError::into_owner_error)?;
        witness.verify_with_native_work(work).await?;
        work.remaining().map_err(Error::from)?;
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
        E::Backend(error) => return Error::from(error),
        E::PlanConflict => (crate::wire::ErrorCode::Conflict, "materialization retry conflicts with its durable plan"),
        E::IncompatibleJournal => (crate::wire::ErrorCode::Storage, "materialization journal is incompatible"),
        E::Contended => (crate::wire::ErrorCode::Conflict, "materialization journal remained contended"),
        E::ExternalMutation => (crate::wire::ErrorCode::Conflict, "materialization paused because an external path mutation was observed"),
    };
    Error::NativeBoundary { code, reason }
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
    let (root, mut custody) = work.copy_path(&allocation.root).map_err(Error::from)?;
    let (operation_directory, mut operation_custody) = work.copy_path(&allocation.operation_directory).map_err(Error::from)?;
    custody.absorb(&mut operation_custody).map_err(Error::from)?;
    let meter = work.clone();
    let backend = acyclic_native_runtime::run_blocking_io_measured(move || {
        acyclic_fs::NativeTreeMaterializationBackend::open_owned_with_work(root, operation_directory, custody, meter)
    }, work.runtime_meter_owner())
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .await.map_err(|error| Error::from(work.runtime_error(error)))?
        .map_err(Error::from)?;
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
            custody.absorb(journal_custody).map_err(Error::from)?;
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
    if !work.remaining().map_err(Error::from)?.has_finite_allowances() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native query requires finite original work" });
    }
    crate::NativeFutureBundle::new(work, || async {
        let store = NativePreparationStore::new(owner, work.clone(), true);
        let operation = native_preparation_operation_with_work(owner, work).map_err(Error::from)?;
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
                let mut root_message = work.display(&error).map_err(Error::from)?;
                let mut preparation_message = native_string(&root_message.0, work)?;
                intent.1.absorb(&mut journal.1).map_err(Error::from)?;
                intent.1.absorb(&mut root_message.1).map_err(Error::from)?;
                intent.1.absorb(&mut preparation_message.1).map_err(Error::from)?;
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
            materializer.backend().reset_target_directory("").await.map_err(Error::from)?;
            owner.verify_with_native_work(true, work).await?;
            checkpoint_preparation(materializer.store(), rolled_back).await?;
        }
        let mut inspection = materializer.inspect_owned(operation).await.map_err(native_materialization_error)?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native preparation journal disappeared" })?;
        if let Some(custody) = inspection.1.as_mut() {
            intent.1.absorb(custody).map_err(Error::from)?;
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

/// Authenticated pre-action packet for one accepted task-preparation completion.
pub(crate) struct NativePreparationCompletionTemplate {
    journal: acyclic_fs::OwnedMaterializationJournal,
    intent: NativePreparationIntent,
    path: acyclic_stream::StreamPath,
    key: acyclic_stream::IdempotencyKey,
    association: [u8; 32],
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
        -> (acyclic_fs::OwnedMaterializationJournal, acyclic_stream::PreparedOwnedBytes<acyclic_fs::NativeAllocation>, acyclic_fs::NativeAllocation)
    {
        let Self { mut journal, intent, path, key, value, output, maximum_bytes: _, association: _, custody } = self;
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
    let key = work.display(&format_args!("harness/native-root-output-bind/v2/{operation}"))
        .map_err(Error::from)?;
    let key = (crate::IdempotencyKey::new(key.0)?, key.1);
    let agent = scope.agent().expect("validated ROOT binding actor");
    let command = crate::core::Command {
        operation_id: operation, idempotency_key: key.0,
        expected_revision: original_revision.unwrap_or_else(|| aggregate.0.reducer().revision()),
        causal_parent: None, action: crate::core::Action::BindConversation { agent }, scope,
    };
    let mut io = crate::store::history::native_work_io(work).map_err(Error::from)?;
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

impl acyclic_fs::MaterializationActionCompletion
    for crate::NativeFutureBundle<'_, (acyclic_fs::OwnedMaterializationJournal, acyclic_fs::NativeAllocation)>
{
    type Error = Error;

    async fn complete(self) -> Result<acyclic_fs::OwnedMaterializationJournal> {
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

    fn path(&self, operation: acyclic_fs::OperationId) -> Result<acyclic_stream::StreamPath> {
        self.path_typed(operation).map_err(NativePreparationJournalError::into_owner_error)
    }

    fn path_typed(
        &self, operation: acyclic_fs::OperationId,
    ) -> std::result::Result<acyclic_stream::StreamPath, NativePreparationJournalError> {
        if operation != native_preparation_operation_for_task_with_work(self.task(), &self.work)? {
            return Err(NativePreparationJournalError::Record("native materializer operation differs from original task"));
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
            .map_err(|_| NativePreparationJournalError::Record("native preparation UUID encoding is invalid"))?;
        self.work.spent(completed)?;
        acyclic_stream::StreamPath::new_native(path, &self.work.runtime_meter_owner())
            .map_err(|error| NativePreparationJournalError::Native(self.work.runtime_error(error)))?
            .map_err(NativePreparationJournalError::Backend)
    }

    /// Authenticates the actual protected Preparing record before accepting one physical action.
    async fn completion_template(
        &self, journal: acyclic_fs::OwnedMaterializationJournal,
    ) -> Result<NativePreparationCompletionTemplate> {
        let operation = journal.plan.operation_id;
        let owner = match &self.authority {
            NativePreparationAuthority::Owner { owner, settlement: false } => *owner,
            _ => return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native completion requires the original running preparation owner" }),
        };
        owner.verify_with_native_work(false, &self.work).await?;
        let (loaded, loaded_custody) = self.load_record(operation, false).await
            .map_err(NativePreparationJournalError::into_owner_error)?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "native completion original preparation is missing" })?;
        if loaded != journal.journal {
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
        let (encoded, encoded_custody) = native_canonical(&journal.journal, &self.work)?;
        self.work.charge(acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64,
            ..acyclic_fs::WorkCounters::default() }).map_err(Error::from)?;
        let association = *blake3::hash(&encoded).as_bytes();
        drop(encoded);
        drop(encoded_custody);
        let (key_bytes, key_custody) = native_bytes(&association, &self.work)?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes, key_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
        let remaining = self.work.remaining().map_err(Error::from)?;
        let mut value_custody = self.work.retain_owned(0).map_err(Error::from)?;
        let value = crate::contract::json_value_bounded_native_trusted(
            &journal.journal, crate::contract::JsonBounds { maximum_bytes: remaining.bytes_encoded,
                maximum_items: remaining.items_examined, maximum_depth: 256 },
            crate::wire::ErrorCode::Storage, &mut |charge| {
                native_codec_charge_typed(charge, &self.work, &mut value_custody).map_err(Error::from)
            },
        );
        let mut value = match value {
            Ok(value) => value,
            Err(error) => return Err(native_codec_error_custody(error, value_custody)),
        };
        let mut custody = self.work.retain_owned(std::mem::size_of::<NativePreparationCompletionTemplate>() as u64)
            .map_err(Error::from)?;
        custody.absorb(&mut intent_custody).map_err(Error::from)?;
        custody.absorb(&mut value_custody).map_err(Error::from)?;
        NativePreparationCompletionTemplate::set_value_work(
            &mut value, journal.revision, acyclic_fs::WorkCounters::UNBOUNDED,
        )?;
        let (largest, largest_custody) = native_canonical(&value, &self.work)?;
        let maximum_bytes = largest.len();
        drop(largest);
        drop(largest_custody);
        NativePreparationCompletionTemplate::set_value_work(&mut value, journal.revision, journal.work)?;
        if maximum_bytes > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "native completion scalar envelope exceeds record limit" });
        }
        let (mut output, output_custody) = native_vec::<u8>(maximum_bytes, &self.work)?;
        let initialized = acyclic_fs::WorkCounters {
            bytes_copied: output.capacity() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        };
        self.work.admit(initialized).map_err(Error::from)?;
        output.resize(output.capacity(), 0);
        self.work.spent(initialized).map_err(Error::from)?;
        let output = acyclic_stream::PreparedOwnedBytes::new(
            output, output_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let template = NativePreparationCompletionTemplate {
            journal, intent, path, key, association, value, output,
            maximum_bytes: maximum_bytes as u64, custody,
        };
        owner.verify_with_native_work(false, &self.work).await?;
        template.verify_owner(owner, &self.work)?;
        Ok(template)
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
        let path = self.path_typed(operation)?;
        let _owner_custody = self.verify_read_owner().await?;
        let mut native_io = crate::store::history::native_work_io(&self.work)?;
        let mut observer = Some(&mut native_io as &mut dyn crate::store::history::HistoryIo);
        let bounds = match crate::store::operations::bounds_with_work(
            self.read_client(), &path, &mut observer,
        ).await? {
            Ok(bounds) => bounds,
            Err(error) if matches!(error.source(), acyclic_stream::StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(NativePreparationJournalError::Owner(error.into_error(&observer))),
        };
        let sequence = match bounds.tail.checked_sub(1) {
            Some(sequence) => sequence,
            None if !adopt_original_work => return Ok(None),
            None => return Err(NativePreparationJournalError::Record("empty materializer journal")),
        };
        let _read_owner_custody = self.verify_read_owner().await?;
        let mut records = crate::store::operations::read_path_records_with_work(
            self.read_client(), &path, sequence, 1, &mut observer,
        ).await?.map_err(|error| NativePreparationJournalError::Owner(error.into_error(&observer)))?;
        let record = records.next(&mut observer).await?
            .ok_or(NativePreparationJournalError::Record("missing materializer journal"))?;
        if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(NativePreparationJournalError::Record("materializer journal exceeds Stream record limit"));
        }
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

    type ActionCompletion<'a> = crate::NativeFutureBundle<'a, (
        acyclic_fs::OwnedMaterializationJournal, acyclic_fs::NativeAllocation,
    )> where Self: 'a;

    async fn prepare_action<'a>(
        &'a self, journal: acyclic_fs::OwnedMaterializationJournal,
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
        }).map_err(Error::from)?;
        let proof = owner.accept_materialization_template_with_native_work(
            &template, &self.work,
        ).await?;
        let mut native_io = crate::store::history::native_work_io(&self.work)
            .map_err(Error::from)?;
        let prepared = crate::distributed::PreparedMaterializationCommit::new(
            template, proof, reservation, &self.work, &mut Some(&mut native_io),
        )?;
        let prepared = prepared.prepare_sdk(owner.stream()).await?;
        let future = crate::NativeFutureBundle::new(&self.work, || async move {
            let counters = prepared.completion_work()?;
            prepared.finish(owner.stream(), counters, None).await
        })?;
        Ok(future)
    }

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
        replacement.work = self.work.journal_work().map_err(Error::from)?;
        let (encoded, encoded_custody) = native_canonical(&replacement.journal, &self.work)?;
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
        }).map_err(Error::from)?;
        let (key_bytes, key_custody) = native_bytes(blake3::hash(&bytes).as_bytes(), &self.work)?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes, key_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
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
    Ok(bytes as u64)
}

/// Only the two audited, closed native record schemas use this helper.
enum NativeRecordSchema {
    Journal,
    PreparationIntent,
}

fn native_decoded_record_typed<T: serde::de::DeserializeOwned>(
    bytes: &[u8], schema: NativeRecordSchema, work: &NativeMaterializationWork,
    retained_bytes: impl FnOnce(&T) -> std::result::Result<u64, NativePreparationJournalError>,
) -> std::result::Result<(T, acyclic_fs::NativeAllocation), NativePreparationJournalError> {
    let maximum_slot = match schema {
        NativeRecordSchema::Journal => [
            std::mem::size_of::<acyclic_fs::MaterializationEdit>(),
            std::mem::size_of::<acyclic_fs::MaterializationPreparationSelection>(),
            std::mem::size_of::<acyclic_fs::MaterializationPreimage>(),
            std::mem::size_of::<u8>(),
        ].into_iter().max().expect("nonempty concrete journal schema"),
        NativeRecordSchema::PreparationIntent => std::mem::size_of::<NativeVolumeMapping>()
            .max(std::mem::size_of::<u8>()),
    } as u64;
    let remaining = work.remaining()?;
    let bounds = crate::contract::JsonBounds {
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
                        .map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(Error::from)?;
                }
                crate::contract::JsonCodecCharge::Read { bytes } => {
                    input_bytes = input_bytes.checked_add(bytes)
                        .ok_or(acyclic_fs::WorkError::Overflow)
                        .map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(Error::from)?;
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes: scratch, .. } => {
                    let owned = items.checked_mul(10).and_then(|slots| slots.checked_mul(maximum_slot))
                        .and_then(|slots| slots.checked_add(input_bytes))
                        .and_then(|owned| owned.checked_add(scratch))
                        .ok_or(acyclic_fs::WorkError::Overflow)
                        .map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(Error::from)?;
                    let mut reservation = work.retain_owned(owned).map_err(Error::from)?;
                    custody.absorb(&mut reservation).map_err(Error::from)?;
                    return Ok(());
                }
                _ => {}
            }
            native_codec_charge_typed(charge, work, &mut custody).map_err(Error::from)
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


fn native_codec_error_custody(error: Error, custody: acyclic_fs::NativeAllocation) -> Error {
    match error {
        Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(custody) },
        Error::NativeDecode { code, source, custody: None } => Error::NativeDecode { code, source, custody: Some(custody) },
        error => error,
    }
}

fn native_canonical<T: serde::Serialize + ?Sized>(
    value: &T, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let remaining = work.remaining().map_err(Error::from)?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: remaining.bytes_encoded,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let mut custody = work.retain_owned(0).map_err(Error::from)?;
    let mut raw_custody: Option<acyclic_fs::NativeAllocation> = None;
    let bytes = crate::contract::canonical_json_bytes_bounded(value, bounds, &mut |charge| {
        match charge {
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                let mut reservation = work.retain_owned(maximum_owned_bytes).map_err(Error::from)?;
                if let Some(existing) = raw_custody.as_mut() {
                    existing.absorb(&mut reservation).map_err(Error::from)?;
                } else {
                    raw_custody = Some(reservation);
                }
                Ok(())
            }
            crate::contract::JsonCodecCharge::Retain { bytes } if raw_custody.is_some() => {
                let retained = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
                let mut reservation = raw_custody.take().expect("checked opaque quota");
                custody.absorb(&mut reservation).map_err(Error::from)?;
                custody.substitute(retained).map_err(Error::from)
            }
            charge => native_codec_charge_typed(charge, work, &mut custody).map_err(Error::from),
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

fn native_vec<T>(capacity: usize, work: &NativeMaterializationWork) -> Result<(Vec<T>, acyclic_fs::NativeAllocation)> {
    let requested = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
    let mut custody = if requested == 0 {
        work.retain_owned(0)
    } else {
        work.claim(requested as u64)
    }.map_err(Error::from)?;
    let mut value = Vec::new();
    value.try_reserve_exact(capacity).map_err(|error|
        Error::from(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(error)))?;
    let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::from(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
    if let Err(cause) = custody.resize_retained(actual as u64) {
        drop(value);
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok((value, custody))
}

fn native_bytes(value: &[u8], work: &NativeMaterializationWork) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let (copied, custody) = native_vec(value.len(), work)?;
    let mut copied = copied;
    work.charge(acyclic_fs::WorkCounters {
        bytes_copied: value.len() as u64, ..acyclic_fs::WorkCounters::default()
    }).map_err(Error::from)?;
    copied.extend_from_slice(value);
    Ok((copied, custody))
}

fn native_owned_bytes_failure(
    failure: acyclic_stream::PreparedOwnedBytesFailure<acyclic_fs::NativeAllocation>,
    work: &NativeMaterializationWork,
) -> Error {
    match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => Error::from(work.runtime_error(error)),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
        | acyclic_stream::PreparedOwnedBytesError::NotUnique => Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native owned byte buffer violated its fixed initialized ownership invariant",
        },
    }
}

fn native_string(value: &str, work: &NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
    work.display(&value).map_err(Error::from)
}

/// Copies one admitted mapping while retaining its actual original-ledger storage.
pub fn copy_native_mapping_with_work(
    mapping: &NativeVolumeMapping, work: &NativeMaterializationWork,
) -> Result<(NativeVolumeMapping, acyclic_fs::NativeAllocation)> {
    let (volume, custody) = mapping.volume.clone_with_native_work(work)?;
    let mut custody = custody;
    let (generation, mut generation_custody) = crate::resources::GenerationRef::new_with_native_work(
            mapping.generation.as_resource().provider(), mapping.generation.as_resource().key(),
            mapping.generation.as_resource().version(), work,
        )?;
    custody.absorb(&mut generation_custody).map_err(Error::from)?;
    let (path, mut path_custody) = work.copy_path(&mapping.path).map_err(Error::from)?;
    custody.absorb(&mut path_custody).map_err(Error::from)?;
    Ok((NativeVolumeMapping { volume, generation, path, writable: mapping.writable,
        namespace: mapping.namespace.clone(),
        authority_revision: mapping.authority_revision }, custody))
}

fn native_manifest(
    manifest: &NativeViewManifest, work: &NativeMaterializationWork,
) -> Result<(NativeViewManifest, acyclic_fs::NativeAllocation)> {
    let (root, custody) = work.copy_path(&manifest.options.root).map_err(Error::from)?;
    let mut custody = custody;
    let options = NativeViewOptions {
        root, maximum_volumes: manifest.options.maximum_volumes,
        work_per_volume: manifest.options.work_per_volume,
    };
    let (mut volumes, mut volumes_custody) = native_vec(manifest.volumes.len(), work)?;
    custody.absorb(&mut volumes_custody).map_err(Error::from)?;
    for mapping in &manifest.volumes {
        let (mapping, mut mapping_custody) = copy_native_mapping_with_work(mapping, work)?;
        custody.absorb(&mut mapping_custody).map_err(Error::from)?;
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
