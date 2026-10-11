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

/// Authenticated, unmounted original ROOT result storage. Only the actual
/// filesystem creator can construct this carrier; locators never mint authority.
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
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>>;
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
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
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

    /// Provider aliases retain this whole source, including its actual owner and
    /// metadata custody. The base providers never point back to this source.
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
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_root_error)
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
        }).map_err(native_root_error)?;
        Ok(Arc::new(self))
    }
}

impl ContentResidencyVerifier for NativeRootOutputSource {
    fn verify<'a>(&'a self, _reference: &'a FileRef) -> BoxProviderFuture<'a, Result<()>> {
        native_root_output_ordinary_denial()
    }

    fn read<'a>(&'a self, _reference: &'a FileRef) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        native_root_output_ordinary_denial()
    }

    fn read_private_prefix<'a>(
        &'a self, _volume: &'a VolumeRef, _prefix: &'a str, _path: &'a str,
        _generation: &'a GenerationRef, _maximum: u64,
    ) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        native_root_output_ordinary_denial()
    }

    fn list_private_directory<'a>(
        &'a self, _volume: &'a VolumeRef, _prefix: &'a str, _path: &'a str,
        _generation: Option<&'a GenerationRef>, _after: Option<&'a str>, _maximum: u32,
    ) -> BoxProviderFuture<'a, Result<PrivateDirectoryPage>> {
        native_root_output_ordinary_denial()
    }

    fn read_private_path<'a>(
        &'a self, _volume: &'a VolumeRef, _prefix: &'a str, _path: &'a str,
        _generation: Option<&'a GenerationRef>,
    ) -> BoxProviderFuture<'a, Result<(FileRef, Vec<u8>)>> {
        native_root_output_ordinary_denial()
    }

    fn verify_manifest<'a>(
        &'a self, _reference: &'a FileRef, _count: u32, _limits: &'a Limits,
    ) -> BoxProviderFuture<'a, Result<()>> {
        native_root_output_ordinary_denial()
    }

    fn load_manifest<'a>(
        &'a self, _reference: &'a FileRef, _count: u32,
    ) -> BoxProviderFuture<'a, Result<Vec<Attachment>>> {
        native_root_output_ordinary_denial()
    }

    fn supports_measured_read(
        &self, _volume: &VolumeRef, _budget: acyclic_fs::WorkBudget,
        _cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<bool>, Error> {
        RootOutputDenial::denied()
    }

    fn read_measured<'a>(
        &'a self, _reference: &'a FileRef, _maximum: u64, _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error>> {
        native_root_output_ordinary_denial()
    }

    fn verify_measured<'a>(
        &'a self, _reference: &'a FileRef, _maximum: u64, _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        native_root_output_ordinary_denial()
    }

    fn load_complete_manifest_measured<'a>(
        &'a self, _reference: &'a FileRef, _maximum: usize, _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<Attachment>>, Error>> {
        native_root_output_ordinary_denial()
    }

    fn load_manifest_measured<'a>(
        &'a self, _reference: &'a FileRef, _count: u32, _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<Attachment>>, Error>> {
        native_root_output_ordinary_denial()
    }

    fn verify_manifest_measured<'a>(
        &'a self, _reference: &'a FileRef, _count: u32, _limits: &'a Limits,
        _budget: acyclic_fs::WorkBudget, _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        native_root_output_ordinary_denial()
    }

    fn supports_native_read(&self, volume: &VolumeRef, work: &NativeMaterializationWork) -> Result<bool> {
        self.require_content_work(volume, work)?;
        self.binding.reader.supports_native_read(volume, work)
    }

    fn read_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum: u64, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
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

    fn stage_once<'a>(
        &'a self, _operation: crate::OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        native_root_output_ordinary_denial()
    }

    fn stage_at<'a>(
        &'a self, _operation: crate::OperationId, _reference: &'a FileRef, _bytes: &'a [u8],
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        native_root_output_ordinary_denial()
    }

    fn observe_staged_once<'a>(
        &'a self, _operation: crate::OperationId, _maximum: u64,
    ) -> BoxProviderFuture<'a, Result<Option<FileRef>>> {
        native_root_output_ordinary_denial()
    }

    fn stage_once_measured<'a>(
        &'a self, _operation: crate::OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str, _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error>> {
        native_root_output_ordinary_denial()
    }

    fn observe_staged_once_measured<'a>(
        &'a self, _operation: crate::OperationId, _maximum: u64, _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error>> {
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

trait RootOutputDenial {
    fn denied() -> Self;
}

fn native_root_output_unsupported() -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
        reason: "original ROOT output requires its authenticated native-work provider boundary" }
}

impl<T> RootOutputDenial for Result<T> {
    fn denied() -> Self { Err(native_root_output_unsupported()) }
}

// The legacy measured error ABI allocates its existing counter box. This is a
// pre-IO rejection, not a qualified native operation or a zero-allocation claim.
impl<T> RootOutputDenial for acyclic_fs::MeasuredResult<T, Error> {
    fn denied() -> Self {
        Err(acyclic_fs::OperationFailure::before_work(native_root_output_unsupported()))
    }
}

struct DeniedRootOutputFuture<T>(std::marker::PhantomData<fn() -> T>);

impl<T: RootOutputDenial> Future for DeniedRootOutputFuture<T> {
    type Output = T;
    fn poll(
        self: std::pin::Pin<&mut Self>, _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<T> {
        std::task::Poll::Ready(T::denied())
    }
}

fn native_root_output_ordinary_denial<'a, T: RootOutputDenial + 'a>() -> BoxProviderFuture<'a, T> {
    // The concrete future is genuinely zero-sized; Box performs no heap attempt.
    Box::pin(DeniedRootOutputFuture(std::marker::PhantomData))
}

fn native_root_output_denial(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Proves the original private ROOT result against its committed bind and
    /// immutable successful filesystem creation acknowledgement, never a head.
    #[allow(clippy::too_many_arguments, reason = "original owner, receipt and signed scope are independent proofs")]
    pub async fn native_root_output_source_with_native_work<P: StreamProvider + 'static>(
        self: &Arc<Self>, owner: &Arc<TaskJournalOwner<P>>, volume: &VolumeRef,
        creation_operation: acyclic_fs::OperationId, generation: &acyclic_fs::Generation<A, O>,
        verifier: &AuthorityVerifier, scope: &Scope, work: &NativeMaterializationWork,
    ) -> Result<NativeRootOutputSource> {
        // This MUST precede every filesystem query and scoped provider capture.
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        if !owner.native_shared_work_matches(work) {
            return Err(native_root_output_denial("ROOT output requires the original paid shared owner"));
        }
        owner.require_root_output_scope_with_native_work(scope, work).await?;
        let proof = owner.authenticated_root_binding_with_native_work(work).await?;
        if volume.provider() != &self.provider
            || volume.class() != crate::conversation::VolumeClass::AgentPrivate
            || volume.owner() != &crate::conversation::VolumeOwner::Agent(proof.agent())
        {
            return Err(native_root_output_denial("ROOT result differs from its original private agent and provider"));
        }
        let bind_operation = native_root_output_binding_operation(
            proof.audience(), volume, creation_operation, work,
        )?;
        if proof.bind_operation() != bind_operation {
            return Err(native_root_output_denial("ROOT result creation differs from its protected original bind"));
        }
        let bind_intent = proof.intent_digest();
        let (name, _name_custody) = volume.storage_name_with_native_work(work)?;
        let created = self.filesystem.workspace_for_creation_operation_with_native_work(
            &name, acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable), creation_operation,
            work, work.cancellation(),
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
            .ok_or_else(|| native_root_output_denial("ROOT result lacks its original filesystem creation acknowledgement"))?;
        if created.id() != generation.id()
            || created.workspace().id() != generation.workspace().id()
            || created.workspace().name().as_str() != name
            || generation.workspace().name().as_str() != name
        {
            return Err(native_root_output_denial("ROOT result generation differs from its immutable creation acknowledgement"));
        }
        let workspace = created.workspace().id();
        let generation = created.id();
        drop(created);
        // Authentication precedes both bounded SDK constructors. Each provider
        // retains its own paid data; the source retains the actual owner too.
        let maximum = owner.input_limits().file_bytes;
        let reader = super::FilesystemContentVerifier::new_with_native_work(
            Arc::clone(self), verifier, scope, maximum, work,
        )?.into_native_shared(work)?;
        let writer = super::FilesystemContentPublisher::new_with_native_work(
            Arc::clone(self), volume, verifier, scope, maximum, work,
        )?.into_native_shared(work)?;
        owner.authenticated_root_binding_with_native_work(work).await?;
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        let custody = work.retain_owned(std::mem::size_of::<NativeRootOutputSource>() as u64)
            .map_err(native_root_error)?;
        let owner: Arc<dyn NativeRootOutputOwner> = Arc::clone(owner) as Arc<dyn NativeRootOutputOwner>;
        Ok(NativeRootOutputSource {
            binding: crate::runtime::ContentBindings { reader, writer: Some(writer) },
            owner, bind_operation, bind_intent, creation_operation, workspace, generation, custody,
        })
    }
}

/// Canonical native ROOT bind identity; this derives an operation, not authority.
/// The protected owner bind must separately commit this exact operation.
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
    }).map_err(native_root_error)?;
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&blake3::hash(&encoded).as_bytes()[..16]);
    Ok(crate::OperationId::from_bytes(bytes))
}

#[cfg(test)]
mod native_root_output_tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_root_output_denial_is_zero_sized_and_typed() {
        assert_eq!(std::mem::size_of::<DeniedRootOutputFuture<Result<FileRef>>>(), 0);
        let result: Result<FileRef> = native_root_output_ordinary_denial().await;
        assert!(matches!(result, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported, ..
        })));
    }

    #[test]
    fn native_root_bind_identity_pins_exact_volume_audience_and_creation() -> Result<()> {
        let audience = crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation, id: "original-root".into(),
        };
        let provider = crate::resources::ProviderRef::new("root-output-test", "filesystem", "2")?;
        let volume = VolumeRef::new(
            provider.clone(), "original-result", crate::conversation::VolumeClass::AgentPrivate,
            crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([41; 16])),
        )?;
        let foreign = VolumeRef::new(
            provider, "foreign-result", volume.class(), volume.owner().clone(),
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            bytes_encoded: 1_048_576, bytes_copied: 1_048_576, bytes_hashed: 1_048_576,
            items_examined: 1_048_576, allocation_operations: 65_536,
            peak_allocation_bytes: 1_048_576, ..acyclic_fs::WorkBudget::default()
        }, &cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let operation = acyclic_fs::OperationId::from_bytes([42; 16]);
        let selected = native_root_output_binding_operation(&audience, &volume, operation, &work)?;
        assert_eq!(selected, native_root_output_binding_operation(&audience, &volume, operation, &work)?);
        assert_ne!(selected, native_root_output_binding_operation(&audience, &foreign, operation, &work)?);
        assert_ne!(selected, native_root_output_binding_operation(&audience, &volume,
            acyclic_fs::OperationId::from_bytes([43; 16]), &work)?);
        let foreign_audience = crate::core::Authority {
            kind: audience.kind, id: "foreign-root".into(),
        };
        assert_ne!(selected, native_root_output_binding_operation(
            &foreign_audience, &volume, operation, &work,
        )?);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        Ok(())
    }
}

/// Issued immutable working view, retaining its actual prepared view and owner.
/// Operation identifiers are data, not evidence of tool admission.
/// Builtin paths are relative to the actual manifest mapping (for example
/// `project/file` or `scratch/file` only when that mapping was prepared).
/// Known SDK data is funded through drop; opaque standard Arc backing is excluded.
pub struct NativeTaskWorkingView {
    held: Arc<dyn NativeTaskWorkingViewHolder>,
    custody: acyclic_fs::NativeAllocation,
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
    pub fn original_task(&self) -> crate::TaskId { self.held.original_task() }
    pub(crate) fn original_fence(&self) -> &crate::scheduler::LeaseFence { self.held.original_fence() }
    pub fn manifest(&self) -> &NativeViewManifest { self.held.manifest() }
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
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_root_error)
    }
    pub fn verify_with_native_work<'a>(&'a self, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_work(work)?;
        self.held.verify(work)
    }
    /// Forwards intrinsically funded physical bytes without rewrapping them.
    /// Original backing custody survives every clone and empty slice.
    pub fn read_builtin_file<'a>(&'a self, operation: crate::OperationId, path: &'a Path,
        maximum: usize, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        self.require_work(work)?;
        self.held.read(operation, path, maximum, work)
    }
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
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_root_error)
    }
    async fn authenticate(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_original(work)?;
        self.view.authenticate_builtin_view(self.owner.native_view_owner(), work).await?;
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
            self.view.read_builtin_file(self.owner.native_view_owner(), operation, path, maximum, work).await
        })
    }
    fn write<'a>(&'a self, operation: crate::OperationId, path: &'a Path, content: &'a str,
        parents: bool, expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.view.write_builtin_file(self.owner.native_view_owner(), operation, path, content, parents, expected, work).await
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
                .map_err(native_root_error)?;
            let held = HeldNativeTaskWorkingView { view: Arc::clone(self), owner: Arc::clone(owner) };
            held.authenticate(work).await?;
            work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
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
            let acquired = match mode {
                NativePublicationAcquisition::Reserve => workspace
                    .reserve_publication_with_native_work(original_operation, work, cancellation).await
                    .map(Some),
                NativePublicationAcquisition::ReadonlyExisting => workspace
                    .publication_reservation_with_native_work(original_operation, work, cancellation).await,
            };
            // Own the real proof before any later owner or selected-head check.
            reservation = acquired.map_err(|(cause, custody)| NativeReservationError::Sdk { cause, custody })?;
            self.owner.verify_with_native_work(false, work).await.map_err(NativeReservationError::Owner)?;
            self.owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)
                .map_err(NativeReservationError::Owner)?;
            if reservation.is_none() {
                return Ok(());
            }
            let head = workspace.head_with_native_work(&work.native_mount_work(), cancellation)
                .await.map_err(|cause| NativeReservationError::Sdk { cause, custody: None })?;
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
        let result = match crate::NativeFutureBundle::new(work, || async {
            Ok(self.workspace.release_publication_with_native_work(self.reservation, work, cancellation).await)
        }) {
            Ok(future) => match future.await {
                Ok(result) => result.map_err(|(cause, custody)| NativeReservationError::Sdk { cause, custody }),
                Err(error) => Err(NativeReservationError::Owner(error)),
            },
            Err(error) => Err(NativeReservationError::Owner(error)),
        };
        match result {
            Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: work.receipt() }),
            Err(error) => Err(NativePublicationReservationFailure {
                error, cleanup_error: None,
                work: work.receipt(), cleanup: Some(self),
                unwitnessed_workspace: None,
            }),
        }
    }
}

/// A sealed reference to an existing native lazy mount, not permission to mount
/// or append a namespace to an already prepared operation.
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
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<(), NativeLazyPublicationFailure>>>;
}

struct NativeLazyPublicationFailure {
    cause: Error,
    source_acknowledged: bool,
}

struct HeldLazyVolume<P: StreamProvider, A, O, D, S> {
    mount: Arc<acyclic_fs::LazyMount<A, O, D, S>>,
    owner: Arc<TaskJournalOwner<P>>,
    callback: Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>,
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
    /// Captures only the actual existing mount and its immutable declared
    /// snapshot. Owner metadata keeps its original root custody; the mount,
    /// returned resource and callbacks keep the volume work seeded before birth.
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
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            owner.verify_with_native_work(false, metadata_work).await?;
            verify_binding(owner, binding, work)?;
            guard.require_volume_with_native_work(owner, binding.volume, binding.writable, work)?;
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            };
            let identity = mount.native_root_identity()
                .ok_or_else(|| native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            let workspace = binding.host.open_volume_with_native_work(binding.volume, work).await?;
            let generation = binding.host.generation_with_native_work(&workspace, binding.generation, work).await?;
            let actual = mount.snapshot_with_work(&work.native_mount_work(), work.cancellation()).await.map_err(Error::from)?;
            if &actual != snapshot || snapshot.workspace_id != workspace.id()
                || snapshot.authored_generation != generation.id()
            {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let callback = guard.for_volume(owner, binding.volume, binding.writable, work)?;
            let custody = work.retain_owned(std::mem::size_of::<Self>() as u64).map_err(native_root_error)?;
            let mut holder_custody = work.retain_owned(std::mem::size_of::<HeldLazyVolume<P, A, O, D, S>>() as u64)
                .map_err(native_root_error)?;
            let (volume, mut volume_custody) = copy_native_volume_with_work(binding.volume, work)?;
            holder_custody.absorb(&mut volume_custody).map_err(native_root_error)?;
            let (generation, mut generation_custody) = copy_native_generation_with_work(binding.generation, work)?;
            holder_custody.absorb(&mut generation_custody).map_err(native_root_error)?;
            let (canonical, _canonical_custody) = native_canonical(&(binding.verifier.audience(), binding.scope), work)?;
            work.charge(acyclic_fs::WorkCounters { bytes_hashed: canonical.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
            let authority_revision = *blake3::hash(&canonical).as_bytes();
            work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
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
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_root_error)
    }
    async fn check(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_work(work)?;
        let native = work.native_mount_work();
        self.callback.check(&native).map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
            .map_err(Error::from)?.await.map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
            .map_err(Error::from)
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
    fn validate_selected_snapshot<'a>(&'a self, frozen: acyclic_fs::LazySnapshotRef,
        work: &'a NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>> {
        // The canonical caller authenticates its Common READ selection before
        // and after this immutable leaf. Old source custody is retained, not
        // adopted into the caller's work or used to grant source WRITE.
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
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            self.mount.with_original_work(&work.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                async move {
                    let scope = scope.map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
                    work.read_working_file_guarded_with_scope(scope, self.path(), self.identity, relative, maximum,
                        || self.check(work)).await
                }
            }).await.map_err(Error::from)?
        })
    }
    fn write<'a>(&'a self, relative: &'a Path, content: &'a str, parents: bool,
        expected: Option<[u8; 32]>, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            self.mount.with_original_work(&work.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                async move {
                    let scope = scope.map_err(acyclic_fs::native_mount::MountLifecycleError::Native).map_err(Error::from)?;
                    work.write_working_file_guarded_with_scope(scope, self.path(), self.identity, relative,
                        content, parents, expected, || self.check(work)).await
                }
            }).await.map_err(Error::from)?
        })
    }
    fn sync<'a>(&'a self, permit: PublicationPermit, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, std::result::Result<(), NativeLazyPublicationFailure>>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let callback = Arc::clone(&self.callback);
            let result = self.mount.sync_with_permit_work_guarded(
                permit, &work.native_mount_work(), work.cancellation(),
                move |actual: &NativeMaterializationWork| {
                    let callback = Arc::clone(&callback);
                    let native = actual.native_mount_work();
                    async move {
                        callback.check(&native)
                            .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
                            .map_err(Error::from)?.await
                            .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
                            .map_err(Error::from)
                    }
                },
            ).await;
            Ok(result.map_err(|failure| NativeLazyPublicationFailure {
                cause: failure.cause, source_acknowledged: failure.source_acknowledged,
            }))
        })
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
    // Aliases the same funded authority installed in the materialized source.
    callback: Option<Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>>,
    original_identity: acyclic_fs::NativeRootIdentity,
    writable: bool,
    publication: PublicationPermit,
    // Retains this volume's original resource gate through its payload lifetime.
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
    ) -> Result<Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>> {
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, work)?;
        if writable {
            owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        }
        owner.verify_with_native_work(false, work).await?;
        let mut custody = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(native_root_error)?;
        let (volume, mut volume_custody) = copy_native_volume_with_work(volume, work)?;
        custody.absorb(&mut volume_custody).map_err(native_root_error)?;
        work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
            ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
        Ok(Arc::new(Self { owner: Arc::clone(owner), volume, writable,
            work: work.clone(), custody }))
    }
}

impl<P: StreamProvider + 'static> acyclic_fs::native_mount::NativeMountCallbackGuard
    for OrdinaryVolumeCallback<P> {
    fn check<'a>(&'a self, native: &'a acyclic_fs::native_mount::NativeMountMeterWork)
        -> std::result::Result<acyclic_fs::native_mount::NativeMountCallbackFuture<'a>,
            acyclic_fs::native_mount::NativeMountError> {
        acyclic_fs::native_mount::NativeMountCallbackFuture::new_guarded::<Error, _, _>(native, || async move {
            let work = native.native_materialization_work();
            if !self.work.shares_allocation_context(&work) || !self.custody.matches_work(&work) {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Read, &work)?;
            if self.writable {
                self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Write, &work)?;
            }
            self.owner.verify_with_native_work(false, &work).await?;
            self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Read, &work)?;
            if self.writable {
                self.owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Write, &work)?;
            }
            Ok(())
        })
    }
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
            work.charge(acyclic_fs::WorkCounters { items_examined: self.volumes.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
            let count = self.volumes.iter().filter(|volume|
                matches!(volume.view, NativePreparedVolumeBackend::HeldLazy(_))).count();
            let mut custody = work.retain_owned(std::mem::size_of::<NativeProcessNamespaceScopes>() as u64)
                .map_err(native_root_error)?;
            let (scopes, mut vector_custody) = native_vec(count, work)?;
            custody.absorb(&mut vector_custody).map_err(native_root_error)?;
            let mut held = NativeProcessNamespaceScopes { scopes, custody };
            for volume in &self.volumes {
                work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                    ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
                if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                    lazy.snapshot(lazy.original_work())?.await?;
                    held.scopes.push(lazy.begin_scope(lazy.original_work())?);
                }
            }
            Ok(held)
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
    /// The actual issued owner Arc is retained by each source callback; no
    /// copied scope, replacement owner or derived grant is created.
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

    /// Prepares and attaches the original selection through its original ledger.
    /// The host's allocation and all authority inputs remain borrowed.
    pub fn prepare_with_native_work<'a, P: StreamProvider + 'static>(
        owner: &'a TaskJournalOwner<P>,
        callback_guards: &'a [crate::runtime::NativeVolumeCallbackGuard<P>],
        allocation: &'a NativePreparationRoot,
        options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        if callback_guards.len() != bindings.len() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source guards differ from the selected volume count" });
        }
        for (guard, binding) in callback_guards.iter().zip(bindings) {
            guard.for_volume(owner, binding.volume, binding.writable, work)?;
        }
        crate::NativeFutureBundle::new(work, || {
            Self::prepare_original(owner, NativePreparationCallbackGuards::Native(callback_guards),
                allocation, options, bindings, work)
        })
    }

    /// Prepares one immutable mixed declaration before any Scratch staging.
    /// Existing lazy mounts retain their actual original allocation context.
    pub fn prepare_mixed_with_native_work<'a, P: StreamProvider + 'static>(
        owner: &'a TaskJournalOwner<P>, guards: &'a [crate::runtime::NativeVolumeCallbackGuard<P>],
        allocation: &'a NativePreparationRoot, options: &'a NativeViewOptions,
        bindings: &'a [BorrowedNativeVolumeBinding<'a, A, O>],
        existing_lazy: &'a [Option<&'a NativeExistingLazyVolume>], work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        if guards.len() != bindings.len() || existing_lazy.len() != bindings.len() {
            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
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
                _ => return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)),
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
                .map_err(|_| native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?;
            if relative.as_os_str().is_empty() || relative != Path::new(binding.path) {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let expected = lazy.held.identity();
            let actual = lazy.held.original_work().root_identity(lazy.held.path()).map_err(native_root_error)?;
            if actual != expected {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual }));
            }
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            };
            if lazy.held.snapshot(lazy.held.original_work())?.await? != *snapshot {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
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
                for (index, ((binding, mapping), (generation, volume_work))) in bindings.iter()
                    .zip(&manifest.volumes).zip(generations).enumerate()
                {
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, &volume_work)?;
                    if let Some(lazy) = existing_lazy.get(index).copied().flatten() {
                        let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else { unreachable!() };
                        if lazy.held.path() != mapping.path || lazy.held.snapshot(&volume_work)?.await? != *snapshot {
                            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                        }
                        let identity = lazy.held.identity();
                        if identities.contains(&identity) {
                            return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                        }
                        identities.push(identity);
                        volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::HeldLazy(Arc::clone(&lazy.held)),
                            callback: None, original_identity: identity, writable: binding.writable,
                            publication: binding.publication, _work: volume_work });
                        continue;
                    }
                    let callback_guard = match &callback_guards {
                        NativePreparationCallbackGuards::Ordinary(owner) => Some(
                            OrdinaryVolumeCallback::new(owner, binding.volume, binding.writable, &volume_work).await?),
                        NativePreparationCallbackGuards::Native(guards) => Some(
                            guards[index].for_volume(owner, binding.volume, binding.writable, &volume_work)?),
                    };
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
                        &volume_work.native_mount_work(), cancellation, callback_guard.as_ref().map(Arc::clone),
                    ).await?;
                    owner.verify_with_native_work(false, work).await?;
                    verify_binding(owner, binding, &volume_work)?;
                    volumes.push(PreparedVolume { view: NativePreparedVolumeBackend::Materialized(view),
                        callback: callback_guard, original_identity: identity, writable: binding.writable,
                        publication: binding.publication, _work: volume_work });
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

    /// Publishes authorized authored Project edits and captures their actual
    /// existing lazy source cuts for the caller's explicit recursive fork.
    pub fn checkpoint_projects_with_native_work<'a, P: StreamProvider>(
        &'a self, owner: NativeViewOwner<'a, P>, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<(usize, acyclic_fs::LazySnapshotRef)>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            if !self._allocation.matches_work(work) || self.original_work.is_none()
                || owner.owner.task_binding() != (self.preparation.task, &self.preparation.original_fence)
            {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            self.authenticate_builtin_view(owner, work).await?;
            let count = self.volumes.iter().filter(|volume|
                matches!(volume.view, NativePreparedVolumeBackend::HeldLazy(_))).count();
            let mut custody = work.retain_owned(std::mem::size_of::<(
                Vec<(usize, acyclic_fs::LazySnapshotRef)>, acyclic_fs::NativeAllocation)>() as u64)
                .map_err(native_root_error)?;
            let (mut snapshots, mut vector_custody) = native_vec(count, work)?;
            custody.absorb(&mut vector_custody).map_err(native_root_error)?;
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view else { continue; };
                self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work).await?;
                if volume.writable {
                    self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work).await?;
                    lazy.sync(volume.publication, lazy.original_work())?.await?.map_err(|failure| failure.cause)?;
                }
                snapshots.push((index, lazy.snapshot(lazy.original_work())?.await?));
            }
            owner.verify_with_native_work(work).await?;
            Ok((snapshots, custody))
        })
    }

    /// Validates exactly one protected Common source selection without flushing
    /// its driver or acquiring the source owner's mutation authority. The held
    /// source's original data custody remains separate from current query work.
    pub fn inspect_project_snapshot_with_native_work<'a, P: StreamProvider + 'static>(
        &'a self, inspection: &'a crate::filesystem::TaskSourceInspection<P, A, O>,
        frozen: acyclic_fs::LazySnapshotRef, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a,
        (usize, acyclic_fs::LazySnapshotRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            if !self.original_work.as_ref().is_some_and(|original| self._allocation.matches_work(original)) {
                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            let authorization = inspection.project_authorization().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source query requires a protected Common project selection",
            })?;
            let project = &authorization.project;
            let generation = &authorization.generation;
            inspection.require_native_project_selection_with_native_work(
                self.preparation.task, project, generation, Some(frozen), work,
            ).await?;
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                    ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
                if &mapping.volume != project { continue; }
                let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view else {
                    return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                };
                let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                    return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                };
                if snapshot.workspace_id != frozen.workspace_id || snapshot.source.identity != frozen.source.identity {
                    return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                }
                let custody = work.retain_owned(std::mem::size_of::<(
                    usize, acyclic_fs::LazySnapshotRef, acyclic_fs::NativeAllocation)>() as u64)
                    .map_err(native_root_error)?;
                inspection.require_native_project_selection_with_native_work(
                    self.preparation.task, project, generation, Some(frozen), work,
                ).await?;
                lazy.validate_selected_snapshot(frozen, work)?.await?;
                inspection.require_native_project_selection_with_native_work(
                    self.preparation.task, project, generation, Some(frozen), work,
                ).await?;
                return Ok((index, frozen, custody));
            }
            Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))
        })
    }
    /// Reads an entire mounted Project/agent-private file under this view's
    /// original task, exact mapping and current READ grant. The caller retains
    /// original SDK tool-call admission/effect ownership; no journal is minted.
    /// The SDK funds intrinsic byte controls before its first content read;
    /// the returned carrier retains actual backing custody through its last alias.
    ///
    /// # Errors
    /// Rejects unmounted locators, stale owners, replacement roots, cancellation
    /// and over-cap files without truncating the returned bytes.
    pub async fn read_builtin_file<P: StreamProvider>(
        &self, owner: NativeViewOwner<'_, P>, _original_operation: crate::OperationId,
        relative: &Path, maximum_bytes: usize, work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let (mapping, volume, file) = self.builtin_volume(relative, VolumeOperation::Read, work)?;
        self.authenticate_builtin_view(owner, work).await?;
        if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
            self.verify_builtin_owner(owner, mapping, VolumeOperation::Read, work).await?;
            return lazy.read(file, maximum_bytes, lazy.original_work())?.await;
        }
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
        if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
            self.verify_builtin_owner(owner, mapping, VolumeOperation::Write, work).await?;
            if expected_sha256.is_some() {
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
            }
            return lazy.write(file, content, create_parents, expected_sha256, lazy.original_work())?.await;
        }
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
        for (mapping, volume) in self.manifest.volumes.iter().zip(&self.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_root_error)?;
            if let NativePreparedVolumeBackend::HeldLazy(lazy) = &volume.view {
                if let Ok(relative) = cwd.strip_prefix(&mapping.path) {
                    return lazy.original_work().validate_working_directory(
                        &mapping.path, volume.original_identity, relative,
                    ).map_err(native_root_error);
                }
            }
        }
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
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest.volumes) {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                ..acyclic_fs::WorkCounters::default() }).map_err(native_root_error)?;
            match &volume.view {
                NativePreparedVolumeBackend::Materialized(view) => {
                    let original = volume._work.native_mount_work();
                    view.validate_for_presentation_with_work(&original, volume._work.cancellation()).await?;
                    view.verify_unchanged_with_work(&original, volume._work.cancellation()).await?;
                },
                NativePreparedVolumeBackend::HeldLazy(lazy) => {
                    let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                        return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    };
                    let actual = lazy.snapshot(lazy.original_work())?.await?;
                    // Demand observations and authorized authored edits advance
                    // overlay/shadow state; the original workspace/source cut cannot.
                    if actual.workspace_id != snapshot.workspace_id || actual.source != snapshot.source
                        || lazy.path() != mapping.path
                        || lazy.original_work().root_identity(&mapping.path).map_err(native_root_error)? != volume.original_identity
                    {
                        return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                    }
                },
            }
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
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                failed_destination = Some(index);
                self.cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(native_root_error)?;
                owner.verify_with_native_work(false, work).await?;
                owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                if !volume.writable {
                    match &volume.view {
                        NativePreparedVolumeBackend::Materialized(view) =>
                            view.verify_unchanged_with_work(&volume._work.native_mount_work(), volume._work.cancellation()).await?,
                        NativePreparedVolumeBackend::HeldLazy(lazy) => {
                            let NativeVolumeNamespace::LazyMount { snapshot } = &mapping.namespace else {
                                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                            };
                            let actual = lazy.snapshot(lazy.original_work())?.await?;
                            if actual.workspace_id != snapshot.workspace_id || actual.source != snapshot.source
                                || actual.authored_generation != snapshot.authored_generation
                            {
                                return Err(native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
                            }
                        },
                    }
                }
            }
            for (index, (volume, mapping)) in self.volumes.iter().zip(&self.manifest.volumes).enumerate() {
                if !volume.writable { continue; }
                failed_destination = Some(index);
                match &volume.view {
                    NativePreparedVolumeBackend::Materialized(view) => {
                        let callback = Arc::clone(volume.callback.as_ref().ok_or_else(||
                            native_root_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout))?);
                        let result = view.sync_with_permit_with_work(
                            volume.publication, &volume._work.native_mount_work(), volume._work.cancellation(),
                            move |actual: &NativeMaterializationWork| {
                                let callback = Arc::clone(&callback);
                                let native = actual.native_mount_work();
                                async move {
                                    callback.check(&native)
                                        .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
                                        .map_err(Error::from)?.await
                                        .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)
                                        .map_err(Error::from)
                                }
                            },
                        ).await;
                        if let Err(failure) = result {
                            if failure.source_acknowledged {
                                let NativeMappingPayload { mapping, mut allocation } = mappings[index].take()
                                    .expect("original preadmitted acknowledged destination");
                                completed.push(mapping);
                                custody.absorb(&mut allocation)
                                    .expect("same original publication ledger and prepaid mapping");
                                failed_destination = None;
                            }
                            return Err(failure.cause);
                        }
                    },
                    NativePreparedVolumeBackend::HeldLazy(lazy) => {
                        owner.verify_with_native_work(false, work).await?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Read, work)?;
                        owner.require_volume_grant_with_native_work(&mapping.volume, VolumeOperation::Write, work)?;
                        if let Err(failure) = lazy.sync(volume.publication, lazy.original_work())?.await? {
                            if failure.source_acknowledged {
                                let NativeMappingPayload { mapping, mut allocation } = mappings[index].take()
                                    .expect("original preadmitted acknowledged destination");
                                completed.push(mapping);
                                custody.absorb(&mut allocation)
                                    .expect("same original publication ledger and prepaid mapping");
                                failed_destination = None;
                            }
                            return Err(failure.cause);
                        }
                    },
                }
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

/// Authenticated pre-action packet for one accepted task-preparation completion.
/// Only the protected preparation store can construct it. Accounting membership
/// never substitutes for the task owner's current authorization.
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
    pub(crate) fn association_digest(&self) -> &[u8; 32] { &self.association }
    pub(crate) fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.custody.matches_work(work)
            && self.journal.custody.as_ref().is_some_and(|guard| guard.matches_work(work))
    }
    /// Checks sealed packet associations only. The owner proof additionally
    /// verifies the live Fresh coordinator condition before accepting an action.
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

    /// Only the revision and existing unsigned work slots may change after the
    /// action. No strings, maps, buffers or new numbers are allocated here.
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

    /// Borrows the fixed source and prepaid output; the prepared History writer
    /// owns its accepted accounting observer and must never reserve here.
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

/// The original accepted future and journal packet; completing it constructs
/// no later SDK dispatch frame or replacement accounting context.
struct NativePreparationActionCompletion<'a> {
    future: crate::NativeFutureBundle<'a, (
        acyclic_fs::OwnedMaterializationJournal, acyclic_fs::NativeAllocation,
    )>,
}

impl acyclic_fs::MaterializationActionCompletion for NativePreparationActionCompletion<'_> {
    type Error = Error;

    async fn complete(self) -> Result<acyclic_fs::OwnedMaterializationJournal> {
        let (mut journal, mut output_custody) = self.future.await?;
        match journal.custody.as_mut() {
            Some(custody) => {
                if let Err(cause) = custody.absorb(&mut output_custody) {
                    // The journal and its intrinsic payload guard retire before
                    // the independent, still-owned output claim leaves.
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

    /// Authenticates the actual protected Preparing record before accepting one
    /// physical action. This cannot be minted by an admission-only observer.
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
        // The immutable initial packet is the key witness. Work can change only
        // inside this one already-accepted revision and never selects a new key.
        let (encoded, encoded_custody) = native_canonical(&journal.journal, &self.work)?;
        self.work.charge(acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64,
            ..acyclic_fs::WorkCounters::default() }).map_err(native_root_error)?;
        let association = *blake3::hash(&encoded).as_bytes();
        drop(encoded);
        drop(encoded_custody);
        let (key_bytes, key_custody) = native_bytes(&association, &self.work)?;
        let key_bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            key_bytes, key_custody, self.work.runtime_meter_owner(),
        ).map_err(|failure| native_owned_bytes_failure(failure, &self.work))?;
        let key = acyclic_stream::IdempotencyKey::new(key_bytes)?;
        let path = self.path(operation)?;
        let remaining = self.work.remaining().map_err(native_root_error)?;
        let mut value_custody = self.work.retain_owned(0).map_err(native_root_error)?;
        let value = crate::contract::json_value_bounded_native_trusted(
            &journal.journal, crate::contract::JsonBounds { maximum_bytes: remaining.bytes_encoded,
                maximum_items: remaining.items_examined, maximum_depth: 256 },
            crate::wire::ErrorCode::Storage, &mut |charge| {
                native_codec_charge_typed(charge, &self.work, &mut value_custody).map_err(native_root_error)
            },
        );
        let mut value = match value {
            Ok(value) => value,
            Err(error) => return Err(native_codec_error_custody(error, value_custody)),
        };
        let mut custody = self.work.retain_owned(std::mem::size_of::<NativePreparationCompletionTemplate>() as u64)
            .map_err(native_root_error)?;
        custody.absorb(&mut intent_custody).map_err(native_root_error)?;
        custody.absorb(&mut value_custody).map_err(native_root_error)?;
        // Every unsigned scalar has at most twenty decimal bytes. The exact
        // fixed tree establishes its concrete output bound before the action.
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
        self.work.admit(initialized).map_err(native_root_error)?;
        output.resize(output.capacity(), 0);
        self.work.spent(initialized).map_err(native_root_error)?;
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
            // Preserve the canonical stage engine's empty-record failure. A
            // read-only observer reports the marker's absence, never absence
            // of the physical namespace, and does not restart preparation.
            None if !adopt_original_work => return Ok(None),
            None => return Err(NativePreparationJournalError::Record("empty materializer journal")),
        };
        let _read_owner_custody = self.verify_read_owner().await?;
        let mut records = crate::store::operations::read_path_records_with_work(
            self.read_client(), &path, sequence, 1, &mut observer,
        ).await?.map_err(|error| NativePreparationJournalError::Owner(error.into_error(&observer)))?;
        let record = records.next(&mut observer).await?
            .ok_or(NativePreparationJournalError::Record("missing materializer journal"))?;
        // The canonical SDK cursor owns the real record payload and metadata;
        // its original observer records arrival before late cancellation.
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

    type ActionCompletion<'a> = NativePreparationActionCompletion<'a> where Self: 'a;

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
        // A node or key emitted by this fixed JSON tree consumes at least one
        // output byte. Scalar replacement additionally visits the closed work
        // fields. These are reserved maxima, never reported completed work.
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
        }).map_err(native_root_error)?;
        let proof = owner.accept_materialization_template_with_native_work(
            &template, &self.work,
        ).await?;
        let mut native_io = crate::store::history::native_work_io(&self.work)
            .map_err(native_root_error)?;
        let prepared = crate::distributed::PreparedMaterializationCommit::new(
            template, proof, reservation, &self.work, &mut Some(&mut native_io),
        )?;
        // Poll the genuine provider's preparation now, not inside the later
        // completion continuation after the physical mutation.
        let prepared = prepared.prepare_sdk(owner.stream()).await?;
        let future = crate::NativeFutureBundle::new(&self.work, || async move {
            let counters = prepared.completion_work()?;
            prepared.finish(owner.stream(), counters, None).await
        })?;
        Ok(NativePreparationActionCompletion { future })
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
        }).map_err(native_root_error)?;
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

struct NativeRootBindingInput {
    authority: Option<crate::core::Authority>,
    issuer: crate::core::AuthorityIssuer,
    scope: Option<Scope>,
    _custody: acyclic_fs::NativeAllocation,
}

struct NativeRootBindingPayload<T> {
    value: T,
    _custody: acyclic_fs::NativeAllocation,
}

/// Derives the original ROOT Bind operation from its actual private-volume
/// creation identity. This identity is data, not a grant or a creation proof.
pub fn native_root_output_binding_operation(
    audience: &crate::core::Authority,
    private_volume: &VolumeRef,
    original_creation: acyclic_fs::OperationId,
    work: &NativeMaterializationWork,
) -> Result<crate::OperationId> {
    let mut io = crate::store::history::native_work_io(work).map_err(native_root_error)?;
    let bytes = crate::store::history::canonical_bytes_with_work(
        &("harness/native-root-output-bind/v2", audience, private_volume, original_creation),
        crate::store::default_projection_read_limits().maximum_bytes,
        &mut Some(&mut io),
    )?;
    crate::store::history::HistoryIo::hashed_bytes(&mut io, bytes.len() as u64)?;
    let digest = blake3::hash(&bytes);
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest.as_bytes()[..16]);
    drop(bytes);
    Ok(crate::OperationId::from_bytes(identity))
}

/// Authenticates or publishes the original protected ROOT conversation Bind
/// and returns that exact SDK command's canonical bytes. Conversation identity
/// need not equal the logical task ID: the original owner authenticates the
/// actor, audience, scope and accepted binding through the canonical engine.
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
    let mut input = NativeRootBindingInput {
        authority: Some(authority.0), issuer: authority.1, scope: Some(authority.2),
        _custody: authority_custody,
    };
    let invalid = |reason| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason,
    };
    let audience = input.authority.as_ref().expect("unconsumed ROOT audience");
    let scope = input.scope.as_ref().expect("unconsumed ROOT scope");
    if audience.kind != crate::core::AggregateKind::Conversation
        || scope.agent().is_none()
        || !scope.capabilities().contains("conversation:bind")
        || limits.file_bytes == 0
    {
        return Err(invalid("ROOT binding requires its original conversation authority and finite event allowance"));
    }
    owner.verify_with_native_work(false, work).await?;
    let verifier = input.issuer.verifier_with_native_work(work)?;
    let verifier = NativeRootBindingPayload { value: verifier.0, _custody: verifier.1 };
    let operation = native_root_output_binding_operation(
        audience, private_volume, original_creation, work,
    )?;
    let reader = crate::store::HistoryReader::new_with_native_work(
        stream, audience, &verifier.value, work,
    )?;
    let reader = NativeRootBindingPayload { value: reader.0, _custody: reader.1 };
    let event = reader.value.operation_event_bounded_with_native_work(
        operation, limits.file_bytes, work,
    ).await?;
    let event = NativeRootBindingPayload { value: event.0, _custody: event.1 };
    let original_revision = match event.value.as_ref() {
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
        stream, input.authority.take().expect("unconsumed ROOT audience"),
        verifier.value, crate::core::SchemaRegistry::new(), work,
    ).await?;
    let mut aggregate = NativeRootBindingPayload {
        value: aggregate.0.with_limits(limits)?, _custody: aggregate.1,
    };
    if original_revision.is_none()
        && aggregate.value.reducer().conversation().and_then(|state| state.agent).is_some()
    {
        return Err(invalid("ROOT conversation changed during original binding admission"));
    }
    let key = work.display(&format_args!("harness/native-root-output-bind/v2/{operation}"))
        .map_err(native_root_error)?;
    let key = NativeRootBindingPayload {
        value: crate::IdempotencyKey::new(key.0)?, _custody: key.1,
    };
    let scope = input.scope.take().expect("unconsumed ROOT scope");
    let agent = scope.agent().expect("validated ROOT binding actor");
    let command = crate::core::Command {
        operation_id: operation, idempotency_key: key.value,
        expected_revision: original_revision.unwrap_or_else(|| aggregate.value.reducer().revision()),
        causal_parent: None, action: crate::core::Action::BindConversation { agent }, scope,
    };
    let mut io = crate::store::history::native_work_io(work).map_err(native_root_error)?;
    let encoded = crate::store::history::canonical_bytes_with_work(
        &command, crate::store::default_projection_read_limits().maximum_bytes,
        &mut Some(&mut io),
    )?;
    let encoded_custody = crate::store::history::HistoryIo::take_native_custody(
        &mut io, encoded.capacity() as u64,
    )?;
    let encoded = acyclic_stream::OwnedBytes::from_vec_with_guard(
        encoded, encoded_custody, work.runtime_meter_owner(),
    ).map_err(|failure| native_root_error(work.runtime_error(failure.into_error())))?;
    let applied = aggregate.value.execute_task_command_with_native_work(command, owner, work).await?;
    let applied = NativeRootBindingPayload { value: applied.0, _custody: applied.1 };
    drop(applied);
    Ok(encoded)
}
