use super::super::root_binding::{boundary, native_root_output_binding_operation};
use super::*;
use crate::{
    NativeFutureBundle, OperationId,
    conversation::{
        BoundedContentFuture, ContentHold, ContentPublisher, ContentResidencyVerifier, FileRef,
    },
    runtime::{ContentBindings, TaskContext},
    store::HistoryWork,
    wire::ErrorCode::Unauthorized,
};

/// Authenticated output from the original private ROOT creation.
pub struct NativeRootOutputSource {
    reader: Arc<dyn ContentResidencyVerifier>,
    writer: Arc<dyn ContentPublisher>,
    owner: Arc<dyn Owner>,
    binding: (OperationId, [u8; 32]),
    custody: NativeAllocation,
}

trait Owner: std::any::Any + Send + Sync {
    fn same_owner(&self, other: &dyn Owner) -> bool;
    fn authorize<'a>(
        &'a self,
        source: &'a NativeRootOutputSource,
        operation: VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>>;
    fn check_context(&self, context: &TaskContext, work: &NativeMaterializationWork) -> Result<()>;
}

impl<P: StreamProvider> Owner for TaskJournalOwner<P> {
    fn same_owner(&self, other: &dyn Owner) -> bool {
        (other as &dyn std::any::Any)
            .downcast_ref::<Self>()
            .is_some_and(|other| self.same_original_root_output_owner(other))
    }

    fn authorize<'a>(
        &'a self,
        source: &'a NativeRootOutputSource,
        operation: VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.require_volume_grant(
                source.volume(),
                operation,
                &mut Some(&mut HistoryWork::new(io)),
            )?;
            let proof = self
                .authenticated_root_binding_with_native_work(work)
                .await?;
            if (proof.bind_operation(), proof.intent_digest()) != source.binding {
                return Err(boundary(
                    Unauthorized,
                    "ROOT content differs from its original protected binding",
                ));
            }
            Ok(())
        })
    }

    fn check_context(&self, context: &TaskContext, work: &NativeMaterializationWork) -> Result<()> {
        self.require_task_context_binding(context, Some(&PreparationIo::from_native_work(work)?))
    }
}

impl NativeRootOutputSource {
    pub(crate) fn retained_content_binding(self: &Arc<Self>) -> ContentBindings {
        ContentBindings {
            reader: self.clone(),
            writer: Some(self.clone()),
        }
    }

    fn content_work<'a>(
        &self,
        volume: &VolumeRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<&'a NativeMaterializationWork> {
        let work = io
            .and_then(PreparationIo::original_native_work)
            .ok_or(boundary(
                crate::wire::ErrorCode::Unsupported,
                "ROOT content requires its original native work",
            ))?;
        if !self.custody.work().shares_allocation_context(work) || volume != self.volume() {
            return Err(boundary(
                Unauthorized,
                "ROOT content differs from its original volume or native work",
            ));
        }
        work.admit(Default::default())?;
        Ok(work)
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.owner.same_owner(other.owner.as_ref())
            && self.binding == other.binding
            && self.volume() == other.volume()
    }

    pub(crate) async fn verify_context_with_native_work(
        &self,
        context: &TaskContext,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let io = PreparationIo::from_native_work(work)?;
        self.content_work(self.volume(), Some(&io))?;
        self.owner.check_context(context, work)?;
        self.owner
            .authorize(self, VolumeOperation::Write, work)?
            .await
    }
}

impl ContentResidencyVerifier for NativeRootOutputSource {
    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        self.content_work(volume, io)?;
        self.reader.supports_read(volume, io)
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        let work = self.content_work(reference.volume(), io)?;
        BoundedContentFuture::new(io, || async move {
            self.owner
                .authorize(self, VolumeOperation::Read, work)?
                .await?;
            self.reader.read(reference, maximum, io)?.await
        })
    }
}

impl ContentPublisher for NativeRootOutputSource {
    fn volume(&self) -> &VolumeRef {
        self.writer.volume()
    }

    fn supports_stage(&self, io: Option<&PreparationIo>) -> Result<()> {
        self.content_work(self.volume(), io)?;
        self.writer.supports_stage(io)
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        let work = self.content_work(self.volume(), io)?;
        BoundedContentFuture::new(io, || async move {
            self.owner
                .authorize(self, VolumeOperation::Write, work)?
                .await?;
            self.writer
                .stage_once(operation, path, bytes, media_type, display_name, io)?
                .await
        })
    }

    fn stage_at<'a>(
        &'a self,
        operation: OperationId,
        reference: &'a FileRef,
        bytes: &'a [u8],
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        let work = self.content_work(self.volume(), io)?;
        BoundedContentFuture::new(io, || async move {
            self.owner
                .authorize(self, VolumeOperation::Write, work)?
                .await?;
            self.writer.stage_at(operation, reference, bytes, io)?.await
        })
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        let work = self.content_work(self.volume(), io)?;
        BoundedContentFuture::new(io, || async move {
            self.owner
                .authorize(self, VolumeOperation::Read, work)?
                .await?;
            self.writer
                .observe_staged_once(operation, maximum, io)?
                .await
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Opens output from the original private ROOT creation and committed bind.
    pub async fn root_output_source<P: StreamProvider>(
        self: &Arc<Self>,
        owner: &Arc<TaskJournalOwner<P>>,
        volume: &VolumeRef,
        creation: acyclic_fs::OperationId,
        generation: &acyclic_fs::Generation<A, O>,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<NativeRootOutputSource>> {
        NativeFutureBundle::new(work, || async {
            let io = PreparationIo::from_native_work(work)?;
            owner.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                    work,
                )?)),
            )?;
            if !owner.native_shared_work_matches(work) {
                return Err(boundary(
                    Unauthorized,
                    "ROOT output requires its original paid shared owner",
                ));
            }
            let proof = owner
                .authenticated_root_binding_with_native_work(work)
                .await?;
            verifier.verify_audience(proof.audience(), &mut Some(&mut &io))?;
            crate::core::inspect_metadata(
                &(scope.capabilities(), owner.input_grants()),
                &mut Some(&mut &io),
            )?;
            if scope.capabilities() != owner.input_grants()
                || scope.agent() != Some(proof.agent())
                || volume.provider() != &self.provider
                || volume.class() != crate::conversation::VolumeClass::AgentPrivate
                || volume.owner() != &crate::conversation::VolumeOwner::Agent(proof.agent())
            {
                return Err(boundary(
                    Unauthorized,
                    "ROOT output differs from its original agent, grants or provider",
                ));
            }
            let binding = (
                native_root_output_binding_operation(proof.audience(), volume, creation, work)?,
                proof.intent_digest(),
            );
            if proof.bind_operation() != binding.0 {
                return Err(boundary(
                    Unauthorized,
                    "ROOT creation differs from its protected original bind",
                ));
            }
            let name = crate::filesystem::fork_preparer_io::volume_storage_name(volume, Some(&io))?;
            let _name_custody = io.guard_owned(name.capacity() as u64)?;
            let created = self
                .filesystem
                .workspace_for_creation_operation_with_native_work(
                    &name,
                    acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable),
                    creation,
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
                .ok_or(boundary(
                    Unauthorized,
                    "ROOT output has no original creation acknowledgement",
                ))?;
            if created.id() != generation.id()
                || created.workspace().id() != generation.workspace().id()
                || created.workspace().name().as_str() != name
                || generation.workspace().name().as_str() != name
            {
                return Err(boundary(
                    Unauthorized,
                    "ROOT output differs from its acknowledged creation generation",
                ));
            }
            drop(created);
            let maximum = owner.input_limits().file_bytes;
            let source = Arc::new(NativeRootOutputSource {
                reader: super::super::FilesystemContentVerifier::new(
                    self.clone(),
                    verifier,
                    scope,
                    maximum,
                    Some(&io),
                )?,
                writer: super::super::FilesystemContentPublisher::new(
                    self.clone(),
                    volume,
                    verifier,
                    scope,
                    maximum,
                    Some(&io),
                )?,
                owner: owner.clone(),
                binding,
                custody: work.claim(
                    std::mem::size_of::<NativeRootOutputSource>() as u64,
                    1,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?,
            });
            source
                .owner
                .authorize(&source, VolumeOperation::Write, work)?
                .await?;
            Ok(source)
        })?
        .await
    }
}
