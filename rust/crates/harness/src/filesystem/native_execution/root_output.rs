use super::*;
/// Authenticated, unmounted original ROOT result storage.
pub struct NativeRootOutputSource {
    pub(super) reader: Arc<dyn ContentResidencyVerifier>,
    pub(super) writer: Arc<dyn ContentPublisher>,
    pub(super) owner: Arc<dyn NativeRootOutputOwner>,
    pub(super) bind_operation: crate::OperationId,
    pub(super) bind_intent: [u8; 32],
    pub(super) creation_operation: acyclic_fs::OperationId,
    pub(super) workspace: acyclic_fs::WorkspaceId,
    pub(super) generation: acyclic_fs::GenerationId,
    pub(super) custody: acyclic_fs::NativeAllocation,
}

pub(super) trait NativeRootOutputOwner: std::any::Any + Send + Sync {
    fn same_owner(&self, other: &dyn NativeRootOutputOwner) -> bool;
    fn authorize<'a>(
        &'a self,
        source: &'a NativeRootOutputSource,
        operation: VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn check_context(
        &self,
        context: &crate::runtime::TaskContext,
        work: &NativeMaterializationWork,
    ) -> Result<()>;
}

impl<P: StreamProvider> NativeRootOutputOwner for TaskJournalOwner<P> {
    fn same_owner(&self, other: &dyn NativeRootOutputOwner) -> bool {
        (other as &dyn std::any::Any)
            .downcast_ref::<Self>()
            .is_some_and(|other| self.same_original_root_output_owner(other))
    }

    fn authorize<'a>(
        &'a self,
        source: &'a NativeRootOutputSource,
        operation: VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.require_volume_grant(
                source.writer.volume(),
                operation,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            let proof = self
                .authenticated_root_binding_with_native_work(work)
                .await?;
            if proof.bind_operation() != source.bind_operation
                || proof.intent_digest() != source.bind_intent
            {
                return Err(boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "ROOT content differs from its original protected binding",
                ));
            }
            Ok(())
        })
    }

    fn check_context(
        &self,
        context: &crate::runtime::TaskContext,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_task_context_binding(
            context,
            Some(&crate::executor::PreparationIo::from_native_work(work)?),
        )
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

    pub(super) fn require_content_work<'a>(
        &self,
        volume: &VolumeRef,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<&'a NativeMaterializationWork> {
        let work = io
            .and_then(crate::filesystem::PreparationIo::original_native_work)
            .ok_or(boundary(
                crate::wire::ErrorCode::Unsupported,
                "ROOT content requires its original native work",
            ))?;
        if !self.custody.work().shares_allocation_context(work) || volume != self.writer.volume() {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "ROOT content differs from its original volume or native work",
            ));
        }
        work.admit(Default::default())?;
        Ok(work)
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.owner.same_owner(other.owner.as_ref())
            && self.bind_operation == other.bind_operation
            && self.bind_intent == other.bind_intent
            && self.creation_operation == other.creation_operation
            && self.workspace == other.workspace
            && self.generation == other.generation
            && self.writer.volume() == other.writer.volume()
    }

    pub(crate) async fn verify_context_with_native_work(
        &self,
        context: &crate::runtime::TaskContext,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.custody.work().shares_allocation_context(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "ROOT output differs from its original native work",
            ));
        }
        self.owner.check_context(context, work)?;
        self.owner
            .authorize(self, VolumeOperation::Write, work)?
            .await
    }
}

impl ContentResidencyVerifier for NativeRootOutputSource {
    fn supports_read(
        &self,
        volume: &VolumeRef,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        self.require_content_work(volume, io)?;
        self.reader.supports_read(volume, io)
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum: u64,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        let work = self.require_content_work(reference.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
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

    fn supports_stage(&self, io: Option<&crate::filesystem::PreparationIo>) -> Result<()> {
        self.require_content_work(self.volume(), io)?;
        self.writer.supports_stage(io)
    }

    fn stage_once<'a>(
        &'a self,
        operation: crate::OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        let work = self.require_content_work(self.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
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
        operation: crate::OperationId,
        reference: &'a FileRef,
        bytes: &'a [u8],
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        let work = self.require_content_work(self.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.owner
                .authorize(self, VolumeOperation::Write, work)?
                .await?;
            self.writer.stage_at(operation, reference, bytes, io)?.await
        })
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation: crate::OperationId,
        maximum_bytes: u64,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (
                Option<FileRef>,
                Option<crate::conversation::ContentHold<'a>>,
            ),
        >,
    > {
        let work = self.require_content_work(self.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.owner
                .authorize(self, VolumeOperation::Read, work)?
                .await?;
            self.writer
                .observe_staged_once(operation, maximum_bytes, io)?
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
        creation_operation: acyclic_fs::OperationId,
        generation: &acyclic_fs::Generation<A, O>,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<NativeRootOutputSource>> {
        crate::NativeFutureBundle::new(work, || async {
            owner.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            if !owner.native_shared_work_matches(work) {
                return Err(boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "ROOT output requires its original paid shared owner",
                ));
            }
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let proof = owner
                .authenticated_root_binding_with_native_work(work)
                .await?;
            verifier.verify_audience(proof.audience(), &mut Some(&mut &io))?;
            crate::core::source_metadata_inspection_with_work(
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
                    crate::wire::ErrorCode::Unauthorized,
                    "ROOT output differs from its original agent, grants or provider",
                ));
            }
            let bind_operation = native_root_output_binding_operation(
                proof.audience(),
                volume,
                creation_operation,
                work,
            )?;
            if proof.bind_operation() != bind_operation {
                return Err(boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "ROOT creation differs from its protected original bind",
                ));
            }
            let bind_intent = proof.intent_digest();
            let _name_custody;
            let name = crate::filesystem::fork_preparer_io::volume_storage_name(volume, Some(&io))?;
            _name_custody = io.guard_owned(name.capacity() as u64)?;
            let created = self
                .filesystem
                .workspace_for_creation_operation_with_native_work(
                    &name,
                    acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable),
                    creation_operation,
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
                .ok_or_else(|| {
                    boundary(
                        crate::wire::ErrorCode::Unauthorized,
                        "ROOT output has no original creation acknowledgement",
                    )
                })?;
            if created.id() != generation.id()
                || created.workspace().id() != generation.workspace().id()
                || created.workspace().name().as_str() != name
                || generation.workspace().name().as_str() != name
            {
                return Err(boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "ROOT output differs from its acknowledged creation generation",
                ));
            }
            let workspace = created.workspace().id();
            let generation = created.id();
            drop(created);
            let maximum = owner.input_limits().file_bytes;
            let reader = crate::filesystem::FilesystemContentVerifier::new(
                Arc::clone(self),
                verifier,
                scope,
                maximum,
                Some(&io),
            )?;
            let writer = crate::filesystem::FilesystemContentPublisher::new(
                Arc::clone(self),
                volume,
                verifier,
                scope,
                maximum,
                Some(&io),
            )?;
            owner
                .authenticated_root_binding_with_native_work(work)
                .await?;
            owner.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )?;
            let custody = work.claim(
                std::mem::size_of::<NativeRootOutputSource>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            Ok(Arc::new(NativeRootOutputSource {
                reader,
                writer,
                owner: Arc::clone(owner) as Arc<dyn NativeRootOutputOwner>,
                bind_operation,
                bind_intent,
                creation_operation,
                workspace,
                generation,
                custody,
            }))
        })?
        .await
    }
}
