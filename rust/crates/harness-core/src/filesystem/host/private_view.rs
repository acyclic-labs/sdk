//! Private namespace reads keep the authored generation and source cut together.
use super::*;
use acyclic_fs::{
    LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore, demand::DemandSource,
    kernel::KernelAccounting,
};

pub(super) const FORMAT: &str = "lazy-v1";

pub(super) trait PrivateView: Send + Sync {
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId;
    fn watch(
        &self,
        sink: Arc<dyn acyclic_fs::demand::SourceChangeSink>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn acyclic_fs::demand::SourceWatch>>>;
    fn validate<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;
    fn snapshot<'a>(
        &'a self,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, LazySnapshotRef>>;
    fn stat<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        path: &'a str,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<acyclic_fs::WorkspaceStat>>>;
    fn read<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        path: &'a str,
        range: acyclic_fs::ByteRange,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>>;
    fn directory<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        path: &'a str,
        after: Option<&'a [u8]>,
        maximum: u32,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (
                acyclic_fs::LazyDirectoryListing,
                acyclic_fs::kernel::KernelOwnedAllocation,
            ),
        >,
    >;
}

impl<A, O, D, S> PrivateView for LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId {
        self.workspace().id()
    }
    fn watch(
        &self,
        sink: Arc<dyn acyclic_fs::demand::SourceChangeSink>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn acyclic_fs::demand::SourceWatch>>> {
        self.watch_source(sink, ctx).map_err(Into::into)
    }
    fn validate<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            self.validate_snapshot(cut, ctx).await.map_err(Into::into)
        })
    }
    fn snapshot<'a>(
        &'a self,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, LazySnapshotRef>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            self.snapshot(ctx).await.map_err(Into::into)
        })
    }
    fn stat<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        path: &'a str,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<acyclic_fs::WorkspaceStat>>>
    {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            self.workspace_stat_at_snapshot(cut, path, ctx)
                .await
                .map_err(Into::into)
        })
    }
    fn read<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        path: &'a str,
        range: acyclic_fs::ByteRange,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>>
    {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            let (bytes, stat, _) = self
                .read_range_at_snapshot(cut, path, Some(range), range.length, ctx)
                .await?;
            let length = stat
                .logical_bytes
                .ok_or(acyclic_fs::LazyWorkspaceError::NotRegularFile)?;
            Ok((bytes.bytes, length))
        })
    }
    fn directory<'a>(
        &'a self,
        cut: &'a LazySnapshotRef,
        path: &'a str,
        after: Option<&'a [u8]>,
        maximum: u32,
        ctx: &'a mut KernelAccounting<'_>,
        io: &'a PreparationIo,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (
                acyclic_fs::LazyDirectoryListing,
                acyclic_fs::kernel::KernelOwnedAllocation,
            ),
        >,
    > {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            self.list_directory_listing_at_snapshot(cut, path, after, maximum, ctx)
                .await
                .map_err(Into::into)
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn private_current_generation(
        &self,
        volume: &VolumeRef,
        expected: Option<&GenerationRef>,
        io: &PreparationIo,
    ) -> Result<Option<GenerationRef>> {
        if self.private_view.is_none() {
            return Ok(None);
        }
        let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let _hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = self.open(&reference, Some(io)).await?;
        if let Some(expected) = expected {
            match self
                .private_snapshot(&workspace, Some(expected), io)
                .await?
            {
                Some(cut) => self.validate_private_snapshot(&cut, io).await?,
                None => {
                    self.generation(&workspace, expected, Some(io)).await?;
                }
            }
        }
        let Some(cut) = self.private_snapshot(&workspace, None, io).await? else {
            return Ok(None);
        };
        self.private_generation_ref(&cut, None, io).map(Some)
    }

    pub(in crate::filesystem) fn private_changes(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<PrivateChanges>> {
        let Some(view) = &self.private_view else {
            return Ok(None);
        };
        let mut ctx = KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        };
        let signal_bytes =
            acyclic_fs::NativeMaterializationWork::shared_allocation_bytes::<PrivateSignal>()
                .map_err(|_| Error::Invalid("private source signal layout overflow".into()))?;
        let allocation = acyclic_fs::kernel::RetainedAllocation::claim(signal_bytes, 1, &mut ctx)?;
        let (sender, receiver) = tokio::sync::watch::channel(true);
        let signal = Arc::new(PrivateSignal {
            sender,
            _allocation: allocation,
        });
        Ok(view
            .watch(signal, &mut ctx)?
            .map(|watch| PrivateChanges { watch, receiver }))
    }

    /// Composes the private projections with the agent's existing authored
    /// volume. Both local and served sessions call this after admission.
    /// Companion state stays on the same authority as the session volumes.
    pub(in crate::filesystem) async fn for_agent_views<P>(
        self: &Arc<Self>,
        streams: Arc<P>,
        agent: &crate::agent::AgentPath,
        volume: &VolumeRef,
        verifier: &crate::core::AuthorityVerifier,
        scope: &crate::core::Scope,
    ) -> Result<Arc<Self>>
    where
        P: acyclic_stream::NativeStreamProvider + ?Sized + 'static,
    {
        use acyclic_fs::demand::{Empty, Routed};
        if volume.provider() != &self.provider
            || volume.class() != VolumeClass::AgentPrivate
            || volume.owner()
                != &crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes(
                    agent.id_bytes(),
                ))
            || self.filesystem.operation_claims().owner_identity()
                != streams.context().operation_claims().owner_identity()
        {
            return Err(Error::Unauthorized(
                "private views require the original agent and volume authority".into(),
            ));
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(error, _)| Error::from(error))?;
        let io = PreparationIo::from_native_work(&work)?;
        let reference = bounded_io::content_workspace(&self.provider, volume, Some(&io))?;
        let _reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = self.open(&reference, Some(&io)).await?;
        let limits = workspace.volume_config().limits;
        let reader = super::super::FilesystemContentVerifier::new(
            self.clone(),
            verifier,
            scope,
            None,
            None,
        )?;
        let views = crate::ui::StreamUiProvider::new(
            streams.clone(),
            reader,
            agent.clone(),
            crate::ui::View::new(64).map_err(|error| Error::Invalid(error.to_string()))?,
            *b"agent-ui-path-v1",
            private_navigation,
        );
        let source = Routed::new(
            Empty,
            acyclic_fs::kernel::LogicalName::new(
                acyclic_fs::kernel::NameEncoding::Utf8,
                b"ui".to_vec(),
            )
            .map_err(|_| io.invalid("invalid private view route"))?,
            views.clone().into_source(),
            limits,
        );
        let source = Arc::new(Routed::new(
            source,
            acyclic_fs::kernel::LogicalName::new(
                acyclic_fs::kernel::NameEncoding::Utf8,
                b"evals".to_vec(),
            )
            .map_err(|_| io.invalid("invalid evaluation view route"))?,
            views.into_evaluation_source(),
            limits,
        ));
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let view = LazyWorkspace::bind(
            workspace,
            source,
            acyclic_fs::StreamAuthorityStore::new(streams),
            &mut ctx,
        )
        .await?;
        self.for_private_view(volume, view.share(&mut ctx)?)
    }

    /// Binds an existing private lazy workspace to the same authorized range reader.
    /// Construction starts no IO, mount, watch, cache or background task.
    pub fn for_private_view<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &self,
        volume: &VolumeRef,
        view: Arc<LazyWorkspace<A, O, D, S>>,
    ) -> Result<Arc<Self>> {
        if volume.class() != VolumeClass::AgentPrivate {
            return Err(Error::Unauthorized(
                "computed private binding requires an agent volume".into(),
            ));
        }
        let mut host = self.for_workspace(volume, view.workspace().clone())?;
        Arc::get_mut(&mut host)
            .ok_or_else(|| Error::Conflict("bind the private view before sharing its host".into()))?
            .private_view = Some(view);
        Ok(host)
    }

    pub(super) async fn private_snapshot(
        &self,
        workspace: &Workspace<A, O>,
        expected: Option<&GenerationRef>,
        io: &PreparationIo,
    ) -> Result<Option<acyclic_fs::kernel::Retained<LazySnapshotRef>>> {
        let Some(view) = self
            .private_view
            .as_ref()
            .filter(|view| view.workspace_id() == workspace.id())
        else {
            return Ok(None);
        };
        if expected.is_some_and(|expected| expected.as_resource().version() != Some(FORMAT)) {
            return Ok(None);
        }
        let owner = io.account(acyclic_fs::kernel::NativeAccounting::take)?;
        let native_io = PreparationIo::from_native_work(&owner.work)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: io.cancellation(),
        };
        let result = async {
            match expected {
                Some(expected) => {
                    expected.validate(&mut Some(&mut native_io.history_io()))?;
                    if expected.as_resource().provider() != &self.provider {
                        return Err(io.invalid("lazy generation belongs to another provider"));
                    }
                    let cut = ctx
                        .decode_json::<LazySnapshotRef>(expected.as_resource().key())
                        .map_err(|error| match error {
                            acyclic_fs::kernel::MetadataDecodeError::Work(error) => {
                                Error::from(error)
                            }
                            acyclic_fs::kernel::MetadataDecodeError::Codec(_) => {
                                io.invalid("invalid lazy generation")
                            }
                        })?;
                    if cut.workspace_id != workspace.id() {
                        return Err(io.invalid("lazy generation belongs to another workspace"));
                    }
                    Ok(cut)
                }
                None => {
                    let cut = view.snapshot(&mut ctx, &native_io)?.await?;
                    Ok(
                        acyclic_fs::kernel::RetainedAllocation::claim(0, 0, &mut ctx)?
                            .into_value(cut),
                    )
                }
            }
        }
        .await;
        drop(ctx);
        io.account(|ctx| owner.restore(ctx))?;
        result.map(Some)
    }

    pub(super) fn private_generation_ref(
        &self,
        cut: &LazySnapshotRef,
        expected: Option<&GenerationRef>,
        io: &PreparationIo,
    ) -> Result<GenerationRef> {
        let bytes;
        let key = match expected {
            Some(expected) => expected.as_resource().key(),
            None => {
                bytes = io.encode(cut, io.remaining()?.bytes_encoded, false)?;
                &bytes
            }
        };
        crate::executor::with_history_io(Some(io), |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let provider = crate::core::copy_provider(&self.provider, &mut quota)?;
            let key = crate::core::copy_copy_vector(key, &mut quota)?;
            let version = crate::core::copy_text(FORMAT, &mut quota)?;
            let resource = crate::resources::ResourceRef::new(
                crate::resources::ResourceKind::Generation,
                provider,
                key,
                Some(version),
                quota.work(),
            )?;
            let reference = GenerationRef::from_resource(resource, quota.work())?;
            quota.transfer();
            Ok(reference)
        })
    }

    pub(super) async fn stat_private_snapshot(
        &self,
        cut: &LazySnapshotRef,
        path: &str,
        io: &PreparationIo,
    ) -> Result<Option<acyclic_fs::WorkspaceStat>> {
        let view = self
            .private_view
            .as_ref()
            .filter(|view| view.workspace_id() == cut.workspace_id)
            .ok_or_else(|| io.invalid("private source binding is unavailable"))?;
        let owner = io.account(acyclic_fs::kernel::NativeAccounting::take)?;
        let native_io = PreparationIo::from_native_work(&owner.work)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: io.cancellation(),
        };
        let result = async { view.stat(cut, path, &mut ctx, &native_io)?.await }.await;
        drop(ctx);
        io.account(|ctx| owner.restore(ctx))?;
        result
    }

    pub(super) async fn read_private_snapshot(
        &self,
        cut: &LazySnapshotRef,
        path: &str,
        range: acyclic_fs::ByteRange,
        io: &PreparationIo,
    ) -> Result<(acyclic_stream::OwnedBytes, u64)> {
        let view = self
            .private_view
            .as_ref()
            .filter(|view| view.workspace_id() == cut.workspace_id)
            .ok_or_else(|| io.invalid("private source binding is unavailable"))?;
        let owner = io.account(acyclic_fs::kernel::NativeAccounting::take)?;
        let native_io = PreparationIo::from_native_work(&owner.work)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: io.cancellation(),
        };
        let result = async { view.read(cut, path, range, &mut ctx, &native_io)?.await }.await;
        drop(ctx);
        io.account(|ctx| owner.restore(ctx))?;
        result
    }

    pub(super) async fn validate_private_snapshot(
        &self,
        cut: &LazySnapshotRef,
        io: &PreparationIo,
    ) -> Result<()> {
        let view = self
            .private_view
            .as_ref()
            .ok_or_else(|| io.invalid("private source binding is unavailable"))?;
        let owner = io.account(acyclic_fs::kernel::NativeAccounting::take)?;
        let native_io = PreparationIo::from_native_work(&owner.work)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: io.cancellation(),
        };
        let result = async { view.validate(cut, &mut ctx, &native_io)?.await }.await;
        drop(ctx);
        io.account(|ctx| owner.restore(ctx))?;
        result
    }
}

struct PrivateSignal {
    sender: tokio::sync::watch::Sender<bool>,
    _allocation: acyclic_fs::kernel::RetainedAllocation,
}

impl acyclic_fs::demand::SourceChangeSink for PrivateSignal {
    fn source_changed(
        &self,
        _: &[acyclic_fs::demand::SourceChange],
        ctx: &mut KernelAccounting<'_>,
    ) -> std::result::Result<(), acyclic_fs::demand::DemandError> {
        ctx.check()?;
        self.sender.send_replace(true);
        Ok(())
    }
    fn delivery_lost(&self) {
        self.sender.send_replace(false);
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

/// One request's retained source wait. Dropping it retires the source owner;
/// sessions without a waiting reader keep no subscription or task.
pub(in crate::filesystem) struct PrivateChanges {
    watch: Box<dyn acyclic_fs::demand::SourceWatch>,
    receiver: tokio::sync::watch::Receiver<bool>,
}

impl PrivateChanges {
    pub(in crate::filesystem) async fn changed(&mut self) -> Result<()> {
        self.receiver
            .changed()
            .await
            .map_err(|_| Error::Storage("private source subscription closed".into()))?;
        if *self.receiver.borrow_and_update() {
            Ok(())
        } else {
            Err(Error::Storage("private source delivery lost".into()))
        }
    }
}

impl Drop for PrivateChanges {
    fn drop(&mut self) {
        // This retires the birth-paid runtime owner, including cancellation.
        let _ = self.watch.stop_accepted_cleanup();
    }
}

fn private_navigation(
    child: &acyclic_stream::StreamPath,
    ctx: &mut KernelAccounting<'_>,
) -> std::result::Result<acyclic_fs::kernel::OwnedSourceChunk, acyclic_fs::demand::DemandError> {
    acyclic_fs::kernel::OwnedSourceChunk::produce(ctx, |sink| {
        write!(sink, "/{}/ui", child.as_str()).map_err(acyclic_fs::demand::DemandError::from)
    })
}
