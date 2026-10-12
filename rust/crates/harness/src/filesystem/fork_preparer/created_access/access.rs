//! Authenticated resource bindings over the existing content providers.
use super::*;

pub(super) enum CreatedFilesystemAccess<A, O> {
    Task {
        request: ResourcePreparationRequest,
        proof: ResourceCreationProofRef,
        origin: ResourceCreationOrigin,
        generation: GenerationRef,
        prepared_generation: GenerationRef,
        creation_operation: acyclic_fs::OperationId,
        reader: Arc<FilesystemContentVerifier<A, O>>,
        writer: Arc<FilesystemContentPublisher<A, O>>,
    },
    Reference {
        allocation: Arc<Self>,
        file: FileRef,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Root {
        allocation: Arc<FilesystemRootBootstrapAllocation<A, O>>,
        _custody: acyclic_fs::NativeAllocation,
        reader: Arc<FilesystemContentVerifier<A, O>>,
        writer: Option<Arc<FilesystemContentPublisher<A, O>>>,
    },
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    CreatedFilesystemAccess<A, O>
{
    pub(super) fn reader(&self) -> &FilesystemContentVerifier<A, O> {
        match self {
            Self::Task { reader, .. } => reader,
            Self::Reference { allocation, .. } => allocation.reader(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Root { reader, .. } => reader,
        }
    }
    fn volume(&self) -> &VolumeRef {
        match self {
            Self::Task { writer, .. } => writer.volume(),
            Self::Reference { file, .. } => file.volume(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Root { allocation, .. } => &allocation.request.volume,
        }
    }
    fn maximum(&self) -> u64 {
        self.reader().maximum_bytes
    }
    fn writer(&self) -> Result<&FilesystemContentPublisher<A, O>> {
        match self {
            Self::Task { writer, .. } => Ok(writer),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Root {
                writer: Some(writer),
                ..
            } => Ok(writer),
            _ => Err(Error::Unauthorized(
                "created resource has no declared publication authority".into(),
            )),
        }
    }
    async fn task_allocation(&self, io: Option<&PreparationIo>) -> Result<AuthenticatedAllocation> {
        let Self::Task {
            request,
            proof,
            origin,
            generation,
            prepared_generation,
            ..
        } = self
        else {
            unreachable!("only task allocations carry child creation identity")
        };
        let current = authenticate(
            &self.reader().host,
            self.reader().verifier.audience(),
            request,
            proof,
            io,
        )
        .await?;
        if &current.origin != origin
            || &current.generation != generation
            || &current.prepared_generation != prepared_generation
            || &current.volume != self.volume()
        {
            return Err(Error::Unauthorized(
                "original created allocation evidence changed".into(),
            ));
        }
        Ok(current)
    }
    pub(super) async fn authenticate(
        &self,
        volume: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if volume != self.volume() {
            return Err(Error::Unauthorized(
                "created content handle names another volume".into(),
            ));
        }
        match self {
            Self::Task { .. } => {
                self.task_allocation(io).await?;
            }
            Self::Reference { .. } => {
                return Err(Error::Unauthorized(
                    "exact-file access does not authorize a directory".into(),
                ));
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Root { allocation, .. } => {
                let owned;
                let io = match io {
                    Some(io) => io,
                    None => {
                        owned = PreparationIo::from_native_work(allocation._custody.work())?;
                        &owned
                    }
                };
                allocation.reauthenticate_with_io(io).await?;
            }
        }
        Ok(())
    }
    async fn authenticate_file(&self, file: &FileRef, io: Option<&PreparationIo>) -> Result<()> {
        match self {
            Self::Reference {
                allocation,
                file: bound,
            } => {
                if file != bound {
                    return Err(Error::Unauthorized(
                        "created reference handle names another immutable file".into(),
                    ));
                }
                if !allocation
                    .task_allocation(io)
                    .await?
                    .reference_files
                    .contains(file)
                {
                    return Err(Error::Unauthorized(
                        "original inherited reference evidence changed".into(),
                    ));
                }
                Ok(())
            }
            _ => self.authenticate(file.volume(), io).await,
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn root(&self) -> Option<&Arc<FilesystemRootBootstrapAllocation<A, O>>> {
        match self {
            Self::Root { allocation, .. } => Some(allocation),
            _ => None,
        }
    }
    async fn publish(
        &self,
        operation: OperationId,
        path: &str,
        bytes: &[u8],
        media: &str,
        display: &str,
        source: Option<&FileRef>,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        #[cfg(not(target_arch = "wasm32"))]
        let root = self.root().map(Arc::as_ref);
        #[cfg(target_arch = "wasm32")]
        let root = None::<&FilesystemRootBootstrapAllocation<A, O>>;
        #[cfg(not(target_arch = "wasm32"))]
        let lease = match root {
            Some(root) => Some(root.publication_lease.as_ref().ok_or_else(|| {
                Error::Unauthorized("original Root content has no publication lease".into())
            })?),
            None => None,
        };
        #[cfg(target_arch = "wasm32")]
        let lease = None;
        if let Some(lease) = lease {
            match io {
                Some(io) => io.verify_publication_lease(lease.as_ref()).await?,
                None => lease.verify().await?,
            }
        }
        let volume = source.map_or(self.volume(), FileRef::volume);
        #[cfg(not(target_arch = "wasm32"))]
        if self.root().is_none()
            && let Some(work) = io.and_then(PreparationIo::original_native_work)
        {
            let authentication = PreparationIo::from_native_work(work)?;
            self.authenticate(volume, Some(&authentication)).await?;
        } else {
            self.authenticate(volume, io).await?;
        }
        #[cfg(target_arch = "wasm32")]
        self.authenticate(volume, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(root), Some(source)) = (root, source) {
            let owned;
            let source_io = match io {
                Some(io) => io,
                None => {
                    owned = PreparationIo::for_unmeasured(root._custody.work().cancellation())?;
                    &owned
                }
            };
            if root
                .source_generation(Some(source), Some(source_io))?
                .is_some()
            {
                self.read_snapshot_file(source, self.reader().maximum_bytes, source_io)
                    .await?
                    .ok_or_else(|| {
                        source_io.invalid(
                            "Root replacement does not authenticate its original lazy source",
                        )
                    })?;
            }
        }
        match source {
            Some(source) => {
                self.writer()?
                    .replace(operation, source, bytes, io, lease, root)
                    .await
            }
            None => {
                self.writer()?
                    .publish_once(operation, path, bytes, media, display, io, lease, root)
                    .await
            }
        }
    }
    async fn observe(
        &self,
        operation: OperationId,
        maximum: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        self.authenticate(self.volume(), io).await?;
        self.writer()?
            .observe_publication(operation, maximum.min(self.maximum()), io)
            .await
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    crate::resource_preparation::ScratchCreationIdentity for CreatedFilesystemAccess<A, O>
{
    fn original_task(&self) -> crate::TaskId {
        crate::TaskId::from_bytes(self.origin().intended_child.into_bytes())
    }
    fn agent(&self) -> crate::AgentId {
        self.reader()
            .scope
            .agent()
            .expect("created reader binds its original agent")
    }
    fn volume(&self) -> &VolumeRef {
        self.volume()
    }
    fn creation_operation(&self) -> acyclic_fs::OperationId {
        let Self::Task {
            creation_operation, ..
        } = self
        else {
            unreachable!("scratch identity comes from a task allocation")
        };
        *creation_operation
    }
    fn initial_generation(&self) -> &GenerationRef {
        let Self::Task { generation, .. } = self else {
            unreachable!("scratch identity comes from a task allocation")
        };
        generation
    }
    fn origin(&self) -> &ResourceCreationOrigin {
        let Self::Task { origin, .. } = self else {
            unreachable!("scratch identity comes from a task allocation")
        };
        origin
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier
    for CreatedFilesystemAccess<A, O>
{
    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        if volume != self.volume() {
            return Ok(false);
        }
        self.reader().supports_read(volume, io)
    }
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        BoundedContentFuture::new(io, || async move {
            if matches!(self, Self::Reference { .. }) {
                self.authenticate_file(file, io).await?;
                return self
                    .reader()
                    .host
                    .read_pinned_io(file, maximum.min(self.maximum()), io)
                    .await;
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(root) = self.root() {
                let owned;
                let binding = match io {
                    Some(io) => io,
                    None => {
                        owned = PreparationIo::for_unmeasured(root._custody.work().cancellation())?;
                        &owned
                    }
                };
                if let Some(bytes) = self.read_snapshot_file(file, maximum, binding).await? {
                    return Ok(bytes);
                }
                self.authenticate_file(file, Some(binding)).await?;
            } else {
                self.authenticate_file(file, io).await?;
            }
            #[cfg(target_arch = "wasm32")]
            self.authenticate_file(file, io).await?;
            self.reader()
                .read(file, maximum.min(self.maximum()), io)?
                .await
        })
    }
    fn read_private_prefix<'a>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: &'a GenerationRef,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if matches!(self, Self::Reference { .. }) {
            return Err(bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unsupported,
                "private directory operations are unavailable for exact-file access",
            ));
        }
        BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if self.root().is_some_and(|root| root.snapshot().is_some()) {
                return self
                    .read_snapshot_path(volume, prefix, path, Some(generation), maximum, io)
                    .await
                    .map(|(_, bytes)| bytes);
            }
            self.authenticate(volume, io).await?;
            self.reader()
                .read_private_prefix(
                    volume,
                    prefix,
                    path,
                    generation,
                    maximum.min(self.maximum()),
                    io,
                )?
                .await
        })
    }
    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, PrivateDirectoryPage>> {
        if matches!(self, Self::Reference { .. }) {
            return Err(bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unsupported,
                "private directory operations are unavailable for exact-file access",
            ));
        }
        BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if self.root().is_some_and(|root| root.snapshot().is_some()) {
                return self
                    .list_snapshot_directory(volume, prefix, path, generation, after, maximum, io)
                    .await;
            }
            self.authenticate(volume, io).await?;
            ContentResidencyVerifier::list_private_directory(
                self.reader(),
                volume,
                prefix,
                path,
                generation,
                after,
                maximum,
                io,
            )?
            .await
        })
    }
    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        maximum: u64,
        io: Option<&'io PreparationIo>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        if matches!(self, Self::Reference { .. }) {
            return Err(bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unsupported,
                "private directory operations are unavailable for exact-file access",
            ));
        }
        BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if self.root().is_some_and(|root| root.snapshot().is_some()) {
                let (file, bytes) = self
                    .read_snapshot_path(volume, prefix, path, generation, maximum, io)
                    .await?;
                let retained = file.logical_owned_bytes()?
                    + if io.is_some_and(|io| io.original_native_work().is_some()) {
                        std::mem::size_of::<FileRef>() as u64
                    } else {
                        0
                    };
                let hold = io.map(|io| io.guard_owned(retained)).transpose()?;
                return Ok((file, bytes, hold));
            }
            self.authenticate(volume, io).await?;
            ContentResidencyVerifier::read_private_path(
                self.reader(),
                volume,
                prefix,
                path,
                generation,
                maximum.min(self.maximum()),
                io,
            )?
            .await
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentPublisher
    for CreatedFilesystemAccess<A, O>
{
    fn volume(&self) -> &VolumeRef {
        self.volume()
    }
    fn supports_stage(&self, io: Option<&PreparationIo>) -> Result<()> {
        self.writer()?.supports_stage(io)
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.root().is_some() {
            return self.stage_once(operation, path, bytes, media, display, io);
        }
        BoundedContentFuture::new(io, || async move {
            self.authenticate(self.volume(), io).await?;
            self.writer()?
                .stage(operation, path, bytes, media, display, io)?
                .await
        })
    }
    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self
                .publish(operation, path, bytes, media, display, None, io)
                .await?;
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok((file, hold))
        })
    }
    fn stage_at<'a>(
        &'a self,
        operation: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self
                .publish(
                    operation,
                    source.path(),
                    bytes,
                    source.descriptor().media_type(),
                    source.display_name(),
                    Some(source),
                    io,
                )
                .await?;
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok((file, hold))
        })
    }
    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self.observe(operation, maximum, io).await?;
            let hold = io
                .map(|io| {
                    io.guard_owned(
                        file.as_ref()
                            .map(FileRef::logical_owned_bytes)
                            .transpose()?
                            .unwrap_or(0),
                    )
                })
                .transpose()?;
            Ok((file, hold))
        })
    }
}
