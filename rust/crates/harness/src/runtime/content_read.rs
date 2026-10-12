use super::*;

#[cfg(feature = "filesystem")]
pub(super) struct ContextContentVerifier<P: acyclic_stream::StreamProvider> {
    pub(super) context: Arc<TaskContext>,
    pub(super) owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) custody: Option<acyclic_fs::NativeAllocation>,
}

/// Retains an issued context's content reader under its exact original owner.

#[cfg(feature = "filesystem")]
pub fn content_residency_verifier<P>(
    context: Arc<TaskContext>,
    owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Arc<dyn ContentResidencyVerifier>>
where
    P: acyclic_stream::StreamProvider + 'static,
{
    let issued = context.as_ref();
    if issued.durable_task_id() != Some(owner.task_id()) || issued.id() != owner.operation_id() {
        return Err(crate::conversation::content_error(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "content reader differs from its original task owner",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let native = io.and_then(crate::executor::PreparationIo::original_native_work);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = native {
        if !owner.native_shared_work_matches(work) {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content reader lacks original shared owner custody",
            ));
        }
    }
    owner.require_task_context_binding(issued, io)?;
    #[cfg(not(target_arch = "wasm32"))]
    let custody = native
        .map(|work| {
            work.claim(
                std::mem::size_of::<ContextContentVerifier<P>>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(Error::from)
        })
        .transpose()?;
    Ok(Arc::new(ContextContentVerifier {
        context,
        owner,
        #[cfg(not(target_arch = "wasm32"))]
        custody,
    }))
}

#[cfg(feature = "filesystem")]
impl<P> ContextContentVerifier<P>
where
    P: acyclic_stream::StreamProvider,
{
    pub(super) async fn authorize(
        &self,
        reference: &FileRef,
        io: Option<&crate::conversation::ContentIo>,
        binding_io: Option<&crate::conversation::ContentIo>,
    ) -> Result<()> {
        self.owner
            .require_task_context_binding(self.context.as_ref(), binding_io)?;
        self.owner.validate_input_file(reference, io)?;
        let mut history = io.map(crate::executor::PreparationIo::history_io);
        self.owner
            .verify_read_access(
                &mut history
                    .as_mut()
                    .map(|history| history as &mut dyn crate::store::HistoryIo),
            )
            .await
    }
}

#[cfg(feature = "filesystem")]
impl<P> ContentResidencyVerifier for ContextContentVerifier<P>
where
    P: acyclic_stream::StreamProvider + 'static,
{
    fn supports_read(
        &self,
        _volume: &VolumeRef,
        io: Option<&crate::conversation::ContentIo>,
    ) -> Result<bool> {
        if let Some(io) = io {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let work = io
                    .original_native_work()
                    .ok_or(crate::conversation::content_error(
                        Some(io),
                        crate::wire::ErrorCode::Unsupported,
                        "content reader requires original native work",
                    ))?;
                if !self
                    .custody
                    .as_ref()
                    .is_some_and(|custody| custody.work().shares_allocation_context(work))
                {
                    return Err(crate::conversation::content_error(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "content callback differs from its original native ledger",
                    ));
                }
            }
            #[cfg(target_arch = "wasm32")]
            return Err(crate::conversation::content_error(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "content reader requires original native work",
            ));
        }
        Ok(true)
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "content exceeds the callback byte bound",
                ));
            }
            let context = self.context.as_ref();
            #[cfg(not(target_arch = "wasm32"))]
            let native_io = self
                .custody
                .as_ref()
                .map(|custody| crate::executor::PreparationIo::from_native_work(custody.work()))
                .transpose()?;
            self.supports_read(reference.volume(), io)?;
            #[cfg(not(target_arch = "wasm32"))]
            let binding_io = io.or(native_io.as_ref());
            #[cfg(target_arch = "wasm32")]
            let binding_io = io;
            self.authorize(reference, io, binding_io).await?;
            #[cfg(not(target_arch = "wasm32"))]
            let bytes = match io.and_then(crate::executor::PreparationIo::original_native_work) {
                Some(work) => context.read_file_with_native_work(reference, work).await?,
                None => context.read_file(reference).await?,
            };
            #[cfg(target_arch = "wasm32")]
            let bytes = context.read_file(reference).await?;
            self.authorize(reference, io, binding_io).await?;
            Ok(bytes)
        })
    }
}

impl TaskContext {
    pub(super) fn installed_root_output_content(&self) -> Option<&ContentBindings> {
        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
        {
            return self.native_root_output.get().map(|binding| &binding.1);
        }
        #[cfg(not(all(feature = "native-execution", not(target_arch = "wasm32"))))]
        None
    }

    pub(super) fn content_binding(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
        if let Some(binding) = self.installed_root_output_content() {
            if volume.is_none_or(|volume| {
                binding
                    .writer
                    .as_ref()
                    .is_some_and(|writer| writer.volume() == volume)
            }) {
                return Some(binding);
            }
        }
        self.output_content
            .as_ref()
            .filter(|binding| {
                volume.is_none_or(|volume| {
                    binding
                        .writer
                        .as_ref()
                        .is_some_and(|writer| writer.volume() == volume)
                })
            })
            .or_else(|| self.scope.created_content(volume))
            .or(self.harness.content.as_ref())
    }

    /// Reads one immutable file through the owner's authenticated provider.

    pub async fn read_file(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.read_bound_file(
            self.content_binding(Some(file.volume())),
            file,
            &self.scope,
            None,
        )
        .await
    }

    /// Reads through the original bound provider on the caller's native ledger.

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn read_file_with_native_work(
        &self,
        file: &FileRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        self.read_bound_file(
            self.content_binding(Some(file.volume())),
            file,
            &self.scope,
            Some(&io),
        )
        .await
    }

    pub(super) fn authorize_file_read(
        &self,
        file: &FileRef,
        scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            scope.limits.validate_file(file, work)?;
            if !read_granted_with_work(&scope.grants, file, work)? {
                return Err(crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "task scope cannot read this file",
                ));
            }
            Ok(())
        })
    }

    /// Resolves an immutable artifact without coupling its provider to the workspace/content binding.

    pub async fn read_artifact(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.read_bound_file(
            self.scope
                .created_content(Some(file.volume()))
                .or(self.harness.artifacts.as_ref()),
            file,
            &self.scope,
            None,
        )
        .await
    }

    /// Discovers a private owner directory without a prior `FileRef`.

    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&str>,
        maximum_entries: u32,
    ) -> Result<PrivateDirectoryPage> {
        self.require_private_directory(volume, granted_prefix, path)?;
        if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let content = self
            .content_binding(Some(volume))
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let page = content
            .reader
            .list_private_directory(
                volume,
                granted_prefix,
                path,
                expected_generation,
                after,
                maximum_entries,
                None,
            )?
            .await?;
        page.validate()?;
        if page.generation.as_resource().provider() != volume.provider()
            || expected_generation.is_some_and(|expected| &page.generation != expected)
            || page.entries.len() > maximum_entries as usize
        {
            return Err(Error::Conflict(
                "private directory provider returned another page".into(),
            ));
        }
        for entry in &page.entries {
            crate::conversation::validate_content_path(&entry.name, &mut None)?;
            if entry.name.contains('/')
                || (path.is_empty() && entry.name == ".system")
                || after.is_some_and(|cursor| entry.name.as_bytes() <= cursor.as_bytes())
            {
                return Err(Error::Conflict(
                    "private directory provider returned a forbidden name".into(),
                ));
            }
        }
        Ok(page)
    }

    /// Resolves the current file at a named private path into verified pinned bytes; the returned `FileRef` can later be shared independently.

    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.require_private_directory(volume, granted_prefix, path)?;
        if path.is_empty() {
            return Err(Error::Invalid("private file path is empty".into()));
        }
        let content = self
            .content_binding(Some(volume))
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let (file, bytes, _hold) = content
            .reader
            .read_private_path(
                volume,
                granted_prefix,
                path,
                expected_generation,
                self.scope.limits.file_bytes,
                None,
            )?
            .await?;
        file.validate(&mut None)?;
        self.scope.limits.validate_file(&file, &mut None)?;
        if file.volume() != volume || file.path() != path {
            return Err(Error::Conflict(
                "private path provider returned another file".into(),
            ));
        }
        file.descriptor().verify(&bytes, &mut None)?;
        Ok((file, bytes))
    }

    pub(super) fn require_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
    ) -> Result<()> {
        volume.validate(&mut None)?;
        if crate::conversation::is_internal_path(path)
            || (!path.is_empty()
                && crate::conversation::validate_content_path(path, &mut None).is_err())
            || (!granted_prefix.is_empty()
                && crate::conversation::validate_content_path(granted_prefix, &mut None).is_err())
        {
            return Err(Error::Invalid(
                "private directory path or volume is invalid".into(),
            ));
        }
        let exact = volume.directory_read_capability(granted_prefix, &mut None)?;
        let full = volume.capability(VolumeOperation::Read, &mut None)?;
        if !self.scope.grants().contains(&exact) && !self.scope.grants().contains(&full) {
            return Err(Error::Unauthorized(
                "task scope cannot discover this directory".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn read_bound_file(
        &self,
        binding: Option<&ContentBindings>,
        file: &FileRef,
        scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.authorize_file_read(file, scope, io)?;
        #[cfg(feature = "filesystem")]
        let created = self.scope.created_reference(file);
        #[cfg(not(feature = "filesystem"))]
        let created: Option<&Arc<dyn ContentResidencyVerifier>> = None;
        let reader = created
            .map(|reader| reader.as_ref())
            .or_else(|| binding.map(|content| content.reader.as_ref()))
            .ok_or_else(|| {
                crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "content reader is not bound",
                )
            })?;
        let bytes = reader
            .read(file, file.descriptor().byte_length(), io)?
            .await?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            #[cfg(not(target_arch = "wasm32"))]
            if io.original_native_work().is_some()
                && file.descriptor().byte_length() != bytes.len() as u64
            {
                return Err(crate::conversation::content_error(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "content reader returned another file length",
                ));
            }
            #[cfg(not(target_arch = "wasm32"))]
            let native = io.original_native_work().is_some();
            #[cfg(target_arch = "wasm32")]
            let native = false;
            if !native {
                io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
            }
            let hash = acyclic_fs::WorkCounters {
                bytes_hashed: bytes.len() as u64,
                ..Default::default()
            };
            io.preflight(hash)?;
            let verified = file.descriptor().verify(&bytes, &mut None);
            io.charge(hash)?;
            verified?;
            return Ok(bytes);
        }
        file.descriptor().verify(&bytes, &mut None)?;
        Ok(bytes)
    }

    /// Observes the original once-publication, never a refreshed source/head.

    pub async fn observe_staged_file_once(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<FileRef>> {
        let writer = self.bound_content_writer(self.content_binding(None), &self.scope, io)?;
        if io.is_some() {
            self.scope.verify_created_write(writer.volume(), io).await?;
        }
        let (file, hold) = writer
            .observe_staged_once(operation_id, self.scope.limits.file_bytes, io)?
            .await?;
        if io.is_some() {
            self.scope.verify_created_write(writer.volume(), io).await?;
        }
        if let Some(hold) = hold {
            hold.into_retained();
        }
        if let Some(file) = &file {
            #[cfg(feature = "filesystem")]
            let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
            #[cfg(feature = "filesystem")]
            let mut work = observer
                .as_mut()
                .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
            #[cfg(not(feature = "filesystem"))]
            let mut work = None;
            self.scope.limits.validate_file(file, &mut work)?;
            if file.volume() != writer.volume() {
                return Err(Error::Storage(
                    "original publisher receipt belongs to another volume".into(),
                ));
            }
        }
        Ok(file)
    }

    #[cfg(feature = "filesystem")]
    /// Observes a once-publication under the supplied bounds.
    pub async fn observe_staged_file_once_measured(
        &self,
        operation_id: OperationId,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            let writer =
                self.bound_content_writer(self.content_binding(None), &self.scope, Some(&io))?;
            let (file, hold) = writer
                .observe_staged_once(operation_id, self.scope.limits.file_bytes, Some(&io))?
                .await?;
            if let Some(hold) = hold {
                hold.into_retained();
            }
            if let Some(file) = &file {
                io.retain_owned(crate::filesystem::UploadRecord::logical_owned_bytes(file)?)?;
                self.scope.limits.validate_file(file, &mut None)?;
                if file.volume() != writer.volume() {
                    return Err(Error::Storage(
                        "original publisher receipt belongs to another volume".into(),
                    ));
                }
            }
            Ok(file)
        }
        .await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt {
                value,
                work: io.receipt(),
            }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Reads through the exact original task scope and owner-backed reader.

    #[cfg(feature = "filesystem")]
    pub async fn read_file_measured(
        &self,
        file: &FileRef,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error>
    {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = self
            .read_bound_file(
                self.content_binding(Some(file.volume())),
                file,
                &self.scope,
                Some(&io),
            )
            .await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt {
                value,
                work: io.receipt(),
            }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }
}
