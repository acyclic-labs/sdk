use super::*;
use crate::conversation::BoundedContentFuture;
use crate::conversation::{ContentIo, content_error};
use crate::wire::ErrorCode;
use acyclic_stream::OwnedBytes;

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
    io: Option<&ContentIo>,
) -> Result<Arc<dyn ContentResidencyVerifier>>
where
    P: acyclic_stream::StreamProvider + 'static,
{
    let issued = context.as_ref();
    if issued.durable_task_id() != Some(owner.task_id()) || issued.id() != owner.operation_id() {
        return Err(content_error(
            io,
            ErrorCode::Unauthorized,
            "content reader differs from its original task owner",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let native = io.and_then(ContentIo::original_native_work);
    let (task, fence) = owner.task_binding();
    if !issued.original_task_binding_matches(task, fence) {
        return Err(content_error(
            io,
            ErrorCode::Unauthorized,
            "content reader differs from its original task fence",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = native {
        issued.require_original_native_work(work)?;
    }
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
        io: Option<&ContentIo>,
    ) -> Result<()> {
        let (task, fence) = self.owner.task_binding();
        if !self.context.original_task_binding_matches(task, fence) {
            return Err(content_error(
                io,
                ErrorCode::Unauthorized,
                "content reader differs from its original task fence",
            ));
        }
        self.owner.validate_input_file(reference, io)?;
        let mut history = io.map(ContentIo::history_io);
        self.owner
            .verify(
                true,
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
    fn supports_read(&self, _volume: &VolumeRef, io: Option<&ContentIo>) -> Result<bool> {
        if let Some(io) = io {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let work = io.original_native_work().ok_or(content_error(
                    Some(io),
                    ErrorCode::Unsupported,
                    "content reader requires original native work",
                ))?;
                if !self
                    .custody
                    .as_ref()
                    .is_some_and(|custody| custody.work().shares_allocation_context(work))
                {
                    return Err(content_error(
                        Some(io),
                        ErrorCode::Unauthorized,
                        "content callback differs from its original native ledger",
                    ));
                }
            }
            #[cfg(target_arch = "wasm32")]
            return Err(content_error(
                Some(io),
                ErrorCode::Unsupported,
                "content reader requires original native work",
            ));
        }
        Ok(true)
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(content_error(
                    io,
                    ErrorCode::Invalid,
                    "content exceeds the callback byte bound",
                ));
            }
            self.supports_read(reference.volume(), io)?;
            self.authorize(reference, io).await?;
            let bytes = self.context.read_file(reference, io).await?;
            self.authorize(reference, io).await?;
            Ok(bytes)
        })
    }
}

impl TaskContext {
    pub(super) fn content_binding(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
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

    /// Reads an immutable file under this task's scope and caller IO limits.
    pub async fn read_file(&self, file: &FileRef, io: Option<&ContentIo>) -> Result<OwnedBytes> {
        self.read_bound_file(
            self.content_binding(Some(file.volume())),
            file,
            &self.scope,
            io,
        )
        .await
    }

    pub(super) fn authorize_file_read(
        &self,
        file: &FileRef,
        scope: &RuntimeScope,
        io: Option<&ContentIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            scope.limits.validate_file(file, work)?;
            if !read_granted_with_work(&scope.grants, file, work)? {
                return Err(content_error(
                    io,
                    ErrorCode::Unauthorized,
                    "task scope cannot read this file",
                ));
            }
            Ok(())
        })
    }

    /// Lists one authenticated private directory page.
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
            if (path.is_empty() && entry.name == ".system")
                || after.is_some_and(|cursor| entry.name.as_bytes() <= cursor.as_bytes())
            {
                return Err(Error::Conflict(
                    "private directory provider returned a forbidden name".into(),
                ));
            }
        }
        Ok(page)
    }

    /// Reads and verifies a pinned file at a private path.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, OwnedBytes)> {
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
        io: Option<&ContentIo>,
    ) -> Result<OwnedBytes> {
        self.authorize_file_read(file, scope, io)?;
        #[cfg(feature = "filesystem")]
        let created = self.scope.created_reference(file);
        #[cfg(not(feature = "filesystem"))]
        let created: Option<&Arc<dyn ContentResidencyVerifier>> = None;
        let reader = created
            .map(|reader| reader.as_ref())
            .or_else(|| binding.map(|content| content.reader.as_ref()))
            .ok_or_else(|| {
                content_error(io, ErrorCode::Unsupported, "content reader is not bound")
            })?;
        let (bytes, hold) =
            verified_content_bytes(reader, file, file.descriptor().byte_length(), io).await?;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained();
        }
        #[cfg(not(feature = "filesystem"))]
        let _ = hold;
        Ok(bytes)
    }

    /// Observes the original immutable publication under caller IO limits.
    pub async fn observe_staged_file_once(
        &self,
        operation_id: OperationId,
        io: Option<&ContentIo>,
    ) -> Result<(Option<FileRef>, u64)> {
        self.observe_bound_file_once(operation_id, io, &self.scope)
            .await
    }

    /// Observes the original once-publication, never a refreshed source/head.
    pub(super) async fn observe_bound_file_once(
        &self,
        operation_id: OperationId,
        io: Option<&ContentIo>,
        scope: &RuntimeScope,
    ) -> Result<(Option<FileRef>, u64)> {
        let writer = self.bound_content_writer(self.content_binding(None), scope, io)?;
        if io.is_some() {
            self.scope.verify_created_write(writer.volume(), io).await?;
        }
        let (file, hold) = writer
            .observe_staged_once(operation_id, scope.limits.file_bytes, io)?
            .await?;
        if io.is_some() {
            self.scope.verify_created_write(writer.volume(), io).await?;
        }
        #[cfg(feature = "filesystem")]
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        #[cfg(not(feature = "filesystem"))]
        let retained = 0;
        #[cfg(not(feature = "filesystem"))]
        let _ = hold;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained();
        }
        if let Some(file) = &file {
            crate::executor::with_history_io(io, |work| scope.limits.validate_file(file, work))?;
            if file.volume() != writer.volume() {
                return Err(Error::Storage(
                    "original publisher receipt belongs to another volume".into(),
                ));
            }
        }
        Ok((file, retained))
    }
}
