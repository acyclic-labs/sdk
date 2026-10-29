use super::*;
#[cfg(feature = "filesystem")]
use crate::conversation::BoundedContentFuture;
use crate::conversation::{ContentIo, content_error};
use crate::wire::ErrorCode;
use acyclic_stream::OwnedBytes;

// An admitted reader's grants can only be retained or narrowed. In particular,
// range and full reads cannot pass an arbitrary replacement to authorization.
pub(super) struct ReadGrants<'a> {
    scope: &'a RuntimeScope,
    grants: &'a crate::Capabilities,
}

impl<'a> ReadGrants<'a> {
    fn bound(scope: &'a RuntimeScope) -> Self {
        Self {
            scope,
            grants: scope.grants(),
        }
    }

    fn intersect(
        self,
        request: Option<&'a crate::Capabilities>,
        io: Option<&ContentIo>,
    ) -> Result<Self> {
        let grants = request.unwrap_or(self.grants);
        if !grants.is_subset_of(self.grants) {
            return Err(content_error(
                io,
                ErrorCode::Unauthorized,
                "content caller cannot widen the bound task scope",
            ));
        }
        Ok(Self {
            scope: self.scope,
            grants,
        })
    }
}

// Journals fence the entire command and authenticate each referenced input against
// its admission. Reuse the issued context for content reads, including its grants,
// immutable descriptor verification and original native accounting.
#[cfg(feature = "filesystem")]
impl ContentResidencyVerifier for TaskContext {
    fn supports_read(&self, volume: &VolumeRef, io: Option<&ContentIo>) -> Result<bool> {
        self.content_binding(Some(volume))
            .map_or(Ok(false), |binding| {
                binding.reader.supports_read(volume, io)
            })
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        ceiling: Option<&'a crate::Capabilities>,
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
            self.read_bound_file(
                self.content_binding(Some(reference.volume())),
                reference,
                &self.scope,
                ceiling,
                io,
            )
            .await
        })
    }
}

impl TaskContext {
    /// Where a tool publishes into `volume`, or, unselected, its new files:
    /// the harness's own project content when it has a writer, which is
    /// where the project's commands write too; otherwise the agent's
    /// private volume.
    pub(crate) fn tool_content(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
        match volume {
            Some(_) => self.content_binding(volume),
            None => self
                .harness
                .content
                .as_ref()
                .filter(|binding| binding.writer.is_some())
                .or(Some(&self.output_content)),
        }
    }

    pub(crate) fn content_binding(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
        let writes = |binding: &&ContentBindings| {
            binding
                .writer
                .as_ref()
                .is_some_and(|writer| volume.is_none_or(|volume| writer.volume() == volume))
        };
        Some(&self.output_content)
            .filter(writes)
            .or_else(|| volume.and(self.harness.artifacts.as_ref()).filter(writes))
            .or(self.harness.content.as_ref())
    }

    /// Reads an immutable file under this task's scope and caller IO limits.
    ///
    /// Effective read grants have no public constructor or replacement field:
    /// ```compile_fail
    /// use acyclic_harness::runtime::ReadGrants;
    /// fn replace<'a>(view: &mut ReadGrants<'a>, grants: &'a acyclic_harness::Capabilities) {
    ///     view.grants = grants;
    /// }
    /// ```
    pub async fn read_file(&self, file: &FileRef, io: Option<&ContentIo>) -> Result<OwnedBytes> {
        self.read_bound_file(
            self.content_binding(Some(file.volume())),
            file,
            &self.scope,
            None,
            io,
        )
        .await
    }

    pub(super) async fn read_bound_range(
        &self,
        file: &FileRef,
        range: acyclic_fs::ByteRange,
        scope: &RuntimeScope,
        grants: &crate::Capabilities,
        io: Option<&ContentIo>,
    ) -> Result<OwnedBytes> {
        let grants = self.authorize_file_read(file, scope, grants, io)?;
        if range
            .offset
            .checked_add(range.length)
            .is_none_or(|end| end > file.descriptor().byte_length())
        {
            return Err(content_error(
                io,
                ErrorCode::Invalid,
                "range exceeds the source file",
            ));
        }
        let reader = self
            .content_binding(Some(file.volume()))
            .map(|content| content.reader.as_ref())
            .ok_or_else(|| {
                content_error(io, ErrorCode::Unsupported, "content reader is not bound")
            })?;
        let bytes = reader
            .read_range(file, range, Some(grants.grants), io)?
            .await?;
        if bytes.len() as u64 != range.length {
            return Err(content_error(
                io,
                ErrorCode::Invalid,
                "range reader returned an incorrect length",
            ));
        }
        Ok(bytes)
    }

    pub(super) fn authorize_file_read<'a>(
        &self,
        file: &FileRef,
        scope: &'a RuntimeScope,
        request: &'a crate::Capabilities,
        io: Option<&ContentIo>,
    ) -> Result<ReadGrants<'a>> {
        let grants = ReadGrants::bound(scope).intersect(Some(request), io)?;
        crate::executor::with_history_io(io, |work| {
            grants.scope.limits.validate_file(file, work)?;
            if !read_granted_with_work(grants.grants, file, work)? {
                return Err(content_error(
                    io,
                    ErrorCode::Unauthorized,
                    "task scope cannot read this file",
                ));
            }
            Ok(())
        })?;
        Ok(grants)
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
                Some(self.scope.grants()),
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
                crate::conversation::ContentAccess {
                    ceiling: Some(self.scope.grants()),
                    io: None,
                },
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
        if !crate::core::directory_read_granted_with_work(
            self.scope.grants(),
            volume,
            granted_prefix,
            &mut None,
        )? {
            return Err(Error::Unauthorized(
                "task scope cannot discover this directory".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn read_bound_file(
        &self,
        binding: Option<&ContentBindings>,
        file: &FileRef,
        scope: &RuntimeScope,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&ContentIo>,
    ) -> Result<OwnedBytes> {
        let grants =
            self.authorize_file_read(file, scope, ceiling.unwrap_or(scope.grants()), io)?;
        let reader = binding
            .map(|content| content.reader.as_ref())
            .ok_or_else(|| {
                content_error(io, ErrorCode::Unsupported, "content reader is not bound")
            })?;
        let (bytes, hold) = verified_content_bytes(
            reader,
            file,
            file.descriptor().byte_length(),
            Some(grants.grants),
            io,
        )
        .await?;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained()?;
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
        self.observe_bound_file_once(self.content_binding(None), operation_id, io, &self.scope)
            .await
    }

    /// Observes the original once-publication, never a refreshed source/head.
    pub(super) async fn observe_bound_file_once(
        &self,
        binding: Option<&ContentBindings>,
        operation_id: OperationId,
        io: Option<&ContentIo>,
        scope: &RuntimeScope,
    ) -> Result<(Option<FileRef>, u64)> {
        let writer = super::content_write::content_writer(binding, io)?;
        let (file, hold) = writer
            .observe_staged_once(
                operation_id,
                scope.limits.file_bytes,
                crate::conversation::ContentAccess {
                    ceiling: Some(scope.grants()),
                    io,
                },
            )?
            .await?;
        #[cfg(feature = "filesystem")]
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        #[cfg(not(feature = "filesystem"))]
        let retained = 0;
        #[cfg(not(feature = "filesystem"))]
        let _ = hold;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained()?;
        }
        if let Some(file) = &file {
            crate::executor::with_history_io(io, |work| scope.limits.validate_file(file, work))?;
            super::content_write::publication_destination(
                self.content_binding(Some(file.volume())),
                scope,
                Some(file),
                file.path(),
                io,
            )?;
        }
        Ok((file, retained))
    }
}
