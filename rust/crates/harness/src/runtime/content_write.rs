use super::*;

pub(super) enum ContentPublication<'a> {
    PathBound,
    OperationBound,
    Replacement(&'a FileRef),
}

impl TaskContext {
/// Stages exact bytes before a task may publish their ref.

pub async fn stage_file(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.content_binding(None),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::PathBound,
        )
        .await
    }

/// Publishes to an explicitly selected authenticated created volume.

#[allow(
        clippy::too_many_arguments,
        reason = "exact volume, retry identity, body and file metadata stay explicit"
    )]
pub async fn stage_file_in(
        &self,
        volume: &VolumeRef,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        let binding = self.content_binding(Some(volume));
        if self.bound_content_writer(binding, &self.scope, None)?.volume() != volume {
            return Err(Error::Unauthorized("selected volume is not the bound destination".into()));
        }
        self.stage_bound_file(
            binding, operation_id, path, bytes, media_type, display_name,
            ContentPublication::PathBound,
        ).await
    }

/// Publishes one exact file contract per operation through its original writer.

pub async fn stage_file_once(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.content_binding(None),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::OperationBound,
        )
        .await
    }

/// The same once-publication contract with real cumulative provider work.

#[cfg(feature = "filesystem")]
#[allow(clippy::too_many_arguments, reason = "complete once-publication identity and finite caller budget")]
pub async fn stage_file_once_measured(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            if bytes.len() as u64 > self.scope.limits.file_bytes || path.len() > self.scope.limits.path_bytes {
                return Err(Error::Invalid("file exceeds task content limits".into()));
            }
            let content = self.content_binding(None)
                .ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
            let writer = self.bound_content_writer(Some(content), &self.scope, Some(&io))?;
            if !content.reader.supports_read(writer.volume(), Some(&io))? {
                return Err(Error::Unsupported("original content reader has no finite engine".into()));
            }
            let (file, hold) = writer.stage_once(operation_id, path, bytes, media_type,
                display_name, Some(&io))?.await?;
            if let Some(hold) = hold { hold.into_retained(); } else {
                io.retain_owned(crate::filesystem::UploadRecord::logical_owned_bytes(&file)?)?;
            }
            self.scope.limits.validate_file(&file, &mut None)?;
            if file.volume() != writer.volume() || file.path() != path
                || file.display_name() != display_name || file.descriptor().media_type() != media_type {
                return Err(Error::Storage("content publisher returned another file identity".into()));
            }
            let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
            io.preflight(hash)?;
            let verified = file.descriptor().verify(bytes, &mut None);
            io.charge(hash)?;
            verified?;
            content.reader.verify(&file, self.scope.limits.file_bytes, Some(&io))?.await?;
            Ok(file)
        }.await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

/// Uses the same once-publication writer and native-work ledger.

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub async fn stage_file_once_with_native_work(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef,acyclic_fs::NativeAllocation)> {
        self.stage_native_file(operation_id, path, bytes, media_type, display_name, None,
            &self.scope, work).await
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) async fn stage_native_file(&self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, source: Option<&FileRef>, scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.require_original_native_dispatch(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if bytes.len() as u64 > scope.limits.file_bytes || path.len() > scope.limits.path_bytes {
            return Err(Error::Invalid("file exceeds task content limits".into()).into());
        }
        let content = self.content_binding(source.map(FileRef::volume))
            .ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = self.bound_content_writer(Some(content), scope, Some(&io))?;
        if !content.reader.supports_read(writer.volume(),Some(&io))? {
            return Err(Error::Unsupported("original content reader has no native-work engine".into()).into());
        }
        if let Some(source) = source {
            self.authorize_native_file_read(source, scope, &io)?;
            if source.volume() != writer.volume() || source.path() != path {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "source differs from the owner-bound destination" });
            }
        }
        writer.supports_stage(Some(&io))?;
        self.scope.verify_created_write(writer.volume(), Some(&io)).await?;
        let output = if let Some(source) = source {
            content.reader.verify(source, scope.limits.file_bytes, Some(&io))?.await?;
            writer.stage_at(operation_id, source, bytes, Some(&io))?.await?
        } else {
            writer.stage_once(operation_id, path, bytes, media_type, display_name, Some(&io))?.await?
        };
        self.scope.verify_created_write(writer.volume(), Some(&io)).await?;
        let file = &output.0;
        scope.limits.validate_file(file, &mut None)?;
        if file.volume() != writer.volume() || file.path() != path
            || file.display_name() != display_name || file.descriptor().media_type() != media_type
            || file.descriptor().byte_length() != bytes.len() as u64
        {
            return Err(Error::Storage("content publisher returned another file identity".into()).into());
        }
        let hash = acyclic_fs::WorkCounters {bytes_hashed:bytes.len() as u64,..Default::default()};
        io.preflight(hash)?;
        let verified = file.descriptor().verify(bytes, &mut None);
        io.charge(hash)?;
        verified?;
        content.reader.verify(file,scope.limits.file_bytes,Some(&io))?.await?;
        let (file, hold) = output;
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        if let Some(hold) = hold { hold.into_retained(); }
        Ok((file, io.take_native_custody(retained)?))
    }

/// Publishes immutable artifact bytes under a caller-retained operation ID.

pub async fn stage_artifact(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.installed_root_output_content()
                .or_else(|| self.scope.created_artifact_content())
                .or_else(|| self.scope.created_content(None))
                .or(self.harness.artifacts.as_ref()),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::PathBound,
        )
        .await
    }

/// Replaces a pinned source through the existing owner-bound publisher.

pub async fn stage_file_at(
        &self,
        operation_id: OperationId,
        source: &FileRef,
        bytes: &[u8],
    ) -> Result<FileRef> {
        self.scope.limits.validate_file(source, &mut None)?;
        if !read_granted(&self.scope.grants, source)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this source".into(),
            ));
        }
        self.stage_bound_file(
            self.content_binding(Some(source.volume())),
            operation_id,
            source.path(),
            bytes,
            source.descriptor().media_type(),
            source.display_name(),
            ContentPublication::Replacement(source),
        )
        .await
    }

pub(super) fn bound_content_writer<'a>(
        &self, binding: Option<&'a ContentBindings>, scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<&'a Arc<dyn ContentPublisher>> {
        let content = binding.ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = content.writer.as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no owner-bound content writer".into()))?;
        #[cfg(feature = "filesystem")]
        let capability = match io {
            Some(io) => io.capability(writer.volume(), "volume:write:")?,
            None => writer.volume().capability(VolumeOperation::Write, &mut None)?,
        };
        #[cfg(not(feature = "filesystem"))]
        let capability = { let _ = io; writer.volume().capability(VolumeOperation::Write, &mut None)? };
        if !scope.grants.contains(&capability) {
            return Err(Error::Unauthorized("task scope cannot write this volume".into()));
        }
        Ok(writer)
    }

/// Checks the original content destination without reading or publishing bytes.

pub(crate) fn authorize_content_publication(&self, source: Option<&FileRef>) -> Result<()> {
        let writer = self.bound_content_writer(self.content_binding(source.map(FileRef::volume)), &self.scope, None)?;
        if source.is_some_and(|source| source.volume() != writer.volume()) {
            return Err(Error::Unauthorized(
                "source differs from the owner-bound destination".into(),
            ));
        }
        Ok(())
    }

#[allow(
        clippy::too_many_arguments,
        reason = "one owner-bound publication path retains its binding, identity, bytes, metadata and receipt mode"
    )]
pub(super) async fn stage_bound_file(
        &self,
        binding: Option<&ContentBindings>,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        publication: ContentPublication<'_>,
    ) -> Result<FileRef> {
        if bytes.len() as u64 > self.scope.limits.file_bytes
            || path.len() > self.scope.limits.path_bytes
        {
            return Err(Error::Invalid("file exceeds task content limits".into()));
        }
        let content =
            binding.ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = self.bound_content_writer(Some(content), &self.scope, None)?;
        self.scope.verify_created_write(writer.volume(), None).await?;
        let file = match publication {
            ContentPublication::Replacement(source) => {
                if source.volume() != writer.volume() || source.path() != path {
                    return Err(Error::Unauthorized(
                        "source differs from the owner-bound destination".into(),
                    ));
                }
                content.reader.verify(source, source.descriptor().byte_length(), None)?.await?;
                writer.stage_at(operation_id, source, bytes, None)?.await?.0
            }
            ContentPublication::OperationBound => {
                writer
                    .stage_once(operation_id, path, bytes, media_type, display_name, None)?
                    .await?.0
            }
            ContentPublication::PathBound => {
                writer
                    .stage(operation_id, path, bytes, media_type, display_name, None)?
                    .await?.0
            }
        };
        self.scope.verify_created_write(writer.volume(), None).await?;
        self.scope.limits.validate_file(&file, &mut None)?;
        if file.volume() != writer.volume()
            || file.path() != path
            || file.display_name() != display_name
            || file.descriptor().media_type() != media_type
        {
            return Err(Error::Storage(
                "content publisher returned another file identity".into(),
            ));
        }
        file.descriptor().verify(bytes, &mut None)?;
        content.reader.verify(&file, file.descriptor().byte_length(), None)?.await?;
        Ok(file)
    }
}
