use super::*;
mod media_type;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Reads a pinned version using optional preparation accounting.
    ///
    /// Observations cannot be forged or have their selected data replaced:
    /// ```compile_fail
    /// use acyclic_harness::filesystem::access::ReadObservation;
    /// fn replace<A, O>(observed: &mut ReadObservation<'_, A, O>, data: acyclic_fs::Generation<A, O>) {
    ///     observed.generation = data;
    /// }
    /// ```
    pub async fn read_content(
        &self,
        reference: &FileRef,
        grant: &ContentGrant,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        grant.require_file_read(reference, io)?;
        let temporary = PreparationIo::temporary(io)?;
        let io = temporary.as_ref().or(io);
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        self.read_file_with_access(
            grant.policy_scope(),
            reference,
            maximum_bytes,
            true,
            None,
            io,
        )
        .await
    }

    /// Resolves a private file and verifies its bytes with optional original IO.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let (file, bytes, _) = self
            .read_private_path_indexed(
                volume,
                grant,
                path,
                expected_generation,
                maximum_bytes,
                ceiling,
                io,
            )
            .await?;
        Ok((file, bytes))
    }

    pub(crate) async fn read_private_path_indexed(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<(
        FileRef,
        acyclic_stream::OwnedBytes,
        Option<crate::OperationId>,
    )> {
        self.require_private_directory(volume, grant, path, io)?;
        super::super::access::require_directory_ceiling(ceiling, volume, path, io)?;
        let name = path.rsplit('/').next().unwrap_or(path);
        crate::executor::with_history_io(io, |work| {
            crate::conversation::validate_content_path(path, work)
        })?;
        if name.len() > crate::conversation::MAX_LABEL_BYTES {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "file or volume label is invalid",
            ));
        }
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let workspace_ref = fork_preparer_io::content_workspace(&self.provider, volume, Some(io))?;
        let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, workspace_hold);
        let workspace = self.open(&workspace_ref.0, Some(io)).await?;
        let generation = match expected_generation {
            Some(expected) => self.generation(&workspace, expected, Some(io)).await?,
            None => io.sdk(workspace.head(io, io.cancellation()).await)?,
        };
        let label = crate::core::filesystem_scope_label(
            grant.policy_scope().capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let view = crate::filesystem::access::access_view(
            &workspace,
            grant.policy_scope(),
            Some(generation.id()),
            super::super::access::capability_payload(label, VolumeOperation::Read, io)?,
            ceiling,
            io,
        )
        .await?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::List, path, io)?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        fork_preparer_io::retain(&generation, Some(io)).await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let requested = (requested, requested_hold);
        let bytes = read_generation(&generation, &requested.0, maximum_bytes, Some(io)).await?;
        let bytes_hold = io.guard_bytes(&bytes)?;
        let bytes = (bytes, bytes_hold);
        let metadata_path = content_metadata_path_io(path, true, Some(io))?;
        let metadata_path_hold = io.guard_owned(metadata_path.capacity() as u64)?;
        let metadata_path = (metadata_path, metadata_path_hold);
        let metadata = read_generation(
            &generation,
            &metadata_path.0,
            io.remaining()?.source_bytes_read,
            Some(io),
        )
        .await;
        let staged = match metadata {
            Ok(metadata) => {
                let metadata_hold = io.guard_bytes(&metadata)?;
                let metadata = (metadata, metadata_hold);
                Some(io.decode(
                    std::marker::PhantomData::<FileRef>,
                    &metadata.0,
                    crate::wire::ErrorCode::Storage,
                )?)
            }
            Err(error) if error.code() == crate::wire::ErrorCode::NotFound => None,
            Err(error) => return Err(error),
        };
        let version = fork_preparer_io::hex_key(&generation.id().digest().into_bytes(), Some(io))?;
        let version_hold = io.guard_owned(version.capacity() as u64)?;
        let publication = staged
            .as_ref()
            .map(|file| file_publication(&file.0, Some(io)))
            .transpose()?;
        let reference = if let Some(mut staged) = staged {
            if staged.0.volume() != volume || staged.0.path() != path {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "private file metadata does not match the file",
                ));
            }
            staged
                .0
                .descriptor()
                .verify(&bytes.0, &mut Some(&mut io.history_io()))?;
            staged
                .0
                .pin_version(version, &mut Some(&mut io.history_io()))?;
            let version_bytes = version_hold.bytes();
            version_hold.into_retained()?;
            staged.1.adopt(version_bytes)?;
            staged.1.substitute(staged.0.logical_owned_bytes()?)?;
            staged
        } else {
            let reference = crate::executor::with_history_io(Some(io), |work| {
                FileRef::new(
                    volume.copy(work)?,
                    stock_string_copy(path, Some(io))?,
                    version,
                    FileDescriptor::from_bytes(&bytes.0, media_type::classify(&bytes.0), work)?,
                    stock_string_copy(name, Some(io))?,
                    work,
                )
            })?;
            version_hold.into_retained()?;
            let hold = io.guard_owned(reference.logical_owned_bytes()?)?;
            (reference, hold)
        };
        grant.require_file_read(&reference.0, Some(io))?;
        if publication.is_some() {
            Self::read_selected_range(
                &workspace,
                &generation,
                &reference.0,
                acyclic_fs::ByteRange {
                    offset: 0,
                    length: 0,
                },
                io,
            )
            .await?;
        }
        reference.1.into_retained()?;
        if let Some(hold) = bytes.1 {
            hold.into_retained()?;
        }
        super::super::access::require_current(&workspace, &view.0, io).await?;
        Ok((reference.0, bytes.0, publication))
    }

    /// Reads a finite private prefix at an explicitly pinned generation.
    pub async fn read_private_prefix(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        generation: &GenerationRef,
        maximum_bytes: u64,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.require_private_directory(volume, grant, path, io)?;
        super::super::access::require_directory_ceiling(ceiling, volume, path, io)?;
        crate::executor::with_history_io(io, |work| {
            crate::conversation::validate_content_path(path, work)
        })?;
        if maximum_bytes == 0 {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "prefix read limit must be positive",
            ));
        }
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let reference = fork_preparer_io::content_workspace(&self.provider, volume, Some(io))?;
        let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = (self).open(&reference, Some(io)).await?;
        let pinned = (self).generation(&workspace, generation, Some(io)).await?;
        let label = crate::core::filesystem_scope_label(
            grant.policy_scope().capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let view = crate::filesystem::access::access_view(
            &workspace,
            grant.policy_scope(),
            Some(pinned.id()),
            super::super::access::capability_payload(label, VolumeOperation::Read, io)?,
            ceiling,
            io,
        )
        .await?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        fork_preparer_io::retain(&pinned, Some(io)).await?;
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let path_hold = io.guard_owned(path.capacity() as u64)?;
        io.remaining()?;
        let stat = io.sdk(pinned.stat(&path, io, io.cancellation()).await)?;
        if stat.kind != FileKind::Regular {
            return Err(io.invalid("prefix requires a regular file"));
        }
        let length = stat.logical_bytes.ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "file length is missing",
            )
        })?;
        io.remaining()?;
        let chunk = io.sdk(
            pinned
                .read_range(&path, 0, maximum_bytes.min(length), io, io.cancellation())
                .await,
        )?;
        drop(path);
        drop(path_hold);
        drop(reference);
        drop(reference_hold);
        super::super::access::require_current(&workspace, &view.0, io).await?;
        Ok(chunk.bytes)
    }

    pub(in crate::filesystem) fn require_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.remaining()?;
        }
        crate::executor::with_history_io(io, |work| -> Result<()> {
            volume.validate(work)?;
            if !path.is_empty() {
                crate::conversation::validate_content_path(path, work)?;
            }
            Ok(())
        })?;
        if volume.provider() != &self.provider {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "private directory belongs to another provider or class",
            ));
        }
        grant.require_directory_path(volume, path, io)
    }

    pub(crate) fn file_generation(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<GenerationRef> {
        crate::model::charge_items(io, reference.version().len() as u64)?;
        let mut bytes = [0; 32];
        hex::decode_to_slice(reference.version(), &mut bytes).map_err(|_| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "file version is not a Filesystem generation",
            )
        })?;
        if let Some(io) = io {
            io.charge(WorkCounters {
                source_bytes_read: reference.version().len() as u64,
                ..Default::default()
            })?;
        }
        fork_preparer_io::generation_ref(
            &self.provider,
            GenerationId::new(Digest::from_bytes(bytes)),
            io,
        )
    }
}
pub(in crate::filesystem) async fn read_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let unmeasured;
    let io = if let Some(io) = io {
        io
    } else {
        unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        &unmeasured
    };
    io.remaining()?;
    let bytes = io.sdk(generation.read(path, maximum, io, io.cancellation()).await)?;
    if io.original_native_work().is_none() {
        let capacity = bytes.retained_capacity().ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "content producer does not expose its owned capacity",
            )
        })?;
        io.retain_owned(capacity as u64)?;
    }
    Ok(bytes)
}

use crate::executor::stock_boundary;

pub(crate) fn file_publication(
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<crate::OperationId> {
    if let Some(io) = io {
        io.charge(WorkCounters {
            items_examined: reference.version().len() as u64,
            ..Default::default()
        })?;
    }
    uuid::Uuid::parse_str(reference.version())
        .map(|id| crate::OperationId::from_bytes(*id.as_bytes()))
        .map_err(|_| Error::Storage("file metadata has no publication operation".into()))
}
