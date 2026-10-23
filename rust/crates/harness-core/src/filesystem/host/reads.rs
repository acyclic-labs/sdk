use super::*;
mod media_type;
#[cfg(all(
    test,
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32"),
    not(acyclic_harness_loom)
))]
mod skills_tests;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
struct SkillRead<'a> {
    volume: &'a VolumeRef,
    grant: &'a ContentGrant,
    path: &'a str,
    source_generation: &'a GenerationRef,
    maximum_bytes: u64,
    access: crate::conversation::ContentAccess<'a, 'a>,
}

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
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        let (file, bytes, _) = self
            .read_private_path_indexed(
                volume,
                grant,
                path,
                expected_generation,
                maximum_bytes,
                crate::conversation::ContentAccess { ceiling, io },
            )
            .await?;
        Ok((file, bytes))
    }

    /// Resolve a file with its immutable publication identity under the caller ceiling.
    pub async fn read_private_path_indexed(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<(
        FileRef,
        acyclic_stream::OwnedBytes,
        Option<crate::OperationId>,
    )> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        self.require_private_directory(volume, grant, path, io)?;
        super::super::access::require_directory_ceiling(ceiling, volume, path, io)?;
        #[cfg(all(
            feature = "filesystem-local",
            feature = "native-execution",
            not(target_arch = "wasm32")
        ))]
        if let Some(sources) = self.skill_sources(volume)?
            && let Some(source_generation) = expected_generation
            && sources.matches(volume, source_generation, path)
        {
            let generation = if let Some(retained) = sources.materialized(source_generation)? {
                retained
            } else {
                self.materialize_skill(
                    &sources,
                    SkillRead {
                        volume,
                        grant,
                        path,
                        source_generation,
                        maximum_bytes,
                        access: crate::conversation::ContentAccess { ceiling, io },
                    },
                )
                .await?
            };
            return Box::pin(self.read_private_path_indexed(
                volume,
                grant,
                path,
                Some(&generation),
                maximum_bytes,
                crate::conversation::ContentAccess { ceiling, io },
            ))
            .await;
        }

        validate_private_read_path(path, io)?;
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
        let view = private_read_view(&workspace, grant, generation.id(), ceiling, io).await?;
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
        let staged = read_content_metadata(&generation, path, io).await?;
        let publication = staged
            .as_ref()
            .map(|file| file_publication(&file.0, Some(io)))
            .transpose()?;
        let reference = selected_reference(volume, path, generation.id(), &bytes.0, staged, io)?;
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
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

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

    /// Resolve the immutable generation selected by a file reference.
    pub fn file_generation(
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
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    async fn materialize_skill(
        &self,
        sources: &crate::context::native_skills::NativeSkills,
        request: SkillRead<'_>,
    ) -> Result<GenerationRef> {
        let SkillRead {
            volume,
            grant,
            path,
            source_generation,
            maximum_bytes,
            access: crate::conversation::ContentAccess { ceiling, io },
        } = request;
        // Keep the cache recheck, pinned transaction and retained receipt
        // under the same journal owner. Disjoint first loads never race HEAD.
        let materializing = sources.materializer.enter().await;
        if let Some(retained) = sources.materialized(source_generation)? {
            drop(materializing);
            return Ok(retained);
        }
        let unmeasured;
        let original_io = io;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name(&mut None)?)?;
        let key = IdempotencyKey::new(format!(
            "skill-body-{}",
            hex::encode(source_generation.as_resource().key())
        ))?;
        // The commit is authoritative if a crash or a failed receipt write
        // separated publication from the local cache. Never reopen the
        // host source when its original bytes are already committed.
        let committed = crate::conversation::BoundedContentFuture::new(Some(io), || {
            self.operation_generation(&workspace, &key, Some(io))
        })?
        .await?;
        let generation = if let Some(committed) = committed {
            committed
        } else {
            let selected = self.open(&workspace, Some(io)).await?;
            let label = crate::core::filesystem_scope_label(
                grant.policy_scope().capabilities(),
                &mut Some(&mut io.history_io()),
            )?;
            let view = super::super::access::access_view(
                &selected,
                grant.policy_scope(),
                None,
                super::super::access::capability_payload(label, VolumeOperation::Read, io)?,
                ceiling,
                io,
            )
            .await?;
            for action in [
                acyclic_fs::access::Action::List,
                acyclic_fs::access::Action::Read,
            ] {
                super::super::access::require(&view.0, action, path, io)?;
            }
            let expected = self.generation_ref_id(view.2, Some(io))?;
            let bytes = sources
                .read(volume, source_generation, path, maximum_bytes, original_io)?
                .await?
                .ok_or_else(|| Error::NotFound("retained skill source is absent".into()))?;
            super::super::access::require_current(&selected, &view.0, io).await?;
            let (directory, _) = path
                .rsplit_once('/')
                .ok_or_else(|| Error::Invalid("skill source has no directory".into()))?;
            self.apply_generation(
                &workspace,
                Some(&expected),
                &[
                    WorkspaceMutation::CreateDirectory {
                        path: format!("/{directory}"),
                    },
                    WorkspaceMutation::PutFile {
                        path: format!("/{path}"),
                        bytes,
                    },
                ],
                key,
                Some(io),
                None,
            )?
            .await?
        };
        let generation = self.generation_ref_id(generation.id(), original_io)?;
        sources.retain_materialized(source_generation, &generation)?;
        drop(materializing);
        Ok(generation)
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

fn selected_reference<'a>(
    volume: &VolumeRef,
    path: &str,
    generation: acyclic_fs::GenerationId,
    bytes: &acyclic_stream::OwnedBytes,
    staged: Option<(FileRef, DecodeReservation<'a>)>,
    io: &'a PreparationIo,
) -> Result<(FileRef, DecodeReservation<'a>)> {
    let version = fork_preparer_io::hex_key(&generation.digest().into_bytes(), Some(io))?;
    let version_hold = io.guard_owned(version.capacity() as u64)?;
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
            .verify(bytes, &mut Some(&mut io.history_io()))?;
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
                FileDescriptor::from_bytes(bytes, media_type::classify(bytes), work)?,
                stock_string_copy(path.rsplit('/').next().unwrap_or(path), Some(io))?,
                work,
            )
        })?;
        version_hold.into_retained()?;
        let hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        (reference, hold)
    };
    Ok(reference)
}

async fn read_content_metadata<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    io: &'a PreparationIo,
) -> Result<Option<(FileRef, DecodeReservation<'a>)>> {
    let metadata_path = content_metadata_path_io(path, true, Some(io))?;
    let metadata_path_hold = io.guard_owned(metadata_path.capacity() as u64)?;
    let metadata_path = (metadata_path, metadata_path_hold);
    let metadata = read_generation(
        generation,
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
    Ok(staged)
}

async fn private_read_view<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &acyclic_fs::Workspace<A, O>,
    grant: &ContentGrant,
    generation: GenerationId,
    ceiling: Option<&crate::Capabilities>,
    io: &'a PreparationIo,
) -> Result<super::super::access::CompiledAccess<'a>> {
    let label = crate::core::filesystem_scope_label(
        grant.policy_scope().capabilities(),
        &mut Some(&mut io.history_io()),
    )?;
    crate::filesystem::access::access_view(
        workspace,
        grant.policy_scope(),
        Some(generation),
        super::super::access::capability_payload(label, VolumeOperation::Read, io)?,
        ceiling,
        io,
    )
    .await
}

fn validate_private_read_path(path: &str, io: Option<&PreparationIo>) -> Result<()> {
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
    Ok(())
}
