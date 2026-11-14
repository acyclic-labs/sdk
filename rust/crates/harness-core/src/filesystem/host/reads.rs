use super::*;
mod capture;
pub(crate) mod media_type;
#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod skills_tests;
#[cfg(all(test, not(loom)))]
mod tests;

struct PrivateReadSelection<'a, A: 'static, O: 'static> {
    workspace: Workspace<A, O>,
    generation: Generation<A, O>,
    view: super::super::access::CompiledAccess<'a>,
    data_access: Option<(Workspace<A, O>, super::super::access::CompiledAccess<'a>)>,
    #[cfg(not(target_arch = "wasm32"))]
    snapshot: Option<acyclic_fs::kernel::Retained<acyclic_fs::LazySnapshotRef>>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PrivateReadSelection<'_, A, O> {
    async fn recheck(&self, grant: &ContentGrant, io: &PreparationIo) -> Result<()> {
        super::super::access::require_current(&self.workspace, &self.view.0, io).await?;
        if let Some((workspace, view)) = &self.data_access {
            super::super::access::require_current(workspace, &view.0, io).await?;
        }
        grant.recheck(Some(io))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    async fn select_private_read<'a>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        ceiling: Option<&crate::Capabilities>,
        io: &'a PreparationIo,
    ) -> Result<PrivateReadSelection<'a, A, O>> {
        self.require_private_directory(volume, grant, path, Some(io))?;
        super::super::access::require_directory_ceiling(ceiling, volume, path, Some(io))?;
        validate_private_read_path(path, Some(io))?;
        let workspace_ref = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, workspace_hold);
        let workspace = self.open(&workspace_ref.0, Some(io)).await?;
        drop(workspace_ref);
        #[cfg(not(target_arch = "wasm32"))]
        let snapshot = self
            .private_snapshot(&workspace, expected_generation, io)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let lazy_generation = snapshot.as_ref().map(|cut| cut.authored_generation);
        #[cfg(target_arch = "wasm32")]
        let lazy_generation: Option<GenerationId> = None;
        let generation = match (lazy_generation, expected_generation) {
            (Some(generation), _) => self.select_generation(&workspace, generation, io).await?,
            (None, Some(expected)) => self.generation(&workspace, expected, Some(io)).await?,
            (None, None) => self.current_generation(&workspace, io).await?,
        };
        let view = private_read_view(&workspace, grant, generation.id(), ceiling, io).await?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::List, path, io)?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        let data_access = super::super::access::selected_data_access(
            &workspace,
            &generation,
            grant.policy_scope(),
            ceiling,
            io,
        )
        .await?;
        if let Some((_, view)) = &data_access {
            super::super::access::require(&view.0, acyclic_fs::access::Action::List, path, io)?;
            super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        }
        bounded_io::retain(&generation, Some(io)).await?;
        Ok(PrivateReadSelection {
            workspace,
            generation,
            view,
            data_access,
            #[cfg(not(target_arch = "wasm32"))]
            snapshot,
        })
    }

    /// Pins the generation used to resolve a private path, including its
    /// absence, under the same authority as a range read. No entries or file
    /// bodies are read. Later reads must still present their original grant.
    ///
    /// # Errors
    /// Refuses a foreign generation, a cancelled operation, or a path denied by
    /// the signed directory grant, caller ceiling or either policy owner.
    pub async fn select_private_generation(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<GenerationRef> {
        let temporary;
        let io = if let Some(io) = access.io {
            io
        } else {
            temporary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &temporary
        };
        let selected = self
            .select_private_read(volume, grant, path, expected_generation, access.ceiling, io)
            .await?;
        selected.recheck(grant, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(cut) = &selected.snapshot {
            self.validate_private_snapshot(cut, io).await?;
            selected.recheck(grant, io).await?;
            return self.private_generation_ref(cut, expected_generation, io);
        }
        self.generation_ref_id(selected.generation.id(), access.io)
    }

    /// Reads no-follow metadata at the selected private generation without
    /// loading file contents. Only path absence returns `None`; directory
    /// authority, caller ceilings and both policy owners are checked as for
    /// a private range read, including on an absent path.
    pub async fn stat_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<Option<acyclic_fs::WorkspaceStat>> {
        let temporary;
        let io = if let Some(io) = access.io {
            io
        } else {
            temporary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &temporary
        };
        let selected = self
            .select_private_read(volume, grant, path, expected_generation, access.ceiling, io)
            .await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let _requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let stat = io.sdk_if_present(
            selected
                .generation
                .stat(&requested, io, io.cancellation())
                .await,
        )?;
        selected.recheck(grant, io).await?;
        Ok(stat)
    }

    /// Resolves a host-owned immutable publication, then uses the same selected
    /// range reader as execution payloads. The original grant and persisted
    /// journal policy protect execution state independently of path spelling.
    pub(crate) async fn read_internal_range(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        class: InternalContentClass,
        range: acyclic_fs::ByteRange,
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        grant.require(volume, VolumeOperation::Read, io)?;
        InternalContentClass::require_path(volume, Some(class), path, io)?;
        let temporary = PreparationIo::temporary(io)?;
        let io = temporary.as_ref().or(io);
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let workspace_ref = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace = self.open(&workspace_ref, Some(io)).await?;
        let generation = io.sdk(workspace.head(io, io.cancellation()).await)?;
        let (file, reservation) = read_content_metadata(&generation, volume, path, io)
            .await?
            .ok_or_else(|| io.invalid("internal publication is absent"))?;
        if file.volume() != volume || file.path() != path {
            return Err(io.invalid("internal publication identity differs"));
        }
        let (file, reservation) = pin_reference((file, reservation), generation.id(), io)?;
        let remaining = file
            .descriptor()
            .byte_length()
            .checked_sub(range.offset)
            .ok_or_else(|| io.invalid("internal read offset is past the immutable file's end"))?;
        let bytes = self
            .read_content_range(
                &file,
                grant,
                acyclic_fs::ByteRange {
                    offset: range.offset,
                    length: range.length.min(remaining),
                },
                Some(io),
            )
            .await?;
        reservation.into_retained()?;
        drop(workspace_ref);
        drop(workspace_hold);
        Ok((file, bytes))
    }

    /// Reads a bounded selection under the original grant and current policy.
    /// The immutable descriptor authenticates the selected range; no complete
    /// payload is materialized. Expiry and revocation fence disclosure too.
    pub async fn read_content_range(
        &self,
        reference: &FileRef,
        grant: &ContentGrant,
        range: acyclic_fs::ByteRange,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        grant.require_file_read(reference, io)?;
        let length = reference.descriptor().byte_length();
        if range
            .offset
            .checked_add(range.length)
            .is_none_or(|end| end > length)
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content range is past the immutable file's end",
            ));
        }
        let temporary = PreparationIo::temporary(io)?;
        let io = temporary.as_ref().or(io);
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let bytes = self
            .read_selection_with_access(grant.policy_scope(), reference, true, None, io)
            .await?
            .read_range(range)
            .await?;
        grant.recheck(Some(io))?;
        Ok(bytes)
    }

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
        maximum_bytes: impl Into<Option<u64>>,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let maximum_bytes = maximum_bytes
            .into()
            .unwrap_or(reference.descriptor().byte_length());
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
        let bytes = self
            .read_file_with_access(
                grant.policy_scope(),
                reference,
                maximum_bytes,
                true,
                None,
                io,
            )
            .await?;
        grant.recheck(Some(io))?;
        Ok(bytes)
    }

    /// Resolves a private file and verifies its bytes with optional original IO.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: impl Into<Option<u64>>,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        let (file, bytes, _) = self
            .read_private_path_indexed(
                volume,
                grant,
                path,
                expected_generation,
                maximum_bytes.into(),
                crate::conversation::ContentAccess { ceiling, io },
            )
            .await?
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::NotFound,
                    crate::conversation::SELECTED_FILE_MISSING,
                )
                .tool_input()
            })?;
        Ok((file, bytes))
    }

    /// Resolve a file with its immutable publication identity under the caller ceiling.
    /// Only absence of the selected path returns `None`; admission and backing
    /// storage failures retain their original errors.
    pub async fn read_private_path_indexed(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: Option<u64>,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<
        Option<(
            FileRef,
            acyclic_stream::OwnedBytes,
            Option<crate::OperationId>,
        )>,
    > {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        self.require_private_directory(volume, grant, path, io)?;
        super::super::access::require_directory_ceiling(ceiling, volume, path, io)?;
        validate_private_read_path(path, io)?;
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let workspace_ref = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, workspace_hold);
        let workspace = self.open(&workspace_ref.0, Some(io)).await?;
        let generation = match expected_generation {
            Some(expected) => self.generation(&workspace, expected, Some(io)).await?,
            None => self.current_generation(&workspace, io).await?,
        };
        let view = private_read_view(&workspace, grant, generation.id(), ceiling, io).await?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::List, path, io)?;
        super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        let data_access = super::super::access::selected_data_access(
            &workspace,
            &generation,
            grant.policy_scope(),
            ceiling,
            io,
        )
        .await?;
        if let Some((_, view)) = &data_access {
            super::super::access::require(&view.0, acyclic_fs::access::Action::List, path, io)?;
            super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        }
        bounded_io::retain(&generation, Some(io)).await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let requested = (requested, requested_hold);
        let bytes = read_generation_optional(
            &generation,
            &requested.0,
            maximum_bytes,
            io,
            path_read_refusal,
        )
        .await?;
        let selected = if let Some(bytes) = bytes {
            let bytes_hold = io.guard_bytes(&bytes)?;
            let bytes = (bytes, bytes_hold);
            let staged = read_content_metadata(&generation, volume, path, io).await?;
            let publication = staged
                .as_ref()
                .map(|file| file_publication(&file.0, Some(io)))
                .transpose()?;
            let reference =
                selected_reference(volume, path, generation.id(), &bytes.0, staged, io)?;
            grant.require_file_read(&reference.0, Some(io))?;
            Some((reference, bytes, publication))
        } else {
            None
        };
        super::super::access::require_current(&workspace, &view.0, io).await?;
        if let Some((workspace, view)) = &data_access {
            super::super::access::require_current(workspace, &view.0, io).await?;
        }
        grant.recheck(Some(io))?;
        selected
            .map(|(reference, bytes, publication)| {
                reference.1.into_retained()?;
                if let Some(hold) = bytes.1 {
                    hold.into_retained()?;
                }
                Ok((reference.0, bytes.0, publication))
            })
            .transpose()
    }

    /// Reads at most `maximum` bytes from `offset` of the private file at
    /// `path` at the selected generation (or head), and the file's length, loading only those
    /// bytes. The grant, the compiled view and its currency are checked as for
    /// [`Self::read_private_path_indexed`]; the selected generation authenticates
    /// the file's bytes, so no descriptor is resolved from them.
    ///
    /// # Errors
    /// When `path` is not a regular file, `offset` is past its end, or the
    /// grant or view does not allow listing and reading it.
    pub(in crate::filesystem) async fn read_private_range(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        range: acyclic_fs::ByteRange,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<(acyclic_stream::OwnedBytes, GenerationRef, u64)> {
        let crate::conversation::ContentAccess { ceiling, io } = access;
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let selected = self
            .select_private_read(volume, grant, path, expected_generation, ceiling, io)
            .await?;
        let generation = &selected.generation;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let requested_hold = io.guard_owned(requested.capacity() as u64)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(cut) = &selected.snapshot {
            let (bytes, length) = self
                .read_private_snapshot(cut, &requested, range, io)
                .await?;
            selected.recheck(grant, io).await?;
            return Ok((
                bytes,
                self.private_generation_ref(cut, expected_generation, io)?,
                length,
            ));
        }
        let (stat, recorded) = generation.stat(&requested, io, io.cancellation()).await;
        let stat = io.sdk((Ok::<_, Error>(stat), recorded))?;
        let stat = stat.map_err(|error| path_read_refusal(error.into()))?;
        let length = match (stat.kind, stat.logical_bytes) {
            (acyclic_fs::kernel::FileKind::Regular, Some(length)) => length,
            _ => {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "private path is not a regular file",
                )
                .tool_input());
            }
        };
        let offset = range.offset.min(length);
        let remaining = length - offset;
        let chunk = io.sdk(
            generation
                .read_range(
                    &requested,
                    offset,
                    remaining.min(range.length),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        selected.recheck(grant, io).await?;
        drop(requested_hold);
        let selected_id = self.generation_ref_id(generation.id(), Some(io))?;
        Ok((io.retain_read_bytes(chunk.bytes)?, selected_id, length))
    }

    /// Read only newly appended bytes after an immutable admitted generation.
    /// The primitive compares the old prefix by shared chunks; neither the old
    /// file nor its hash is copied into an actor checkpoint or re-read in full.
    pub(in crate::filesystem) async fn read_private_append(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        generation: &GenerationRef,
        original: Option<&GenerationRef>,
        maximum_delta: Option<u64>,
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let temporary;
        let io = if let Some(io) = access.io {
            io
        } else {
            temporary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &temporary
        };
        let selected = self
            .select_private_read(volume, grant, path, Some(generation), access.ceiling, io)
            .await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let _path_hold = io.guard_owned(requested.capacity() as u64)?;
        let Some(stat) = io.sdk_if_present(
            selected
                .generation
                .stat(&requested, io, io.cancellation())
                .await,
        )?
        else {
            selected.recheck(grant, io).await?;
            return Ok(None);
        };
        let length = stat
            .logical_bytes
            .filter(|_| stat.kind == acyclic_fs::kernel::FileKind::Regular)
            .ok_or_else(|| crate::Error::tool_rejected("append source is not a regular file"))?;
        let prefix = if let Some(original) = original {
            let original = self
                .generation(&selected.workspace, original, Some(io))
                .await?;
            let stat = io.sdk(original.stat(&requested, io, io.cancellation()).await)?;
            let bytes = stat
                .logical_bytes
                .filter(|_| stat.kind == acyclic_fs::kernel::FileKind::Regular)
                .ok_or_else(|| {
                    crate::Error::tool_rejected("admitted append source is not a regular file")
                })?;
            if !io.sdk(
                selected
                    .generation
                    .matches_content_prefix(&original, &requested, bytes, io, io.cancellation())
                    .await,
            )? {
                return Err(crate::Error::tool_rejected(
                    "append changed its admitted prefix",
                ));
            }
            bounded_io::retain(&original, Some(io)).await?;
            bytes
        } else {
            0
        };
        let delta = length
            .checked_sub(prefix)
            .filter(|delta| maximum_delta.is_none_or(|maximum| *delta <= maximum))
            .ok_or_else(|| crate::Error::tool_rejected("append delta exceeds admitted capacity"))?;
        let chunk = io.sdk(
            selected
                .generation
                .read_range(&requested, prefix, delta, io, io.cancellation())
                .await,
        )?;
        selected.recheck(grant, io).await?;
        io.retain_read_bytes(chunk.bytes).map(Some)
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
        let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = (self).open(&reference, Some(io)).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let snapshot = self
            .private_snapshot(&workspace, Some(generation), io)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let authored = snapshot.as_ref().map(|cut| cut.authored_generation);
        #[cfg(target_arch = "wasm32")]
        let authored: Option<GenerationId> = None;
        let pinned = match authored {
            Some(authored) => self.select_generation(&workspace, authored, io).await?,
            None => self.generation(&workspace, generation, Some(io)).await?,
        };
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
        let data_access = super::super::access::selected_data_access(
            &workspace,
            &pinned,
            grant.policy_scope(),
            ceiling,
            io,
        )
        .await?;
        if let Some((_, view)) = &data_access {
            super::super::access::require(&view.0, acyclic_fs::access::Action::Read, path, io)?;
        }
        bounded_io::retain(&pinned, Some(io)).await?;
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let path_hold = io.guard_owned(path.capacity() as u64)?;
        io.remaining()?;
        #[cfg(not(target_arch = "wasm32"))]
        let lazy = if let Some(cut) = &snapshot {
            Some(
                self.read_private_snapshot(
                    cut,
                    &path,
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: maximum_bytes,
                    },
                    io,
                )
                .await?
                .0,
            )
        } else {
            None
        };
        #[cfg(target_arch = "wasm32")]
        let lazy: Option<acyclic_stream::OwnedBytes> = None;
        let bytes = if let Some(bytes) = lazy {
            bytes
        } else {
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
            io.sdk(
                pinned
                    .read_range(&path, 0, maximum_bytes.min(length), io, io.cancellation())
                    .await,
            )?
            .bytes
        };
        drop(path);
        drop(path_hold);
        drop(reference);
        drop(reference_hold);
        super::super::access::require_current(&workspace, &view.0, io).await?;
        if let Some((workspace, view)) = &data_access {
            super::super::access::require_current(workspace, &view.0, io).await?;
        }
        grant.recheck(Some(io))?;
        Ok(bytes)
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
        let generation = self.file_generation_id(reference, io)?;
        bounded_io::generation_ref(&self.provider, generation, io)
    }

    /// Decode the complete generation identity, including its storage
    /// association, that a file reference names.
    pub fn file_generation_id(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_fs::GenerationId> {
        bounded_io::generation_version(reference.version(), io)
    }
}
// Only the selected user path is classified here. Metadata, backing storage,
// cancellation and accounting failures retain their original terminal cause.
fn path_read_refusal(error: WorkspaceError) -> Error {
    let (code, reason) = match error {
        WorkspaceError::NotFound => (
            crate::wire::ErrorCode::NotFound,
            crate::conversation::SELECTED_FILE_MISSING,
        ),
        WorkspaceError::NotRegularFile | WorkspaceError::IsDirectory => (
            crate::wire::ErrorCode::Invalid,
            "selected path is not a regular file",
        ),
        WorkspaceError::ReadLimitExceeded => (
            crate::wire::ErrorCode::Invalid,
            "selected file exceeds the admitted read bound",
        ),
        error => return error.into(),
    };
    Error::ToolRejected { code, reason }
}

// An absent application policy reads the immutable file's own extent through
// the Filesystem primitive, on the original I/O and allocation ledger.
pub(in crate::filesystem) async fn read_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    maximum: Option<u64>,
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
    io.retain_read_bytes(bytes)
}

async fn read_generation_optional<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    maximum: Option<u64>,
    io: &PreparationIo,
    classify: fn(WorkspaceError) -> Error,
) -> Result<Option<acyclic_stream::OwnedBytes>> {
    io.remaining()?;
    let (result, recorded) = generation.read(path, maximum, io, io.cancellation()).await;
    // Check the original accounting outcome before classifying or allocating
    // a converted error. File-input refusals cannot replace a ledger failure.
    let result = io.sdk((Ok::<_, Error>(result), recorded))?;
    let result = match result {
        Ok(bytes) => Ok(Some(bytes)),
        Err(WorkspaceError::NotFound) => Ok(None),
        Err(error) => Err(classify(error)),
    };
    result?.map(|bytes| io.retain_read_bytes(bytes)).transpose()
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
    let reference = if let Some(staged) = staged {
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
        pin_reference(staged, generation, io)?
    } else {
        let version = bounded_io::hex_key(&generation.to_bytes(), Some(io))?;
        let version_hold = io.guard_owned(version.capacity() as u64)?;
        let reference = crate::executor::with_history_io(Some(io), |work| {
            FileRef::new(
                volume.copy(work)?,
                stock_string_copy(path, Some(io))?,
                version,
                FileDescriptor::from_bytes(bytes, media_type::captured(bytes), work)?,
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

// Metadata stores the publication operation. A selected read instead names
// the authenticated filesystem generation containing that protected index.
fn pin_reference<'a>(
    mut staged: (FileRef, DecodeReservation<'a>),
    generation: acyclic_fs::GenerationId,
    io: &'a PreparationIo,
) -> Result<(FileRef, DecodeReservation<'a>)> {
    let version = bounded_io::hex_key(&generation.to_bytes(), Some(io))?;
    let version_hold = io.guard_owned(version.capacity() as u64)?;
    staged
        .0
        .pin_version(version, &mut Some(&mut io.history_io()))?;
    let version_bytes = version_hold.bytes();
    version_hold.into_retained()?;
    staged.1.adopt(version_bytes)?;
    staged.1.substitute(staged.0.logical_owned_bytes()?)?;
    Ok(staged)
}

async fn read_content_metadata<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>,
    volume: &VolumeRef,
    path: &str,
    io: &'a PreparationIo,
) -> Result<Option<(FileRef, DecodeReservation<'a>)>> {
    let metadata_key = content_metadata_key_io(volume, path, Some(io))?;
    let metadata_key_hold = io.guard_owned(metadata_key.retained_bytes() as u64)?;
    let metadata_key = (metadata_key, metadata_key_hold);
    let metadata = read_file_annotation(generation, path, &metadata_key.0, None, Some(io)).await?;
    let staged = match metadata {
        Some(metadata) => {
            let metadata_hold = io.guard_bytes(&metadata)?;
            let metadata = (metadata, metadata_hold);
            let staged = decode_content_metadata(&metadata.0, io)?;
            if staged.0.volume() != volume || staged.0.path() != path {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "private file metadata does not match the file",
                ));
            }
            let original = super::ranges::matching_publication(
                &generation.workspace(),
                generation,
                &staged.0,
                &metadata_key.0,
                &metadata.0,
                io,
            )
            .await?;
            // A join can replace the payload while retaining the destination's
            // index. Only an authenticated original publication may be treated
            // as stale; malformed or substituted records still fail above.
            if original.is_some() {
                Some(staged)
            } else {
                None
            }
        }
        None => None,
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
    crate::executor::with_history_io(io, |work| {
        crate::conversation::validate_content_path(path, work)
    })?;
    Ok(())
}
