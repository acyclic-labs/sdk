use super::*;
mod media_type;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Reads a pinned version using optional preparation accounting.
    pub async fn read_content(
        &self,
        reference: &FileRef,
        grant: &ContentGrant,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        reference.validate(&mut None)?;
        grant.require_file_read(reference, io)?;
        self.read_pinned_receipt_io(reference, maximum_bytes, io)
            .await?
            .retain()
            .await
    }

    /// Discovers a granted private directory at the owner's current head, or continues at the immutable generation returned by a prior page.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, DirectoryPage)> {
        self.require_private_directory(volume, grant, path, None)?;
        if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name(&mut None)?)?;
        let observation = self.resolve(&workspace, None).await?;
        let generation = expected_generation.unwrap_or(&observation.generation);
        self.retain_generation(&workspace, generation).await?;
        let requested = if path.is_empty() {
            "/".to_owned()
        } else {
            format!("/{path}")
        };
        let mut page = self
            .list_after(
                &workspace,
                Some(generation),
                &requested,
                after,
                maximum_entries + u32::from(path.is_empty()),
            )
            .await?;
        if path.is_empty() {
            page.entries.retain(|entry| {
                entry
                    .name
                    .unicode_text(
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .ok()
                    .map(|receipt| receipt.value)
                    .as_deref()
                    != Some(".system")
            });
            if page.entries.len() > maximum_entries as usize {
                page.entries.truncate(maximum_entries as usize);
                page.has_more = true;
            }
        }
        Ok((generation.clone(), page))
    }

    /// Uses the ordinary immutable list engine, with a fresh owner check around each SDK logical action.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(
        clippy::too_many_arguments,
        reason = "exact subtree and guarded pinned pagination"
    )]
    pub(in crate::filesystem) async fn list_private_directory_guarded_with_native_work<F, Fut>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        work: &acyclic_fs::NativeMaterializationWork,
        mut guard: F,
    ) -> Result<((GenerationRef, DirectoryPage), acyclic_fs::NativeAllocation)>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        volume.validate(&mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )))?;
        if volume.provider() != &self.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "private directory belongs to another provider",
            });
        }
        if !path.is_empty() {
            crate::conversation::validate_content_path(
                path,
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )?;
        }
        grant.require_directory_path(
            volume,
            path,
            Some(&PreparationIo::from_native_work(work)?),
        )?;
        if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "private directory page limit is invalid",
            });
        }
        let scratch = PreparationIo::from_native_work(work)?;
        let output = PreparationIo::from_native_work(work)?;
        output.retain_owned(std::mem::size_of::<(GenerationRef, DirectoryPage)>() as u64)?;
        let workspace_ref =
            fork_preparer_io::content_workspace(&self.provider, volume, Some(&scratch))?;
        guard().await?;
        let workspace = (self).open(&workspace_ref, Some(&scratch)).await?;
        guard().await?;
        let generation = if let Some(expected) = expected_generation {
            (self)
                .generation(&workspace, expected, Some(&scratch))
                .await?
        } else {
            workspace
                .head(work, work.cancellation())
                .await
                .map_err(|error| (error, None))
                .map_err(|(source, custody)| Error::NativeWorkspace {
                    source: Box::new(source),
                    custody,
                })?
        };
        guard().await?;
        fork_preparer_io::retain(&generation, Some(&scratch)).await?;
        guard().await?;
        let (requested, requested_custody) = work
            .display(&format_args!("/{path}"))
            .map_err(Error::from)?;
        scratch.absorb_native_custody(requested_custody)?;
        let page_custody;
        let mut page;
        (page, page_custody) = generation
            .list_directory(
                &requested,
                after,
                maximum_entries
                    .checked_add(u32::from(path.is_empty()))
                    .ok_or_else(|| Error::NativeWorkspace {
                        source: Box::new(acyclic_fs::WorkspaceError::Work(
                            acyclic_fs::WorkError::Overflow,
                        )),
                        custody: None,
                    })?,
                work,
                work.cancellation(),
            )
            .await
            .map_err(|source| Error::NativeWorkspace {
                source: Box::new(source),
                custody: None,
            })?;
        output
            .absorb_native_custody(page_custody.ok_or_else(|| {
                output.invalid("native directory page has no allocation custody")
            })?)?;
        guard().await?;
        if path.is_empty() {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: page.entries.len() as u64,
                ..Default::default()
            })
            .map_err(Error::from)?;
            let mut removed_bytes = 0;
            page.entries.retain(|entry| {
                let name = &entry.name;
                let internal = match name.encoding() {
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => name.as_bytes() == b".system",
                    NameEncoding::WindowsUtf16Le => name.as_bytes() == b".\0s\0y\0s\0t\0e\0m\0",
                };
                if internal {
                    removed_bytes += name.logical_owned_bytes();
                }
                !internal
            });
            output.release(removed_bytes)?;
            while page.entries.len() > maximum_entries as usize {
                let removed = page
                    .entries
                    .pop()
                    .ok_or_else(|| output.invalid("overscanned directory page is empty"))?;
                let bytes = removed.name.logical_owned_bytes();
                drop(removed);
                output.release(bytes)?;
                page.has_more = true;
            }
        }
        let reference =
            fork_preparer_io::generation_ref(&self.provider, generation.id(), Some(&output))?;
        let retained = output.owned_bytes()?;
        let custody = output.into_native_custody(retained)?;
        Ok(((reference, page), custody))
    }

    /// Resolves a private file and verifies its bytes with optional original IO.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let (file, bytes, _) = self
            .read_private_path_indexed(volume, grant, path, expected_generation, maximum_bytes, io)
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
        io: Option<&PreparationIo>,
    ) -> Result<(
        FileRef,
        acyclic_stream::OwnedBytes,
        Option<crate::OperationId>,
    )> {
        self.require_private_directory(volume, grant, path, io)?;
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
        let _workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace = self.open(&workspace_ref, Some(io)).await?;
        let generation = match expected_generation {
            Some(reference) => self.generation(&workspace, reference, Some(io)).await?,
            None => io.sdk(workspace.head(io, io.cancellation()).await)?,
        };
        fork_preparer_io::retain(&generation, Some(io)).await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let _requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let bytes = read_generation(&generation, &requested, maximum_bytes, Some(io)).await?;
        let metadata_path = content_metadata_path_io(path, true, Some(io))?;
        let _metadata_path_hold = io.guard_owned(metadata_path.capacity() as u64)?;
        let metadata = read_generation(
            &generation,
            &metadata_path,
            io.remaining()?.source_bytes_read,
            Some(io),
        )
        .await;
        let staged: Option<FileRef> = match metadata {
            Ok(metadata) => Some(io.decode_json(&metadata)?),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let _staged_hold = staged
            .as_ref()
            .map(|file| io.guard_owned(file.logical_owned_bytes()?))
            .transpose()?;
        let (descriptor, display_name) = if let Some(staged) = &staged {
            if staged.volume() != volume || staged.path() != path {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "private file metadata does not match the file",
                ));
            }
            crate::executor::with_history_io(Some(io), |work| {
                staged.descriptor().verify(&bytes, work)
            })?;
            (
                FileDescriptor::new(
                    *staged.descriptor().sha256(),
                    staged.descriptor().byte_length(),
                    stock_string_copy(staged.descriptor().media_type(), Some(io))?,
                )?,
                stock_string_copy(staged.display_name(), Some(io))?,
            )
        } else {
            (
                crate::executor::with_history_io(Some(io), |work| {
                    FileDescriptor::from_bytes(&bytes, media_type::classify(&bytes), work)
                })?,
                stock_string_copy(name, Some(io))?,
            )
        };
        let version = fork_preparer_io::hex_key(&generation.id().digest().into_bytes(), Some(io))?;
        let reference = crate::executor::with_history_io(Some(io), |work| {
            FileRef::new(
                volume.copy(work)?,
                stock_string_copy(path, Some(io))?,
                version,
                descriptor,
                display_name,
                work,
            )
        })?;
        grant.require_file_read(&reference, Some(io))?;
        if staged.is_some() {
            // Upload metadata carries the publication operation, not a sentinel.
            // Reuse range authentication to bind it to the original atomic write.
            self.read_pinned_range(
                &reference,
                acyclic_fs::ByteRange {
                    offset: 0,
                    length: 0,
                },
                Some(io),
            )
            .await?;
        }
        let publication = staged
            .as_ref()
            .map(|file| file_publication(file, Some(io)))
            .transpose()?;
        Ok((reference, bytes, publication))
    }

    /// Reads a finite private prefix at an explicitly pinned generation.
    pub async fn read_private_prefix(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        generation: &GenerationRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.require_private_directory(volume, grant, path, io)?;
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
