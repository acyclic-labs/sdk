use super::*;

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
        let bytes = self.read_pinned_io(reference, maximum_bytes, io).await?;
        if io.is_none() {
            let generation = self.file_generation(reference)?;
            let workspace = workspace_ref(
                self.provider.clone(),
                &reference.volume().storage_name(&mut None)?,
            )?;
            self.retain_generation(&workspace, &generation).await?;
        }
        Ok(bytes)
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
        let observation = self.resolve(&workspace).await?;
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
            page.entries
                .retain(|entry| entry.name.unicode_text().as_deref() != Some(".system"));
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
        volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if volume.provider() != &self.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "private directory belongs to another provider",
            });
        }
        if !path.is_empty() {
            crate::conversation::validate_content_path(
                path,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
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
        let workspace = fork_preparer_io::open(self, &workspace_ref, &scratch).await?;
        guard().await?;
        let generation = if let Some(expected) = expected_generation {
            fork_preparer_io::generation(self, &workspace, expected, &scratch).await?
        } else {
            workspace
                .head(work, work.cancellation())
                .await
                .map_err(|error| (error, None))
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
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
                        source: acyclic_fs::WorkspaceError::Work(acyclic_fs::WorkError::Overflow),
                        custody: None,
                    })?,
                work,
                work.cancellation(),
            )
            .await
            .map(|(page, allocation)| (page, allocation.expect("native directory allocation")))
            .map_err(|source| Error::NativeWorkspace {
                source,
                custody: None,
            })?;
        output.absorb_native_custody(page_custody)?;
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
                let removed = page.entries.pop().expect("overscanned page is nonempty");
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
        self.require_private_directory(volume, grant, path, io)?;
        let name = path.rsplit('/').next().unwrap_or(path);
        crate::executor::with_history_io(io, |work| {
            crate::conversation::validate_content_path(path, work)
        })?;
        if name.len() > crate::conversation::MAX_LABEL_BYTES {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "file or volume label is invalid",
            ));
        }
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name(&mut None)?)?;
        let observation = self.resolve(&workspace).await?;
        let generation = expected_generation.unwrap_or(&observation.generation);
        self.retain_generation(&workspace, generation).await?;
        let bytes = self
            .read(
                &workspace,
                Some(generation),
                &format!("/{}", path),
                maximum_bytes,
            )
            .await?;
        let metadata = self
            .read(
                &workspace,
                Some(generation),
                &content_metadata_path_io(path, true, None)?,
                u64::MAX,
            )
            .await;
        let (descriptor, display_name) = match metadata {
            Ok(metadata) => {
                let staged: FileRef = crate::contract::json_from_slice(&metadata)
                    .map_err(|_| Error::Storage("private file metadata is corrupt".into()))?;
                if staged.volume() != volume
                    || staged.path() != path
                    || staged.version() != "pending"
                {
                    return Err(Error::Storage(
                        "private file metadata does not match the file".into(),
                    ));
                }
                staged.descriptor().verify(&bytes, &mut None)?;
                (
                    staged.descriptor().clone(),
                    staged.display_name().to_owned(),
                )
            }
            Err(Error::NotFound(_)) => (
                FileDescriptor::from_bytes(&bytes, "application/octet-stream", &mut None)?,
                name.to_owned(),
            ),
            Err(error) => return Err(error),
        };
        let reference = FileRef::new(
            volume.clone(),
            path.to_owned(),
            hex::encode(generation.as_resource().key()),
            descriptor,
            display_name,
            &mut None,
        )?;
        grant.require_file_read(&reference, io)?;
        Ok((reference, bytes))
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
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "prefix read limit must be positive",
            ));
        }
        let unmeasured;
        let io = match io {
            Some(io) => io,
            None => {
                unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &unmeasured
            }
        };
        let reference = fork_preparer_io::content_workspace(&self.provider, volume, Some(io))?;
        let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = fork_preparer_io::open(self, &reference, io).await?;
        let pinned = fork_preparer_io::generation(self, &workspace, generation, io).await?;
        fork_preparer_io::retain(&pinned, Some(io)).await?;
        let path = crate::contract::preparation_format(format_args!("/{path}"), Some(io))?;
        let path_hold = io.guard_owned(path.capacity() as u64)?;
        io.remaining()?;
        let stat = io.sdk(pinned.stat(&path, io, io.cancellation()).await)?;
        if stat.kind != FileKind::Regular {
            return Err(io.invalid("prefix requires a regular file"));
        }
        let length = stat.logical_bytes.ok_or_else(|| {
            fork_preparer_io::failure(
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
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "private directory belongs to another provider or class",
            ));
        }
        grant.require_directory_path(volume, path, io)
    }

    pub(in crate::filesystem) async fn read_pinned(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.read_pinned_io(reference, maximum_bytes, None).await
    }

    pub(in crate::filesystem) fn file_generation(
        &self,
        reference: &FileRef,
    ) -> Result<GenerationRef> {
        self.file_generation_with_io(reference, None)
    }

    pub(crate) fn file_generation_with_io(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<GenerationRef> {
        if let Some(io) = io {
            return GenerationRef::from_hex_with_io(&self.provider, reference.version(), io);
        }
        let raw = hex::decode(reference.version())
            .map_err(|_| Error::Invalid("file version is not a Filesystem generation".into()))?;
        GenerationRef::new(self.provider.clone(), raw.into(), None)
    }
}
pub(in crate::filesystem) async fn read_generation_with_io<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    generation: &acyclic_fs::Generation<A, O>,
    path: &str,
    maximum: u64,
    io: &PreparationIo,
) -> Result<acyclic_stream::OwnedBytes> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        return generation
            .read(path, maximum, original, io.cancellation())
            .await
            .map_err(|source| Error::NativeWorkspace {
                source,
                custody: None,
            });
    }
    let bytes = io.receive(
        generation
            .read(path, maximum, io.remaining()?, io.cancellation())
            .await,
    )?;
    let capacity = bytes.retained_capacity().ok_or_else(|| {
        fork_preparer_io::failure(
            Some(io),
            crate::wire::ErrorCode::Unsupported,
            "content producer does not expose its owned capacity",
        )
    })?;
    io.retain_owned(capacity as u64)?;
    Ok(bytes)
}
