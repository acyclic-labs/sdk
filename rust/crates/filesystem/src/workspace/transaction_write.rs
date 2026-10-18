use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Creates or replaces one complete UTF-8 file.
    pub async fn write_text(&mut self, path: &str, text: &str) -> Result<(), WorkspaceError> {
        self.write(
            path,
            Bytes::copy_from_slice(text.as_bytes()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Creates or replaces one complete file under the supplied work limits.
    pub async fn write<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .write_with_accounting(path, bytes, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    async fn write_with_accounting(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path(path, accounting, cancellation)?;
        let existing = self
            .checkout
            .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
            .await?
            .0
            .record;
        let mut operations =
            crate::kernel::Owned::allocate(if existing.is_some() { 2 } else { 1 }, accounting)?;
        if let Some(record) = existing {
            if record.kind != FileKind::Regular {
                return Err(WorkspaceError::NotRegularFile);
            }
            Self::reset_file(
                &path.value,
                Some(record),
                &mut operations,
                accounting,
                cancellation,
            )?;
        }
        let operation = if existing.is_some() {
            AuthoredMutation::Write {
                path: path.value,
                offset: 0,
                bytes: bytes.into(),
            }
        } else {
            AuthoredMutation::CreateFile {
                path: path.value,
                bytes: bytes.into(),
                metadata: FileMetadata::default(),
            }
        };
        Self::push_mutation(
            &mut operations,
            crate::kernel::Owned::new(operation, path.allocation, path.bytes),
            accounting,
        )?;
        self.apply_authored(operations, accounting, cancellation)
            .await
    }

    /// Stages directory prefixes and ordered file replacements as one transaction batch.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn write_batch<W: crate::facade::FsWork>(
        &mut self,
        directories: &[&str],
        files: &[(&str, acyclic_stream::OwnedBytes)],
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .write_batch_with_accounting::<true>(directories, files, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn write_batch_with_accounting<const RESET: bool>(
        &mut self,
        directories: &[&str],
        files: &[(&str, acyclic_stream::OwnedBytes)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        if self.checkout.has_pending_mutations()
            || self.checkout.volume_config().case_sensitivity
                != crate::model::CaseSensitivity::Sensitive
        {
            return self
                .write_batch_serial(directories, files, accounting, cancellation)
                .await;
        }
        let (paths, directory_count) =
            self.batch_paths(directories, files, accounting, cancellation)?;
        let mut kinds = self
            .batch_kinds(&paths.value, accounting, cancellation)
            .await?;
        classify_batch_paths(&paths.value, directory_count, &mut kinds.value, accounting)?;
        let capacity = directory_count
            .checked_add(
                files
                    .len()
                    .checked_mul(2)
                    .ok_or(crate::WorkError::Overflow)?,
            )
            .ok_or(crate::WorkError::Overflow)?;
        let mut operations = crate::kernel::Owned::allocate(capacity, accounting)?;
        // Transfer the original path ownership after all cross-path decisions.
        // Only resize plus write needs two independently owned copies.
        let backing = paths.value.capacity() * std::mem::size_of::<NamespacePath>();
        let mut path_allocation = paths.allocation;
        let mut paths = paths.value.into_iter();
        for (index, path) in paths.by_ref().enumerate() {
            let bytes = path.retained_bytes();
            let allocation = crate::kernel::split_allocation(&mut path_allocation, bytes)?;
            let path = crate::kernel::Owned::new(path, allocation, bytes);
            let existing = kinds.value.get(index).copied().flatten().is_some();
            let mutation = if index < directory_count {
                if existing {
                    path.retire(accounting)?;
                    continue;
                }
                AuthoredMutation::CreateDirectory {
                    path: path.value,
                    metadata: FileMetadata::default(),
                }
            } else {
                let (_, content) = files
                    .get(index - directory_count)
                    .ok_or(FsError::InvalidDiff)?;
                if existing && RESET {
                    let (copy, allocation) =
                        path.value.clone_with_accounting(accounting, cancellation)?;
                    let bytes = copy.retained_bytes();
                    Self::push_mutation(
                        &mut operations,
                        crate::kernel::Owned::new(
                            AuthoredMutation::Resize {
                                path: copy,
                                logical_bytes: 0,
                            },
                            allocation,
                            bytes,
                        ),
                        accounting,
                    )?;
                }
                if existing {
                    AuthoredMutation::Write {
                        path: path.value,
                        offset: 0,
                        bytes: content.clone(),
                    }
                } else {
                    AuthoredMutation::CreateFile {
                        path: path.value,
                        bytes: content.clone(),
                        metadata: FileMetadata::default(),
                    }
                }
            };
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(mutation, path.allocation, path.bytes),
                accounting,
            )?;
        }
        drop(paths);
        crate::kernel::retire_allocation(path_allocation, backing as u64, accounting)?;
        kinds.retire(accounting)?;
        if operations.value.is_empty() {
            return operations.retire(accounting).map_err(Into::into);
        }
        self.checkout
            .apply_authored_bulk_transaction_with_accounting(operations, accounting, cancellation)
            .await?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn write_batch_serial(
        &mut self,
        directories: &[&str],
        files: &[(&str, acyclic_stream::OwnedBytes)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut candidate = Self {
            workspace: self.workspace.clone(),
            checkout: self.checkout.private_candidate(accounting, cancellation)?,
            idempotency_key: self.idempotency_key,
            requires_rebase: self.requires_rebase,
        };
        for directory in directories {
            candidate
                .create_dirs(directory, accounting, cancellation)
                .await?;
        }
        for (path, bytes) in files {
            candidate
                .write_with_accounting(path, bytes.clone(), accounting, cancellation)
                .await?;
        }
        self.checkout = candidate.checkout;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn batch_paths(
        &self,
        directories: &[&str],
        files: &[(&str, acyclic_stream::OwnedBytes)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::Owned<Vec<NamespacePath>>, usize), WorkspaceError> {
        let config = self.checkout.volume_config();
        let mut paths = crate::kernel::Owned::allocate(0, accounting)?;
        for directory in directories {
            let path = self.mutation_path(directory, accounting, cancellation)?;
            for depth in 1..=path.value.depth() {
                let (prefix, allocation) = path.value.prefix_with_accounting(
                    depth,
                    config.limits,
                    accounting,
                    cancellation,
                )?;
                let bytes = prefix.retained_bytes();
                paths.ensure_for_push(usize::MAX, accounting)?;
                Self::push_mutation(
                    &mut paths,
                    crate::kernel::Owned::new(prefix, allocation, bytes),
                    accounting,
                )?;
            }
            path.retire(accounting)?;
        }
        let directory_count = paths.value.len();
        for (file, _) in files {
            let path = self.mutation_path(file, accounting, cancellation)?;
            paths.ensure_for_push(usize::MAX, accounting)?;
            Self::push_mutation(&mut paths, path, accounting)?;
        }
        Ok((paths, directory_count))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn batch_kinds(
        &mut self,
        paths: &[NamespacePath],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Option<FileKind>>>, WorkspaceError> {
        let config = self.checkout.volume_config();
        let mut kinds = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        let maximum = usize::try_from(config.limits.maximum_paths_per_batch)
            .unwrap_or(usize::MAX)
            .max(1);
        for chunk in paths.chunks(maximum) {
            // Like create_dirs, existing paths require the exact name binding
            // and kind, not a directory's child-entry payload. Sibling
            // additions by another writer are safe to rebase over.
            let (lookup, allocation) = self
                .checkout
                .lookup_batch_no_follow_with_accounting::<false>(
                    chunk,
                    false,
                    accounting,
                    cancellation,
                )
                .await
                .map_err(|error| match error {
                    FsError::Path(crate::kernel::PathLookupError::NotDirectory) => {
                        WorkspaceError::NotDirectory
                    }
                    error => error.into(),
                })?;
            for entry in &lookup.entries {
                accounting.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<Option<FileKind>>() as u64,
                    ..Default::default()
                })?;
                kinds.value.push(entry.record.map(|record| record.kind));
            }
            let bytes = lookup.retained_allocation_bytes;
            drop(lookup);
            crate::kernel::retire_allocation(allocation, bytes, accounting)?;
        }
        Ok(kinds)
    }

    /// Creates missing directory prefixes without replacing existing objects.
    pub async fn create_dir_all<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.create_dirs(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn create_dirs(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        if PortablePath::validate(path, self.checkout.volume_config().limits)
            .map_err(|error| WorkspaceError::from(crate::kernel::NamespacePathError::Path(error)))?
            == 0
        {
            return Ok(());
        }
        for end in path
            .match_indices('/')
            .skip(1)
            .map(|(index, _)| index)
            .chain(std::iter::once(path.len()))
        {
            let prefix = path.get(..end).ok_or(crate::FsError::InvalidDiff)?;
            let path = self.mutation_path(prefix, accounting, cancellation)?;
            // Existing prefixes require the exact name binding and kind, not
            // the directory's child-entry payload. Sibling additions are safe.
            match self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, false)
                .await?
                .0
                .record
            {
                Some(record) if record.kind == FileKind::Directory => path.retire(accounting)?,
                Some(_) => {
                    path.retire(accounting)?;
                    return Err(WorkspaceError::NotDirectory);
                }
                None => {
                    let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
                    Self::push_mutation(
                        &mut operations,
                        crate::kernel::Owned::new(
                            AuthoredMutation::CreateDirectory {
                                path: path.value,
                                metadata: FileMetadata::default(),
                            },
                            path.allocation,
                            path.bytes,
                        ),
                        accounting,
                    )?;
                    self.apply_authored(operations, accounting, cancellation)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Streams immutable content under the supplied work limits.
    pub async fn stage_content<R: crate::kernel::AsyncBlobSource, W: crate::facade::FsWork>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::StagedContent, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .checkout
            .stage_content_with_accounting(
                source,
                maximum_source_bytes,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::from);
        work.finish(result, &accounting)
    }

    /// Removes one authenticated namespace binding.
    pub async fn remove<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .remove_record(path, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(super) async fn remove_record(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path(path, accounting, cancellation)?;
        let record = self
            .checkout
            .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
            .await?
            .0
            .record
            .ok_or(WorkspaceError::NotFound)?;
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_mutation(
            &mut operations,
            crate::kernel::Owned::new(
                AuthoredMutation::Remove {
                    path: path.value,
                    expected_file_id: Some(record.file_id),
                },
                path.allocation,
                path.bytes,
            ),
            accounting,
        )?;
        self.apply_authored(operations, accounting, cancellation)
            .await
    }

    /// Removes one binding only while its file identity still matches.
    pub async fn remove_if<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        expected_file_id: FileId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(expected_file_id),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    pub(super) async fn mutate_pair<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        build: impl FnOnce(NamespacePath, NamespacePath) -> AuthoredMutation,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut source = self.mutation_path(source, &mut accounting, cancellation)?;
            let mut destination = self.mutation_path(destination, &mut accounting, cancellation)?;
            source.allocation.absorb(&mut destination.allocation)?;
            let bytes = source
                .bytes
                .checked_add(destination.bytes)
                .ok_or(crate::WorkError::Overflow)?;
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(
                    build(source.value, destination.value),
                    source.allocation,
                    bytes,
                ),
                &mut accounting,
            )?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Atomically renames one binding with an exact replacement policy.
    pub async fn rename<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        replace: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(
            source,
            destination,
            |source, destination| AuthoredMutation::Rename {
                source,
                destination,
                replace,
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates one hard link without copying file content.
    pub async fn hard_link<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(
            source,
            destination,
            |source, destination| AuthoredMutation::HardLink {
                source,
                destination,
            },
            work,
            cancellation,
        )
        .await
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn classify_batch_paths(
    paths: &[NamespacePath],
    directory_count: usize,
    kinds: &mut [Option<FileKind>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    if paths.len() != kinds.len() || directory_count > paths.len() {
        return Err(FsError::InvalidDiff.into());
    }
    // Preserve directory-error precedence before processing file duplicates.
    for (index, kind) in kinds.iter().enumerate() {
        accounting.charge(crate::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        if kind.is_some_and(|kind| {
            kind != if index < directory_count {
                FileKind::Directory
            } else {
                FileKind::Regular
            }
        }) {
            return Err(if index < directory_count {
                WorkspaceError::NotDirectory
            } else {
                WorkspaceError::NotRegularFile
            });
        }
    }

    let mut ordered = crate::kernel::Owned::allocate(paths.len(), accounting)?;
    let classified = (|| -> Result<(), WorkspaceError> {
        for (ordinal, key) in paths.iter().enumerate() {
            accounting.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<crate::kernel::IndexedKey<'_, NamespacePath>>()
                    as u64,
                ..Default::default()
            })?;
            ordered
                .value
                .push(crate::kernel::IndexedKey { ordinal, key });
        }
        crate::kernel::sort_indexed(&mut ordered.value, accounting)?;
        for adjacent in ordered.value.windows(2) {
            let [previous, current] = adjacent else {
                return Err(FsError::InvalidDiff.into());
            };
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if previous.key != current.key {
                continue;
            }
            let kind = kinds.get_mut(current.ordinal).ok_or(FsError::InvalidDiff)?;
            *kind = Some(if current.ordinal < directory_count {
                FileKind::Directory
            } else if previous.ordinal < directory_count {
                return Err(WorkspaceError::NotRegularFile);
            } else {
                FileKind::Regular
            });
        }
        Ok(())
    })();
    let retired = ordered.retire(accounting);
    classified?;
    retired?;
    Ok(())
}

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
#[path = "transaction_batch_properties.rs"]
mod batch_properties;

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
#[path = "transaction_batch_differential.rs"]
mod batch_differential;
#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
#[path = "transaction_batch_original.rs"]
mod batch_original;

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
#[path = "transaction_batch_index_properties.rs"]
mod batch_index_properties;
