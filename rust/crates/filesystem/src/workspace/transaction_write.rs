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
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let existing = self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
                .await?
                .0
                .record;
            let mut operations = crate::kernel::Owned::allocate(
                if existing.is_some() { 2 } else { 1 },
                &mut accounting,
            )?;
            if let Some(record) = existing {
                if record.kind != FileKind::Regular {
                    return Err(WorkspaceError::NotRegularFile);
                }
                Self::reset_file(
                    &path.value,
                    Some(record),
                    &mut operations,
                    &mut accounting,
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
                &mut accounting,
            )?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
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
            let path = self.mutation_path(&path[..end], accounting, cancellation)?;
            match self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
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
