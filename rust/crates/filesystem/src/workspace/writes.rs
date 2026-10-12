use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Creates or atomically replaces one complete UTF-8 file.
    pub async fn write_text(&mut self, path: &str, text: &str) -> Result<(), WorkspaceError> {
        self.write(path, Bytes::copy_from_slice(text.as_bytes()), WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Creates or replaces one complete file under supplied work limits.
    pub async fn write<W: crate::facade::FsWork>(&mut self, path: &str, bytes: impl Into<acyclic_stream::OwnedBytes>, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let bytes = bytes.into();
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let record = self.checkout.lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation).await?.0.record;
            let operations = match record {
                None => {
                    let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
                    Self::push_mutation(&mut operations, crate::kernel::Owned::new(AuthoredMutation::CreateFile { path: path.value, bytes, metadata: FileMetadata::default() }, path.allocation, path.bytes), &mut accounting)?;
                    operations
                }
                Some(record) if record.kind == FileKind::Regular => {
                    let (copy, allocation) = path.value.clone_with_accounting(&mut accounting, cancellation)?;
                    let copied_bytes = copy.retained_bytes();
                    let mut operations = crate::kernel::Owned::allocate(2, &mut accounting)?;
                    Self::push_mutation(&mut operations, crate::kernel::Owned::new(AuthoredMutation::Resize { path: copy, logical_bytes: 0 }, allocation, copied_bytes), &mut accounting)?;
                    Self::push_mutation(&mut operations, crate::kernel::Owned::new(AuthoredMutation::Write { path: path.value, offset: 0, bytes }, path.allocation, path.bytes), &mut accounting)?;
                    operations
                }
                Some(_) => { path.retire(&mut accounting)?; return Err(WorkspaceError::NotRegularFile); }
            };
            self.apply_authored(operations, &mut accounting, cancellation).await
        }.await;
        work.finish(result, &accounting)
    }

    /// Removes one existing binding under supplied work limits.
    pub async fn remove<W: crate::facade::FsWork>(&mut self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.remove_record(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(super) async fn remove_record(&mut self, path: &str, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(), WorkspaceError> {
        let path = self.mutation_path(path, accounting, cancellation)?;
        let record = self.checkout.lookup_no_follow_with_accounting(&path.value, accounting, cancellation).await?.0.record.ok_or(WorkspaceError::NotFound)?;
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_mutation(&mut operations, crate::kernel::Owned::new(AuthoredMutation::Remove { path: path.value, expected_file_id: Some(record.file_id) }, path.allocation, path.bytes), accounting)?;
        self.apply_authored(operations, accounting, cancellation).await
    }

    async fn mutate_pair<W: crate::facade::FsWork>(&mut self, source: &str, destination: &str, build: impl FnOnce(NamespacePath, NamespacePath) -> AuthoredMutation, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut source = self.mutation_path(source, &mut accounting, cancellation)?;
            let mut destination = self.mutation_path(destination, &mut accounting, cancellation)?;
            source.allocation.absorb(&mut destination.allocation)?;
            let bytes = source.bytes.checked_add(destination.bytes).ok_or(crate::WorkError::Overflow)?;
            let operation = crate::kernel::Owned::new(build(source.value, destination.value), source.allocation, bytes);
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(&mut operations, operation, &mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation).await
        }.await;
        work.finish(result, &accounting)
    }

    /// Creates one same-workspace hard link.
    pub async fn hard_link<W: crate::facade::FsWork>(&mut self, source: &str, destination: &str, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(source, destination, |source, destination| AuthoredMutation::HardLink { source, destination }, work, cancellation).await
    }

    /// Renames one binding with an exact replacement policy.
    pub async fn rename<W: crate::facade::FsWork>(&mut self, source: &str, destination: &str, replace: bool, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(source, destination, |source, destination| AuthoredMutation::Rename { source, destination, replace }, work, cancellation).await
    }
    /// Creates or replaces one complete file by concatenating already staged
    pub async fn write_staged(
        &mut self,
        path: &str,
        parts: &[crate::StagedContent],
    ) -> Result<(), WorkspaceError> {
        if parts.is_empty() {
            return Err(WorkspaceError::EmptyContentSet);
        }
        let path = customer_path(path, self.checkout.volume_config())?;
        let existing = self
            .checkout
            .lookup_no_follow(
                &path,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record;
        let mut operations = match existing {
            None => vec![AuthoredMutation::CreateFile {
                path: path.clone(),
                bytes: acyclic_stream::OwnedBytes::new(),
                metadata: FileMetadata::default(),
            }],
            Some(record) if record.kind == FileKind::Regular => vec![AuthoredMutation::Resize {
                path: path.clone(),
                logical_bytes: 0,
            }],
            Some(_) => return Err(WorkspaceError::NotRegularFile),
        };
        let mut offset = 0_u64;
        for part in parts {
            if part.logical_bytes() != 0 {
                operations.push(AuthoredMutation::WriteFromContent {
                    path: path.clone(),
                    offset,
                    content: *part,
                });
            }
            offset = offset
                .checked_add(part.logical_bytes())
                .ok_or(WorkspaceError::ContentLengthOverflow)?;
        }
        self.apply(operations).await
    }

    /// Removes one binding only when its authenticated identity still matches.
    pub async fn remove_if(
        &mut self,
        path: &str,
        expected_file_id: crate::FileId,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::Remove {
            path,
            expected_file_id: Some(expected_file_id),
        }])
        .await
    }

    /// Replaces one destination with a complete copy-on-write clone.
    pub async fn copy(&mut self, source: &str, destination: &str) -> Result<(), WorkspaceError> {
        let config = self.checkout.volume_config();
        let source = customer_path(source, config)?;
        let destination = customer_path(destination, config)?;
        let source_record = self
            .checkout
            .lookup_no_follow(
                &source,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(WorkspaceError::NotFound)?;
        let logical_bytes = regular_file_bytes(source_record)?;
        let destination_record = self
            .checkout
            .lookup_no_follow(
                &destination,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record;
        let mut operations = match destination_record {
            None => vec![AuthoredMutation::CreateFile {
                path: destination.clone(),
                bytes: acyclic_stream::OwnedBytes::new(),
                metadata: FileMetadata::default(),
            }],
            Some(record) if record.kind == FileKind::Regular => vec![AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 0,
            }],
            Some(_) => return Err(WorkspaceError::NotRegularFile),
        };
        if logical_bytes != 0 {
            operations.push(AuthoredMutation::CloneRange(crate::FileCloneRequest {
                source,
                source_offset: 0,
                destination,
                destination_offset: 0,
                length: logical_bytes,
            }));
        }
        self.apply(operations).await
    }

    /// Changes one regular file's logical length while preserving sparse layout.
    pub async fn resize(&mut self, path: &str, logical_bytes: u64) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::Resize {
            path,
            logical_bytes,
        }])
        .await
    }

    /// Punches a hole or installs allocated zeros over one exact range.
    pub async fn zero_range(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        allocated: bool,
        extend: bool,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::ZeroRange {
            path,
            range,
            allocated,
            extend,
        }])
        .await
    }

    /// Preallocates one sparse range without replacing existing content.
    pub async fn preallocate(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        keep_size: bool,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::Preallocate {
            path,
            range,
            keep_size,
        }])
        .await
    }

    /// Clones one immutable range without reading or copying content bytes.
    pub async fn clone_range(
        &mut self,
        source: &str,
        source_offset: u64,
        destination: &str,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), WorkspaceError> {
        let config = self.checkout.volume_config();
        self.apply(vec![AuthoredMutation::CloneRange(
            crate::FileCloneRequest {
                source: customer_path(source, config)?,
                source_offset,
                destination: customer_path(destination, config)?,
                destination_offset,
                length,
            },
        )])
        .await
    }

    pub(super) async fn apply(&mut self, operations: Vec<AuthoredMutation>) -> Result<(), WorkspaceError> {
        self.checkout.apply_authored_transaction(operations, WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .map(|_| ()).map_err(|failure| WorkspaceError::from(failure.error))
    }
}
