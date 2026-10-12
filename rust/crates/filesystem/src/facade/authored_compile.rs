use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


    /// Lowers every authored kind on the caller's existing allocation context.
    pub(super) async fn compile_authored_mutation_with_accounting(
        &self,
        authored: crate::kernel::Owned<AuthoredMutation>,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, FsError> {
        let crate::kernel::Owned { value, mut allocation, mut bytes } = authored;
        let path_bytes = value.logical_owned_bytes();
        let result = async {
            accounting.check()?;
            match value {
                AuthoredMutation::Reidentify { path, file_id } => {
                    let current = crate::kernel::lookup_path_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.root, &path,
                        self.volume.config, accounting, cancellation, None,
                    ).await?.record.ok_or(FsError::NotFound)?;
                    if current.kind == FileKind::Directory {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if current.file_id == file_id {
                        return Ok(None);
                    }
                    let (existing, existing_allocation) =
                        crate::kernel::lookup_file_record_async_with_accounting(
                            &self.volume.fs.inner.objects, self.root.file_table, file_id,
                            DecodeLimits::for_volume(self.volume.config), accounting,
                            cancellation, Some(&self.dependencies.data_custody),
                        ).await?;
                    drop(existing_allocation);
                    if existing.is_some_and(|existing| {
                        existing.kind != current.kind
                            || existing.metadata != current.metadata
                            || existing.payload != current.payload
                    }) {
                        return Err(FsError::FileIdentityCollision);
                    }
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Restore { path, record: FileRecord {
                            file_id, link_count: 1, ..current
                        } })?;
                    Ok(None)
                }
                AuthoredMutation::CreateFile { path, bytes: content, metadata } => {
                    let file_id = self.authored_file_id_with_accounting(
                        FileKind::Regular, &path, accounting,
                    )?;
                    self.compile_create_file_with_accounting(
                        path, content, metadata, file_id, operations,
                        &mut allocation, &mut bytes, staged_metadata, accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateFileFromContent { path, content, metadata, file_id } => {
                    let metadata = self.stage_authored_metadata_with_accounting(
                        metadata, staged_metadata, accounting, cancellation,
                    ).await?;
                    let file_id = match file_id {
                        Some(file_id) => file_id,
                        None => self.authored_file_id_with_accounting(
                            FileKind::Regular, &path, accounting,
                        )?,
                    };
                    self.compile_regular_content_with_accounting(
                        path, content, metadata, file_id, operations,
                        &mut allocation, &mut bytes, accounting,
                    )?;
                    Ok(Some(file_id))
                }
                AuthoredMutation::CreateDirectory { path, metadata } => {
                    let entries = self.stage_empty_tree_with_accounting(accounting, cancellation).await?;
                    self.compile_record_with_accounting(
                        operations, path, FileKind::Directory, FilePayload::Directory { entries },
                        metadata, &mut allocation, &mut bytes, staged_metadata, accounting,
                        cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateSymbolicLink { path, target, metadata } => {
                    let content = self.stage_authored_blob_with_accounting(
                        target, accounting, cancellation,
                    ).await?;
                    self.compile_record_with_accounting(
                        operations, path, FileKind::SymbolicLink, FilePayload::SymbolicLink {
                            target_bytes: content.logical_bytes, target: content.root,
                        }, metadata, &mut allocation, &mut bytes, staged_metadata,
                        accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateEmptySpecial { path, kind, metadata } => {
                    if !matches!(kind, FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary) {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if !kind.is_supported_by_profile(self.volume.config.profile) {
                        return Err(FsError::UnsupportedFileKind);
                    }
                    self.compile_record_with_accounting(
                        operations, path, kind, FilePayload::Empty, metadata,
                        &mut allocation, &mut bytes, staged_metadata, accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateDevice { path, kind, major, minor, metadata } => {
                    if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if !kind.is_supported_by_profile(self.volume.config.profile) {
                        return Err(FsError::UnsupportedFileKind);
                    }
                    self.compile_record_with_accounting(
                        operations, path, kind, FilePayload::Device { major, minor }, metadata,
                        &mut allocation, &mut bytes, staged_metadata, accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateReparsePoint { path, payload, metadata } => {
                    if !FileKind::ReparsePoint.is_supported_by_profile(self.volume.config.profile) {
                        return Err(FsError::UnsupportedFileKind);
                    }
                    let content = self.stage_authored_blob_with_accounting(
                        payload, accounting, cancellation,
                    ).await?;
                    self.compile_record_with_accounting(
                        operations, path, FileKind::ReparsePoint, FilePayload::ReparsePoint {
                            payload_bytes: content.logical_bytes, payload: content.root,
                        }, metadata, &mut allocation, &mut bytes, staged_metadata,
                        accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::Remove { path, expected_file_id } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Remove { path, expected_file_id: expected_file_id
                            .map_or(MetadataField::Unavailable, MetadataField::Value) })?;
                    Ok(None)
                }
                AuthoredMutation::Rename { source, destination, replace } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Rename { source, destination, replace })?;
                    Ok(None)
                }
                AuthoredMutation::HardLink { source, destination } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Link { source, destination })?;
                    Ok(None)
                }
                AuthoredMutation::Write { path, offset, bytes: content } => {
                    let mutation = if content.is_empty() {
                        drop(content);
                        Mutation::ValidateRegular { path }
                    } else {
                        let content = self.stage_authored_blob_with_accounting(
                            content, accounting, cancellation,
                        ).await?;
                        Mutation::Write { path, offset, length: content.logical_bytes,
                            content: content.root, content_offset: 0 }
                    };
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting, mutation)?;
                    Ok(None)
                }
                AuthoredMutation::WriteFromContent { path, offset, content } => {
                    let mutation = if content.logical_bytes == 0 {
                        Mutation::ValidateRegular { path }
                    } else {
                        Mutation::Write { path, offset, length: content.logical_bytes,
                            content: content.root, content_offset: 0 }
                    };
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting, mutation)?;
                    Ok(None)
                }
                AuthoredMutation::SetMetadata { path, metadata } => {
                    let metadata = self.stage_authored_metadata_with_accounting(
                        metadata, staged_metadata, accounting, cancellation,
                    ).await?;
                    let mutation = if path.is_root() {
                        // The root has a record but no namespace binding.
                        drop(path);
                        let remaining = bytes.checked_sub(path_bytes).ok_or(WorkError::Overflow)?;
                        let retired = crate::kernel::split_allocation(&mut allocation, path_bytes)?;
                        bytes = remaining;
                        crate::kernel::retire_allocation(retired, path_bytes, accounting)?;
                        Mutation::File { file_id: self.root.root_file_id,
                            mutation: FileMutation::SetMetadata { metadata } }
                    } else {
                        Mutation::SetMetadata { path, metadata }
                    };
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting, mutation)?;
                    Ok(None)
                }
                AuthoredMutation::Resize { path, logical_bytes } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Resize { path, logical_bytes })?;
                    Ok(None)
                }
                AuthoredMutation::ZeroRange { path, range, allocated, extend } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::ZeroRange { path, offset: range.offset, length: range.length,
                            allocated, extend })?;
                    Ok(None)
                }
                AuthoredMutation::Preallocate { path, range, keep_size } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Preallocate { path, offset: range.offset, length: range.length,
                            keep_size })?;
                    Ok(None)
                }
                AuthoredMutation::CloneRange(request) => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::CloneRange { source: request.source, source_offset: request.source_offset,
                            destination: request.destination, destination_offset: request.destination_offset,
                            length: request.length })?;
                    Ok(None)
                }
            }
        }.await;
        // The exhaustive lowering future has dropped every non-transferred input.
        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
        result
    }


    pub(super) fn validate_profile_kind(
        &self,
        kind: FileKind,
        work: WorkCounters,
    ) -> Result<(), OperationFailure<FsError>> {
        if kind.is_supported_by_profile(self.volume.config.profile) {
            Ok(())
        } else {
            Err(OperationFailure::new(FsError::UnsupportedFileKind, work))
        }
    }


    pub(super) async fn compile_record_with_accounting(
        &self,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        path: NamespacePath,
        kind: FileKind,
        payload: FilePayload,
        metadata: FileMetadata,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        bytes: &mut u64,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        let metadata = self.stage_authored_metadata_with_accounting(
            metadata, staged_metadata, accounting, cancellation,
        ).await?;
        let file_id = self.authored_file_id_with_accounting(kind, &path, accounting)?;
        push_authored_operation(operations, allocation, bytes, accounting,
            Mutation::Create { path, record: FileRecord {
                file_id, kind, link_count: 1, metadata, payload,
            } })?;
        Ok(file_id)
    }


    pub(crate) async fn compile_copied_record(
        &self, input: crate::kernel::Owned<NamespacePath>, mut record: FileRecord,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<(Mutation, FileId)>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            match &input.allocation {
                KernelOwnedAllocation::NativeOriginal(allocation)
                    if allocation.work().shares_ledger(original)
                        && allocation.bytes() == input.value.retained_bytes() => {}
                _ => return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout)),
            }
        }
        let bytes = input.bytes;
        let mut input = crate::kernel::Owned::new(Some(input.value), input.allocation, bytes);
        let result = async {
            accounting.check()?;
            let path = input.value.take().expect("copied path is consumed once");
            if path.is_root() && record.kind != FileKind::Directory {
                return Err(FsError::UnsupportedFileKind);
            }
            accounting.charge(WorkCounters {
                bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
            })?;
            if !path.is_root() && record.kind == FileKind::Directory {
                let entries = self.stage_empty_tree_with_accounting(accounting, cancellation).await?;
                record.payload = FilePayload::Directory { entries };
            }
            if path.is_root() {
                let file_id = self.root.root_file_id;
                Ok((Mutation::File {
                    file_id, mutation: FileMutation::SetMetadata { metadata: record.metadata },
                }, file_id))
            } else {
                let file_id = self.authored_file_id_with_accounting(record.kind, &path, accounting)?;
                record.file_id = file_id;
                record.link_count = 1;
                Ok((Mutation::Create { path, record }, file_id))
            }
        }.await;
        result.map(|value| crate::kernel::Owned::new(value, input.allocation, bytes))
    }


    /// Copies inline bytes or stages the actual source before lowering content.
    pub(super) async fn compile_create_file_with_accounting(
        &self,
        path: NamespacePath,
        content: acyclic_stream::OwnedBytes,
        metadata: FileMetadata,
        file_id: FileId,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        bytes: &mut u64,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        if content.len() as u64 > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let metadata = self.stage_authored_metadata_with_accounting(
            metadata, staged_metadata, accounting, cancellation,
        ).await?;
        if content.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
            accounting.charge(WorkCounters {
                bytes_copied: content.len() as u64, ..WorkCounters::UNCHARGED
            })?;
            let payload = FilePayload::InlineRegular(InlineFileData::new(&content)?);
            drop(content);
            push_authored_operation(operations, allocation, bytes, accounting,
                Mutation::Create { path, record: FileRecord {
                    file_id, kind: FileKind::Regular, link_count: 1, metadata, payload,
                } })?;
        } else {
            let content = self.stage_authored_blob_with_accounting(
                content, accounting, cancellation,
            ).await?;
            self.compile_regular_content_with_accounting(
                path, content, metadata, file_id, operations, allocation, bytes, accounting,
            )?;
        }
        Ok(file_id)
    }


    pub(super) fn compile_regular_content_with_accounting(
        &self,
        path: NamespacePath,
        content: StagedContent,
        metadata: ObjectId,
        file_id: FileId,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        bytes: &mut u64,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let record = FileRecord {
            file_id, kind: FileKind::Regular, link_count: 1, metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(&[])?),
        };
        if content.logical_bytes == 0 {
            return push_authored_operation(operations, allocation, bytes, accounting,
                Mutation::Create { path, record });
        }
        let crate::kernel::Owned {
            value: creation_path, allocation: mut creation_allocation, bytes: mut creation_bytes,
        } = clone_authored_path(&path, accounting)?;
        let result = push_authored_operation(
            operations, &mut creation_allocation, &mut creation_bytes, accounting,
            Mutation::Create { path: creation_path, record },
        );
        // The pushed operation owns the cloned path, or the failed push dropped it.
        crate::kernel::retire_allocation(creation_allocation, creation_bytes, accounting)?;
        result?;
        push_authored_operation(operations, allocation, bytes, accounting,
            Mutation::Write { path, offset: 0, length: content.logical_bytes,
                content: content.root, content_offset: 0 })
    }


    pub(super) fn authored_file_id_with_accounting(
        &self,
        kind: FileKind,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<FileId, FsError> {
        if let Some(operation) = self.authored_operation_id {
            let mut bytes = (b"acyclic-fs-authored-file-id-v1\0".len()
                + operation.into_bytes().len() + 1) as u64;
            for component in path.components() {
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                bytes = bytes.checked_add(1 + size_of::<u64>() as u64)
                    .and_then(|value| value.checked_add(component.as_bytes().len() as u64))
                    .ok_or(WorkError::Overflow)?;
            }
            accounting.charge(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED })?;
        }
        Ok(self.authored_file_id(kind, path))
    }


    pub(super) fn authored_file_id(&self, kind: FileKind, path: &NamespacePath) -> FileId {
        let Some(operation_id) = self.authored_operation_id else {
            return FileId::new();
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-authored-file-id-v1\0");
        hasher.update(&operation_id.into_bytes());
        hasher.update(&[kind.tag()]);
        for component in path.components() {
            hasher.update(&[component.encoding().tag()]);
            hasher.update(
                &u64::try_from(component.as_bytes().len())
                    .unwrap_or(u64::MAX)
                    .to_le_bytes(),
            );
            hasher.update(component.as_bytes());
        }
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        FileId::from_bytes(bytes)
    }


    /// Creates one file through the atomic authored compiler.
    pub async fn create(
        &mut self,
        authored: AuthoredMutation,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        match &authored {
            AuthoredMutation::CreateFile { bytes, .. }
            | AuthoredMutation::CreateSymbolicLink { target: bytes, .. }
                if bytes.len() as u64 > self.volume.config.limits.maximum_read_bytes =>
                return Err(OperationFailure::before_work(FsError::FileRead(FileRangeReadError::InvalidRange))),
            AuthoredMutation::CreateEmptySpecial { kind, .. } => {
                if !matches!(kind, FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary) {
                    return Err(OperationFailure::before_work(FsError::InvalidSpecialKind));
                }
                self.validate_profile_kind(*kind, WorkCounters::UNCHARGED)?;
            }
            AuthoredMutation::CreateDevice { kind, .. } => {
                if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
                    return Err(OperationFailure::before_work(FsError::InvalidSpecialKind));
                }
                self.validate_profile_kind(*kind, WorkCounters::UNCHARGED)?;
            }
            AuthoredMutation::CreateReparsePoint { payload, .. } => {
                self.validate_profile_kind(FileKind::ReparsePoint, WorkCounters::UNCHARGED)?;
                if payload.len() as u64 > self.volume.config.limits.maximum_read_bytes {
                    return Err(OperationFailure::before_work(FsError::FileRead(FileRangeReadError::InvalidRange)));
                }
            }
            AuthoredMutation::CreateFile { .. }
            | AuthoredMutation::CreateFileFromContent { .. }
            | AuthoredMutation::CreateDirectory { .. }
            | AuthoredMutation::CreateSymbolicLink { .. } => {}
            _ => return Err(OperationFailure::before_work(FsError::InvalidSpecialKind)),
        }
        self.apply_authored_transaction(vec![authored], budget, cancellation).await.map(|receipt| {
            FsReceipt {
                value: receipt.value.created_file_ids.into_iter().next().flatten()
                    .expect("one creation emits one file identity"),
                work: receipt.work,
            }
        })
    }
}

pub(super) fn push_authored_operation(
    operations: &mut crate::kernel::Owned<Vec<Mutation>>,
    allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
    bytes: &mut u64,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    mutation: Mutation,
) -> Result<(), FsError> {
    if operations.value.len() == operations.value.capacity() {
        return Err(FsError::PendingMutationAllocationFailed);
    }
    accounting.charge(WorkCounters {
        bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
    })?;
    let path_bytes = mutation_path_owned_bytes(&mutation);
    let remaining = bytes.checked_sub(path_bytes).ok_or(WorkError::Overflow)?;
    let retained = operations.bytes.checked_add(path_bytes).ok_or(WorkError::Overflow)?;
    let mut mutation = crate::kernel::Owned::new(
        mutation, crate::kernel::split_allocation(allocation, path_bytes)?, path_bytes,
    );
    *bytes = remaining;
    let transferred = operations.allocation.absorb(&mut mutation.allocation);
    if let Err(error) = transferred {
        mutation.retire(accounting)?;
        return Err(error);
    }
    operations.value.push(mutation.value);
    operations.bytes = retained;
    Ok(())
}

pub(super) fn clone_authored_path(
    path: &NamespacePath,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<NamespacePath>, FsError> {
    let cancellation = match &*accounting {
        crate::kernel::allocation::KernelAccounting::Standalone { cancellation, .. }
        | crate::kernel::allocation::KernelAccounting::BorrowedStandalone { cancellation, .. } => *cancellation,
        #[cfg(not(target_arch = "wasm32"))]
        crate::kernel::allocation::KernelAccounting::NativeOriginal { cancellation, .. } => *cancellation,
    };
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    let bytes = path.retained_bytes();
    Ok(crate::kernel::Owned::new(path, allocation, bytes))
}

pub(super) fn clone_borrowed_authored_descriptor(
    authored: &AuthoredMutation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<AuthoredMutation>, FsError> {
    use AuthoredMutation::*;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let mut bytes = 0_u64;
    let result = (|| {
        let mut copy_path = |source: &NamespacePath| -> Result<NamespacePath, FsError> {
            let mut path = clone_authored_path(source, accounting)?;
            let Some(total) = bytes.checked_add(path.bytes) else {
                path.retire(accounting)?;
                return Err(WorkError::Overflow.into());
            };
            if let Err(error) = allocation.absorb(&mut path.allocation) {
                path.retire(accounting)?;
                return Err(error);
            }
            bytes = total;
            Ok(path.value)
        };
        Ok::<_, FsError>(match authored {
            Reidentify { path, file_id } => Reidentify { path: copy_path(path)?, file_id: *file_id },
            CreateFile { path, bytes, metadata } => CreateFile { path: copy_path(path)?, bytes: bytes.clone(), metadata: *metadata },
            CreateFileFromContent { path, content, metadata, file_id } =>
             CreateFileFromContent { path: copy_path(path)?, content: *content,
                    metadata: *metadata, file_id: *file_id },
            CreateDirectory { path, metadata } => CreateDirectory { path: copy_path(path)?, metadata: *metadata },
            CreateSymbolicLink { path, target, metadata } =>
             CreateSymbolicLink { path: copy_path(path)?, target: target.clone(), metadata: *metadata },
            CreateEmptySpecial { path, kind, metadata } =>
             CreateEmptySpecial { path: copy_path(path)?, kind: *kind, metadata: *metadata },
            CreateDevice { path, kind, major, minor, metadata } =>
             CreateDevice { path: copy_path(path)?, kind: *kind, major: *major, minor: *minor,
                    metadata: *metadata },
            CreateReparsePoint { path, payload, metadata } =>
             CreateReparsePoint { path: copy_path(path)?, payload: payload.clone(), metadata: *metadata },
            Remove { path, expected_file_id } => Remove { path: copy_path(path)?, expected_file_id: *expected_file_id },
            Rename { source, destination, replace } =>
             Rename { source: copy_path(source)?, destination: copy_path(destination)?, replace: *replace },
            HardLink { source, destination } =>
             HardLink { source: copy_path(source)?, destination: copy_path(destination)? },
            Write { path, offset, bytes } => Write { path: copy_path(path)?, offset: *offset, bytes: bytes.clone() },
            WriteFromContent { path, offset, content } =>
             WriteFromContent { path: copy_path(path)?, offset: *offset, content: *content },
            SetMetadata { path, metadata } => SetMetadata { path: copy_path(path)?, metadata: *metadata },
            Resize { path, logical_bytes } => Resize { path: copy_path(path)?, logical_bytes: *logical_bytes },
            ZeroRange { path, range, allocated, extend } =>
             ZeroRange { path: copy_path(path)?, range: *range, allocated: *allocated, extend: *extend },
            Preallocate { path, range, keep_size } =>
             Preallocate { path: copy_path(path)?, range: *range, keep_size: *keep_size },
            CloneRange(request) =>
            CloneRange(FileCloneRequest {
                    source: copy_path(&request.source)?, source_offset: request.source_offset,
                    destination: copy_path(&request.destination)?, destination_offset: request.destination_offset,
                    length: request.length,
                }),
    })
    })();
    match result {
        Ok(value) => Ok(crate::kernel::Owned::new(value, allocation, bytes)),
        Err(error) => {
            crate::kernel::retire_allocation(allocation, bytes, accounting)?;
            Err(error)
        }
    }
}

