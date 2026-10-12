use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
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
        let metadata = self
            .stage_authored_metadata_with_accounting(
                metadata,
                staged_metadata,
                accounting,
                cancellation,
            )
            .await?;
        let file_id = self.authored_file_id(kind, &path, accounting)?;
        push_authored_operation(
            operations,
            allocation,
            bytes,
            accounting,
            Mutation::Create {
                path,
                record: FileRecord {
                    file_id,
                    kind,
                    link_count: 1,
                    metadata,
                    payload,
                },
            },
        )?;
        Ok(file_id)
    }

    pub(crate) async fn compile_copied_record(
        &self,
        input: crate::kernel::Owned<NamespacePath>,
        mut record: FileRecord,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<(Mutation, FileId)>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            match &input.allocation {
                KernelOwnedAllocation::NativeOriginal(allocation)
                    if allocation.work().shares_ledger(original)
                        && allocation.bytes() == input.value.retained_bytes() => {}
                _ => {
                    return Err(FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ));
                }
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
                bytes_copied: size_of::<Mutation>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if !path.is_root() && record.kind == FileKind::Directory {
                let entries = self
                    .stage_empty_tree_with_accounting(accounting, cancellation)
                    .await?;
                record.payload = FilePayload::Directory { entries };
            }
            if path.is_root() {
                let file_id = self.root.root_file_id;
                Ok((
                    Mutation::File {
                        file_id,
                        mutation: FileMutation::SetMetadata {
                            metadata: record.metadata,
                        },
                    },
                    file_id,
                ))
            } else {
                let file_id = self.authored_file_id(record.kind, &path, accounting)?;
                record.file_id = file_id;
                record.link_count = 1;
                Ok((Mutation::Create { path, record }, file_id))
            }
        }
        .await;
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
        let metadata = self
            .stage_authored_metadata_with_accounting(
                metadata,
                staged_metadata,
                accounting,
                cancellation,
            )
            .await?;
        if content.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
            accounting.charge(WorkCounters {
                bytes_copied: content.len() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            let payload = FilePayload::InlineRegular(InlineFileData::new(&content)?);
            drop(content);
            push_authored_operation(
                operations,
                allocation,
                bytes,
                accounting,
                Mutation::Create {
                    path,
                    record: FileRecord {
                        file_id,
                        kind: FileKind::Regular,
                        link_count: 1,
                        metadata,
                        payload,
                    },
                },
            )?;
        } else {
            let content = self
                .stage_authored_blob_with_accounting(content, accounting, cancellation)
                .await?;
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
            file_id,
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(&[])?),
        };
        if content.logical_bytes == 0 {
            return push_authored_operation(
                operations,
                allocation,
                bytes,
                accounting,
                Mutation::Create { path, record },
            );
        }
        let crate::kernel::Owned {
            value: creation_path,
            allocation: mut creation_allocation,
            bytes: mut creation_bytes,
        } = clone_authored_path(&path, accounting)?;
        let result = push_authored_operation(
            operations,
            &mut creation_allocation,
            &mut creation_bytes,
            accounting,
            Mutation::Create {
                path: creation_path,
                record,
            },
        );
        // The pushed operation owns the cloned path, or the failed push dropped it.
        crate::kernel::retire_allocation(creation_allocation, creation_bytes, accounting)?;
        result?;
        push_authored_operation(
            operations,
            allocation,
            bytes,
            accounting,
            Mutation::Write {
                path,
                offset: 0,
                length: content.logical_bytes,
                content: content.root,
                content_offset: 0,
            },
        )
    }

    pub(super) fn authored_file_id(
        &self,
        kind: FileKind,
        path: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<FileId, FsError> {
        let Some(operation) = self.authored_operation_id else {
            return Ok(FileId::new());
        };
        let domain = b"acyclic-fs-authored-file-id-v1\0";
        let mut hashed = (domain.len() + operation.into_bytes().len() + 1) as u64;
        for component in path.components() {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            hashed = hashed
                .checked_add(1 + size_of::<u64>() as u64)
                .and_then(|value| value.checked_add(component.as_bytes().len() as u64))
                .ok_or(WorkError::Overflow)?;
        }
        accounting.charge(WorkCounters {
            bytes_hashed: hashed,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        hasher.update(&operation.into_bytes());
        hasher.update(&[kind.tag()]);
        for component in path.components() {
            hasher.update(&[component.encoding().tag()]);
            hasher.update(&usize_to_u64(component.as_bytes().len()).to_le_bytes());
            hasher.update(component.as_bytes());
        }
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Ok(FileId::from_bytes(bytes))
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
            {
                return Err(OperationFailure::before_work(FsError::FileRead(
                    FileRangeReadError::InvalidRange,
                )));
            }
            AuthoredMutation::CreateEmptySpecial { kind, .. } => {
                if !matches!(
                    kind,
                    FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary
                ) {
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
                    return Err(OperationFailure::before_work(FsError::FileRead(
                        FileRangeReadError::InvalidRange,
                    )));
                }
            }
            AuthoredMutation::CreateFile { .. }
            | AuthoredMutation::CreateFileFromContent { .. }
            | AuthoredMutation::CreateDirectory { .. }
            | AuthoredMutation::CreateSymbolicLink { .. } => {}
            _ => return Err(OperationFailure::before_work(FsError::InvalidSpecialKind)),
        }
        self.apply_authored_transaction(vec![authored], budget, cancellation)
            .await
            .map(|receipt| OperationReceipt {
                value: receipt
                    .value
                    .created_file_ids
                    .into_iter()
                    .next()
                    .flatten()
                    .expect("one creation emits one file identity"),
                work: receipt.work,
            })
    }
}
