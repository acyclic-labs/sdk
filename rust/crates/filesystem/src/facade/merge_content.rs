use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_conflict_record(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        file_id: FileId,
    ) -> Result<Option<FileRecord>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        self.workspace_conflict_record_with_cancellation(
            workspace,
            generation,
            file_id,
            &cancellation,
        )
        .await
    }

    pub(crate) async fn workspace_conflict_record_with_cancellation(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        file_id: FileId,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, WorkspaceError> {
        let mut accounting = WorkBudget::UNBOUNDED.accounting(cancellation);
        self.workspace_merge_record_with_accounting(
            workspace,
            generation,
            &MergeConflict::File(file_id),
            &mut accounting,
            cancellation,
        )
        .await
    }

    pub(crate) async fn workspace_merge_record_with_accounting(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        conflict: &MergeConflict,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, WorkspaceError> {
        // The output is scalar. Retire temporary reads on success, refusal and
        // withdrawal while preserving the caller's scratch and retained credit.
        let mut scope = accounting.allocation_scope()?;
        let accounting = &mut *scope;
        accounting.check()?;
        let root = self
            .read_join_root_with_accounting(
                generation.object_id(),
                workspace.config,
                accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let table = root.value.file_table;
        root.retire(accounting).map_err(WorkspaceError::engine)?;
        let limits = DecodeLimits::for_volume(workspace.config);
        let id = match conflict {
            MergeConflict::File(id) => *id,
            MergeConflict::Binding { directory_id, .. } => *directory_id,
        };
        let (record, _allocation) = lookup_file_record_async_with_accounting(
            &self.inner.objects,
            table,
            id,
            limits,
            accounting,
            cancellation,
            None,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let MergeConflict::Binding { name, .. } = conflict else {
            return Ok(record);
        };
        let Some(record) = record else {
            return Ok(None);
        };
        let Some(id) = record
            .binding_with_accounting(&self.inner.objects, name, limits, accounting, cancellation)
            .await
            .map_err(WorkspaceError::engine)?
        else {
            return Ok(None);
        };
        lookup_file_record_async_with_accounting(
            &self.inner.objects,
            table,
            id,
            limits,
            accounting,
            cancellation,
            None,
        )
        .await
        .map(|(record, _allocation)| record)
        .map_err(WorkspaceError::engine)
    }
    pub(crate) async fn workspace_conflict_bytes(
        &self,
        workspace: &Volume<A, O>,
        record: FileRecord,
        cancellation: &CancellationToken,
    ) -> Result<Option<acyclic_stream::OwnedBytes>, WorkspaceError> {
        let logical_bytes = match record.payload {
            FilePayload::InlineRegular(data) => {
                crate::foundation::usize_to_u64(data.as_bytes().len())
            }

            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => return Ok(None),
        };
        if logical_bytes > workspace.config.limits.maximum_read_bytes {
            return Ok(None);
        }
        let mut accounting = WorkBudget::UNBOUNDED.accounting(cancellation);
        self.workspace_merge_range_with_accounting(
            workspace,
            record,
            ByteRange {
                offset: 0,
                length: logical_bytes,
            },
            &mut accounting,
            cancellation,
        )
        .await
        .map(|read| Some(read.bytes))
    }

    pub(crate) async fn workspace_merge_range_with_accounting(
        &self,
        workspace: &Volume<A, O>,
        record: FileRecord,
        range: ByteRange,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::FileRangeRead, WorkspaceError> {
        let mut scope = accounting.allocation_scope()?;
        scope.check()?;
        if range.length > workspace.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(crate::kernel::FileRangeReadError::InvalidRange).into());
        }
        let before = readonly_standalone_live(&scope);
        let result = crate::kernel::read_file_range_with_accounting(
            &self.inner.objects,
            FileRangeRequest {
                record,
                range,
                maximum_spans: workspace.config.limits.maximum_directory_page_entries,
                limits: DecodeLimits::for_volume(workspace.config),
                budget: scope.remaining()?,
            },
            &mut scope,
            cancellation,
        )
        .await
        .map_err(WorkspaceError::from);
        let retained = result.as_ref().map_or(0, |read| read.logical_owned_bytes);
        readonly_cleanup_standalone(&mut scope, before, retained)?;
        if result.is_ok() {
            scope.keep();
        }
        result
    }
    pub(crate) async fn same_file_state(
        &self,
        workspace: &Volume<A, O>,
        left: FileRecord,
        right: FileRecord,
    ) -> Result<bool, WorkspaceError> {
        if left.kind != right.kind
            || left.payload != right.payload
            || left.link_count != 1
            || right.link_count != 1
        {
            return Ok(false);
        }
        if left.metadata == right.metadata {
            return Ok(true);
        }
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let left = read_record_metadata_to_file_root_with_accounting(
            workspace,
            left,
            &mut accounting,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let right = read_record_metadata_to_file_root_with_accounting(
            workspace,
            right,
            &mut accounting,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(left.without_timestamps() == right.without_timestamps())
    }

    pub(crate) async fn stage_merge_regular_record(
        &self,
        workspace: &Volume<A, O>,
        mut record: FileRecord,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
    ) -> Result<FileRecord, WorkspaceError> {
        let bytes = bytes.into();
        if record.kind != FileKind::Regular
            || u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                > workspace.config.limits.maximum_read_bytes
        {
            return Err(WorkspaceError::InvalidMergeResolution);
        }
        if bytes.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
            record.payload = FilePayload::InlineRegular(
                InlineFileData::new(&bytes).map_err(WorkspaceError::engine)?,
            );
            return Ok(record);
        }
        let maximum_blob_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX).max(1);
        let mut source = std::io::Cursor::new(bytes);
        let blob = build_blob(
            &self.inner.objects,
            &mut source,
            BlobBuildOptions {
                chunk_bytes: u32::try_from(
                    workspace
                        .config
                        .limits
                        .maximum_object_bytes
                        .min(1024 * 1024),
                )
                .unwrap_or(u32::MAX)
                .max(1),
                page_items: workspace.config.limits.maximum_directory_page_entries,
                page_bytes: u32::try_from(workspace.config.limits.maximum_object_bytes)
                    .unwrap_or(u32::MAX)
                    .max(1),
                maximum_blob_bytes,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let payload = apply_regular_mutation_async(
            &self.inner.objects,
            FilePayload::InlineRegular(InlineFileData::new(&[]).map_err(WorkspaceError::engine)?),
            RegularMutation::Write {
                offset: 0,
                length: blob.logical_bytes,
                content: blob.root,
                content_offset: 0,
            },
            workspace.config,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?;
        record.payload = payload.payload;
        Ok(record)
    }
}
