use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_conflict_record(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        file_id: FileId,
    ) -> Result<Option<FileRecord>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        self.workspace_merge_record_with_accounting(
            workspace,
            generation,
            &MergeConflict::File(file_id),
            &mut accounting,
            &cancellation,
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
    ) -> Result<Option<acyclic_stream::OwnedBytes>, WorkspaceError> {
        let logical_bytes = match record.payload {
            FilePayload::InlineRegular(data) => {
                return Ok(Some(acyclic_stream::OwnedBytes::from(
                    data.as_bytes().to_vec(),
                )));
            }
            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => return Ok(None),
        };
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        crate::kernel::read_file_range_with_accounting(
            &self.inner.objects,
            FileRangeRequest {
                record,
                range: ByteRange {
                    offset: 0,
                    length: logical_bytes,
                },
                maximum_spans: None,
                limits: DecodeLimits::for_volume(workspace.config),
                budget: WorkBudget::UNBOUNDED,
            },
            &mut accounting,
            &cancellation,
        )
        .await
        .map(|read| Some(read.bytes))
        .map_err(WorkspaceError::engine)
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
        if record.kind != FileKind::Regular {
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
                maximum_blob_bytes: Some(maximum_blob_bytes),
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
