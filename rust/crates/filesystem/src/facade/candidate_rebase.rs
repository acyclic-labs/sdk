use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(crate) async fn rebase_native_candidate(
        &self,
        mut base: Self,
        mut candidate: Self,
        rename: Option<[&NamespacePath; 2]>,
        maximum_changes: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(Self, Self)>, FsError> {
        accounting.check()?;
        if self.candidate_base_unchanged(&base) {
            return Ok(Some((base, candidate)));
        }
        // Only working-tree mutations can be transplanted. Generation and
        // authority transitions retain their original adoption fence.
        if candidate.volume.id != base.volume.id
            || self.volume.id != base.volume.id
            || self.generation_root != base.generation_root
            || self.base_root != base.base_root
            || self.authority_head != base.authority_head
            || self.live_operation_id != base.live_operation_id
            || self.last_commit != base.last_commit
            || self.prepared_merge_parent != base.prepared_merge_parent
            || candidate.generation_root != base.generation_root
            || candidate.base_root != base.base_root
            || candidate.authority_head != base.authority_head
            || candidate.live_operation_id != base.live_operation_id
            || candidate.last_commit != base.last_commit
            || candidate.prepared_merge_parent != base.prepared_merge_parent
            || candidate.mode != base.mode
            || self.mode != base.mode
        {
            return Ok(None);
        }
        if let Some(paths) = rename
            && !self
                .rename_bindings_unchanged(&mut base, paths, accounting, cancellation)
                .await?
        {
            return Ok(None);
        }
        let Some(operation) = crate::kernel::resolve_three(
            &base.authored_operation_id,
            &self.authored_operation_id,
            &candidate.authored_operation_id,
        ) else {
            return Ok(None);
        };
        let table = crate::kernel::merge_file_tables_with_accounting(
            &self.volume.fs.inner.objects,
            crate::kernel::MergeFileTableRequest {
                base: &base.root,
                ours: &self.root,
                theirs: &candidate.root,
                maximum_changes,
                maximum_conflicts: 1,
            },
            DecodeLimits::for_volume(self.volume.config),
            accounting,
            cancellation,
        )
        .await?;
        let Some(table) = table else { return Ok(None) };
        base.root.set_file_table(self.root.file_table)?;
        base.authored_operation_id = self.authored_operation_id;
        if !self.candidate_base_unchanged(&base) {
            return Ok(None);
        }
        // Reuse both paid root buffers and the original dependency proof.
        candidate.root.set_file_table(table)?;
        match candidate
            .admit_rebased_bindings(
                self.root.file_table,
                maximum_changes,
                accounting,
                cancellation,
            )
            .await
        {
            Err(FsError::NameCollision) => return Ok(None),
            result => result?,
        }
        candidate.authored_operation_id = operation;
        cancellation.check()?;
        Ok(Some((base, candidate)))
    }
}
