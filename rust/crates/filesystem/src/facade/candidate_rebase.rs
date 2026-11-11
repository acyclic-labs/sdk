use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> candidates::NativeCandidateRebase<A, O> {
    pub(crate) async fn rebase(
        self,
        mut base: Checkout<A, O>,
        mut candidate: Checkout<A, O>,
        rename: Option<[&NamespacePath; 2]>,
        maximum_changes: impl Into<Option<u32>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(Checkout<A, O>, Checkout<A, O>)>, FsError> {
        let target = self.checkout;
        let maximum_changes = maximum_changes.into();
        accounting.check()?;
        if let Some(paths) = rename
            && !target
                .rename_bindings_unchanged(&mut base, paths, accounting, cancellation)
                .await?
        {
            return Ok(None);
        }
        let Some(operation) = crate::kernel::resolve_three(
            &base.authored_operation_id,
            &target.authored_operation_id,
            &candidate.authored_operation_id,
        ) else {
            return Ok(None);
        };
        let table = crate::kernel::merge_file_tables_with_accounting(
            &target.volume.fs.inner.objects,
            crate::kernel::MergeFileTableRequest {
                base: &base.root,
                ours: &target.root,
                theirs: &candidate.root,
                maximum_changes,
                maximum_conflicts: Some(1),
                content: crate::kernel::ContentMerge::Extents,
            },
            DecodeLimits::for_volume(target.volume.config),
            accounting,
            cancellation,
        )
        .await?;
        let Some(table) = table else { return Ok(None) };
        base.root.set_file_table(target.root.file_table)?;
        base.authored_operation_id = target.authored_operation_id;
        if !target.candidate_base_unchanged(&base) {
            return Ok(None);
        }
        // Reuse both paid root buffers and the original dependency proof.
        candidate.root.set_file_table(table)?;
        match candidate
            .admit_rebased_bindings(
                target.root.file_table,
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
