use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(super) async fn rename_bindings_unchanged(
        &self,
        base: &mut Self,
        paths: [&NamespacePath; 2],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        // Resolving both endpoints from the canonical root authenticates their
        // ancestry as well as their bindings. A table merge alone cannot do so:
        // two individually valid moves can otherwise combine into a cycle.
        for (index, requested) in paths.into_iter().enumerate() {
            let before = lookup_path_async_with_accounting(
                &self.volume.fs.inner.objects,
                &base.root,
                requested,
                self.volume.config,
                accounting,
                cancellation,
                None,
            )
            .await?;
            let canonical = base
                .canonical_rename_endpoint(requested, index == 0, before, accounting, cancellation)
                .await?;
            let result: Result<bool, FsError> = async {
                let path = canonical.as_ref().map_or(requested, |owned| &owned.0);
                let before = if canonical.is_some() {
                    lookup_path_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        &base.root,
                        path,
                        self.volume.config,
                        accounting,
                        cancellation,
                        None,
                    )
                    .await?
                } else {
                    before
                };
                let after = match lookup_path_async_with_accounting(
                    &self.volume.fs.inner.objects,
                    &self.root,
                    path,
                    self.volume.config,
                    accounting,
                    cancellation,
                    None,
                )
                .await
                {
                    Err(FsError::Path(PathLookupError::NotDirectory)) => return Ok(false),
                    result => result?,
                };
                let identity = |lookup: PathLookup| {
                    (
                        lookup.record.map(|record| (record.file_id, record.kind)),
                        lookup.parent.map(|record| (record.file_id, record.kind)),
                        lookup.resolved_components,
                    )
                };
                Ok(identity(before) == identity(after))
            }
            .await;
            if let Some(canonical) = canonical {
                let bytes = canonical.0.retained_bytes();
                super::names::retire_name_owned(canonical, bytes, accounting)?;
            }
            if !result? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn canonical_rename_endpoint(
        &mut self,
        path: &NamespacePath,
        source: bool,
        lookup: PathLookup,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(NamespacePath, KernelOwnedAllocation)>, FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Ok(None);
        }
        if source && lookup.record.is_none() {
            return self
                .canonical_existing_path(path, &[], accounting, cancellation)
                .await
                .map(Some);
        }
        if source || usize::from(lookup.resolved_components) >= path.depth().saturating_sub(1) {
            return Ok(None);
        }
        let (parent, name) = path.split_last().ok_or(FsError::InvalidDiff)?;
        let parent = self
            .canonicalize_path_components(parent, &[], accounting, cancellation)
            .await?
            .ok_or(FsError::NotFound)?;
        let child = parent.0.child_with_accounting(
            name,
            self.volume.config.limits,
            accounting,
            cancellation,
        );
        let bytes = parent.0.retained_bytes();
        super::names::retire_name_owned(parent, bytes, accounting)?;
        child.map(Some)
    }
}
