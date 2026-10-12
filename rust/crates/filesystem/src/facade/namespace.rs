use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {
    /// Finds bounded sorted aliases using the canonical cached reverse traversal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn paths_for_file_id_with_native_work(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<NamespacePath>, NativeAllocation), WorkspaceError> {
        cancellation.check()?;
        self.validate_native_work(work)
            .map_err(WorkspaceError::Filesystem)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (paths, allocation) = crate::workspace::reverse_paths_to_file_root_with_accounting(
            &self.volume,
            &self.root,
            self.generation_root,
            file_id,
            maximum_entries,
            &mut accounting,
            cancellation,
        )
        .await?;
        Ok((
            paths,
            native_pinned_allocation(allocation).map_err(WorkspaceError::Filesystem)?,
        ))
    }

    /// Projects only the first sorted alias; all reverse-query buffers keep
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn first_path_for_file_id_with_native_work(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<(String, NativeAllocation)>, WorkspaceError> {
        let (value, allocation) = self
            .paths_for_file_id_with_native_work(file_id, maximum_entries, work, cancellation)
            .await?;
        let paths = (value, allocation);
        let Some(path) = paths.0.first() else {
            return Ok(None);
        };
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (value, allocation) =
            path.unicode_text_with_accounting(&mut accounting, cancellation)?;
        Ok(Some((
            value,
            native_pinned_allocation(allocation).map_err(WorkspaceError::Filesystem)?,
        )))
    }

    /// Resolves an exact no-follow path on the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn lookup_record_with_native_work(
        &self,
        path: &NamespacePath,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let lookup = lookup_path_async_with_accounting(
            &self.volume.fs.inner.objects,
            &self.root,
            path,
            self.volume.config,
            &mut accounting,
            cancellation,
            None,
        )
        .await?;
        Ok(lookup.record)
    }

    pub(super) async fn resolve_file(
        &self,
        path: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRecord, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.validate_native_work(original)?;
        }
        lookup_path_async_with_accounting(
            &self.volume.fs.inner.objects,
            &self.root,
            path,
            self.volume.config,
            accounting,
            cancellation,
            None,
        )
        .await?
        .record
        .ok_or(FsError::NotFound)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Resolves an exact path without following links or scanning unrelated subtrees.
    pub async fn lookup_no_follow<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<PathLookup, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .lookup_no_follow_with_accounting(path, &mut accounting, cancellation, true)
            .await;
        let work = accounting.spent();
        let result = result.map(|(mut lookup, allocation)| {
            drop(allocation);
            lookup.work = work;
            lookup
        });
        budget.finish(result, &accounting)
    }

    /// Resolves one exact path and decodes its authenticated metadata without
    pub async fn lookup_no_follow_with_metadata<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<PathMetadataLookup>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .lookup_no_follow_with_metadata_with_accounting(path, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn lookup_no_follow_with_metadata_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<PathMetadataLookup>, FsError> {
        let (lookup, allocation) = self
            .lookup_no_follow_with_accounting(path, accounting, cancellation, false)
            .await?;
        drop(allocation);
        let Some(record) = lookup.record else {
            return Ok(None);
        };
        let metadata = read_record_metadata_to_file_root_with_accounting(
            &self.volume,
            record,
            accounting,
            cancellation,
        )
        .await?;
        if self.tracks_observations() {
            self.observe_base_metadata_ids_with_accounting(
                std::slice::from_ref(&record.file_id),
                accounting,
                cancellation,
            )
            .await?;
        }
        Ok(Some(PathMetadataLookup { record, metadata }))
    }

    pub(crate) async fn lookup_no_follow_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> Result<(PathLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            self.synchronize_live_with_accounting(accounting, cancellation)
                .await?;
            let mut lookup = self
                .lookup_no_follow_at_current_with_accounting(
                    path,
                    accounting,
                    cancellation,
                    capture_terminal,
                )
                .await?;
            if lookup.0.record.is_none()
                && self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
            {
                accounting.check()?;
                if !path.is_root()
                    && let Some((value, allocation)) = self
                        .canonicalize_path_components(
                            path.components(),
                            &[],
                            accounting,
                            cancellation,
                        )
                        .await?
                {
                    let corrected = (value, allocation);
                    let retried = self
                        .lookup_no_follow_at_current_with_accounting(
                            &corrected.0,
                            accounting,
                            cancellation,
                            capture_terminal,
                        )
                        .await?;
                    drop(corrected);
                    lookup = retried;
                }
            }
            lookup.0.work = accounting.spent();
            Ok::<_, FsError>(lookup)
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(mut lookup, allocation)| {
            lookup.work = accounting.spent();
            (lookup, allocation)
        })
    }

    pub(crate) async fn lookup_no_follow_at_current_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> Result<(PathLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            let lookup = if self.tracks_observations() {
                let candidate = if self.has_pending_mutations() {
                    Some(
                        crate::kernel::lookup_path_async_with_accounting(
                            &self.volume.fs.inner.objects,
                            &self.root,
                            path,
                            self.volume.config,
                            accounting,
                            cancellation,
                            Some(&self.dependencies.data_custody),
                        )
                        .await?,
                    )
                } else {
                    None
                };
                let base = self.base_root.file_table;
                if let Some(candidate) = candidate
                    && self
                        .dependencies
                        .proof()
                        .observes_path(base, path, capture_terminal)
                {
                    return Ok((candidate, accounting.claim_owned(0, 0)?));
                }
                let (value, allocation) = self
                    .observe_base_for_candidate_with_accounting(
                        path,
                        candidate.is_some(),
                        capture_terminal,
                        accounting,
                        cancellation,
                    )
                    .await?;
                let mut observed = (value, allocation);
                let base_lookup = observed.0.lookup;
                let dependencies = std::mem::take(&mut observed.0.dependencies);
                self.observe_staged_paths_with_accounting(
                    base,
                    [path],
                    capture_terminal,
                    dependencies,
                    &mut observed.1,
                    accounting,
                    cancellation,
                )?;
                drop(observed);
                candidate.unwrap_or(base_lookup)
            } else {
                crate::kernel::lookup_path_async_with_accounting(
                    &self.volume.fs.inner.objects,
                    &self.root,
                    path,
                    self.volume.config,
                    accounting,
                    cancellation,
                    Some(&self.dependencies.data_custody),
                )
                .await?
            };
            Ok((lookup, accounting.claim_owned(0, 0)?))
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(mut lookup, allocation)| {
            lookup.work = accounting.spent();
            (lookup, allocation)
        })
    }

    pub(super) async fn observe_base_for_candidate_with_accounting(
        &self,
        path: &NamespacePath,
        has_candidate: bool,
        capture_terminal: bool,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            crate::kernel::ObservedPathLookup,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        FsError,
    > {
        let observed = crate::kernel::observe_path_async_with_accounting(
            &self.volume.fs.inner.objects,
            &self.base_root,
            path,
            self.volume.config,
            accounting,
            cancellation,
            Some(&self.dependencies.data_custody),
            capture_terminal,
        )
        .await;
        match observed {
            Ok(observed) => Ok(observed.into_parts()),
            Err(FsError::Path(PathLookupError::NotDirectory)) if has_candidate => {
                crate::kernel::observe_path_async_with_accounting(
                    &self.volume.fs.inner.objects,
                    &self.base_root,
                    path,
                    self.volume.config,
                    accounting,
                    cancellation,
                    Some(&self.dependencies.data_custody),
                    false,
                )
                .await
                .map(|observed| observed.into_parts())
            }
            Err(error) => Err(error),
        }
    }

    /// Resolves a bounded path batch while sharing directory and file-table frontiers.
    pub async fn lookup_batch_no_follow<W: FsWork>(
        &mut self,
        paths: &[NamespacePath],
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<PathBatchLookup> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .lookup_batch_no_follow_with_accounting(paths, false, &mut accounting, cancellation)
            .await;
        let work = accounting.spent();
        let result = result.map(|(mut lookup, allocation)| {
            lookup.work = work;
            (lookup, allocation)
        });
        budget.finish_owned(result, &accounting)
    }

    pub(crate) async fn lookup_batch_no_follow_with_accounting(
        &mut self,
        paths: &[NamespacePath],
        selected_restore: bool,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            PathBatchLookup,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        FsError,
    > {
        if selected_restore
            && (self.mode.access != AccessMode::ReadOnly
                || self.mode.consistency != ConsistencyMode::Pinned
                || self.tracks_observations()
                || *self.root != self.base_root)
        {
            return Err(FsError::MutationNotAllowed);
        }
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            self.synchronize_live_with_accounting(accounting, cancellation)
                .await?;
            let config = self.volume.config;
            let base = self.base_root.file_table;
            if !self.tracks_observations() {
                let lookup = if selected_restore {
                    crate::kernel::lookup_paths_selected_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        &self.root,
                        paths,
                        config,
                        accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                    )
                    .await?
                } else {
                    crate::kernel::lookup_paths_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        &self.root,
                        paths,
                        config,
                        accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                    )
                    .await?
                };
                return Ok(lookup.into_parts());
            }
            // The unchanged root answers and observes in the same shared walk.
            if *self.root == self.base_root {
                let (value, allocation) = crate::kernel::observe_paths_async_with_accounting(
                    &self.volume.fs.inner.objects,
                    &self.root,
                    paths,
                    config,
                    accounting,
                    cancellation,
                    Some(&self.dependencies.data_custody),
                )
                .await?
                .into_parts();
                let mut observed = (value, allocation);
                let dependencies = std::mem::take(&mut observed.0.dependencies);
                self.observe_staged_paths_with_accounting(
                    base,
                    paths,
                    true,
                    dependencies,
                    &mut observed.1,
                    accounting,
                    cancellation,
                )?;
                let bytes = observed
                    .0
                    .lookup
                    .entries
                    .capacity()
                    .checked_mul(size_of::<crate::kernel::PathBatchEntry>())
                    .map(crate::foundation::usize_to_u64)
                    .ok_or(WorkError::Overflow)?;
                let output_allocation = match &mut observed.1 {
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                        crate::kernel::allocation::KernelOwnedAllocation::Standalone
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(
                        allocation,
                    ) => crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(
                        allocation.split_off(bytes)?,
                    ),
                };
                let entries = std::mem::take(&mut observed.0.lookup.entries);
                let lookup = PathBatchLookup {
                    entries,
                    retained_allocation_bytes: bytes,
                    work: accounting.spent(),
                };
                drop(observed);
                return Ok((lookup, output_allocation));
            }
            let (value, allocation) = crate::kernel::lookup_paths_async_with_accounting(
                &self.volume.fs.inner.objects,
                &self.root,
                paths,
                config,
                accounting,
                cancellation,
                Some(&self.dependencies.data_custody),
            )
            .await?
            .into_parts();
            let lookup = (value, allocation);
            let mut unobserved =
                crate::kernel::Owned::<Vec<&NamespacePath>>::allocate(0, accounting)?;
            {
                let proof = self.dependencies.proof();
                for path in paths {
                    let examined = WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(examined)?;
                    let observed = proof.observes_path(base, path, true);
                    accounting.record_completed(examined)?;
                    if !observed {
                        unobserved.ensure_for_push(paths.len(), accounting)?;
                        let copied = WorkCounters {
                            bytes_copied: size_of::<&NamespacePath>() as u64,
                            ..WorkCounters::UNCHARGED
                        };
                        accounting.admit(copied)?;
                        unobserved.value.push(path);
                        accounting.record_completed(copied)?;
                    }
                }
            }
            if !unobserved.value.is_empty() {
                let (value, allocation) = crate::kernel::observe_path_refs_async_with_accounting(
                    &self.volume.fs.inner.objects,
                    &self.base_root,
                    &unobserved.value,
                    config,
                    accounting,
                    cancellation,
                    Some(&self.dependencies.data_custody),
                )
                .await?
                .into_parts();
                let mut observed = (value, allocation);
                let dependencies = std::mem::take(&mut observed.0.dependencies);
                unobserved.retire(accounting)?;
                self.observe_staged_paths_with_accounting(
                    base,
                    paths,
                    true,
                    dependencies,
                    &mut observed.1,
                    accounting,
                    cancellation,
                )?;
                drop(observed);
            } else {
                unobserved.retire(accounting)?;
            }
            Ok((lookup.0, lookup.1))
        }
        .await;
        let retained = result.as_ref().map_or(0, |(lookup, _)| {
            (lookup.entries.capacity() * size_of::<crate::kernel::PathBatchEntry>()) as u64
        });
        readonly_cleanup_standalone(accounting, before, retained)?;
        result.map(|(mut lookup, allocation)| {
            lookup.work = accounting.spent();
            (lookup, allocation)
        })
    }
    pub(super) fn observe_staged_paths_with_accounting<'p>(
        &mut self,
        base: ObjectId,
        paths: impl IntoIterator<Item = &'p NamespacePath>,
        capture_terminal: bool,
        dependencies: Vec<Dependency>,
        allocation: &mut KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        self.dependencies.observe_paths(
            base,
            paths,
            capture_terminal,
            dependencies,
            allocation,
            self.volume.config.limits.maximum_checkout_dependencies,
            accounting,
            cancellation,
        )
    }

    pub(super) async fn lookup_file_record_by_id_with_accounting(
        &self,
        file_id: FileId,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = crate::kernel::lookup_file_record_async_with_accounting(
            &self.volume.fs.inner.objects,
            self.root.file_table,
            file_id,
            DecodeLimits::for_volume(self.volume.config),
            accounting,
            cancellation,
            Some(&self.dependencies.data_custody),
        )
        .await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(record, allocation)| {
            drop(allocation);
            record
        })
    }
}
