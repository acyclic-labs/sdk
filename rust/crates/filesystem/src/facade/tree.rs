use super::*;

pub(super) async fn list_directory_to_file_root_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    root: &GenerationRoot,
    path: &NamespacePath,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<
    (
        crate::kernel::DirectoryPage,
        crate::kernel::allocation::KernelOwnedAllocation,
    ),
    FsError,
> {
    let before = readonly_standalone_live(accounting);
    let result = async {
        let record = crate::kernel::lookup_path_async_with_accounting(
            &volume.fs.inner.objects,
            root,
            path,
            volume.config,
            accounting,
            cancellation,
            None,
        )
        .await?
        .record
        .ok_or(FsError::NotFound)?;
        let FilePayload::Directory { entries } = record.payload else {
            return Err(FsError::NotDirectory);
        };
        if record.kind != FileKind::Directory {
            return Err(FsError::NotDirectory);
        }
        crate::kernel::list_tree_entries_async_with_accounting(
            &volume.fs.inner.objects,
            entries,
            (
                (after).map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum_entries,
            ),
            DecodeLimits::for_volume(volume.config),
            accounting,
            cancellation,
            None,
        )
        .await
    }
    .await;
    let retained = match &result {
        Ok((page, _)) if before.is_some() => page.entries.iter().try_fold(
            page.entries
                .capacity()
                .checked_mul(size_of::<crate::kernel::TreeEntry>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(WorkError::Overflow)?,
            |bytes, entry| {
                bytes
                    .checked_add(entry.name.retained_bytes() as u64)
                    .ok_or(FsError::Work(WorkError::Overflow))
            },
        )?,
        _ => 0,
    };
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(crate) async fn stage_empty_tree_with_accounting(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_tree_page(
                    &TreePage::Leaf(Vec::new()),
                    self.volume.config.limits.maximum_directory_page_entries,
                    observe,
                    allocation_error,
                )
            })?;
        self.volume
            .fs
            .put_encoded_with_accounting(ObjectKind::TreePage, encoded, accounting, cancellation)
            .await
    }

    /// Returns one bounded authenticated page from a directory.
    pub async fn list_directory<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::OwnedOutput<DirectoryPage> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .list_directory_with_accounting(
                path,
                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum_entries,
                &mut accounting,
                cancellation,
            )
            .await;
        let work = accounting.spent();
        let result = result.map(|(mut page, allocation)| {
            page.work = work;
            (page, allocation)
        });
        budget.finish_owned(result, &accounting)
    }

    pub(crate) async fn list_directory_with_accounting(
        &mut self,
        path: &NamespacePath,
        after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            DirectoryPage,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        FsError,
    > {
        if matches!(after, std::ops::Bound::Included(_)) && self.tracks_observations() {
            return Err(FsError::UnsupportedCheckout);
        }
        let (lookup, _allocation) = self
            .lookup_no_follow_with_accounting(path, accounting, cancellation, false)
            .await?;
        self.list_directory_record_with_accounting(
            lookup.record.ok_or(FsError::NotFound)?,
            after,
            maximum_entries,
            accounting,
            cancellation,
        )
        .await
    }

    // Alias resolution already knows the exact parent record. Keeping the
    // listing and observation body independent of path resolution avoids
    // recursively resolving aliases while scanning their siblings.
    pub(super) async fn list_directory_record_with_accounting(
        &mut self,
        record: FileRecord,
        after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            DirectoryPage,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        FsError,
    > {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        // The future is destroyed before its exact physical frame claim.
        let mut frame_allocation = None;
        let (mut future, frame_bytes) = crate::heap_future::in_heap_admitted(
            (&mut *self, record, after, &mut *accounting, cancellation),
            |(_, _, _, accounting, _), bytes| {
                frame_allocation = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
                Ok::<_, FsError>(())
            },
            |(this, record, after, accounting, cancellation)| async move {
                let FilePayload::Directory { entries } = record.payload else {
                    return Err(FsError::NotDirectory);
                };
                if record.kind != FileKind::Directory {
                    return Err(FsError::NotDirectory);
                }
                let (page, allocation) = crate::kernel::list_tree_entries_async_with_accounting(
                    &this.volume.fs.inner.objects,
                    entries,
                    (after, maximum_entries),
                    DecodeLimits::for_volume(this.volume.config),
                    accounting,
                    cancellation,
                    Some(&this.dependencies.data_custody),
                )
                .await?;
                let page = (page, allocation);
                if this.tracks_observations() {
                    let (cursor, allocation) = match after {
                        std::ops::Bound::Excluded(name) => {
                            accounting.charge(WorkCounters {
                                items_examined: 1,
                                ..WorkCounters::UNCHARGED
                            })?;
                            let (name, allocation) =
                                name.clone_with_accounting(accounting, cancellation)?;
                            (Some(name), allocation)
                        }
                        std::ops::Bound::Unbounded => (None, accounting.claim_owned(0, 0)?),
                        std::ops::Bound::Included(_) => {
                            unreachable!("included cursors cannot track observations")
                        }
                    };
                    let mut region = (
                        DependencyRegion::DirectoryRange {
                            directory_id: record.file_id,
                            after: cursor,
                            maximum_entries,
                        },
                        allocation,
                    );
                    let (probe, allocation) = AuthenticatedGenerationProbe::new_with_accounting(
                        &this.volume.fs.inner.objects,
                        probe_limits(this.volume.config),
                        accounting,
                    )?;
                    let probe = (
                        probe.with_data_custody(Some(&this.dependencies.data_custody)),
                        allocation,
                    );
                    let state = probe
                        .0
                        .probe_async_with_accounting(
                            GenerationId::new(this.root.base.digest),
                            &region.0,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    {
                        let (probe, _allocation) = probe;
                        probe.retire_with_accounting(accounting)?;
                    }
                    let mut staged =
                        crate::kernel::Owned::<Vec<Dependency>>::allocate(1, accounting)?;
                    staged.allocation.absorb(&mut region.1)?;
                    let copied = WorkCounters {
                        items_examined: 1,
                        bytes_copied: size_of::<Dependency>() as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    staged.value.push(Dependency {
                        region: region.0,
                        expected: state,
                    });
                    accounting.record_completed(copied)?;
                    this.extend_observations_with_accounting(staged, accounting, cancellation)?;
                }
                Ok((page.0, page.1))
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        if let Some(mut allocation) = frame_allocation {
            accounting.release_owned(&mut allocation, frame_bytes)?;
        }
        let retained = result.as_ref().map_or(0, |(page, _)| {
            (page.entries.capacity() * size_of::<crate::kernel::TreeEntry>()) as u64
                + page
                    .entries
                    .iter()
                    .map(|entry| entry.name.retained_bytes() as u64)
                    .sum::<u64>()
        });
        readonly_cleanup_standalone(accounting, before, retained)?;
        result.map(|(mut page, allocation)| {
            page.work = accounting.spent();
            (page, allocation)
        })
    }
}

pub(super) fn directory_entries(record: Option<FileRecord>) -> Option<ObjectId> {
    let record = record?;
    match record.payload {
        FilePayload::Directory { entries } => Some(entries),
        _ => None,
    }
}
