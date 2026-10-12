use super::*;

/// One path-independent file-record change between immutable generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRecordChange {
    /// Stable file identity.
    pub file_id: FileId,
    /// Complete record in the earlier generation.
    pub before: Option<FileRecord>,
    /// Complete record in the later generation.
    pub after: Option<FileRecord>,
}

/// One exact name binding change within a stable directory identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryBindingChange {
    /// Stable parent directory identity.
    pub directory_id: FileId,
    /// Exact profile-specific component.
    pub name: LogicalName,
    /// Binding in the earlier generation.
    pub before: Option<crate::kernel::TreeEntry>,
    /// Binding in the later generation.
    pub after: Option<crate::kernel::TreeEntry>,
}

/// Bounded Merkle-aware semantic diff between two immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationDiff {
    /// Content, metadata, kind, link-count, or directory-root changes by file identity.
    pub files: Vec<FileRecordChange>,
    /// Added, removed, renamed, or retargeted names by parent directory identity.
    pub bindings: Vec<DirectoryBindingChange>,
    /// Additional changes exist beyond the caller's single total bound.
    pub truncated: bool,
}

/// Terminal result of preparing an unpublished two-parent merge generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergePreparation {
    /// The checkout now holds this candidate and may commit or discard it.
    Prepared {
        /// Authenticated two-parent generation identity.
        generation_id: GenerationId,
    },
    /// No checkout state changed; exact conflicts are bounded by the caller.
    Conflicted {
        /// Stable conflicting regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the supplied bound.
        truncated: bool,
    },
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_join_changes<W: FsWork>(
        &self,
        base: GenerationId,
        source: &crate::Generation<A, O>,
        maximum_changes: u32,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = readonly_standalone_live(&accounting).is_some();
        let result = async {
            let accounting = &mut accounting;

            if maximum_changes == 0 || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let config = source.workspace.volume.config;
            let (base_root, allocation) = read_generation_root_with_accounting(
                &self.inner.objects,
                ObjectId {
                    kind: ObjectKind::GenerationRoot,
                    digest: base.digest(),
                },
                config,
                accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::Filesystem)?;
            let bytes = usize_to_u64(base_root.parents.capacity() * size_of::<GenerationId>());
            let base_root = Owned::new(base_root, allocation, bytes);
            let (source_root, allocation) = read_generation_root_with_accounting(
                &self.inner.objects,
                ObjectId {
                    kind: ObjectKind::GenerationRoot,
                    digest: source.id.digest(),
                },
                config,
                accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::Filesystem)?;
            let bytes = usize_to_u64(source_root.parents.capacity() * size_of::<GenerationId>());
            let source_root = Owned::new(source_root, allocation, bytes);
            let result = async {
                if source_root.value.volume_id != source.workspace.volume.id
                    || base_root.value.root_file_id != source_root.value.root_file_id
                {
                    return Err(WorkspaceError::IncompatibleWorkspace);
                }
                let (diff, allocation) = diff_generation_file_tables(
                    &self.inner.objects,
                    base_root.value.file_table,
                    source_root.value.file_table,
                    maximum_changes,
                    config,
                    accounting,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::Filesystem)?;
                if diff.truncated {
                    drop(diff);
                    drop(allocation);
                    return Err(WorkspaceError::JoinLimit);
                }
                Ok((diff, allocation))
            }
            .await;
            source_root
                .retire(accounting)
                .map_err(WorkspaceError::Filesystem)?;
            base_root
                .retire(accounting)
                .map_err(WorkspaceError::Filesystem)?;
            result
        }
        .await
        .map_err(|error| match error {
            WorkspaceError::Filesystem(error) if standalone => WorkspaceError::engine(error),
            error => error,
        });
        budget.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    /// Computes one bounded semantic diff, skipping equal Merkle subtrees.
    pub async fn diff_generations(
        &self,
        before: GenerationId,
        after: GenerationId,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationDiff> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.diff_generations",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&self.id.into_bytes())
        );
        let mut accounting = budget.accounting(cancellation);
        let result = crate::obs::in_span(&span, async {
            if maximum_changes == 0 {
                return Err(FsError::InvalidDiff);
            }
            if before == after {
                return Ok(GenerationDiff {
                    files: Vec::new(),
                    bindings: Vec::new(),
                    truncated: false,
                });
            }
            let before = self
                .fs
                .read_join_root_with_accounting(
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: before.digest(),
                    },
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let after = self
                .fs
                .read_join_root_with_accounting(
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: after.digest(),
                    },
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let result = async {
                if before.value.volume_id != self.id || after.value.volume_id != self.id {
                    return Err(FsError::VolumeMismatch);
                }
                let (diff, allocation) = diff_generation_file_tables(
                    &self.fs.inner.objects,
                    before.value.file_table,
                    after.value.file_table,
                    maximum_changes,
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
                drop(allocation);
                Ok(diff)
            }
            .await;
            after.retire(&mut accounting)?;
            before.retire(&mut accounting)?;
            result
        })
        .await;
        measured(result, &accounting).observe_on(&span, "diff_generations")
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Diffs two candidates under the supplied work limits.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn candidate_diff<W: FsWork>(
        &self,
        candidate: &Self,
        maximum_changes: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if candidate.volume.id != self.volume.id {
                return Err(FsError::VolumeMismatch);
            }
            let output = diff_generation_file_tables(
                &self.volume.fs.inner.objects,
                self.root.file_table,
                candidate.root.file_table,
                maximum_changes,
                self.volume.config,
                &mut accounting,
                cancellation,
            )
            .await?;
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some()
                && matches!(output.1, KernelOwnedAllocation::Standalone)
            {
                return Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ));
            }
            Ok(output)
        }
        .await;
        work.finish(result, &accounting)
    }
}

pub(super) async fn diff_generation_file_tables<O: AsyncObjectStore>(
    objects: &O,
    before: ObjectId,
    after: ObjectId,
    maximum_changes: u32,
    config: VolumeConfig,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<
    (
        GenerationDiff,
        crate::kernel::allocation::KernelOwnedAllocation,
    ),
    FsError,
> {
    let (records, allocation) = crate::kernel::diff_file_records_with_accounting(
        objects,
        Some(before),
        Some(after),
        maximum_changes,
        DecodeLimits::for_volume(config),
        accounting,
        cancellation,
    )
    .await?;
    let record_bytes = records.retained_allocation_bytes;
    let mut records = crate::kernel::Owned::new(records, allocation, record_bytes);
    let maximum = usize::try_from(maximum_changes).unwrap_or(usize::MAX);
    let mut files = crate::kernel::Owned::<Vec<FileRecordChange>>::allocate(
        records.value.changes.len(),
        accounting,
    )?;
    let mut bindings =
        crate::kernel::Owned::<Vec<DirectoryBindingChange>>::allocate(0, accounting)?;
    let mut truncated = records.value.truncated;
    for change in records.value.changes.drain(..) {
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if files.value.len() + bindings.value.len() >= maximum {
            truncated = true;
            break;
        }
        let before_entries = directory_entries(change.before);
        let after_entries = directory_entries(change.after);
        files.value.push(FileRecordChange {
            file_id: change.key,
            before: change.before,
            after: change.after,
        });
        if before_entries == after_entries {
            continue;
        }
        let remaining_changes = maximum.saturating_sub(files.value.len() + bindings.value.len());
        if remaining_changes == 0 {
            truncated = true;
            continue;
        }
        let (entries, allocation) = crate::kernel::diff_tree_entries_with_accounting(
            objects,
            before_entries,
            after_entries,
            u32::try_from(remaining_changes).unwrap_or(u32::MAX),
            DecodeLimits::for_volume(config),
            accounting,
            cancellation,
        )
        .await?;
        let bytes = entries.retained_allocation_bytes;
        let mut entries = crate::kernel::Owned::new(entries, allocation, bytes);
        truncated |= entries.value.truncated;
        let next_bytes = bindings
            .bytes
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        bindings.allocation.absorb(&mut entries.allocation)?;
        bindings.bytes = next_bytes;
        let buffer_bytes = crate::foundation::usize_to_u64(
            entries
                .value
                .changes
                .capacity()
                .checked_mul(size_of::<
                    crate::kernel::ValueChange<LogicalName, crate::kernel::TreeEntry>,
                >())
                .ok_or(WorkError::Overflow)?,
        );
        for entry in entries.value.changes.drain(..) {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            bindings.ensure_for_push(maximum, accounting)?;
            bindings.value.push(DirectoryBindingChange {
                directory_id: change.key,
                name: entry.key,
                before: entry.before,
                after: entry.after,
            });
        }
        // The original diff Vec is physically destroyed before retiring its
        drop(entries);
        accounting.release_owned(&mut bindings.allocation, buffer_bytes)?;
        bindings.bytes = bindings
            .bytes
            .checked_sub(buffer_bytes)
            .ok_or(WorkError::Overflow)?;
    }
    records.retire(accounting)?;
    bindings.allocation.absorb(&mut files.allocation)?;
    let crate::kernel::Owned {
        value: files,
        allocation: files_allocation,
        ..
    } = files;
    let crate::kernel::Owned {
        value: bindings,
        allocation,
        ..
    } = bindings;
    drop(files_allocation);
    Ok((
        GenerationDiff {
            files,
            bindings,
            truncated,
        },
        allocation,
    ))
}
