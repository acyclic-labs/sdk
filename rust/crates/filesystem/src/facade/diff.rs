use super::*;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};

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
        let copied = WorkCounters {
            bytes_copied: size_of::<FileRecordChange>() as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        files.value.push(FileRecordChange {
            file_id: change.key,
            before: change.before,
            after: change.after,
        });
        accounting.record_completed(copied)?;
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
            let copied = WorkCounters {
                bytes_copied: size_of::<DirectoryBindingChange>() as u64,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            bindings.value.push(DirectoryBindingChange {
                directory_id: change.key,
                name: entry.key,
                before: entry.before,
                after: entry.after,
            });
            accounting.record_completed(copied)?;
        }
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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_join_changes<W: FsWork>(
        &self,
        base: GenerationId,
        source: &crate::Generation<A, O>,
        maximum_changes: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), crate::workspace::WorkspaceError> {
        use crate::workspace::WorkspaceError;
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if maximum_changes == 0 || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let config = source.workspace.volume.config;
            let (base_root, base_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: base.digest(),
                    },
                    config,
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::Filesystem)?;
            let bytes = crate::foundation::usize_to_u64(
                base_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let base_root = crate::kernel::Owned::new(base_root, base_root_allocation, bytes);
            let (source_root, source_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: source.id.digest(),
                    },
                    config,
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::Filesystem)?;
            let bytes = crate::foundation::usize_to_u64(
                source_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let source_root = crate::kernel::Owned::new(source_root, source_root_allocation, bytes);
            if source_root.value.volume_id != source.workspace.volume.id
                || base_root.value.root_file_id != source_root.value.root_file_id
            {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let output = diff_generation_file_tables(
                &self.inner.objects,
                base_root.value.file_table,
                source_root.value.file_table,
                maximum_changes,
                config,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::Filesystem)?;
            base_root.retire(&mut accounting)?;
            source_root.retire(&mut accounting)?;
            if output.0.truncated {
                return Err(WorkspaceError::JoinLimit);
            }
            Ok(output)
        }
        .await;
        work.finish(result, &accounting)
    }
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    /// Computes one bounded semantic diff without scanning equal Merkle subtrees.
    pub async fn diff_generations(
        &self,
        before: GenerationId,
        after: GenerationId,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationDiff> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
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
            let (before_root, before_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.fs.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: before.digest(),
                    },
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let bytes = crate::foundation::usize_to_u64(
                before_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let before_root = crate::kernel::Owned::new(before_root, before_root_allocation, bytes);
            let (after_root, after_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.fs.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: after.digest(),
                    },
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let bytes = crate::foundation::usize_to_u64(
                after_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let after_root = crate::kernel::Owned::new(after_root, after_root_allocation, bytes);
            if before_root.value.volume_id != self.id || after_root.value.volume_id != self.id {
                return Err(FsError::VolumeMismatch);
            }
            let (diff, _allocation) = diff_generation_file_tables(
                &self.fs.inner.objects,
                before_root.value.file_table,
                after_root.value.file_table,
                maximum_changes,
                self.config,
                &mut accounting,
                cancellation,
            )
            .await?;
            before_root.retire(&mut accounting)?;
            after_root.retire(&mut accounting)?;
            Ok(diff)
        }
        .await;
        work::measured(result, &accounting)
    }
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
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
            diff_generation_file_tables(
                &self.volume.fs.inner.objects,
                self.root.file_table,
                candidate.root.file_table,
                maximum_changes,
                self.volume.config,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        work.finish(result, &accounting)
    }
}
