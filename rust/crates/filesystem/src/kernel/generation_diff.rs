//! Shared Merkle-aware generation deltas used by publication, rebasing and joins.
use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::merge::directory_entries;
use super::{DecodeLimits, FileRecord, LogicalName};
use crate::{
    AsyncObjectStore, CancellationToken, FileId, FsError, ObjectId, WorkCounters, WorkError,
};
use std::mem::size_of;

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

impl FileRecordChange {
    pub(crate) fn directory_children_only(&self) -> bool {
        matches!((self.before, self.after), (Some(before), Some(after))
            if before.file_id == after.file_id
                && before.kind == super::FileKind::Directory
                && after.kind == super::FileKind::Directory
                && before.metadata == after.metadata
                && before.link_count == after.link_count
                && matches!(before.payload, super::FilePayload::Directory { .. })
                && matches!(after.payload, super::FilePayload::Directory { .. }))
    }
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

impl GenerationDiff {
    pub(crate) fn retained_bytes(&self) -> Result<u64, WorkError> {
        let files = self
            .files
            .capacity()
            .checked_mul(size_of::<FileRecordChange>());
        let bindings = self
            .bindings
            .capacity()
            .checked_mul(size_of::<DirectoryBindingChange>());
        let bytes = files
            .and_then(|files| bindings.and_then(|bindings| files.checked_add(bindings)))
            .ok_or(WorkError::Overflow)?;
        let bytes = self.bindings.iter().try_fold(bytes, |bytes, binding| {
            [
                Some(&binding.name),
                binding.before.as_ref().map(|entry| &entry.name),
                binding.after.as_ref().map(|entry| &entry.name),
            ]
            .into_iter()
            .flatten()
            .try_fold(bytes, |bytes, name| {
                bytes
                    .checked_add(name.retained_bytes())
                    .ok_or(WorkError::Overflow)
            })
        })?;
        Ok(crate::foundation::usize_to_u64(bytes))
    }
}

pub(crate) async fn diff_generation_file_tables<O: AsyncObjectStore>(
    objects: &O,
    before: ObjectId,
    after: ObjectId,
    maximum_changes: u32,
    limits: DecodeLimits,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationDiff, KernelOwnedAllocation), FsError> {
    let (records, allocation) = crate::kernel::diff_file_records_with_accounting(
        objects,
        Some(before),
        Some(after),
        maximum_changes,
        limits,
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
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let bytes = entries.retained_allocation_bytes;
        let entries = crate::kernel::Owned::new(entries, allocation, bytes);
        truncated |=
            append_binding_changes(&mut bindings, entries, change.key, maximum, accounting)?;
    }
    records.retire(accounting)?;
    bindings.allocation.absorb(&mut files.allocation)?;
    let crate::kernel::Owned {
        value: files,
        allocation: _files_allocation,
        ..
    } = files;
    let crate::kernel::Owned {
        value: bindings,
        allocation,
        ..
    } = bindings;
    Ok((
        GenerationDiff {
            files,
            bindings,
            truncated,
        },
        allocation,
    ))
}

fn append_binding_changes(
    bindings: &mut super::Owned<Vec<DirectoryBindingChange>>,
    mut entries: super::Owned<super::persistent_diff::Diff<LogicalName, super::TreeEntry>>,
    directory_id: FileId,
    maximum: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    let truncated = entries.value.truncated;
    let next_bytes = bindings
        .bytes
        .checked_add(entries.bytes)
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
            directory_id,
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
    Ok(truncated)
}
