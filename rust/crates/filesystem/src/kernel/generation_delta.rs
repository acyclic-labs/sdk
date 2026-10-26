//! Content-free composition of an authenticated, independent generation delta.
use super::allocation::KernelAccounting;
use super::file_table_mutation::apply_file_table_mutations_with_accounting;
use super::persistent_btree::Format;
use super::persistent_io::{clone_key, clone_value};
use super::tree_mutation::{TreeFormat, apply_tree_mutations_with_accounting};
use super::{
    DecodeLimits, DirectoryBindingChange, FileRecord, FileRecordChange, FileTableMutation,
    GenerationDiff, Owned, TreeMutation,
};
use crate::{AsyncObjectStore, CancellationToken, FsError, ObjectId, WorkError};

/// Changes only the named records and directory bindings. Every non-directory
/// record retains its exact before-value fence; directory child-root changes
/// retain exact name fences while composing with unrelated names. No extent or
/// blob is opened, and earlier proposals' deltas are never traversed again.
pub(crate) async fn apply_generation_delta<O: AsyncObjectStore>(
    objects: &O,
    table: ObjectId,
    delta: &GenerationDiff,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let mut frame = crate::heap_future::AdmittedFrame::new(accounting);
    let (future, _) = crate::heap_future::in_heap_admitted(
        &mut frame,
        |frame, bytes| frame.admit(bytes),
        |frame| async move {
            apply_delta(
                objects,
                table,
                delta,
                limits,
                frame.accounting(),
                cancellation,
            )
            .await
        },
    )?;
    let mut future: acyclic_stream::BoxProviderFuture<'_, _> = future;
    let result = future.as_mut().await;
    drop(future);
    frame.finish(result)
}

async fn apply_delta<O: AsyncObjectStore>(
    objects: &O,
    table: ObjectId,
    delta: &GenerationDiff,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    if delta.truncated {
        return Err(FsError::InvalidDiff);
    }
    let mut mutations = Owned::<Vec<FileTableMutation>>::allocate(delta.files.len(), accounting)?;
    let result = async {
        let mut bindings = delta.bindings.as_slice();
        let mut previous = None;
        for change in &delta.files {
            accounting.charge_items(1)?;
            if previous.is_some_and(|file| file >= change.file_id) {
                return Err(FsError::InvalidDiff);
            }
            previous = Some(change.file_id);
            let mut count = 0;
            while let Some(binding) = bindings.get(count) {
                if binding.directory_id > change.file_id {
                    break;
                }
                accounting.charge_items(1)?;
                if binding.directory_id != change.file_id {
                    return Err(FsError::InvalidDiff);
                }
                count += 1;
            }
            let (selected, remaining) = bindings.split_at(count);
            bindings = remaining;
            let (current, allocation) = super::lookup_file_record_async_with_accounting(
                objects,
                table,
                change.file_id,
                limits,
                accounting,
                cancellation,
                None,
            )
            .await?;
            let current = Owned::new(current, allocation, 0);
            let expected = current.value;
            let replacement = if change.directory_children_only() {
                compose_directory(
                    objects,
                    change,
                    current.value,
                    selected,
                    limits,
                    accounting,
                    cancellation,
                )
                .await
            } else if current.value == change.before {
                Ok(change.after)
            } else {
                Err(FsError::InvalidDiff)
            };
            let replacement = current.retire(accounting).and(replacement)?;
            if let Some(mutation) =
                super::merge::file_table_mutation(change.file_id, expected, replacement)
            {
                // Directory composition fences the exact head record returned
                // by the lookup, including its unrelated child-root changes.
                mutations.value.push(mutation);
            }
        }
        if !bindings.is_empty() {
            return Err(FsError::InvalidDiff);
        }
        if mutations.value.is_empty() {
            return Ok(table);
        }
        Ok(apply_file_table_mutations_with_accounting(
            objects,
            table,
            &mutations.value,
            u32::try_from(delta.files.len()).map_err(|_| WorkError::Overflow)?,
            limits,
            accounting,
            cancellation,
        )
        .await?
        .value)
    }
    .await;
    mutations.retire(accounting).and(result)
}

async fn compose_directory<O: AsyncObjectStore>(
    objects: &O,
    change: &FileRecordChange,
    current: Option<FileRecord>,
    bindings: &[DirectoryBindingChange],
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let before = change.before.ok_or(FsError::InvalidDiff)?;
    let current = current.ok_or(FsError::InvalidDiff)?;
    let entries = super::merge::directory_entries(Some(current)).ok_or(FsError::InvalidDiff)?;
    if current.kind != before.kind
        || current.metadata != before.metadata
        || current.link_count != before.link_count
    {
        return Err(FsError::InvalidDiff);
    }
    let mut mutations = Owned::<Vec<TreeMutation>>::allocate(bindings.len(), accounting)?;
    let result = async {
        let mut previous = None;
        for binding in bindings {
            accounting.charge_items(1)?;
            if previous.is_some_and(|name| name >= &binding.name) {
                return Err(FsError::InvalidDiff);
            }
            previous = Some(&binding.name);
            append_binding(&mut mutations, binding, limits, accounting)?;
        }
        let entries = if mutations.value.is_empty() {
            entries
        } else {
            apply_tree_mutations_with_accounting(
                objects,
                entries,
                &mutations.value,
                u32::try_from(bindings.len()).map_err(|_| WorkError::Overflow)?,
                limits,
                accounting,
                cancellation,
            )
            .await?
            .value
        };
        Ok(Some(FileRecord {
            payload: super::FilePayload::Directory { entries },
            ..current
        }))
    }
    .await;
    mutations.retire(accounting).and(result)
}

fn append_binding(
    mutations: &mut Owned<Vec<TreeMutation>>,
    binding: &DirectoryBindingChange,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let (mutation, mut allocation, bytes) = if let Some(entry) = &binding.after {
        let (entry, allocation) =
            clone_value::<TreeFormat>(entry, limits.maximum_name_bytes, accounting, None, 0)?;
        let bytes = TreeFormat::value_owned_bytes(&entry);
        let mutation = match &binding.before {
            Some(before) => TreeMutation::Replace {
                entry,
                expected_file_id: before.file_id,
            },
            None => TreeMutation::Insert(entry),
        };
        (mutation, allocation, bytes)
    } else {
        let before = binding.before.as_ref().ok_or(FsError::InvalidDiff)?;
        let (name, allocation) = clone_key::<TreeFormat>(
            &binding.name,
            limits.maximum_name_bytes,
            accounting,
            None,
            0,
        )?;
        let bytes = TreeFormat::key_owned_bytes(&name);
        (
            TreeMutation::Remove {
                name,
                expected_file_id: Some(before.file_id),
            },
            allocation,
            bytes,
        )
    };
    mutations.allocation.absorb(&mut allocation)?;
    mutations.bytes = mutations
        .bytes
        .checked_add(bytes)
        .ok_or(WorkError::Overflow)?;
    mutations.value.push(mutation);
    Ok(())
}
