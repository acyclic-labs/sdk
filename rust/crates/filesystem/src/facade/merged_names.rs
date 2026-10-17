use super::names::{
    CASE_FOLD_COLLISION_SCAN_PAGE, folded_binding_match, name_fold_key, retire_name_owned,
};
use super::*;

async fn admit_tree_binding<O: AsyncObjectStore>(
    objects: &O,
    tree: ObjectId,
    name: &LogicalName,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let folded = name_fold_key(name, accounting)?;
    let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
    let result = async {
        loop {
            let (page, allocation) = crate::kernel::list_tree_entries_async_with_accounting(
                objects,
                tree,
                (
                    after.as_ref().map_or(std::ops::Bound::Unbounded, |owned| {
                        std::ops::Bound::Excluded(&owned.0)
                    }),
                    CASE_FOLD_COLLISION_SCAN_PAGE,
                ),
                limits,
                accounting,
                cancellation,
                None,
            )
            .await?;
            let bytes = page.entries.iter().try_fold(
                page.entries
                    .capacity()
                    .checked_mul(size_of::<crate::kernel::TreeEntry>())
                    .ok_or(WorkError::Overflow)? as u64,
                |bytes, entry| {
                    bytes
                        .checked_add(entry.name.retained_bytes() as u64)
                        .ok_or(WorkError::Overflow)
                },
            )?;
            let page = Owned::new(page, allocation, bytes);
            let result = (|| {
                if folded_binding_match(
                    name,
                    &folded.0,
                    false,
                    &[],
                    page.value.entries.iter(),
                    accounting,
                )?
                .is_some()
                {
                    return Err(FsError::NameCollision);
                }
                if let Some(old) = after.take() {
                    let bytes = old.0.retained_bytes() as u64;
                    retire_name_owned(old, bytes, accounting)?;
                }
                after = page
                    .value
                    .entries
                    .last()
                    .filter(|_| page.value.has_more)
                    .map(|entry| entry.name.clone_with_accounting(accounting, cancellation))
                    .transpose()?;
                Ok(page.value.has_more)
            })();
            page.retire(accounting)?;
            if !result? {
                return Ok(());
            }
        }
    }
    .await;
    if let Some(after) = after {
        let bytes = after.0.retained_bytes() as u64;
        retire_name_owned(after, bytes, accounting)?;
    }
    let bytes = folded.0.capacity() as u64;
    retire_name_owned(folded, bytes, accounting)?;
    result
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(super) async fn admit_rebased_bindings(
        &self,
        before: ObjectId,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Ok(());
        }
        let objects = &self.volume.fs.inner.objects;
        let limits = DecodeLimits::for_volume(self.volume.config);
        let (files, allocation) = crate::kernel::diff_file_records_with_accounting(
            objects,
            Some(before),
            Some(self.root.file_table),
            maximum,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let bytes = files.retained_allocation_bytes;
        let files = Owned::new(files, allocation, bytes);
        let result = async {
            if files.value.truncated {
                return Err(FsError::MergeChangeLimit);
            }
            for file in &files.value.changes {
                let Some(FileRecord {
                    payload: FilePayload::Directory { entries: after },
                    ..
                }) = file.after
                else {
                    continue;
                };
                let before = file.before.and_then(|record| match record.payload {
                    FilePayload::Directory { entries } => Some(entries),
                    _ => None,
                });
                if before == Some(after) {
                    continue;
                }
                let (bindings, allocation) = crate::kernel::diff_tree_entries_with_accounting(
                    objects,
                    before,
                    Some(after),
                    maximum,
                    limits,
                    accounting,
                    cancellation,
                )
                .await?;
                let bytes = bindings.retained_allocation_bytes;
                let bindings = Owned::new(bindings, allocation, bytes);
                let result = async {
                    if bindings.value.truncated {
                        return Err(FsError::MergeChangeLimit);
                    }
                    for binding in &bindings.value.changes {
                        if binding.after.is_some() {
                            admit_tree_binding(
                                objects,
                                after,
                                &binding.key,
                                limits,
                                accounting,
                                cancellation,
                            )
                            .await?;
                        }
                    }
                    Ok(())
                }
                .await;
                bindings.retire(accounting)?;
                result?;
            }
            Ok(())
        }
        .await;
        files.retire(accounting)?;
        result
    }
}
