use super::*;
use crate::kernel::{DecodeLimits, GenerationRoot, Owned};

/// Authenticated immutable namespace without a facade or mutable checkout.
pub(crate) struct NamespaceView<'a, O> {
    pub(crate) objects: &'a O,
    pub(crate) config: crate::VolumeConfig,
    pub(crate) root: &'a GenerationRoot,
}

pub(super) struct NamespaceEntry {
    pub(super) name: LogicalName,
    pub(super) record: crate::kernel::FileRecord,
}

pub(super) struct NamespacePage {
    pub(super) entries: Vec<NamespaceEntry>,
    pub(super) has_more: bool,
}

impl<O: AsyncObjectStore> NamespaceView<'_, O> {
    pub(super) async fn records(
        &self,
        ids: &[FileId],
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
        let mut matches = (
            NamespaceRecordSearch {
                records: NamespaceRecords::new(),
                complete: ids.is_empty(),
            },
            accounting.claim_owned(0, 0)?,
        );
        if ids.is_empty() || maximum_entries == 0 {
            return Ok(matches);
        }
        matches.0.complete = super::reverse_traversal::collect_namespace(
            self,
            ids,
            maximum_entries,
            &mut matches,
            accounting,
            cancellation,
        )
        .await?;
        super::reverse_traversal::reverse_sort(&mut matches.0.records, accounting)?;
        Ok(matches)
    }

    pub(super) async fn list(
        &self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespacePage, KernelOwnedAllocation), FsError> {
        let before = crate::facade::readonly_standalone_live(accounting);
        let result = self
            .list_inner(path, after, maximum_entries, accounting, cancellation)
            .await;
        let retained = result.as_ref().map_or(0, |(page, _)| {
            workspace_vec_bytes(&page.entries)
                + page
                    .entries
                    .iter()
                    .map(|entry| entry.name.retained_bytes() as u64)
                    .sum::<u64>()
        });
        crate::facade::readonly_cleanup_standalone(accounting, before, retained)?;
        result
    }

    async fn list_inner(
        &self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespacePage, KernelOwnedAllocation), FsError> {
        let record = crate::kernel::lookup_path_async_with_accounting(
            self.objects,
            self.root,
            path,
            self.config,
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
        let (page, allocation) = crate::kernel::list_tree_entries_async_with_accounting(
            self.objects,
            entries,
            (
                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum_entries,
            ),
            DecodeLimits::for_volume(self.config),
            accounting,
            cancellation,
            None,
        )
        .await?;
        let mut source = (page, allocation);
        let count = source.0.entries.len();
        if count == 0 {
            let bytes = workspace_vec_bytes(&source.0.entries);
            let (page, mut allocation) = source;
            let has_more = page.has_more;
            drop(page);
            accounting.release_owned(&mut allocation, bytes)?;
            return Ok((
                NamespacePage {
                    entries: Vec::new(),
                    has_more,
                },
                allocation,
            ));
        }
        let mut ids = Owned::<Vec<FileId>>::allocate(count, accounting)?;
        accounting.charge(copy_work((count * std::mem::size_of::<FileId>()) as u64))?;
        ids.value
            .extend(source.0.entries.iter().map(|entry| entry.file_id));
        let mut output = Owned::<Vec<NamespaceEntry>>::allocate(count, accounting)?;
        let (records, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
            self.objects,
            self.root.file_table,
            &ids.value,
            maximum_entries,
            DecodeLimits::for_volume(self.config),
            accounting,
            cancellation,
            None,
        )
        .await?;
        let records = (records, allocation);
        if count != records.0.len() {
            return Err(FsError::InvalidDirectoryRecord);
        }
        let source_bytes = workspace_vec_bytes(&source.0.entries);
        output.allocation.absorb(&mut source.1)?;
        for (entry, record) in source.0.entries.drain(..).zip(&records.0) {
            let record = record.ok_or(FsError::InvalidDirectoryRecord)?;
            if record.file_id != entry.file_id || record.kind != entry.kind {
                return Err(FsError::InvalidDirectoryRecord);
            }
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                bytes_copied: std::mem::size_of::<NamespaceEntry>() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            output.value.push(NamespaceEntry {
                name: entry.name,
                record,
            });
        }
        let has_more = source.0.has_more;
        drop(source);
        accounting.release_owned(&mut output.allocation, source_bytes)?;
        ids.retire(accounting)?;
        let records_bytes = workspace_vec_bytes(&records.0);
        reverse_drop(records, records_bytes, accounting)?;
        Ok((
            NamespacePage {
                entries: output.value,
                has_more,
            },
            output.allocation,
        ))
    }
}
