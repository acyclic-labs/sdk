use super::*;
use crate::kernel::GenerationRoot;

/// Authenticated immutable namespace without a facade or mutable checkout.
pub(crate) struct NamespaceView<'a, O> {
    pub(crate) objects: &'a O,
    pub(crate) config: crate::VolumeConfig,
    pub(crate) root: &'a GenerationRoot,
}

impl<O: AsyncObjectStore> NamespaceView<'_, O> {
    /// Reconstructs only the first authenticated alias for one identity.
    /// No selected-record or alias collection is allocated.
    pub(crate) async fn first_path(
        &self,
        file_id: FileId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::kernel::Owned<NamespacePath>>, FsError> {
        let mut accounting = accounting.allocation_scope()?;
        let result = async {
            let record = {
                let mut lookup = accounting.allocation_scope()?;
                crate::kernel::lookup_file_record_async_with_accounting(
                    self.objects,
                    self.root.file_table,
                    file_id,
                    crate::kernel::DecodeLimits::for_volume(self.config),
                    &mut lookup,
                    cancellation,
                    None,
                )
                .await?
                .0
            };
            let Some(record) = record else {
                return Ok(None);
            };
            let mut paths = crate::kernel::direct_paths::DirectPaths::new(
                self.root,
                record,
                self.config,
                &mut accounting,
            )?;
            let result = paths
                .next(self.objects, &mut accounting, cancellation)
                .await;
            let retired = paths.retire(&mut accounting);
            result.and_then(|path| retired.map(|()| path))
        }
        .await;
        if result.is_ok() {
            accounting.keep();
        }
        result
    }

    pub(crate) async fn records(
        &self,
        ids: &[FileId],
        maximum_entries: impl Into<Option<u32>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
        let maximum_entries = maximum_entries.into();
        let mut matches = (
            NamespaceRecordSearch {
                records: NamespaceRecords::new(),
                complete: ids.is_empty(),
            },
            accounting.claim_owned(0, 0)?,
        );
        if ids.is_empty() || maximum_entries == Some(0) {
            return Ok(matches);
        }
        let result = super::reverse_traversal::collect_selected(
            self,
            ids,
            maximum_entries,
            &mut matches,
            accounting,
            cancellation,
        )
        .await;
        match result {
            Ok(complete) => matches.0.complete = complete,
            Err(error) => {
                let bytes = super::reverse_traversal::namespace_records_bytes(&matches.0.records);
                reverse_drop(matches, bytes, accounting)?;
                return Err(error);
            }
        }
        Ok(matches)
    }
}
