use super::*;
use crate::kernel::GenerationRoot;

/// Authenticated immutable namespace without a facade or mutable checkout.
pub(crate) struct NamespaceView<'a, O> {
    pub(crate) objects: &'a O,
    pub(crate) config: crate::VolumeConfig,
    pub(crate) root: &'a GenerationRoot,
}

impl<O: AsyncObjectStore> NamespaceView<'_, O> {
    pub(crate) async fn records(
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
