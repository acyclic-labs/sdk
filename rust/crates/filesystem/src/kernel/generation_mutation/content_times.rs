//! A regular file whose bytes or length change takes the batch's change time.
//! The stamp reuses the record the batch already resolved, so it costs one
//! metadata read and at most one metadata write per changed file, charged to
//! the batch's work, whatever the number of operations on that file.

use super::{KernelAccounting, Owned, TransactionState};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::FileId;
use crate::kernel::DecodeLimits;
use crate::model::VolumeConfig;

#[cfg(test)]
thread_local! {
    /// Metadata records stamped on this thread, for work-count tests.
    pub(crate) static STAMPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl Owned<TransactionState> {
    pub(super) async fn stamp_content_change<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        id: FileId,
        at_ns: i64,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        let Some(stored) = self.leaf(accounting, |state, leaf| {
            let record = state.record_mut(leaf, id)?;
            Ok((!record.stamped).then_some(record.working.metadata))
        })?
        else {
            return Ok(());
        };
        let mut metadata = crate::kernel::merge::read_metadata(
            store,
            stored,
            DecodeLimits::for_volume(config),
            accounting,
            cancellation,
        )
        .await?;
        let metadata = if metadata.stamp_content_change(at_ns) {
            crate::kernel::merge::write_metadata(store, metadata, accounting, cancellation).await?
        } else {
            stored
        };
        #[cfg(test)]
        STAMPS.with(|stamps| stamps.set(stamps.get() + 1));
        self.leaf(accounting, |state, leaf| {
            let record = state.record_mut(leaf, id)?;
            record.working.metadata = metadata;
            record.stamped = true;
            Ok(())
        })
    }
}
