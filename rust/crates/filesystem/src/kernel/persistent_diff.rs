//! Bounded Merkle-aware value diffs for persistent authenticated B+trees.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::file_table_mutation::FileTableFormat;
use super::persistent_btree::{Format, Page};
use super::persistent_io::{self, OwnedPage, PageLease};
use super::persistent_point::{Owned, retire_allocation, split_allocation};
use super::tree_mutation::TreeFormat;
use super::{CanonicalDecodeError, DecodeLimits, FileRecord, LogicalName, TreeEntry};
use crate::FsError;
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{WorkCounters, WorkError};
use crate::storage::ObjectId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValueChange<K, V> {
    pub(crate) key: K,
    pub(crate) before: Option<V>,
    pub(crate) after: Option<V>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Diff<K, V> {
    pub(crate) changes: Vec<ValueChange<K, V>>,
    pub(crate) truncated: bool,
    pub(crate) work: WorkCounters,
    pub(crate) retained_allocation_bytes: u64,
}

mod frontier;
use frontier::Cursor;

type Changes<F> = Owned<
    Vec<
        ValueChange<
            <F as acyclic_collections::page::Format>::Key,
            <F as acyclic_collections::page::Format>::Value,
        >,
    >,
>;

fn page_view<F: Format>(page: &OwnedPage<F>) -> &Page<F> {
    match &page.page {
        PageLease::Owned(page) => page,
        PageLease::Shared { page, .. } => page.as_ref(),
    }
}

async fn diff_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Diff<F::Key, F::Value>, KernelOwnedAllocation), FsError> {
    if maximum_changes == 0 {
        return Err(FsError::InvalidDiff);
    }
    let mut cursor = Cursor::<F>::new(before, after, limits, accounting)?;
    let mut values = match Changes::<F>::allocate(0, accounting) {
        Ok(values) => values,
        Err(error) => {
            cursor.retire(accounting)?;
            return Err(allocation_error(error));
        }
    };
    let maximum = usize::try_from(maximum_changes).unwrap_or(usize::MAX);
    let result = async {
        while let Some(change) = cursor.next(store, accounting, cancellation).await? {
            if values.value.len() == maximum {
                change.retire(accounting)?;
                return Ok(true);
            }
            if let Err(error) = values.ensure_for_push(maximum, accounting) {
                change.retire(accounting)?;
                return Err(error);
            }
            let Owned {
                value,
                mut allocation,
                bytes,
            } = change;
            values.value.push(value);
            values.allocation.absorb(&mut allocation)?;
            values.bytes = values.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
        }
        Ok(false)
    }
    .await;
    let retirement = cursor.retire(accounting);
    match result.and_then(|truncated| retirement.map(|()| truncated)) {
        Err(error) => {
            values.retire(accounting)?;
            Err(allocation_error(error))
        }
        Ok(truncated) => {
            let Owned {
                value: changes,
                allocation,
                bytes,
            } = values;
            Ok((
                Diff {
                    changes,
                    truncated,
                    work: accounting.spent(),
                    retained_allocation_bytes: bytes,
                },
                allocation,
            ))
        }
    }
}

fn allocation_error(error: FsError) -> FsError {
    match error {
        FsError::Decode(CanonicalDecodeError::AllocationFailed) => FsError::DiffAllocationFailed,
        other => other,
    }
}

pub(crate) async fn diff_file_records_with_accounting<S: AsyncObjectStore>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Diff<crate::FileId, FileRecord>, KernelOwnedAllocation), FsError> {
    diff_with_accounting::<S, FileTableFormat>(
        store,
        before,
        after,
        maximum_changes,
        limits,
        accounting,
        cancellation,
    )
    .await
}

pub(crate) async fn diff_tree_entries_with_accounting<S: AsyncObjectStore>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Diff<LogicalName, TreeEntry>, KernelOwnedAllocation), FsError> {
    diff_with_accounting::<S, TreeFormat>(
        store,
        before,
        after,
        maximum_changes,
        limits,
        accounting,
        cancellation,
    )
    .await
}

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "tests/persistent_diff.rs"]
mod tests;
