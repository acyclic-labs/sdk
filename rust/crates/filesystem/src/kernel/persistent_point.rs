//! Single-key authenticated lookup sharing the canonical page and clone leaves.

use super::allocation::{KernelAccounting, KernelOwnedAllocation, VisitedObjectSet};
use super::persistent_batch::Error;
use super::persistent_btree::{Format, Page, children_within, leaf_within};
use super::{DecodeLimits, persistent_io};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters};
use crate::storage::ObjectId;

pub(crate) struct Receipt<V> {
    pub(crate) value: Option<V>,
    pub(crate) work: WorkCounters,
}
pub(crate) type Failure = OperationFailure<Error>;

pub(crate) struct Owned<T> {
    pub(crate) value: T,
    pub(crate) bytes: u64,
    pub(crate) allocation: KernelOwnedAllocation,
}

impl<T> Owned<T> {
    pub(crate) fn new(value: T, allocation: KernelOwnedAllocation, bytes: u64) -> Self {
        Self { value, allocation, bytes }
    }

    pub(crate) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        let Self { value, allocation, bytes } = self;
        drop(value);
        retire_allocation(allocation, bytes, accounting)
    }
}

impl<T> Owned<Vec<T>> {
    pub(crate) fn allocate(count: usize, accounting: &mut KernelAccounting<'_>) -> Result<Self, crate::FsError> {
        let requested = crate::foundation::usize_to_u64(
            count.checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?,
        );
        let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(count) {
            drop(values);
            #[cfg(not(target_arch = "wasm32"))]
            let native = matches!(&allocation, KernelOwnedAllocation::NativeOriginal(_));
            retire_allocation(allocation, requested, accounting)?;
            #[cfg(not(target_arch = "wasm32"))]
            if native { return Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error))); }
            drop(error);
            return Err(super::CanonicalDecodeError::AllocationFailed.into());
        }
        let actual = crate::foundation::usize_to_u64(values.capacity() * std::mem::size_of::<T>());
        let checked = accounting.reconcile_owned(&mut allocation, requested, actual);
        let owned = Self::new(values, allocation, actual);
        if let Err(error) = checked { owned.retire(accounting)?; return Err(error); }
        Ok(owned)
    }

    pub(crate) fn ensure_for_push(&mut self, maximum: usize, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if self.value.len() >= maximum { return Err(super::CanonicalDecodeError::AllocationFailed.into()); }
        if self.value.len() < self.value.capacity() { return Ok(()); }
        let next = self.value.capacity().saturating_mul(2).max(1).min(maximum);
        let previous = self.value.capacity();
        let requested = crate::foundation::usize_to_u64(
            (next - previous).checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?,
        );
        let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
        if let Err(error) = self.value.try_reserve_exact(next - self.value.len()) {
            #[cfg(not(target_arch = "wasm32"))]
            let native = matches!(&allocation, KernelOwnedAllocation::NativeOriginal(_));
            retire_allocation(allocation, requested, accounting)?;
            #[cfg(not(target_arch = "wasm32"))]
            if native { return Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error))); }
            drop(error);
            return Err(super::CanonicalDecodeError::AllocationFailed.into());
        }
        let actual = crate::foundation::usize_to_u64((self.value.capacity() - previous) * std::mem::size_of::<T>());
        let checked = accounting.reconcile_owned(&mut allocation, requested, actual);
        absorb(&mut self.allocation, allocation)?;
        self.bytes = self.bytes.checked_add(actual).ok_or(crate::WorkError::Overflow)?;
        checked
    }
}

pub(crate) fn split_allocation(allocation: &mut KernelOwnedAllocation, bytes: u64) -> Result<KernelOwnedAllocation, crate::FsError> {
    match allocation {
        KernelOwnedAllocation::Standalone => Ok(KernelOwnedAllocation::Standalone),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(guard) => Ok(KernelOwnedAllocation::NativeOriginal(guard.split_off(bytes)?)),
    }
}

pub(crate) fn retire_allocation(
    allocation: KernelOwnedAllocation,
    bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    match allocation {
        KernelOwnedAllocation::Standalone => match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.release(bytes),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.release(bytes),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => unreachable!("standalone allocation has standalone custody"),
        }.map_err(|error| error.into_work_or(crate::FsError::Decode(super::CanonicalDecodeError::AllocationFailed))),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(guard) => { drop(guard); Ok(()) }
    }
}

pub(super) fn absorb(
    destination: &mut KernelOwnedAllocation,
    mut source: KernelOwnedAllocation,
) -> Result<(), crate::FsError> {
    match (destination, &mut source) {
        (KernelOwnedAllocation::Standalone, KernelOwnedAllocation::Standalone) => Ok(()),
        #[cfg(not(target_arch = "wasm32"))]
        (KernelOwnedAllocation::NativeOriginal(destination), KernelOwnedAllocation::NativeOriginal(source)) => {
            destination.absorb(source)?;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        _ => unreachable!("one query never crosses allocation ledgers"),
    }
}

pub(crate) async fn lookup_async<S, F>(
    store: &S, root: ObjectId, key: &F::Key, limits: DecodeLimits,
    budget: WorkBudget, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<Receipt<F::Value>, Failure>
where S: AsyncObjectStore, F: Format {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = lookup_async_with_accounting::<S, F>(
        store, root, key, limits, &mut accounting, cancellation, data_custody,
    ).await;
    let work = accounting.spent();
    result.map(|(value, _)| Receipt { value, work })
        .map_err(|error| OperationFailure::new(project_failure(error), work))
}

pub(crate) async fn lookup_async_with_accounting<S, F>(
    store: &S, root: ObjectId, key: &F::Key, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(Option<F::Value>, KernelOwnedAllocation), crate::FsError>
where S: AsyncObjectStore, F: Format {
    accounting.check()?;
    if root.kind != F::kind() { return Err(query_error(Error::WrongRootKind)); }
    if !limits.page_limits_valid(1) { return Err(query_error(Error::InvalidLimits)); }
    let mut visited = VisitedObjectSet::new_with_accounting(
        usize::from(limits.maximum_page_height), accounting,
    )?;
    let mut lower: Option<Owned<F::Key>> = None;
    let mut upper: Option<Owned<F::Key>> = None;
    let mut page = root;
    let mut height = 0_usize;
    let result = async {
        loop {
            cancellation.check()?;
            accounting.check()?;
            if height >= usize::from(limits.maximum_page_height) {
                return Err(query_error(Error::HeightExceeded));
            }
            height += 1;
            let inserted = VisitedObjectSet::insert_with_accounting(&mut visited, page, accounting)?;
            if !inserted.inserted { return Err(query_error(Error::CycleOrAlias)); }
            let (decoded, allocation) = persistent_io::read_page_with_accounting::<S, F>(
                store, page, limits, accounting, cancellation, data_custody,
            ).await?;
            let decoded_bytes = decoded.logical_bytes;
            let decoded = Owned::new(decoded, allocation, decoded_bytes);
            let view = match &decoded.value.page {
                persistent_io::PageLease::Owned(page) => page,
                persistent_io::PageLease::Shared { page, .. } => page.as_ref(),
            };
            let step = (|| {
                match view {
                    Page::Leaf(values) => {
                        if !leaf_within::<F>(values, lower.as_ref().map(|key| &key.value), upper.as_ref().map(|key| &key.value)) {
                            return Err(query_error(Error::ChildBoundsMismatch));
                        }
                        let position = super::search::binary_search_with_accounting(values, accounting, |value| F::key(value).cmp(key))?;
                        let result = position.ok()
                            .and_then(|index| values.get(index))
                            .map(|value| persistent_io::clone_value_with_accounting_and_custody::<F>(value, limits.maximum_name_bytes, accounting, data_custody, 0))
                            .transpose()?;
                        Ok(Some(match result {
                            Some((value, allocation)) => (Some(value), allocation),
                            None => (None, accounting.claim_owned(0, 0)?),
                        }))
                    }
                    Page::Internal(children) => {
                        if !children_within(children, lower.as_ref().map(|key| &key.value), upper.as_ref().map(|key| &key.value)) {
                            return Err(query_error(Error::ChildBoundsMismatch));
                        }
                        let selected = super::search::partition_point_with_accounting(children, accounting, |child| child.first <= *key)?.saturating_sub(1);
                        let child = children.get(selected).ok_or_else(|| query_error(Error::InvalidRouting))?;
                        if let Some(previous) = lower.take() { previous.retire(accounting)?; }
                        let (value, allocation) = persistent_io::clone_key_with_accounting_and_custody::<F>(&child.first, limits.maximum_name_bytes, accounting, data_custody, 0)?;
                        let bytes = F::key_owned_bytes(&value);
                        lower = Some(Owned::new(value, allocation, bytes));
                        if let Some(next) = children.get(selected + 1) {
                            if let Some(previous) = upper.take() { previous.retire(accounting)?; }
                            let (value, allocation) = persistent_io::clone_key_with_accounting_and_custody::<F>(&next.first, limits.maximum_name_bytes, accounting, data_custody, 0)?;
                            let bytes = F::key_owned_bytes(&value);
                            upper = Some(Owned::new(value, allocation, bytes));
                        }
                        page = child.page;
                        Ok(None)
                    }
                }
            })();
            decoded.retire(accounting)?;
            if let Some(result) = step? { return Ok(result); }
        }
    }.await;
    if let Some(lower) = lower { lower.retire(accounting)?; }
    if let Some(upper) = upper { upper.retire(accounting)?; }
    visited.retire(accounting)?;
    result
}

pub(super) fn query_error(error: Error) -> crate::FsError {
    crate::FsError::FileRecord(super::file_table::map_batch_error(error))
}

pub(super) fn project_failure(error: crate::FsError) -> Error {
    match error {
        crate::FsError::FileRecord(error) => super::file_table::unmap_batch_error(error),
        crate::FsError::Cancelled(_) => Error::Cancelled,
        crate::FsError::Work(error) => Error::Work(error),
        crate::FsError::Object(error) => Error::Storage(error),
        crate::FsError::Decode(super::CanonicalDecodeError::AllocationFailed) => Error::AllocationFailed,
        crate::FsError::PendingMutationAllocationFailed => Error::AllocationFailed,
        crate::FsError::Decode(error) => Error::Decode(error),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => Error::Storage(crate::ObjectStoreError::Native(error)),
        _ => unreachable!("canonical query leaves return only typed query, object, decode, work or cancellation errors"),
    }
}

#[cfg(test)]
#[path = "tests/persistent_point.rs"]
mod tests;
