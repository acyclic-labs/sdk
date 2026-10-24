//! One authenticated, bounded page-read boundary for persistent tree kernels.

use super::allocation::{
    AllocationError, AllocationLedger, KernelAccounting, KernelOwnedAllocation,
};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::file_read::{ReadOwned, read_store_error, release_read_allocation};
use super::persistent_btree::{Child, Format, Page};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::FsError;
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::heap_future::in_heap;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

pub(crate) struct OwnedPage<F: Format> {
    pub(crate) page: PageLease<F>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<Arc<crate::NativeAllocation>>,
    pub(crate) logical_bytes: u64,
}

pub(crate) enum PageLease<F: Format> {
    Owned(Page<F>),
    Shared {
        page: Arc<Page<F>>,
        logical_bytes: u64,
    },
}

impl<F: Format> OwnedPage<F> {
    pub(crate) fn into_owned_with_accounting(
        self,
        source: super::allocation::KernelOwnedAllocation,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<((Page<F>, u64), super::allocation::KernelOwnedAllocation), FsError> {
        let input = super::file_read::ReadOwned {
            value: self,
            custody: source,
        };
        match input.value.page {
            PageLease::Owned(page) => Ok(((page, input.value.logical_bytes), input.custody)),
            PageLease::Shared {
                page,
                logical_bytes,
            } => {
                #[cfg(not(target_arch = "wasm32"))]
                if accounting.native_work().is_some() {
                    let cloned = super::file_read::decode_owned(accounting, |steps| {
                        clone_page::<F>(&page, &mut |step| steps(step))
                    })?;
                    let bytes = match &cloned.1 {
                        super::allocation::KernelOwnedAllocation::NativeOriginal(owned) => {
                            owned.bytes()
                        }
                        super::allocation::KernelOwnedAllocation::Standalone => 0,
                    };
                    return Ok(((cloned.0, bytes), cloned.1));
                }
                let (allocations, work, budget, _) = accounting
                    .standalone_parts()
                    .ok_or(crate::ObjectStoreError::Corrupt)?;
                allocations
                    .claim_bytes(logical_bytes, u64::from(logical_bytes != 0), work, budget)
                    .map_err(allocation_error)?;
                charge_copy(logical_bytes, &budget, allocations, work)?;
                Ok((
                    ((*page).clone(), logical_bytes),
                    super::allocation::KernelOwnedAllocation::Standalone,
                ))
            }
        }
    }
}

fn allocation_error(error: AllocationError) -> FsError {
    error.into_work_or(CanonicalDecodeError::AllocationFailed.into())
}

fn retained_bytes(read: &ObjectRead) -> u64 {
    match read.retention {
        ObjectReadRetention::Shared => 0,
        ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    }
}

fn invalid_batch_result() -> FsError {
    read_store_error(ObjectStoreError::Rejected(
        "object store returned an invalid page batch".to_owned(),
    ))
}

const PAGE_READ: WorkCounters = WorkCounters {
    page_reads: 1,
    ..WorkCounters::UNCHARGED
};
fn charge_copy(
    copied: u64,
    budget: &WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<(), FsError> {
    match work.charge(
        &WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::default()
        },
        budget,
    ) {
        Ok(()) => Ok(()),
        Err(WorkError::Overflow) => Err(WorkError::Overflow.into()),
        Err(error) => {
            allocations.release(copied).map_err(allocation_error)?;
            Err(error.into())
        }
    }
}

mod batch;
mod cache;
mod decode;

pub(crate) use batch::read_pages_with_accounting;
use cache::admit_decoded_with_accounting;
pub(crate) use cache::read_page_with_accounting;
use decode::claim_read_allocation;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
use decode::clone_page;
pub(crate) use decode::{clone_key, clone_value, decode_read_with_accounting};

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "../tests/persistent_io.rs"]
mod tests;
