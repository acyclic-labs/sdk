//! One authenticated, bounded page-read boundary for persistent tree kernels.

use super::allocation::{
    AllocationError, AllocationLedger, KernelAccounting, KernelOwnedAllocation,
};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::file_read::{ReadOwned, read_store_error, release_read_allocation};
use super::persistent_btree::shared::SharedPage;
use super::persistent_btree::{Child, Format, Page, PageRef};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::FsError;
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

pub(crate) struct OwnedPage<F: Format> {
    pub(crate) page: PageLease<F>,
    pub(crate) logical_bytes: u64,
}

pub(crate) enum PageLease<F: Format> {
    Owned(Page<F>),
    Shared {
        page: Arc<SharedPage<F>>,
        #[cfg(not(target_arch = "wasm32"))]
        _native_custody: Option<Arc<crate::NativeAllocation>>,
    },
}

impl<F: Format> PageLease<F> {
    pub(crate) fn view(&self) -> PageRef<'_, F> {
        match self {
            Self::Owned(Page::Leaf(values)) => PageRef::Leaf(values.into()),
            Self::Owned(Page::Internal(children)) => PageRef::Internal(children.into()),
            Self::Shared { page, .. } => page.view(),
        }
    }
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
            shared @ PageLease::Shared { .. } => {
                let result = (|| {
                    let mut observed = super::file_read::OriginalReadAccounting::new(accounting)?;
                    match clone_page::<F>(shared.view(), &mut |step| observed.observe(step)) {
                        Ok(page) => {
                            let (allocation, bytes) = observed.into_parts();
                            Ok(((page, bytes), allocation))
                        }
                        Err(error) => {
                            observed.release_failed()?;
                            Err(error)
                        }
                    }
                })();
                drop(shared);
                super::persistent_point::retire_allocation(
                    input.custody,
                    input.value.logical_bytes,
                    accounting,
                )?;
                result
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
mod batch;
mod cache;
mod decode;
mod publish;
pub(crate) use publish::{retain_written_internal, retain_written_leaf};

pub(crate) use batch::read_pages_with_accounting;
use cache::admit_decoded_with_accounting;
pub(crate) use cache::{cached_page_with_accounting, read_page_with_accounting};
use decode::claim_read_allocation;
use decode::clone_page;
pub(crate) use decode::{clone_key, clone_value, decode_read_with_accounting};

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "../tests/persistent_io.rs"]
mod tests;
