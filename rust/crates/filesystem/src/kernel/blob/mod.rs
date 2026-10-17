//! Authenticated fixed-size blob chunks and sparse range reads.

use super::allocation::{AllocationError, LogicalVecCapacity, VisitedObjectSet};
use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::codec::ReadAllocationStep;
use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::file_read::{
    ReadOwned, allocate_read_buffer, owned_read_bytes, read_object_with_accounting,
    release_read_allocation, reserve_decoded_items,
};
use crate::FsError;
use crate::async_storage::{AsyncObjectStore, DecodedCacheKey};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{
    ByteRange, HashedObject, ObjectId, ObjectKind, ObjectReadRequest, ObjectStoreError,
    object_digest,
};
use std::future::Future;
use std::io::Read;
use std::mem::size_of;
use thiserror::Error;

const DOMAIN: &[u8; 8] = b"ACYFSBLB";
const VERSION: u16 = 1;
const BLOB_PAGE_HEADER_BYTES: usize = 8 + 2 + 8 + 8 + 1 + 4;
const BLOB_PAGE_ITEM_BYTES: usize = 8 + 8 + 32;

/// One authenticated fixed-size content chunk reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobChunkRef {
    /// Inclusive logical offset.
    pub first_offset: u64,
    /// Exclusive logical end.
    pub end_offset: u64,
    /// Complete immutable chunk object.
    pub chunk: ObjectId,
}

/// One authenticated blob-index child reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobChild {
    /// Inclusive logical offset.
    pub first_offset: u64,
    /// Exclusive logical end.
    pub end_offset: u64,
    /// Child blob-index page.
    pub page: ObjectId,
}

/// Contents of one blob-index page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlobNode {
    /// Ordered chunk references.
    Leaf(Vec<BlobChunkRef>),
    /// Ordered index-page references.
    Internal(Vec<BlobChild>),
}

/// One self-bounded authenticated blob-index page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobPage {
    /// Inclusive logical offset represented by this page.
    pub first_offset: u64,
    /// Exclusive logical end represented by this page.
    pub end_offset: u64,
    /// Leaf chunks or internal children.
    pub node: BlobNode,
}

impl BlobPage {
    fn validate(&self, maximum_items: u32) -> Result<(), CanonicalDecodeError> {
        let (count, ranges_valid, kinds_valid) = match &self.node {
            BlobNode::Leaf(items) => (
                items.len(),
                contiguous(
                    self.first_offset,
                    self.end_offset,
                    items
                        .iter()
                        .map(|item| (item.first_offset, item.end_offset)),
                ),
                items
                    .iter()
                    .all(|item| item.chunk.kind == ObjectKind::BlobChunk),
            ),
            BlobNode::Internal(items) => (
                items.len(),
                contiguous(
                    self.first_offset,
                    self.end_offset,
                    items
                        .iter()
                        .map(|item| (item.first_offset, item.end_offset)),
                ),
                items.iter().all(|item| item.page.kind == ObjectKind::Blob),
            ),
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(CanonicalDecodeError::FieldTooLarge {
                observed: u32::try_from(count).unwrap_or(u32::MAX),
                maximum: maximum_items,
            });
        }
        if count == 0 && (self.first_offset != 0 || self.end_offset != 0) {
            return Err(invariant("non-empty blob bounds require entries"));
        }
        if count != 0 && (!ranges_valid || self.first_offset >= self.end_offset) {
            return Err(invariant("blob ranges are not positive and contiguous"));
        }
        if !kinds_valid {
            return Err(invariant("blob reference has the wrong object kind"));
        }
        Ok(())
    }
}

fn contiguous(first: u64, end: u64, mut ranges: impl Iterator<Item = (u64, u64)>) -> bool {
    let Some((item_first, item_end)) = ranges.next() else {
        return first == 0 && end == 0;
    };
    if item_first != first || item_first >= item_end {
        return false;
    }
    let mut prior_end = item_end;
    for (next_first, next_end) in ranges {
        if next_first != prior_end || next_first >= next_end {
            return false;
        }
        prior_end = next_end;
    }
    prior_end == end
}

mod codec;
pub(crate) use codec::blob_page_encoded_length;
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) use codec::page_write_step;
use codec::*;
pub use codec::{blob_page_id, decode_blob_page, encode_blob_page};
pub(crate) use codec::{decode_blob_page_with_accounting, encode_blob_page_with_accounting};
mod source;
use source::*;
pub use source::{AsyncBlobSource, OwnedSourceChunk};
mod build;
pub(crate) use build::build_blob_with_accounting;
use build::*;
pub use build::{BlobBuild, BlobBuildOptions, build_blob};
mod index;
use index::*;
mod range;
pub(crate) use range::read_blob_range_with_accounting;
pub use range::{BlobRead, read_blob_range};
mod error;
#[cfg(not(target_arch = "wasm32"))]
pub use error::BlobDemandFailure;
use error::*;
pub use error::{BlobBuildError, BlobBuildFailure, BlobReadError, BlobReadFailure};

#[cfg(all(test, feature = "memory"))]
#[path = "../tests/blob/mod.rs"]
mod tests;
