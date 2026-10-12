//! Authenticated fixed-size blob chunks and sparse range reads.

use super::allocation::{AllocationError, LogicalVecCapacity, VisitedObjectSet};
use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::file_read::{ReadOwned, ReadDecodeError, reserve_decoded_items_with_steps,
    allocate_read_buffer, completed_read_work, owned_read_bytes, read_object_with_accounting, release_read_allocation};
use crate::FsError;
use super::codec::{
    CanonicalDecodeError, DecodeLimits, Decoder, Encoder,
};
use super::codec::ReadAllocationStep;
#[cfg(test)]
use super::codec::{DecodedPageKind, DecodedPageShape};
use crate::async_storage::{AsyncObjectStore, DecodedCacheKey};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::heap_future::in_heap;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{
    ByteRange, HashedObject, ObjectId, ObjectKind, ObjectReadRequest,
    ObjectReadRetention, ObjectStore, ObjectStoreError, object_digest,
};
use bytes::Bytes;
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

/// Encodes one canonical blob-index page.
///
/// # Errors
///
/// Rejects unbounded, discontinuous, empty-with-bounds, or wrongly typed pages.
pub fn encode_blob_page(
    page: &BlobPage,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    encode_blob_page_with_steps(
        page, maximum_items, |_| Ok(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
}

/// Observes the real reserve/capacity/header boundaries of the same canonical writer.
pub(crate) fn encode_blob_page_with_steps<E: From<CanonicalDecodeError>>(
    page: &BlobPage,
    maximum_items: u32,
    observe: impl FnMut(super::codec::EncodeAllocationStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    let encoded_length = blob_page_encoded_length(page, maximum_items)?;
    let encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, encoded_length, observe, allocation_error,
    )?;
    encode_blob_page_body(page, encoder).map_err(Into::into)
}

fn encode_blob_page_body(
    page: &BlobPage, mut encoder: Encoder,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    encoder.u64(page.first_offset);
    encoder.u64(page.end_offset);
    match &page.node {
        BlobNode::Leaf(items) => {
            encoder.u8(1);
            encoder.u32(count(items.len())?);
            for item in items {
                encoder.u64(item.first_offset);
                encoder.u64(item.end_offset);
                encoder.fixed(item.chunk.digest.as_bytes());
            }
        }
        BlobNode::Internal(items) => {
            encoder.u8(2);
            encoder.u32(count(items.len())?);
            for item in items {
                encoder.u64(item.first_offset);
                encoder.u64(item.end_offset);
                encoder.fixed(item.page.digest.as_bytes());
            }
        }
    }
    Ok(encoder.finish())
}

fn blob_page_encoded_length(
    page: &BlobPage,
    maximum_items: u32,
) -> Result<usize, CanonicalDecodeError> {
    page.validate(maximum_items)?;
    let items = match &page.node {
        BlobNode::Leaf(items) => items.len(),
        BlobNode::Internal(items) => items.len(),
    };
    count(items)?;
    items
        .checked_mul(BLOB_PAGE_ITEM_BYTES)
        .and_then(|bytes| BLOB_PAGE_HEADER_BYTES.checked_add(bytes))
        .ok_or(CanonicalDecodeError::LengthOverflow)
}

/// Decodes one canonical blob-index page under hard allocation limits.
///
/// # Errors
///
/// Fails closed on malformed bytes, unknown versions/tags, or bad ranges.
pub fn decode_blob_page(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<BlobPage, CanonicalDecodeError> {
    decode_blob_page_with_steps(bytes, limits, &mut |_| Ok(()))
}

pub(crate) fn decode_blob_page_with_accounting(
    bytes: &[u8], limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
) -> Result<(BlobPage, u64, KernelOwnedAllocation), FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let native = accounting.native_work().is_some();
    let mut observed = super::file_read::OriginalReadAccounting::new(accounting)?;
    let decoded = decode_blob_page_with_steps::<ReadDecodeError>(
        bytes, limits, &mut |step| observed.observe(step),
    );
    let page = match decoded {
        Ok(page) => page,
        Err(error) => {
            observed.release_failed()?;
            #[cfg(not(target_arch = "wasm32"))]
            if native {
                return Err(match error.into_original_fs_error() {
                    FsError::Decode(error) => BlobReadError::Decode(error).into(),
                    error => error,
                });
            }
            return Err(read_decode_error(error));
        }
    };
    let owner = ReadOwned { value: page, custody: observed.into_allocation() };
    let bytes = match &owner.value.node {
        BlobNode::Leaf(items) => items.capacity() * size_of::<BlobChunkRef>(),
        BlobNode::Internal(items) => items.capacity() * size_of::<BlobChild>(),
    };
    Ok((owner.value, usize_to_u64(bytes), owner.custody))
}

pub(crate) fn decode_blob_page_with_steps<E: From<CanonicalDecodeError>>(
    bytes: &[u8], limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<BlobPage, E> {
    let mut decoder = Decoder::new_with_steps(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let first_offset = decoder.u64_with_steps(observe)?;
    let end_offset = decoder.u64_with_steps(observe)?;
    let tag = decoder.u8_with_steps(observe)?;
    let item_count = decoder.u32_with_steps(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }.into());
    }
    let mut leaf = Vec::new();
    let mut internal = Vec::new();
    match tag {
        1 => {
            reserve_decoded_items_with_steps(&mut leaf, capacity(item_count)?, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                leaf.push(BlobChunkRef {
                    first_offset: decoder.u64_with_steps(observe)?,
                    end_offset: decoder.u64_with_steps(observe)?,
                    chunk: ObjectId {
                        kind: ObjectKind::BlobChunk,
                        digest: crate::foundation::Digest::from_bytes(decoder.fixed_with_steps::<32, E>(observe)?),
                    },
                });
                observe(ReadAllocationStep::Item)?;
            }
        }
        2 => {
            reserve_decoded_items_with_steps(&mut internal, capacity(item_count)?, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                internal.push(BlobChild {
                    first_offset: decoder.u64_with_steps(observe)?,
                    end_offset: decoder.u64_with_steps(observe)?,
                    page: ObjectId {
                        kind: ObjectKind::Blob,
                        digest: crate::foundation::Digest::from_bytes(decoder.fixed_with_steps::<32, E>(observe)?),
                    },
                });
                observe(ReadAllocationStep::Item)?;
            }
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "blob_page",
                tag,
            }.into());
        }
    }
    decoder.finish()?;
    let page = BlobPage {
        first_offset,
        end_offset,
        node: if tag == 1 {
            BlobNode::Leaf(leaf)
        } else {
            BlobNode::Internal(internal)
        },
    };
    page.validate(limits.maximum_page_items)?;
    Ok(page)
}

#[cfg(test)]
fn blob_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    decoder.u64()?;
    decoder.u64()?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let kind = match tag {
        1 | 2 => {
            for _ in 0..item_count {
                decoder.u64()?;
                decoder.u64()?;
                let _: [u8; 32] = decoder.fixed()?;
            }
            if tag == 1 {
                DecodedPageKind::Leaf
            } else {
                DecodedPageKind::Internal
            }
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "blob_page",
                tag,
            });
        }
    };
    decoder.finish()?;
    Ok(DecodedPageShape {
        kind,
        items: usize::try_from(item_count).map_err(|_| CanonicalDecodeError::LengthOverflow)?,
        nested_bytes: 0,
    })
}

/// Computes the typed identity of one canonical blob-index page.
///
/// # Errors
///
/// Returns the same validation errors as [`encode_blob_page`].
pub fn blob_page_id(page: &BlobPage, maximum_items: u32) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_blob_page(page, maximum_items)?;
    Ok(ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &bytes),
    })
}

/// Hard bounds for streaming immutable-blob construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobBuildOptions {
    /// Positive maximum bytes per authenticated content chunk.
    pub chunk_bytes: u32,
    /// At least two references per blob-index page.
    pub page_items: u32,
    /// Maximum canonical bytes in one authenticated blob-index page.
    pub page_bytes: u32,
    /// Maximum logical bytes consumed from the source.
    pub maximum_blob_bytes: u64,
}

/// Successful streaming blob construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobBuild {
    /// Authenticated blob-index root.
    pub root: ObjectId,
    /// Exact logical content length.
    pub logical_bytes: u64,
    /// Exact source, encoding, and backend work receipt.
    pub work: WorkCounters,
}

/// An owned source chunk together with its actual logical backing capacity.
///
/// A slice may retain more bytes than it exposes. Static storage owns no
/// allocation; allocator/refcount bookkeeping headers are not logical payload.
#[derive(Debug)]
pub struct OwnedSourceChunk {
    /// Exact bytes supplied by this read.
    pub bytes: acyclic_stream::OwnedBytes,
    /// Payload capacity retained by these bytes, not their visible length.
    pub logical_owned_bytes: u64,
    intrinsic: bool,
}


impl OwnedSourceChunk {
    /// Borrows the supplied bytes without detaching their native custody.
    #[must_use]
    pub fn bytes(&self) -> &acyclic_stream::OwnedBytes { &self.bytes }

    /// Exact backing capacity reported by the physical source producer.
    #[must_use]
    pub const fn logical_owned_bytes(&self) -> u64 { self.logical_owned_bytes }

    /// Bytes whose retained capacity is accounted by the ordinary caller.
    pub fn claimed(bytes: acyclic_stream::OwnedBytes, logical_owned_bytes: u64) -> Self {
        Self { bytes, logical_owned_bytes, intrinsic: false }
    }

    /// Bytes already retained by their original native allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn intrinsic(bytes: acyclic_stream::OwnedBytes, logical_owned_bytes: u64) -> Self {
        Self { bytes, logical_owned_bytes, intrinsic: true }
    }

    /// Whether native ownership already accounts for this chunk's capacity.
    pub const fn is_intrinsic(&self) -> bool { self.intrinsic }

    /// Transfers bytes and native custody without releasing the allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_owned_parts(self) -> (acyclic_stream::OwnedBytes, u64, bool) {
        (self.bytes, self.logical_owned_bytes, self.intrinsic)
    }
}

/// Nonblocking bounded byte source for browser, network, and native streams.
pub trait AsyncBlobSource {
    /// Fills some prefix of `destination`, returning zero only at end of stream.
    fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> impl Future<Output = std::io::Result<usize>>;

    /// Optionally returns one owned chunk without copying into a caller buffer.
    fn read_owned(
        &mut self,
        _maximum: usize,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = std::io::Result<Option<OwnedSourceChunk>>> {
        async { Ok(None) }
    }

    /// Reads directly under the original invocation; custom sources reject before I/O.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work<'a>(
        &'a mut self, _destination: &'a mut [u8],
        _original: &'a crate::NativeMaterializationWork,
        _cancellation: &'a CancellationToken,
    ) -> impl Future<Output = Result<usize, FsError>> {
        async { Err(BlobBuildError::NativeSourceUnavailable.into()) }
    }

    /// Returns intrinsically guarded bytes without transferring their allocation twice.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_owned_with_native_work(
        &mut self, _maximum: usize, _original: &crate::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<Option<OwnedSourceChunk>, FsError>> {
        async { Ok(None) }
    }

    /// Drains exact work performed by the preceding read, when the source
    /// already measures its own I/O. Returning `Some` replaces the builder's
    /// inferred source-byte count for that read; `None` retains that count.
    fn take_work(&mut self) -> Option<WorkCounters> {
        None
    }
}

struct ImmediateBlobSource<'a, T>(&'a mut T);

impl<T: Read> AsyncBlobSource for ImmediateBlobSource<'_, T> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "blob source read cancelled",
            ));
        }
        let maximum = destination.len().min(usize::try_from(budget.source_bytes_read).unwrap_or(usize::MAX));
        if maximum == 0 && !destination.is_empty() {
            return Err(std::io::Error::other("blob source byte budget exhausted"));
        }
        Read::read(self.0, destination.get_mut(..maximum)
            .ok_or_else(|| std::io::Error::other("invalid bounded source buffer"))?)
    }
}

impl<T: AsRef<[u8]>> AsyncBlobSource for std::io::Cursor<T> {
    async fn read<'a>(
        &'a mut self, destination: &'a mut [u8], budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        ImmediateBlobSource(self).read(destination, budget, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self, destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        cancellation.check()?;
        let remaining = self.get_ref().as_ref().len()
            .saturating_sub(usize::try_from(self.position()).unwrap_or(usize::MAX));
        let maximum = destination.len().min(remaining);
        let delta = WorkCounters {
            source_bytes_read: usize_to_u64(maximum),
            bytes_copied: usize_to_u64(maximum),
            ..WorkCounters::UNCHARGED
        };
        original.admit(delta)?;
        let read = Read::read(self, destination)
            .map_err(BlobBuildError::Source)?;
        original.spent(WorkCounters {
            source_bytes_read: usize_to_u64(read),
            bytes_copied: usize_to_u64(read),
            ..WorkCounters::UNCHARGED
        })?;
        Ok(read)
    }
}

impl AsyncBlobSource for &[u8] {
    async fn read<'a>(
        &'a mut self, destination: &'a mut [u8], budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        ImmediateBlobSource(self).read(destination, budget, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self, destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork, cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        cancellation.check()?;
        let maximum = destination.len().min(self.len());
        let delta = WorkCounters { source_bytes_read: usize_to_u64(maximum),
            bytes_copied: usize_to_u64(maximum), ..WorkCounters::UNCHARGED };
        original.admit(delta)?;
        let read = Read::read(self, destination).map_err(BlobBuildError::Source)?;
        original.spent(WorkCounters { source_bytes_read: usize_to_u64(read),
            bytes_copied: usize_to_u64(read), ..WorkCounters::UNCHARGED })?;
        Ok(read)
    }
}

impl AsyncBlobSource for std::fs::File {
    async fn read<'a>(
        &'a mut self, destination: &'a mut [u8], budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        ImmediateBlobSource(self).read(destination, budget, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self, destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork, cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        cancellation.check()?;
        let maximum = destination.len().min(
            usize::try_from(original.remaining()?.source_bytes_read).unwrap_or(usize::MAX),
        );
        if maximum == 0 && !destination.is_empty() {
            original.admit(WorkCounters { source_bytes_read: 1, ..WorkCounters::UNCHARGED })?;
        }
        original.admit(WorkCounters { source_bytes_read: usize_to_u64(maximum),
            ..WorkCounters::UNCHARGED })?;
        let destination = destination.get_mut(..maximum).ok_or(BlobBuildError::TooLarge)?;
        let read = Read::read(self, destination).map_err(BlobBuildError::Source)?;
        original.spent(WorkCounters { source_bytes_read: usize_to_u64(read),
            ..WorkCounters::UNCHARGED })?;
        Ok(read)
    }
}

/// Streams a source into fixed-size authenticated chunks and a bounded index.
///
/// # Errors
///
/// Rejects invalid options, oversized input, source/backend failures, arithmetic
/// overflow, or work beyond the admitted budget. It never materializes the
/// complete input in one buffer.
pub fn build_blob<S: crate::ImmediateObjectStore, R: Read>(
    store: &S,
    source: &mut R,
    options: BlobBuildOptions,
    budget: WorkBudget,
) -> Result<BlobBuild, BlobBuildFailure> {
    let cancellation = CancellationToken::new();
    crate::async_storage::poll_immediate(build_blob_async(
        store,
        &mut ImmediateBlobSource(source),
        options,
        budget,
        &cancellation,
    ))
}

/// Asynchronously streams a source into authenticated chunks and a bounded,
/// incremental blob index.
///
/// # Errors
///
/// Returns the same receipt-bearing failures as [`build_blob`], including
/// cooperative cancellation before the next source or object-store operation.
pub async fn build_blob_async<S: AsyncObjectStore, R: AsyncBlobSource>(
    store: &S, source: &mut R, options: BlobBuildOptions, budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BlobBuild, BlobBuildFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    build_blob_with_accounting(store, source, options, &mut accounting, cancellation).await
        .map_err(|error| build_failed(blob_build_error(error), accounting.spent()))
}

/// The canonical streaming producer, borrowing either standalone or original accounting.
pub(crate) async fn build_blob_with_accounting<S: AsyncObjectStore, R: AsyncBlobSource>(
    store: &S, source: &mut R, options: BlobBuildOptions,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<BlobBuild, FsError> {
    accounting.check()?;
    if options.chunk_bytes == 0 || effective_page_width(options).is_none()
        || options.maximum_blob_bytes == 0
    {
        return Err(BlobBuildError::InvalidOptions.into());
    }
    let chunk_capacity = usize::try_from(options.chunk_bytes)
        .map_err(|_| BlobBuildError::InvalidOptions)?;
    let mut batching = BlobBatchStore::new(store, accounting)?;
    let mut index = BlobIndexBuilder::new(options);
    let result = async {
        let mut logical_bytes = 0_u64;
        loop {
            accounting.check()?;
            let detection_bytes = options.maximum_blob_bytes
                .saturating_sub(logical_bytes).saturating_add(1);
            let maximum = chunk_capacity.min(usize::try_from(detection_bytes).unwrap_or(usize::MAX));
            let owned = read_owned_blob_source(source, maximum, accounting, cancellation).await?;
            let (chunk, filled) = if let Some(chunk) = owned {
                let filled = chunk.bytes.len();
                (chunk, filled)
            } else {
                read_blob_chunk(source, maximum, accounting, cancellation).await?
            };
            if filled == 0 {
                drop_source_chunk(chunk, accounting)?;
                break;
            }
            logical_bytes = accept_owned_blob_chunk(
                &mut batching, &mut index, chunk, maximum, logical_bytes,
                options.maximum_blob_bytes, accounting, cancellation,
            ).await?;
            if filled < maximum { break; }
        }
        let root = index.finish(&mut batching, accounting, cancellation).await?;
        batching.flush(accounting, cancellation).await?;
        Ok(BlobBuild { root, logical_bytes, work: accounting.spent() })
    }.await;
    let index_cleanup = index.release(accounting);
    let batch_cleanup = batching.release(accounting);
    match result {
        Err(error) => Err(error),
        Ok(value) => { index_cleanup?; batch_cleanup?; Ok(value) }
    }
}

fn blob_build_error(error: FsError) -> BlobBuildError {
    match error {
        FsError::BlobBuild(error) => error,
        FsError::Work(error) => BlobBuildError::Work(error),
        FsError::Cancelled(_) => BlobBuildError::Cancelled,
        FsError::Object(error) => BlobBuildError::Storage(error),
        FsError::Decode(error) => BlobBuildError::Encode(error),
        error => BlobBuildError::Context(Box::new(error)),
    }
}

fn build_allocation_error(error: AllocationError) -> FsError {
    error.into_work_or(FsError::BlobBuild(BlobBuildError::AllocationFailed))
}

fn vector_bytes<T>(capacity: usize) -> Result<u64, FsError> {
    capacity.checked_mul(size_of::<T>()).map(usize_to_u64)
        .ok_or_else(|| WorkError::Overflow.into())
}

fn empty_blob_vec<T>() -> ReadOwned<Vec<T>> {
    ReadOwned { value: Vec::new(), custody: KernelOwnedAllocation::Standalone }
}

/// Admission and reconciliation loans contain no allocator or copy operation.
fn reserve_blob_vec<T>(
    owner: &mut ReadOwned<Vec<T>>, capacity: usize, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if owner.value.capacity() >= capacity { return Ok(()); }
    let before = vector_bytes::<T>(owner.value.capacity())?;
    let requested = vector_bytes::<T>(capacity)?;
    let delta = requested.checked_sub(before).ok_or(WorkError::Overflow)?;
    let mut admitted = accounting.claim_owned(delta, 1)?;
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(owner.custody, KernelOwnedAllocation::Standalone)
        && matches!(admitted, KernelOwnedAllocation::NativeOriginal(_))
    {
        owner.custody = admitted;
    } else {
        owner.custody.absorb(&mut admitted)?;
    }
    #[cfg(target_arch = "wasm32")]
    owner.custody.absorb(&mut admitted)?;
    if let Err(error) = owner.value.try_reserve_exact(capacity - owner.value.len()) {
        let _ = accounting.reconcile_owned(&mut owner.custody, requested, before);
        return Err(BlobBuildError::ReserveFailed(error).into());
    }
    let actual = vector_bytes::<T>(owner.value.capacity())?;
    accounting.reconcile_owned(&mut owner.custody, requested, actual)
}

fn release_blob_vec<T>(
    owner: ReadOwned<Vec<T>>, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let bytes = vector_bytes::<T>(owner.value.capacity())?;
    drop(owner.value);
    match owner.custody {
        KernelOwnedAllocation::Standalone => release_blob_bytes(bytes, accounting),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(guard) => { drop(guard); Ok(()) }
    }
}

fn completed_blob_backend(
    measured: WorkCounters, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    match accounting {
        KernelAccounting::Standalone { allocations, work, budget, .. } => {
            *work = work.with_backend(measured, allocations.live_bytes())?;
            work.verify(*budget)?;
        }
        KernelAccounting::BorrowedStandalone { allocations, work, budget, .. } => {
            **work = work.with_backend(measured, allocations.live_bytes())?;
            work.verify(*budget)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => return Err(BlobBuildError::NativeSourceUnavailable.into()),
    }
    Ok(())
}

async fn read_owned_blob_source<R: AsyncBlobSource>(
    source: &mut R, maximum: usize, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<OwnedSourceChunk>, FsError> {
    accounting.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return source.read_owned_with_native_work(maximum, original, cancellation).await;
    }
    let result = source.read_owned(maximum, accounting.remaining()?, cancellation).await;
    let measured = source.take_work();
    if let Some(measured) = measured { completed_blob_backend(measured, accounting)?; }
    let chunk = result.map_err(BlobBuildError::Source)?;
    if let Some(chunk) = chunk {
        // This is an explicit by-value transfer from the actual source producer.
        let retained = chunk.logical_owned_bytes;
        let (custody, capacity_result) = accounting.record_actual_owned(retained)?;
        let completed = if measured.is_none() {
            WorkCounters { source_bytes_read: usize_to_u64(chunk.bytes.len()), ..WorkCounters::UNCHARGED }
        } else { WorkCounters::UNCHARGED };
        let completed_result = accounting.record_completed(completed);
        if let Err(error) = capacity_result.and(completed_result) {
            drop(chunk);
            drop(custody);
            let _ = release_blob_bytes(retained, accounting);
            return Err(error);
        }
        return Ok(Some(chunk));
    }
    Ok(None)
}

async fn read_blob_chunk<R: AsyncBlobSource>(
    source: &mut R, maximum: usize, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(OwnedSourceChunk, usize), FsError> {
    let mut buffer = Some(empty_blob_vec());
    #[cfg(not(target_arch = "wasm32"))]
    let mut prepared = None;
    let result = async {
        #[cfg(not(target_arch = "wasm32"))]
        let native = accounting.native_work().cloned();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &native {
            original.admit(WorkCounters {
                source_bytes_read: usize_to_u64(maximum), ..WorkCounters::UNCHARGED
            })?;
        }
        let owned = buffer.as_mut().expect("source storage is prepared once");
        reserve_blob_vec(owned, maximum, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &native {
            let capacity = owned.value.capacity();
            let copied = original.reserve_copy(usize_to_u64(capacity))?;
            owned.value.resize(capacity, 0);
            copied.record(usize_to_u64(capacity))?;
            let owned = buffer.take().expect("original source storage");
            prepared = Some(acyclic_stream::PreparedOwnedBytes::new(
                owned.value, owned.custody, original.runtime_meter_owner(),
            ).map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error).into(),
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                => {
                    FsError::from(crate::NativeTreeMaterializationError::InvalidLayout)
                }
            })?);
        }
        if let Some(owned) = buffer.as_mut() {
            let zeroed = WorkCounters { bytes_copied: usize_to_u64(maximum), ..WorkCounters::UNCHARGED };
            accounting.admit(zeroed)?;
            owned.value.resize(maximum, 0);
            completed_read_work(accounting, zeroed)?;
        }
        let mut filled = 0_usize;
        while filled < maximum {
            accounting.check()?;
            #[cfg(not(target_arch = "wasm32"))]
            let destination = match prepared.as_mut() {
                Some(prepared) => &mut prepared.writable()[filled..maximum],
                None => buffer.as_mut().expect("ordinary source storage").value.get_mut(filled..)
                    .ok_or(BlobBuildError::TooLarge)?,
            };
            #[cfg(target_arch = "wasm32")]
            let destination = buffer.as_mut().expect("ordinary source storage").value.get_mut(filled..)
                .ok_or(BlobBuildError::TooLarge)?;
            #[cfg(not(target_arch = "wasm32"))]
            let read = if let Some(original) = &native {
                source.read_with_native_work(destination, original, cancellation).await
            } else {
                read_standalone_blob_source(source, destination, accounting, cancellation).await
            };
            #[cfg(target_arch = "wasm32")]
            let read = read_standalone_blob_source(source, destination, accounting, cancellation).await;
            let read = read?;
            if read > maximum - filled { return Err(BlobBuildError::TooLarge.into()); }
            if read == 0 { break; }
            filled += read;
        }
        Ok(filled)
    }.await;
    match result {
        Err(error) => {
            #[cfg(not(target_arch = "wasm32"))]
            drop(prepared);
            if let Some(buffer) = buffer { let _cleanup = release_blob_vec(buffer, accounting); }
            Err(error)
        }
        Ok(filled) => {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(prepared) = prepared {
                let logical_owned_bytes = usize_to_u64(prepared.maximum_len());
                let bytes = prepared.freeze(filled).map_err(|failed| {
                    drop(failed);
                    FsError::from(crate::NativeTreeMaterializationError::InvalidLayout)
                })?;
                return Ok((OwnedSourceChunk::intrinsic(bytes, logical_owned_bytes), filled));
            }
            let mut buffer = buffer.expect("ordinary source storage");
            buffer.value.truncate(filled);
            let logical_owned_bytes = usize_to_u64(buffer.value.capacity());
            let bytes = owned_read_bytes(buffer)?;
            Ok((OwnedSourceChunk::claimed(bytes, logical_owned_bytes), filled))
        }
    }
}

async fn read_standalone_blob_source<R: AsyncBlobSource>(
    source: &mut R, destination: &mut [u8], accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<usize, FsError> {
    let result = source.read(destination, accounting.remaining()?, cancellation).await;
    let measured = source.take_work();
    if let Some(measured) = measured { completed_blob_backend(measured, accounting)?; }
    let read = result.map_err(BlobBuildError::Source)?;
    if measured.is_none() {
        completed_read_work(accounting, WorkCounters {
            source_bytes_read: usize_to_u64(read), ..WorkCounters::UNCHARGED
        })?;
    }
    Ok(read)
}

fn release_blob_bytes(bytes: u64, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    // Cleanup is unconditional, including after cancellation or a late denial.
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => {
            allocations.release(bytes).map_err(build_allocation_error)
        }
        KernelAccounting::BorrowedStandalone { allocations, .. } => {
            allocations.release(bytes).map_err(build_allocation_error)
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => Ok(()),
    }
}

fn drop_source_chunk(chunk: OwnedSourceChunk, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let bytes = chunk.logical_owned_bytes;
    drop(chunk);
    release_blob_bytes(bytes, accounting)
}

fn hashed_blob_object(
    kind: ObjectKind, bytes: acyclic_stream::OwnedBytes, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<HashedObject, FsError> {
    accounting.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return HashedObject::new_with_native_work(kind, bytes, original, cancellation).map_err(Into::into);
    }
    let hash = WorkCounters {
        bytes_hashed: usize_to_u64(bytes.len()).checked_add(crate::storage::OBJECT_DIGEST_ENVELOPE_BYTES)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hash)?;
    let object = HashedObject::new(kind, bytes);
    completed_read_work(accounting, hash)?;
    Ok(object)
}

async fn accept_owned_blob_chunk<S: AsyncObjectStore>(
    batching: &mut BlobBatchStore<'_, S>, index: &mut BlobIndexBuilder,
    chunk: OwnedSourceChunk, maximum: usize, logical_bytes: u64, maximum_blob_bytes: u64,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<u64, FsError> {
    let chunk_bytes = usize_to_u64(chunk.bytes.len());
    let end = logical_bytes.checked_add(chunk_bytes).ok_or(BlobBuildError::TooLarge);
    let end = match end {
        Ok(end) if chunk.bytes.len() <= maximum && end <= maximum_blob_bytes => end,
        _ => { drop_source_chunk(chunk, accounting)?; return Err(BlobBuildError::TooLarge.into()); }
    };
    let retained = chunk.logical_owned_bytes;
    let object = match hashed_blob_object(ObjectKind::BlobChunk, chunk.bytes, accounting, cancellation) {
        Ok(object) => object,
        Err(error) => { let _ = release_blob_bytes(retained, accounting); return Err(error); }
    };
    let chunk_id = object.object_id();
    batching.put(object, retained, accounting, cancellation).await?;
    index.push_chunk(batching, BlobChunkRef {
        first_offset: logical_bytes, end_offset: end, chunk: chunk_id,
    }, accounting, cancellation).await?;
    Ok(end)
}

const BLOB_BATCH_OBJECTS: usize = 4;
const BLOB_BATCH_BYTES: u64 = 4 * 1024 * 1024;

struct BlobBatchStore<'a, S> {
    inner: &'a S,
    writes: ReadOwned<Vec<HashedObject>>,
    bytes: u64,
}

impl<'a, S: AsyncObjectStore> BlobBatchStore<'a, S> {
    fn new(inner: &'a S, accounting: &mut KernelAccounting<'_>) -> Result<Self, FsError> {
        let mut writes = empty_blob_vec();
        if let Err(error) = reserve_blob_vec(&mut writes, BLOB_BATCH_OBJECTS, accounting) {
            let _ = release_blob_vec(writes, accounting);
            return Err(error);
        }
        Ok(Self { inner, writes, bytes: 0 })
    }

    async fn flush(&mut self, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken)
        -> Result<(), FsError>
    {
        if self.writes.value.is_empty() { return Ok(()); }
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let mut writes = empty_blob_vec();
            let result = async {
                reserve_blob_vec(&mut writes, self.writes.value.len(), accounting)?;
                // Only scalar identities and zero-copy byte aliases are transferred.
                for object in &self.writes.value {
                    writes.value.push(crate::storage::ObjectWrite {
                        object_id: object.object_id(), bytes: object.bytes().clone(),
                    });
                }
                self.inner.put_many_with_native_work(&writes.value, original, cancellation).await
                    .map_err(FsError::from)
            }.await;
            let cleanup = release_blob_vec(writes, accounting);
            result?;
            cleanup?;
            self.writes.value.clear();
            self.bytes = 0;
            return Ok(());
        }
        let result = self.inner.put_many_hashed(
            &self.writes.value, accounting.remaining()?, cancellation,
        ).await;
        let measured = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        completed_blob_backend(measured, accounting)?;
        result.map_err(|failure| FsError::from(failure.error))?;
        self.writes.value.clear();
        let bytes = std::mem::take(&mut self.bytes);
        release_blob_bytes(bytes, accounting)
    }

    async fn put(
        &mut self, object: HashedObject, retained: u64, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        self.bytes = self.bytes.checked_add(retained).ok_or(WorkError::Overflow)?;
        self.writes.value.push(object);
        if self.writes.value.len() >= BLOB_BATCH_OBJECTS || self.bytes >= BLOB_BATCH_BYTES {
            self.flush(accounting, cancellation).await?;
        }
        Ok(())
    }

    fn release(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let bytes = self.bytes;
        release_blob_vec(self.writes, accounting)?;
        release_blob_bytes(bytes, accounting)
    }
}

struct BlobIndexBuilder {
    options: BlobBuildOptions,
    width: usize,
    leaf: ReadOwned<Vec<BlobChunkRef>>,
    levels: ReadOwned<Vec<ReadOwned<Vec<BlobChild>>>>,
}

impl BlobIndexBuilder {
    fn new(options: BlobBuildOptions) -> Self {
        Self { options, width: effective_page_width(options).unwrap_or(0),
            leaf: empty_blob_vec(), levels: empty_blob_vec() }
    }

    async fn push_chunk<S: AsyncObjectStore>(
        &mut self, store: &mut BlobBatchStore<'_, S>, chunk: BlobChunkRef,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        reserve_blob_vec(&mut self.leaf, self.width, accounting)?;
        self.leaf.value.push(chunk);
        if self.leaf.value.len() == self.width { self.flush_leaf(store, accounting, cancellation).await?; }
        Ok(())
    }

    async fn flush_leaf<S: AsyncObjectStore>(
        &mut self, store: &mut BlobBatchStore<'_, S>, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.leaf.value.is_empty() { return Ok(()); }
        let items = std::mem::take(&mut self.leaf.value);
        let first_offset = items.first().ok_or(BlobBuildError::InvalidIndexState)?.first_offset;
        let end_offset = items.last().ok_or(BlobBuildError::InvalidIndexState)?.end_offset;
        let page = BlobPage { first_offset, end_offset, node: BlobNode::Leaf(items) };
        let result = put_blob_page(store, &page, self.options, accounting, cancellation).await;
        let BlobNode::Leaf(mut items) = page.node else { unreachable!() };
        items.clear();
        self.leaf.value = items;
        let page = result?;
        self.push_child(store, 0, BlobChild { first_offset, end_offset, page }, accounting, cancellation).await
    }

    fn push_child<'a, 'store, 'accounting, S: AsyncObjectStore>(
        &'a mut self, store: &'a mut BlobBatchStore<'store, S>, mut level: usize, mut child: BlobChild,
        accounting: &'a mut KernelAccounting<'accounting>, cancellation: &'a CancellationToken,
    ) -> impl Future<Output = Result<(), FsError>> + crate::async_storage::StorageFuture + 'a {
        async move {
        loop {
            while self.levels.value.len() <= level {
                let capacity = self.levels.value.len().checked_add(1).ok_or(WorkError::Overflow)?;
                reserve_blob_vec(&mut self.levels, capacity, accounting)?;
                self.levels.value.push(empty_blob_vec());
            }
            let items = self.levels.value.get_mut(level).ok_or(BlobBuildError::InvalidIndexState)?;
            reserve_blob_vec(items, self.width, accounting)?;
            items.value.push(child);
            if items.value.len() < self.width { return Ok(()); }
            child = self.flush_internal(store, level, accounting, cancellation).await?;
            level = level.checked_add(1).ok_or(WorkError::Overflow)?;
        }
        }
    }

    async fn flush_internal<S: AsyncObjectStore>(
        &mut self, store: &mut BlobBatchStore<'_, S>, level: usize,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<BlobChild, FsError> {
        let owner = self.levels.value.get_mut(level).ok_or(BlobBuildError::InvalidIndexState)?;
        let items = std::mem::take(&mut owner.value);
        let first_offset = items.first().ok_or(BlobBuildError::InvalidIndexState)?.first_offset;
        let end_offset = items.last().ok_or(BlobBuildError::InvalidIndexState)?.end_offset;
        let page = BlobPage { first_offset, end_offset, node: BlobNode::Internal(items) };
        let result = put_blob_page(store, &page, self.options, accounting, cancellation).await;
        let BlobNode::Internal(mut items) = page.node else { unreachable!() };
        items.clear();
        owner.value = items;
        Ok(BlobChild { first_offset, end_offset, page: result? })
    }

    async fn finish<S: AsyncObjectStore>(
        &mut self, store: &mut BlobBatchStore<'_, S>, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        if self.leaf.value.is_empty() && self.levels.value.is_empty() {
            return put_blob_page(store, &BlobPage {
                first_offset: 0, end_offset: 0, node: BlobNode::Leaf(Vec::new()),
            }, self.options, accounting, cancellation).await;
        }
        self.flush_leaf(store, accounting, cancellation).await?;
        loop {
            let level = self.levels.value.iter().position(|items| !items.value.is_empty())
                .ok_or(BlobBuildError::InvalidIndexState)?;
            let higher_empty = self.levels.value.iter().skip(level + 1).all(|items| items.value.is_empty());
            let items = self.levels.value.get(level).ok_or(BlobBuildError::InvalidIndexState)?;
            if higher_empty && items.value.len() == 1 {
                return Ok(items.value.first().ok_or(BlobBuildError::InvalidIndexState)?.page);
            }
            let child = self.flush_internal(store, level, accounting, cancellation).await?;
            self.push_child(store, level + 1, child, accounting, cancellation).await?;
        }
    }

    fn release(mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        for level in self.levels.value.drain(..) { release_blob_vec(level, accounting)?; }
        release_blob_vec(self.leaf, accounting)?;
        release_blob_vec(self.levels, accounting)
    }
}

fn effective_page_width(options: BlobBuildOptions) -> Option<usize> {
    let item_bound = usize::try_from(options.page_items).ok()?;
    let byte_bound = usize::try_from(options.page_bytes).ok()?
        .checked_sub(BLOB_PAGE_HEADER_BYTES)? / BLOB_PAGE_ITEM_BYTES;
    let width = item_bound.min(byte_bound);
    (width >= 2).then_some(width)
}

pub(crate) fn encode_blob_page_with_accounting(
    page: &BlobPage, maximum_items: u32, accounting: &mut KernelAccounting<'_>,
) -> Result<ReadOwned<Vec<u8>>, FsError> {
    let length = blob_page_encoded_length(page, maximum_items).map_err(BlobBuildError::Encode)?;
    let encoded = WorkCounters {
        bytes_encoded: usize_to_u64(length), page_writes: 1, ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoded)?;
    let mut owner = empty_blob_vec();
    let mut claimed = 0_u64;
    // The ordinary encoder and original observer share every canonical field write.
    let result = (|| {
        let encoder = Encoder::with_exact_capacity_with_steps(
            DOMAIN, VERSION, length,
            |step| -> Result<(), FsError> {
                match step {
                    super::codec::EncodeAllocationStep::Reserve { capacity } => {
                        let requested = usize_to_u64(capacity);
                        owner.custody = accounting.claim_owned(requested, 1)?;
                        claimed = requested;
                    }
                    super::codec::EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes } => {
                        claimed = actual_bytes;
                        accounting.reconcile_owned(&mut owner.custody, admitted_bytes, actual_bytes)?;
                    }
                    super::codec::EncodeAllocationStep::BeforeHeader { .. }
                        => accounting.check()?,
                    super::codec::EncodeAllocationStep::HeaderWritten { bytes } => {
                        completed_read_work(accounting, WorkCounters {
                            bytes_encoded: bytes, ..WorkCounters::UNCHARGED
                        })?;
                    }
                }
                Ok(())
            },
            |error| FsError::BlobBuild(BlobBuildError::ReserveFailed(error)),
        )?;
        let body = WorkCounters {
            bytes_encoded: usize_to_u64(length).checked_sub(usize_to_u64(DOMAIN.len() + 2))
                .ok_or(WorkError::Overflow)?,
            page_writes: 1, ..WorkCounters::UNCHARGED
        };
        accounting.admit(body)?;
        owner.value = encode_blob_page_body(page, encoder).map_err(BlobBuildError::Encode)?;
        completed_read_work(accounting, body)?;
        Ok(())
    })();
    match result {
        Ok(()) => Ok(owner),
        Err(error) => {
            drop(owner.value);
            match owner.custody {
                KernelOwnedAllocation::Standalone => {
                    let _ = release_blob_bytes(claimed, accounting);
                }
                #[cfg(not(target_arch = "wasm32"))]
                KernelOwnedAllocation::NativeOriginal(guard) => drop(guard),
            }
            Err(error)
        }
    }
}

async fn put_blob_page<S: AsyncObjectStore>(
    store: &mut BlobBatchStore<'_, S>, page: &BlobPage, options: BlobBuildOptions,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let length = blob_page_encoded_length(page, options.page_items).map_err(BlobBuildError::Encode)?;
    if usize_to_u64(length) > u64::from(options.page_bytes) {
        return Err(BlobBuildError::PageTooLarge {
            observed: usize_to_u64(length), maximum: u64::from(options.page_bytes),
        }.into());
    }
    let owner = encode_blob_page_with_accounting(page, options.page_items, accounting)?;
    let retained = usize_to_u64(owner.value.capacity());
    let bytes = owned_read_bytes(owner)?;
    let object = match hashed_blob_object(ObjectKind::Blob, bytes, accounting, cancellation) {
        Ok(object) => object,
        Err(error) => { let _ = release_blob_bytes(retained, accounting); return Err(error); }
    };
    let page = object.object_id();
    store.put(object, retained, accounting, cancellation).await?;
    Ok(page)
}

/// Streaming blob-build failure retaining exact completed work.
pub type BlobBuildFailure = OperationFailure<BlobBuildError>;

/// An actual source failure in storage funded before the source operation.
pub struct BlobDemandFailure {
    error: Box<Option<crate::DemandError>>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: crate::NativeAllocation,
}

impl BlobDemandFailure {
    /// Borrows the actual initialized provider cause.
    pub fn error(&self) -> &crate::DemandError {
        self.error.as_ref().as_ref().expect("published source failure is initialized")
    }

    /// Visits owned cause fields without cloning or replacing the prepaid holder.
    pub fn error_mut(&mut self) -> &mut crate::DemandError {
        self.error.as_mut().as_mut().expect("published source failure is initialized")
    }
}

impl std::fmt::Debug for BlobDemandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("BlobDemandFailure").field("error", self.error()).finish()
    }
}

impl std::fmt::Display for BlobDemandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), formatter)
    }
}

impl std::error::Error for BlobDemandFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(self.error()) }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct PreparedBlobDemandFailure {
    storage: BlobDemandFailure,
}

#[cfg(not(target_arch = "wasm32"))]
impl PreparedBlobDemandFailure {
    pub(crate) fn new(original: &crate::NativeMaterializationWork) -> Result<Self, FsError> {
        let allocation = original.claim(usize_to_u64(size_of::<Option<crate::DemandError>>()))?;
        let prepared = Self { storage: BlobDemandFailure {
            error: Box::new(None), _allocation: allocation,
        } };
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(prepared)
    }

    pub(crate) fn shares_ledger(&self, original: &crate::NativeMaterializationWork) -> bool {
        self.storage._allocation.work().shares_ledger(original)
    }

    pub(crate) fn failed(mut self, error: crate::DemandError) -> BlobDemandFailure {
        *self.storage.error = Some(error);
        self.storage
    }
}

/// Stable streaming blob-build errors.
#[derive(Debug, Error)]
pub enum BlobBuildError {
    /// Cooperative cancellation occurred before the next source or store operation.
    #[error("blob build was cancelled")]
    Cancelled,
    /// Chunk, page, or total-size bounds are invalid.
    #[error("blob build options are invalid")]
    InvalidOptions,
    /// Canonical blob-index page exceeds its explicit byte ceiling.
    #[error("blob index page has {observed} bytes; maximum is {maximum}")]
    PageTooLarge {
        /// Canonical encoded bytes.
        observed: u64,
        /// Configured page-byte ceiling.
        maximum: u64,
    },
    /// Fallible bounded index scratch allocation was unavailable.
    #[error("blob build scratch allocation failed")]
    AllocationFailed,
    /// The allocator's actual refusal, without replacing its cause.
    #[error(transparent)]
    ReserveFailed(std::collections::TryReserveError),
    /// A custom source has not supplied an original-accounted producer.
    #[error("blob source has no original native read primitive")]
    NativeSourceUnavailable,
    /// Exact accounting or provider cause outside the standalone error vocabulary.
    #[error(transparent)]
    Context(Box<FsError>),
    /// Incremental index finalization reached an impossible state.
    #[error("blob build index state is invalid")]
    InvalidIndexState,
    /// Input exceeded the admitted logical size.
    #[error("blob source exceeds its admitted logical size")]
    TooLarge,
    /// Source stream failed.
    #[error("blob source failed: {0}")]
    Source(#[source] std::io::Error),
    /// The original provider's typed cause in its actual preadmitted holder.
    #[error(transparent)]
    Demand(#[from] BlobDemandFailure),
    /// Canonical blob-index encoding failed.
    #[error(transparent)]
    Encode(#[from] CanonicalDecodeError),
    /// Immutable-object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Exact work overflowed or exceeded its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

fn build_failed(error: BlobBuildError, work: WorkCounters) -> BlobBuildFailure {
    OperationFailure::new(error, work)
}

/// Successful authenticated logical blob range and exact work evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobRead {
    /// Requested bytes. Native aliases retain the original physical allocation guard.
    pub bytes: acyclic_stream::OwnedBytes,
    /// Actual controlled output-vector capacity retained by `bytes`.
    pub logical_owned_bytes: u64,
    /// Exact completed kernel and backend work in the supplied accounting context.
    pub work: WorkCounters,
    /// Exact authenticated forward page or chunk exposed by this traversal.
    pub next_residency: Option<ResidencyHint>,
}

impl BlobRead {
    /// Exact logical payload capacity measured before freezing the byte carrier.
    pub fn logical_owned_bytes(&self) -> u64 { self.logical_owned_bytes }
}

/// Reads only blob-index pages and chunks intersecting one exact range.
///
/// # Errors
///
/// Rejects invalid bounds, missing/corrupt objects, graph cycles, child-bound
/// forgery, incomplete coverage, or any work outside the admitted budget.
pub fn read_blob_range<S: ObjectStore>(
    store: &S, root: ObjectId, range: ByteRange, limits: DecodeLimits, budget: WorkBudget,
) -> Result<BlobRead, BlobReadFailure> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(budget, &cancellation);
    let result = (|| {
        let mut machine = BlobRangeMachine::new(root, range, limits, &mut accounting)?;
        while !machine.pending.is_empty() {
            let pending = machine.prepare(&mut accounting)?;
            let (object, maximum) = pending.request(limits);
            let result = ObjectStore::read(store, object, maximum, accounting.remaining()?);
            let (read, retained) = super::file_read::accept_standalone_object_read(result, &mut accounting)?;
            match pending {
                PendingBlobRead::Page { expected_first, expected_end, height, .. } => {
                    let decoded = ReadOwned::decoded(decode_blob_page_with_accounting(&read, limits, &mut accounting)?);
                    machine.accept_decoded_page(&decoded.value.0, expected_first, expected_end, height, &mut accounting)?;
                    drop(decoded.value.0);
                    release_read_allocation(decoded.value.1, decoded.custody, &mut accounting)?;
                }
                PendingBlobRead::Chunk(chunk) => machine.accept_chunk(&read, chunk, &mut accounting)?,
            }
            drop(read);
            super::file_read::release_standalone_read_bytes(retained, &mut accounting)?;
        }
        machine.finish(&mut accounting)
    })();
    result.map_err(|error| failed(blob_error(error), accounting.spent()))
}

/// Asynchronously reads an authenticated range through the same transition
/// machine as the synchronous and original-ledger readers.
///
/// # Errors
///
/// Rejects malformed objects, cancellation, and work beyond the admitted budget.
pub async fn read_blob_range_async<S: AsyncObjectStore>(
    store: &S, root: ObjectId, range: ByteRange, limits: DecodeLimits, budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BlobRead, BlobReadFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    read_blob_range_with_accounting(store, root, range, limits, &mut accounting, cancellation).await
        .map_err(|error| failed(blob_error(error), accounting.spent()))
}

pub(crate) async fn read_blob_range_with_accounting<S: AsyncObjectStore>(
    store: &S, root: ObjectId, range: ByteRange, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<BlobRead, FsError> {
    read_blob_range_mode(store, root, range, limits, accounting, cancellation, true).await
}

pub(crate) async fn read_blob_range_for_copy_with_accounting<S: AsyncObjectStore>(
    store: &S, root: ObjectId, range: ByteRange, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<BlobRead, FsError> {
    read_blob_range_mode(store, root, range, limits, accounting, cancellation, false).await
}

async fn read_blob_range_mode<S: AsyncObjectStore>(
    store: &S, root: ObjectId, range: ByteRange, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, publish_output: bool,
) -> Result<BlobRead, FsError> {
    let mut machine = BlobRangeMachine::new(root, range, limits, accounting)?;
    machine.publish_output = publish_output;
    while !machine.pending.is_empty() {
        if machine.read_chunk_batch(store, accounting, cancellation).await? { continue; }
        let pending = machine.prepare(accounting)?;
        let (object, maximum) = pending.request(limits);
        if let PendingBlobRead::Page { expected_first, expected_end, height, .. } = pending {
            let key = DecodedCacheKey::new::<BlobPage>(object, limits);
            #[cfg(not(target_arch = "wasm32"))]
            let cached = if let Some(original) = accounting.native_work() {
                store.decoded_cache_get_with_native_work(key, original)
            } else { store.decoded_cache_get(key) };
            #[cfg(target_arch = "wasm32")]
            let cached = store.decoded_cache_get(key);
            if let Some(cached) = cached.map_err(crate::kernel::file_read::read_store_error)? {
                let page = cached.value.downcast_ref::<BlobPage>().ok_or_else(|| FsError::BlobRead(ObjectStoreError::Corrupt.into()))?;
                machine.accept_decoded_page(page, expected_first, expected_end, height, accounting)?;
                
                drop(cached);
                continue;
            }
            let (read, retained) = read_object_with_accounting(store, object, maximum, accounting, cancellation).await?;
            let decoded = ReadOwned::decoded(decode_blob_page_with_accounting(&read, limits, accounting)?);
            let (cached, allocated) = super::file_read::cache_decoded_read(store, key, decoded, accounting)?;
            let page = cached.value.downcast_ref::<BlobPage>().ok_or_else(|| FsError::BlobRead(ObjectStoreError::Corrupt.into()))?;
            machine.accept_decoded_page(page, expected_first, expected_end, height, accounting)?;
            
            drop(cached);
            super::file_read::release_standalone_read_bytes(allocated, accounting)?;
            drop(read);
            super::file_read::release_standalone_read_bytes(retained, accounting)?;
        } else if let PendingBlobRead::Chunk(chunk) = pending {
            let (read, retained) = read_object_with_accounting(store, object, maximum, accounting, cancellation).await?;
            machine.accept_chunk(&read, chunk, accounting)?;
            drop(read);
            super::file_read::release_standalone_read_bytes(retained, accounting)?;
        }
    }
    machine.finish(accounting)
}

#[derive(Clone, Copy)]
enum PendingBlobRead {
    Page { page: ObjectId, expected_first: u64, expected_end: Option<u64>, height: u16 },
    Chunk(BlobChunkRef),
}

impl PendingBlobRead {
    fn request(self, limits: DecodeLimits) -> (ObjectId, u64) {
        match self {
            Self::Page { page, .. } => (page, limits.maximum_page_object_bytes()),
            Self::Chunk(chunk) => (chunk.chunk, chunk.end_offset - chunk.first_offset),
        }
    }
}

struct BlobRangeMachine {
    range: ByteRange,
    range_end: u64,
    limits: DecodeLimits,
    allocation: usize,
    maximum_pending: usize,
    pending: Vec<PendingBlobRead>,
    pending_capacity: LogicalVecCapacity,
    visited: Option<super::persistent_point::Owned<VisitedObjectSet>>,
    output: Vec<u8>,
    successor: Option<(u64, ResidencyHint)>,
    publish_output: bool,
    // Every physical payload is dropped before the corresponding native guard.
    pending_custody: KernelOwnedAllocation,
    output_custody: KernelOwnedAllocation,
}

impl BlobRangeMachine {
    fn new(root: ObjectId, range: ByteRange, limits: DecodeLimits, accounting: &mut KernelAccounting<'_>) -> Result<Self, FsError> {
        accounting.check()?;
        if root.kind != ObjectKind::Blob { return Err(BlobReadError::WrongRootKind.into()); }
        let range_end = range.offset.checked_add(range.length).ok_or(BlobReadError::InvalidRange)?;
        let allocation = usize::try_from(range.length).map_err(|_| BlobReadError::InvalidRange)?;
        if !limits.page_limits_valid(1) { return Err(BlobReadError::InvalidLimits.into()); }
        let maximum_visited = usize::try_from(limits.maximum_visited_pages).map_err(|_| BlobReadError::InvalidLimits)?;
        let maximum_pending = maximum_visited.checked_add(
            usize::try_from(limits.maximum_page_items).map_err(|_| BlobReadError::InvalidLimits)?
        ).ok_or(BlobReadError::InvalidLimits)?;
        let visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting)?;
        let output = allocate_read_buffer(allocation, accounting)?;
        let mut machine = Self {
            range, range_end, limits, allocation, maximum_pending, pending: Vec::new(),
            pending_capacity: LogicalVecCapacity::default(), visited: Some(visited), output: output.value, successor: None,
            pending_custody: KernelOwnedAllocation::Standalone, output_custody: output.custody,
            publish_output: true,
        };
        machine.push(PendingBlobRead::Page { page: root, expected_first: 0, expected_end: None, height: 1 }, accounting)?;
        Ok(machine)
    }

    fn push(&mut self, pending: PendingBlobRead, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.pending_capacity.ensure_for_push_with_accounting(
            &mut self.pending, self.maximum_pending, &mut self.pending_custody, accounting,
        )?;
        self.pending.push(pending);
        Ok(())
    }

    fn prepare(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<PendingBlobRead, FsError> {
        accounting.check()?;
        let pending = self.pending.pop().ok_or(BlobReadError::TraversalState)?;
        if let PendingBlobRead::Page { page, height, .. } = pending {
            if height > self.limits.maximum_page_height { return Err(BlobReadError::CycleOrHeight.into()); }
            let visited_owner = self.visited.as_mut().ok_or(BlobReadError::TraversalState)?;
            let visited = VisitedObjectSet::insert_with_accounting(visited_owner, page, accounting)?;
            if !visited.inserted { return Err(BlobReadError::CycleOrHeight.into()); }
            accounting.charge(WorkCounters { page_reads: 1, ..WorkCounters::UNCHARGED })?;
        }
        Ok(pending)
    }

    fn accept_decoded_page(
        &mut self, page: &BlobPage, expected_first: u64, expected_end: Option<u64>, height: u16,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if page.first_offset != expected_first || expected_end.is_some_and(|end| page.end_offset != end) {
            return Err(BlobReadError::ChildBoundsMismatch.into());
        }
        if expected_end.is_none() && self.range_end > page.end_offset { return Err(BlobReadError::InvalidRange.into()); }
        match &page.node {
            BlobNode::Internal(children) => {
                let height = height.checked_add(1).ok_or(BlobReadError::CycleOrHeight)?;
                for child in children.iter().rev() {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if child.first_offset < self.range_end && child.end_offset > self.range.offset {
                        self.push(PendingBlobRead::Page { page: child.page, expected_first: child.first_offset,
                            expected_end: Some(child.end_offset), height }, accounting)?;
                    } else if self.range.length != 0 && child.first_offset >= self.range_end {
                        self.retain_successor(child.first_offset, ObjectReadRequest { object_id: child.page,
                            maximum_bytes: self.limits.maximum_page_object_bytes() });
                    }
                }
            }
            BlobNode::Leaf(chunks) => {
                for chunk in chunks.iter().rev() {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if chunk.first_offset < self.range_end && chunk.end_offset > self.range.offset {
                        self.push(PendingBlobRead::Chunk(*chunk), accounting)?;
                    } else if self.range.length != 0 && chunk.first_offset >= self.range_end {
                        self.retain_successor(chunk.first_offset, ObjectReadRequest { object_id: chunk.chunk,
                            maximum_bytes: chunk.end_offset - chunk.first_offset });
                    }
                }
            }
        }
        Ok(())
    }

    fn retain_successor(&mut self, offset: u64, request: ObjectReadRequest) {
        if self.successor.is_none_or(|(current, _)| offset < current) {
            self.successor = Some((offset, ResidencyHint { request, reason: ResidencyReason::SequentialRange }));
        }
    }

    fn accept_chunk(&mut self, bytes: &[u8], chunk: BlobChunkRef, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        if usize_to_u64(bytes.len()) != chunk.end_offset - chunk.first_offset { return Err(BlobReadError::ChunkLengthMismatch.into()); }
        let start = usize::try_from(self.range.offset.max(chunk.first_offset) - chunk.first_offset).map_err(|_| BlobReadError::InvalidRange)?;
        let end = usize::try_from(self.range_end.min(chunk.end_offset) - chunk.first_offset).map_err(|_| BlobReadError::InvalidRange)?;
        let region = bytes.get(start..end).ok_or(BlobReadError::InvalidRange)?;
        if self.output.len().checked_add(region.len()).is_none_or(|length| length > self.allocation) {
            return Err(BlobReadError::IncompleteCoverage.into());
        }
        let copied = WorkCounters { bytes_copied: usize_to_u64(region.len()),
            output_bytes: if self.publish_output { usize_to_u64(region.len()) } else { 0 },
            items_returned: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(copied)?;
        self.output.extend_from_slice(region);
        completed_read_work(accounting, copied)
    }

    async fn read_chunk_batch<S: AsyncObjectStore>(
        &mut self, store: &S, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        if !matches!(self.pending.last(), Some(PendingBlobRead::Chunk(_))) { return Ok(false); }
        let count = self.pending.iter().rev().take_while(|pending| matches!(pending, PendingBlobRead::Chunk(_))).count();
        if count < 2 { return Ok(false); }
        #[cfg(not(target_arch = "wasm32"))]
        let native = accounting.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let mut observed = super::file_read::OriginalReadAccounting::new(accounting)?;
        let mut chunks = Vec::new();
        let mut requests = Vec::new();
        let prepared = (|| -> Result<(), ReadDecodeError> {
            reserve_decoded_items_with_steps(&mut chunks, count, &mut |step| observed.observe(step))?;
            reserve_decoded_items_with_steps(&mut requests, count, &mut |step| observed.observe(step))?;
            let copied = usize_to_u64(size_of::<BlobChunkRef>() + size_of::<ObjectReadRequest>());
            for pending in self.pending.iter().rev().take(count) {
                let PendingBlobRead::Chunk(chunk) = pending else { return Err(FsError::from(BlobReadError::TraversalState).into()) };
                let completed = WorkCounters { items_examined: 1, bytes_copied: copied, ..WorkCounters::UNCHARGED };
                observed.admit(completed)?;
                chunks.push(*chunk);
                requests.push(ObjectReadRequest { object_id: chunk.chunk, maximum_bytes: chunk.end_offset - chunk.first_offset });
                observed.record_completed(completed)?;
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            drop(chunks);
            drop(requests);
            observed.release_failed()?;
            #[cfg(not(target_arch = "wasm32"))]
            if native { return Err(error.into_original_fs_error()); }
            return Err(read_decode_error(error));
        }
        let (custody, scratch_bytes) = observed.into_parts();
        let batch = ReadOwned { value: (chunks, requests, scratch_bytes), custody };
        let read_result = super::file_read::read_objects_with_accounting(
            store, &batch.value.1, accounting, cancellation,
        ).await;
        let applied = match read_result {
            Err(error) => Err(error),
            Ok(reads) => {
                let retained = if native { Ok(0) } else {
                    super::file_read::retained_object_reads_bytes(&reads.value, reads.value.capacity())
                };
                let applied = (|| -> Result<bool, FsError> {
                    if reads.value.len() != count { return Err(BlobReadError::TraversalState.into()); }
                    self.pending.truncate(self.pending.len() - count);
                    for (chunk, read) in batch.value.0.iter().zip(&reads.value) {
                        self.accept_chunk(&read.bytes, *chunk, accounting)?;
                    }
                    Ok(true)
                })();
                drop(reads.value);
                let retired = retained.and_then(|bytes| release_read_allocation(bytes, reads.custody, accounting));
                retired.and(applied)
            }
        };
        drop(batch.value);
        release_read_allocation(scratch_bytes, batch.custody, accounting)?;
        applied
    }

    fn finish(mut self, accounting: &mut KernelAccounting<'_>) -> Result<BlobRead, FsError> {
        if self.output.len() != self.allocation { return Err(BlobReadError::IncompleteCoverage.into()); }
        let logical_owned_bytes = usize_to_u64(self.output.capacity());
        let output = ReadOwned {
            value: std::mem::take(&mut self.output),
            custody: std::mem::replace(&mut self.output_custody, KernelOwnedAllocation::Standalone),
        };
        let bytes = owned_read_bytes(output)?;
        let pending_bytes = self.pending_capacity.logical_bytes();
        drop(self.pending);
        release_read_allocation(pending_bytes, self.pending_custody, accounting)?;
        if let Some(visited) = self.visited {
            visited.retire(accounting)?;
        }
        Ok(BlobRead { bytes, logical_owned_bytes, work: accounting.spent(), next_residency: self.successor.map(|(_, hint)| hint) })
    }
}

fn blob_error(error: FsError) -> BlobReadError {
    match error {
        FsError::BlobRead(error) => error,
        FsError::Work(error) => error.into(),
        FsError::Cancelled(_) => BlobReadError::Cancelled,
        FsError::FileRead(super::FileRangeReadError::AllocationFailed) => BlobReadError::AllocationFailed,
        FsError::FileRead(super::FileRangeReadError::Work(error)) => error.into(),
        error => BlobReadError::Context(Box::new(error)),
    }
}

fn read_decode_error(error: ReadDecodeError) -> FsError {
    match error {
        ReadDecodeError::Canonical(error) => BlobReadError::Decode(error).into(),
        ReadDecodeError::AllocationFailed(_) => BlobReadError::Decode(CanonicalDecodeError::AllocationFailed).into(),
        ReadDecodeError::Accounting(error) => error,
    }
}


/// Authenticated blob-read failure with exact work retained on every path.
pub type BlobReadFailure = OperationFailure<BlobReadError>;

fn failed(error: BlobReadError, work: WorkCounters) -> BlobReadFailure {
    OperationFailure::new(error, work)
}

/// Stable authenticated blob-read errors.
#[derive(Debug, Error)]
pub enum BlobReadError {
    /// Cooperative cancellation occurred before the next storage boundary.
    #[error("blob range reading was cancelled")]
    Cancelled,
    /// The private resumable reader entered an impossible transition.
    #[error("blob range transition state is invalid")]
    TraversalState,
    /// Root has the wrong typed object class.
    #[error("blob root is not a blob-index page")]
    WrongRootKind,
    /// Requested range overflows or exceeds the blob.
    #[error("requested blob range is invalid")]
    InvalidRange,
    /// Decode and traversal limits are invalid or unrepresentable.
    #[error("blob read limits are invalid")]
    InvalidLimits,
    /// Bounded output, decode, or traversal scratch allocation failed.
    #[error("blob read scratch allocation failed")]
    AllocationFailed,
    /// Graph repeats a page or exceeds its height bound.
    #[error("blob index contains a cycle or exceeds its height bound")]
    CycleOrHeight,
    /// Parent routing bounds do not match the authenticated child.
    #[error("blob child bounds do not match its page")]
    ChildBoundsMismatch,
    /// Intersecting chunks did not cover the exact request.
    #[error("blob chunks do not cover the requested range exactly")]
    IncompleteCoverage,
    /// Authenticated chunk bytes do not equal the indexed logical span.
    #[error("blob chunk length does not match its index span")]
    ChunkLengthMismatch,
    /// Immutable-object backend rejected a read.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page failed decoding.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// Shared original-ledger read boundary failed.
    #[error(transparent)]
    Context(Box<FsError>),
}

impl From<AllocationError> for BlobReadError {
    fn from(error: AllocationError) -> Self {
        error.into_work_or(Self::AllocationFailed)
    }
}

fn count(value: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn capacity(value: u32) -> Result<usize, CanonicalDecodeError> {
    usize::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/blob.rs"]
mod tests;
