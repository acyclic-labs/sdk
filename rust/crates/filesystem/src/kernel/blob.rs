//! Authenticated fixed-size blob chunks and sparse range reads.

use super::allocation::{AllocationError, AllocationLedger, LogicalVecCapacity, VisitedObjectSet};
use super::codec::{
    CanonicalDecodeError, DecodeLimits, DecodedPageKind, DecodedPageShape, Decoder, Encoder,
};
use super::frontier;
use super::frontier::Machine as _;
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{
    ByteRange, ObjectId, ObjectKind, ObjectRead, ObjectReadRequest, ObjectReadRetention,
    ObjectReceipt, ObjectStore, ObjectStoreError, object_digest,
};
use bytes::Bytes;
use std::future::Future;
use std::io::Read;
use std::mem::size_of;
use std::sync::Arc;
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
    let encoded_length = blob_page_encoded_length(page, maximum_items)?;
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, encoded_length)?;
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
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let first_offset = decoder.u64()?;
    let end_offset = decoder.u64()?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let mut leaf = Vec::new();
    let mut internal = Vec::new();
    match tag {
        1 => {
            leaf.try_reserve_exact(capacity(item_count)?)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                leaf.push(BlobChunkRef {
                    first_offset: decoder.u64()?,
                    end_offset: decoder.u64()?,
                    chunk: ObjectId {
                        kind: ObjectKind::BlobChunk,
                        digest: crate::foundation::Digest::from_bytes(decoder.fixed()?),
                    },
                });
            }
        }
        2 => {
            internal
                .try_reserve_exact(capacity(item_count)?)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                internal.push(BlobChild {
                    first_offset: decoder.u64()?,
                    end_offset: decoder.u64()?,
                    page: ObjectId {
                        kind: ObjectKind::Blob,
                        digest: crate::foundation::Digest::from_bytes(decoder.fixed()?),
                    },
                });
            }
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "blob_page",
                tag,
            });
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

/// Nonblocking bounded byte source for browser, network, and native streams.
pub trait AsyncBlobSource {
    /// Fills some prefix of `destination`, returning zero only at end of stream.
    fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        cancellation: &'a CancellationToken,
    ) -> impl Future<Output = std::io::Result<usize>>;
}

impl<T: Read> AsyncBlobSource for T {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "blob source read cancelled",
            ));
        }
        Read::read(self, destination)
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
        source,
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
    store: &S,
    source: &mut R,
    options: BlobBuildOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BlobBuild, BlobBuildFailure> {
    if cancellation.is_cancelled() {
        return Err(OperationFailure::before_work(BlobBuildError::Cancelled));
    }
    if options.chunk_bytes == 0
        || effective_page_width(options).is_none()
        || options.maximum_blob_bytes == 0
    {
        return Err(build_failed(
            BlobBuildError::InvalidOptions,
            WorkCounters::default(),
        ));
    }
    let chunk_capacity = usize::try_from(options.chunk_bytes)
        .map_err(|_| build_failed(BlobBuildError::InvalidOptions, WorkCounters::default()))?;
    let mut index = BlobIndexBuilder::new(options);
    let mut logical_bytes = 0_u64;
    let mut work = WorkCounters::default();
    loop {
        let detection_bytes = options
            .maximum_blob_bytes
            .saturating_sub(logical_bytes)
            .saturating_add(1);
        let allocation = chunk_capacity.min(usize::try_from(detection_bytes).unwrap_or(usize::MAX));
        let (mut bytes, prospective, simultaneous) =
            allocate_chunk_buffer(allocation, index.live_allocation_bytes, work, budget)?;
        work = prospective;
        let mut filled = 0_usize;
        while filled < bytes.len() {
            if cancellation.is_cancelled() {
                return Err(build_failed(BlobBuildError::Cancelled, work));
            }
            match AsyncBlobSource::read(source, &mut bytes[filled..], cancellation).await {
                Ok(0) => break,
                Ok(count) => {
                    filled = filled.saturating_add(count);
                    work = build_add(
                        work,
                        WorkCounters {
                            source_bytes_read: u64::try_from(count).unwrap_or(u64::MAX),
                            ..WorkCounters::default()
                        },
                    )?;
                    build_verify(work, budget)?;
                }
                Err(error) => return Err(build_failed(BlobBuildError::Source(error), work)),
            }
        }
        if filled == 0 {
            break;
        }
        bytes.truncate(filled);
        let filled_u64 = u64::try_from(filled).unwrap_or(u64::MAX);
        logical_bytes = logical_bytes
            .checked_add(filled_u64)
            .ok_or_else(|| build_failed(BlobBuildError::TooLarge, work))?;
        if logical_bytes > options.maximum_blob_bytes {
            return Err(build_failed(BlobBuildError::TooLarge, work));
        }
        let first_offset = logical_bytes - filled_u64;
        let chunk_bytes = Bytes::from(bytes);
        let chunk = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &chunk_bytes),
        };
        work = build_put(
            store,
            chunk,
            chunk_bytes,
            simultaneous,
            budget,
            work,
            cancellation,
        )
        .await?;
        index
            .push_chunk(
                store,
                BlobChunkRef {
                    first_offset,
                    end_offset: logical_bytes,
                    chunk,
                },
                budget,
                &mut work,
                cancellation,
            )
            .await?;
        if filled < allocation {
            break;
        }
    }
    let root = index.finish(store, budget, &mut work, cancellation).await?;
    Ok(BlobBuild {
        root,
        logical_bytes,
        work,
    })
}

fn allocate_chunk_buffer(
    allocation: usize,
    index_live_bytes: u64,
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<(Vec<u8>, WorkCounters, u64), BlobBuildFailure> {
    let allocation_bytes = u64::try_from(allocation).unwrap_or(u64::MAX);
    let simultaneous = index_live_bytes
        .checked_add(allocation_bytes)
        .ok_or_else(|| build_failed(BlobBuildError::TooLarge, work))?;
    let mut prospective = build_add(
        work,
        WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        },
    )?;
    prospective.peak_allocation_bytes = prospective.peak_allocation_bytes.max(simultaneous);
    build_verify(prospective, budget)?;
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(allocation).is_err() {
        return Err(build_failed(BlobBuildError::AllocationFailed, work));
    }
    bytes.resize(allocation, 0);
    Ok((bytes, prospective, simultaneous))
}

struct BlobIndexBuilder {
    options: BlobBuildOptions,
    width: usize,
    leaf: Vec<BlobChunkRef>,
    levels: Vec<Vec<BlobChild>>,
    live_allocation_bytes: u64,
}

impl BlobIndexBuilder {
    fn new(options: BlobBuildOptions) -> Self {
        Self {
            options,
            width: effective_page_width(options).unwrap_or(0),
            leaf: Vec::new(),
            levels: Vec::new(),
            live_allocation_bytes: 0,
        }
    }

    async fn push_chunk<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        chunk: BlobChunkRef,
        budget: WorkBudget,
        work: &mut WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<(), BlobBuildFailure> {
        reserve_page_items(
            &mut self.leaf,
            self.width,
            &mut self.live_allocation_bytes,
            work,
            budget,
        )?;
        self.leaf.push(chunk);
        if self.leaf.len() == self.width {
            self.flush_leaf(store, budget, work, cancellation).await?;
        }
        Ok(())
    }

    async fn flush_leaf<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        budget: WorkBudget,
        work: &mut WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<(), BlobBuildFailure> {
        let mut items = std::mem::take(&mut self.leaf);
        if items.is_empty() {
            return Ok(());
        }
        let page = BlobPage {
            first_offset: items[0].first_offset,
            end_offset: items[items.len() - 1].end_offset,
            node: BlobNode::Leaf(items),
        };
        let page_id = put_blob_page(
            store,
            &page,
            self.options,
            self.live_allocation_bytes,
            budget,
            work,
            cancellation,
        )
        .await?;
        let child = BlobChild {
            first_offset: page.first_offset,
            end_offset: page.end_offset,
            page: page_id,
        };
        let BlobNode::Leaf(reusable) = page.node else {
            unreachable!();
        };
        items = reusable;
        items.clear();
        self.leaf = items;
        self.push_child(store, 0, child, budget, work, cancellation)
            .await
    }

    async fn push_child<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        mut level: usize,
        mut child: BlobChild,
        budget: WorkBudget,
        work: &mut WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<(), BlobBuildFailure> {
        loop {
            self.ensure_level(level, work, budget)?;
            reserve_page_items(
                &mut self.levels[level],
                self.width,
                &mut self.live_allocation_bytes,
                work,
                budget,
            )?;
            self.levels[level].push(child);
            if self.levels[level].len() < self.width {
                return Ok(());
            }
            child = self
                .flush_internal(store, level, budget, work, cancellation)
                .await?;
            level = level
                .checked_add(1)
                .ok_or_else(|| build_failed(BlobBuildError::TooLarge, *work))?;
        }
    }

    async fn flush_internal<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        level: usize,
        budget: WorkBudget,
        work: &mut WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<BlobChild, BlobBuildFailure> {
        let items = std::mem::take(&mut self.levels[level]);
        let page = BlobPage {
            first_offset: items[0].first_offset,
            end_offset: items[items.len() - 1].end_offset,
            node: BlobNode::Internal(items),
        };
        let page_id = put_blob_page(
            store,
            &page,
            self.options,
            self.live_allocation_bytes,
            budget,
            work,
            cancellation,
        )
        .await?;
        let child = BlobChild {
            first_offset: page.first_offset,
            end_offset: page.end_offset,
            page: page_id,
        };
        let BlobNode::Internal(mut reusable) = page.node else {
            unreachable!();
        };
        reusable.clear();
        self.levels[level] = reusable;
        Ok(child)
    }

    fn ensure_level(
        &mut self,
        level: usize,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<(), BlobBuildFailure> {
        while self.levels.len() <= level {
            let bytes = u64::try_from(size_of::<Vec<BlobChild>>()).unwrap_or(u64::MAX);
            let next_live = self
                .live_allocation_bytes
                .checked_add(bytes)
                .ok_or_else(|| build_failed(BlobBuildError::TooLarge, *work))?;
            let mut prospective = work
                .checked_add(WorkCounters {
                    allocation_operations: 1,
                    ..WorkCounters::default()
                })
                .map_err(|error| build_failed(error.into(), *work))?;
            prospective.peak_allocation_bytes = prospective.peak_allocation_bytes.max(next_live);
            build_verify(prospective, budget)?;
            if self.levels.try_reserve_exact(1).is_err() {
                return Err(build_failed(BlobBuildError::AllocationFailed, *work));
            }
            self.levels.push(Vec::new());
            self.live_allocation_bytes = next_live;
            *work = prospective;
        }
        Ok(())
    }

    async fn finish<S: AsyncObjectStore>(
        mut self,
        store: &S,
        budget: WorkBudget,
        work: &mut WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, BlobBuildFailure> {
        if self.leaf.is_empty() && self.levels.is_empty() {
            return put_blob_page(
                store,
                &BlobPage {
                    first_offset: 0,
                    end_offset: 0,
                    node: BlobNode::Leaf(Vec::new()),
                },
                self.options,
                self.live_allocation_bytes,
                budget,
                work,
                cancellation,
            )
            .await;
        }
        self.flush_leaf(store, budget, work, cancellation).await?;
        loop {
            let Some(level) = self.levels.iter().position(|items| !items.is_empty()) else {
                return Err(build_failed(BlobBuildError::InvalidIndexState, *work));
            };
            let higher_is_empty = self.levels[level + 1..].iter().all(Vec::is_empty);
            if higher_is_empty && self.levels[level].len() == 1 {
                return Ok(self.levels[level][0].page);
            }
            let child = self
                .flush_internal(store, level, budget, work, cancellation)
                .await?;
            self.push_child(store, level + 1, child, budget, work, cancellation)
                .await?;
        }
    }
}

fn effective_page_width(options: BlobBuildOptions) -> Option<usize> {
    let item_bound = usize::try_from(options.page_items).ok()?;
    let byte_bound = usize::try_from(options.page_bytes)
        .ok()?
        .checked_sub(BLOB_PAGE_HEADER_BYTES)?
        / BLOB_PAGE_ITEM_BYTES;
    let width = item_bound.min(byte_bound);
    (width >= 2).then_some(width)
}

fn reserve_page_items<T>(
    items: &mut Vec<T>,
    width: usize,
    live_allocation_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), BlobBuildFailure> {
    if items.capacity() >= width {
        return Ok(());
    }
    let bytes = width
        .checked_mul(size_of::<T>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| build_failed(BlobBuildError::TooLarge, *work))?;
    let next_live = live_allocation_bytes
        .checked_add(bytes)
        .ok_or_else(|| build_failed(BlobBuildError::TooLarge, *work))?;
    let mut prospective = work
        .checked_add(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| build_failed(error.into(), *work))?;
    prospective.peak_allocation_bytes = prospective.peak_allocation_bytes.max(next_live);
    build_verify(prospective, budget)?;
    if items.try_reserve_exact(width - items.capacity()).is_err() {
        return Err(build_failed(BlobBuildError::AllocationFailed, *work));
    }
    *live_allocation_bytes = next_live;
    *work = prospective;
    Ok(())
}

async fn put_blob_page<S: AsyncObjectStore>(
    store: &S,
    page: &BlobPage,
    options: BlobBuildOptions,
    live_allocation_bytes: u64,
    budget: WorkBudget,
    work: &mut WorkCounters,
    cancellation: &CancellationToken,
) -> Result<ObjectId, BlobBuildFailure> {
    let encoded = encode_blob_page(page, options.page_items)
        .map_err(|error| build_failed(BlobBuildError::Encode(error), *work))?;
    if encoded.len() > usize::try_from(options.page_bytes).unwrap_or(usize::MAX) {
        return Err(build_failed(
            BlobBuildError::PageTooLarge {
                observed: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
                maximum: u64::from(options.page_bytes),
            },
            *work,
        ));
    }
    let page_id = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &encoded),
    };
    let encoded_bytes = u64::try_from(encoded.capacity()).unwrap_or(u64::MAX);
    let simultaneous = live_allocation_bytes
        .checked_add(encoded_bytes)
        .ok_or_else(|| build_failed(BlobBuildError::TooLarge, *work))?;
    let mut encoded_work = build_add(
        *work,
        WorkCounters {
            page_writes: 1,
            bytes_encoded: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
            allocation_operations: 1,
            ..WorkCounters::default()
        },
    )?;
    encoded_work.peak_allocation_bytes = encoded_work.peak_allocation_bytes.max(simultaneous);
    build_verify(encoded_work, budget)?;
    *work = build_put(
        store,
        page_id,
        Bytes::from(encoded),
        simultaneous,
        budget,
        encoded_work,
        cancellation,
    )
    .await?;
    Ok(page_id)
}

async fn build_put<S: AsyncObjectStore>(
    store: &S,
    object: ObjectId,
    bytes: Bytes,
    live_allocation_bytes: u64,
    budget: WorkBudget,
    work: WorkCounters,
    cancellation: &CancellationToken,
) -> Result<WorkCounters, BlobBuildFailure> {
    let mut remaining = work
        .remaining(budget)
        .map_err(|error| build_failed(BlobBuildError::Work(error), work))?;
    remaining.peak_allocation_bytes = budget
        .peak_allocation_bytes
        .checked_sub(live_allocation_bytes)
        .ok_or_else(|| build_failed(BlobBuildError::Work(WorkError::Overflow), work))?;
    match AsyncObjectStore::put(store, object, bytes, remaining, cancellation).await {
        Ok(receipt) => {
            let backend_peak = live_allocation_bytes
                .checked_add(receipt.work.peak_allocation_bytes)
                .ok_or_else(|| build_failed(BlobBuildError::Work(WorkError::Overflow), work))?;
            let mut backend_work = receipt.work;
            backend_work.peak_allocation_bytes = 0;
            let mut combined = build_add(work, backend_work)?;
            combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(backend_peak);
            Ok(combined)
        }
        Err(failure) => Err(failure.map_with_prior_work(work, BlobBuildError::Storage)),
    }
}

fn build_add(left: WorkCounters, right: WorkCounters) -> Result<WorkCounters, BlobBuildFailure> {
    left.checked_add(right)
        .map_err(|error| build_failed(BlobBuildError::Work(error), left))
}

fn build_verify(work: WorkCounters, budget: WorkBudget) -> Result<(), BlobBuildFailure> {
    work.verify(budget)
        .map_err(|error| build_failed(BlobBuildError::Work(error), work))
}

/// Streaming blob-build failure retaining exact completed work.
pub type BlobBuildFailure = OperationFailure<BlobBuildError>;

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
    /// Incremental index finalization reached an impossible state.
    #[error("blob build index state is invalid")]
    InvalidIndexState,
    /// Input exceeded the admitted logical size.
    #[error("blob source exceeds its admitted logical size")]
    TooLarge,
    /// Source stream failed.
    #[error("blob source failed: {0}")]
    Source(#[source] std::io::Error),
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
    /// Requested bytes.
    pub bytes: Bytes,
    /// Exact kernel and backend work.
    pub work: WorkCounters,
    /// Exact authenticated forward page or chunk exposed by this traversal.
    pub next_residency: Option<ResidencyHint>,
}

/// Reads only blob-index pages and chunks intersecting one exact range.
///
/// # Errors
///
/// Rejects invalid bounds, missing/corrupt objects, graph cycles, child-bound
/// forgery, incomplete coverage, or any work outside the admitted budget.
pub fn read_blob_range<S: ObjectStore>(
    store: &S,
    root: ObjectId,
    range: ByteRange,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<BlobRead, BlobReadFailure> {
    let mut machine = BlobRangeMachine::new(root, range, limits, budget)?;
    frontier::drive_sync(store, &mut machine)
}

/// Asynchronously reads one authenticated logical blob range through the same
/// transition machine as [`read_blob_range`].
///
/// # Errors
///
/// Returns the same typed authentication and work failures, plus cooperative
/// cancellation before the next immutable-object read.
pub async fn read_blob_range_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    range: ByteRange,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BlobRead, BlobReadFailure> {
    if cancellation.is_cancelled() {
        return Err(failed(BlobReadError::Cancelled, WorkCounters::default()));
    }
    let mut machine = BlobRangeMachine::new(root, range, limits, budget)?;
    drive_blob_async(store, &mut machine, cancellation).await
}

async fn drive_blob_async<S: AsyncObjectStore>(
    store: &S,
    machine: &mut BlobRangeMachine,
    cancellation: &CancellationToken,
) -> Result<BlobRead, BlobReadFailure> {
    loop {
        if cancellation.is_cancelled() {
            return Err(machine.cancelled());
        }
        if let Some(output) = machine.complete()? {
            return Ok(output);
        }
        let request = machine.prepare_read()?;
        let key = (request.page.kind == ObjectKind::Blob)
            .then(|| DecodedCacheKey::new::<BlobPage>(request.page, machine.limits));
        if let Some(key) = key {
            let cached = store
                .decoded_cache_get(key)
                .map_err(|error| failed(error.into(), machine.work))?;
            if let Some(cached) = cached {
                machine.accept_cached_page(cached)?;
                continue;
            }
        }
        let receipt = AsyncObjectStore::read(
            store,
            request.page,
            request.maximum_bytes,
            request.remaining,
            cancellation,
        )
        .await
        .map_err(|failure| machine.storage_failure(request.prospective, failure))?;
        machine.accept_read_with_cache(store, key, request.prospective, &receipt)?;
    }
}

#[derive(Clone, Copy)]
enum PendingBlobRead {
    Page {
        page: ObjectId,
        expected_first: u64,
        expected_end: Option<u64>,
        height: u16,
    },
    Chunk(BlobChunkRef),
}

struct BlobRangeMachine {
    range: ByteRange,
    range_end: u64,
    limits: DecodeLimits,
    budget: WorkBudget,
    allocation: usize,
    maximum_pending: usize,
    pending: Vec<PendingBlobRead>,
    pending_capacity: LogicalVecCapacity,
    awaiting: Option<PendingBlobRead>,
    visited: VisitedObjectSet,
    output: Vec<u8>,
    work: WorkCounters,
    allocations: AllocationLedger,
    successor: Option<(u64, ResidencyHint)>,
}

impl BlobRangeMachine {
    fn new(
        root: ObjectId,
        range: ByteRange,
        limits: DecodeLimits,
        budget: WorkBudget,
    ) -> Result<Self, BlobReadFailure> {
        if root.kind != ObjectKind::Blob {
            return Err(failed(
                BlobReadError::WrongRootKind,
                WorkCounters::default(),
            ));
        }
        let range_end = range
            .offset
            .checked_add(range.length)
            .ok_or_else(|| failed(BlobReadError::InvalidRange, WorkCounters::default()))?;
        let allocation = usize::try_from(range.length)
            .map_err(|_| failed(BlobReadError::InvalidRange, WorkCounters::default()))?;
        if !limits.page_limits_valid(1) {
            return Err(failed(
                BlobReadError::InvalidLimits,
                WorkCounters::default(),
            ));
        }
        let maximum_visited = usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| failed(BlobReadError::InvalidLimits, WorkCounters::default()))?;
        let maximum_page_items = usize::try_from(limits.maximum_page_items)
            .map_err(|_| failed(BlobReadError::InvalidLimits, WorkCounters::default()))?;
        let maximum_pending = maximum_visited
            .checked_add(maximum_page_items)
            .ok_or_else(|| failed(BlobReadError::InvalidLimits, WorkCounters::default()))?;
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::default();
        allocations
            .claim_bytes(range.length, u64::from(allocation != 0), &mut work, budget)
            .map_err(|error| failed(error.into(), work))?;
        let visited = VisitedObjectSet::new(maximum_visited, &mut allocations, &mut work, budget)
            .map_err(|error| failed(error.into(), work))?;
        let mut pending = Vec::new();
        let mut pending_capacity = LogicalVecCapacity::default();
        pending_capacity
            .ensure_for_push(
                &mut pending,
                maximum_pending,
                &mut allocations,
                &mut work,
                budget,
            )
            .map_err(|error| failed(error.into(), work))?;
        pending.push(PendingBlobRead::Page {
            page: root,
            expected_first: 0,
            expected_end: None,
            height: 1,
        });
        let mut output = Vec::new();
        if output.try_reserve_exact(allocation).is_err() {
            return Err(failed(BlobReadError::AllocationFailed, work));
        }
        Ok(Self {
            range,
            range_end,
            limits,
            budget,
            allocation,
            maximum_pending,
            pending,
            pending_capacity,
            awaiting: None,
            visited,
            output,
            work,
            allocations,
            successor: None,
        })
    }

    fn finish(&mut self) -> Result<BlobRead, BlobReadFailure> {
        if self.output.len() != self.allocation {
            return Err(failed(BlobReadError::IncompleteCoverage, self.work));
        }
        Ok(BlobRead {
            bytes: Bytes::from(std::mem::take(&mut self.output)),
            work: self.work,
            next_residency: self.successor.map(|(_, hint)| hint),
        })
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, BlobReadFailure> {
        let pending = self
            .pending
            .pop()
            .ok_or_else(|| failed(BlobReadError::TraversalState, self.work))?;
        let (object, maximum_bytes) = match pending {
            PendingBlobRead::Page { page, height, .. } => {
                if height > self.limits.maximum_page_height {
                    return Err(failed(BlobReadError::CycleOrHeight, self.work));
                }
                let visited = self
                    .visited
                    .insert(page, &mut self.allocations, &mut self.work, self.budget)
                    .map_err(|error| failed(error.into(), self.work))?;
                if !visited.inserted {
                    return Err(failed(BlobReadError::CycleOrHeight, self.work));
                }
                (page, self.limits.maximum_page_object_bytes())
            }
            PendingBlobRead::Chunk(chunk) => (chunk.chunk, chunk.end_offset - chunk.first_offset),
        };
        let mut remaining = self
            .work
            .remaining(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        remaining.peak_allocation_bytes = self
            .budget
            .peak_allocation_bytes
            .checked_sub(self.allocations.live_bytes())
            .ok_or_else(|| failed(BlobReadError::Work(WorkError::Overflow), self.work))?;
        self.awaiting = Some(pending);
        Ok(frontier::ReadRequest {
            page: object,
            maximum_bytes,
            remaining,
            prospective: self.work,
        })
    }

    fn accept_read(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<BlobRead>, BlobReadFailure> {
        let pending = self
            .awaiting
            .take()
            .ok_or_else(|| failed(BlobReadError::TraversalState, self.work))?;
        self.work =
            merge_blob_backend_work(prospective, receipt.work, self.allocations.live_bytes())
                .map_err(|error| failed(error.into(), prospective))?;
        match pending {
            PendingBlobRead::Page {
                expected_first,
                expected_end,
                height,
                ..
            } => self.accept_page(receipt, expected_first, expected_end, height)?,
            PendingBlobRead::Chunk(chunk) => self.accept_chunk(receipt, chunk)?,
        }
        Ok(None)
    }

    fn accept_read_with_cache<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        key: Option<DecodedCacheKey>,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<(), BlobReadFailure> {
        let pending = self
            .awaiting
            .take()
            .ok_or_else(|| failed(BlobReadError::TraversalState, self.work))?;
        self.work =
            merge_blob_backend_work(prospective, receipt.work, self.allocations.live_bytes())
                .map_err(|error| failed(error.into(), prospective))?;
        match pending {
            PendingBlobRead::Page {
                expected_first,
                expected_end,
                height,
                ..
            } => self.accept_page_with_cache(
                store,
                key.ok_or_else(|| failed(BlobReadError::TraversalState, self.work))?,
                receipt,
                expected_first,
                expected_end,
                height,
            ),
            PendingBlobRead::Chunk(chunk) => self.accept_chunk(receipt, chunk),
        }
    }

    fn add_items(&mut self, count: u64) -> Result<(), BlobReadFailure> {
        self.work = add_work(
            self.work,
            WorkCounters {
                items_examined: count,
                ..WorkCounters::default()
            },
        )?;
        verify_work(self.work, self.budget)
    }

    fn accept_page(
        &mut self,
        receipt: &ObjectReceipt<ObjectRead>,
        expected_first: u64,
        expected_end: Option<u64>,
        height: u16,
    ) -> Result<(), BlobReadFailure> {
        let (page, decoded_bytes, retained_bytes) = self.decode_page(receipt)?;
        let result = self.accept_decoded_page(&page, expected_first, expected_end, height);
        self.allocations
            .release(decoded_bytes)
            .map_err(|error| failed(error.into(), self.work))?;
        self.allocations
            .release(retained_bytes)
            .map_err(|error| failed(error.into(), self.work))?;
        result
    }

    fn decode_page(
        &mut self,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<(BlobPage, u64, u64), BlobReadFailure> {
        self.work = add_work(
            self.work,
            WorkCounters {
                page_reads: 1,
                ..WorkCounters::default()
            },
        )?;
        verify_work(self.work, self.budget)?;
        let retained_bytes = match receipt.value.retention {
            ObjectReadRetention::Shared => 0,
            ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        self.allocations
            .claim_bytes(retained_bytes, 0, &mut self.work, self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        let shape = blob_page_decode_shape(&receipt.value, self.limits)
            .map_err(|error| failed(error.into(), self.work))?;
        self.add_items(u64::try_from(shape.items).unwrap_or(u64::MAX))?;
        let item_bytes = match shape.kind {
            DecodedPageKind::Leaf => size_of::<BlobChunkRef>(),
            DecodedPageKind::Internal => size_of::<BlobChild>(),
        };
        let decoded_bytes = shape
            .items
            .checked_mul(item_bytes)
            .map(crate::foundation::usize_to_u64)
            .ok_or_else(|| failed(BlobReadError::AllocationFailed, self.work))?;
        self.allocations
            .claim_bytes(
                decoded_bytes,
                u64::from(decoded_bytes != 0),
                &mut self.work,
                self.budget,
            )
            .map_err(|error| failed(error.into(), self.work))?;
        let page = decode_blob_page(&receipt.value, self.limits)
            .map_err(|error| failed(BlobReadError::Decode(error), self.work))?;
        Ok((page, decoded_bytes, retained_bytes))
    }

    fn accept_cached_page(&mut self, cached: DecodedCacheValue) -> Result<(), BlobReadFailure> {
        let pending = self
            .awaiting
            .take()
            .ok_or_else(|| failed(BlobReadError::TraversalState, self.work))?;
        let PendingBlobRead::Page {
            expected_first,
            expected_end,
            height,
            ..
        } = pending
        else {
            return Err(failed(BlobReadError::TraversalState, self.work));
        };
        self.work = add_work(
            self.work,
            WorkCounters {
                page_reads: 1,
                ..WorkCounters::default()
            },
        )?;
        verify_work(self.work, self.budget)?;
        let page = cached
            .value
            .downcast::<BlobPage>()
            .map_err(|_| failed(ObjectStoreError::Corrupt.into(), self.work))?;
        self.accept_decoded_page(&page, expected_first, expected_end, height)
    }

    fn accept_page_with_cache<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        key: DecodedCacheKey,
        receipt: &ObjectReceipt<ObjectRead>,
        expected_first: u64,
        expected_end: Option<u64>,
        height: u16,
    ) -> Result<(), BlobReadFailure> {
        let (page, decoded_bytes, retained_bytes) = self.decode_page(receipt)?;
        let admitted = store.decoded_cache_admit(
            key,
            DecodedCacheValue {
                value: Arc::new(page),
                logical_bytes: decoded_bytes,
            },
        );
        let result = match admitted {
            Ok(DecodedCacheAdmission::Shared(value)) => {
                self.allocations
                    .release(decoded_bytes)
                    .map_err(|error| failed(error.into(), self.work))?;
                let page = value
                    .value
                    .downcast::<BlobPage>()
                    .map_err(|_| failed(ObjectStoreError::Corrupt.into(), self.work))?;
                self.accept_decoded_page(&page, expected_first, expected_end, height)
            }
            Ok(DecodedCacheAdmission::Uncached(value)) => {
                let page = value
                    .value
                    .downcast::<BlobPage>()
                    .map_err(|_| failed(ObjectStoreError::Corrupt.into(), self.work))?;
                let result = self.accept_decoded_page(&page, expected_first, expected_end, height);
                self.allocations
                    .release(decoded_bytes)
                    .map_err(|error| failed(error.into(), self.work))?;
                result
            }
            Err(error) => {
                self.allocations
                    .release(decoded_bytes)
                    .map_err(|release| failed(release.into(), self.work))?;
                Err(failed(error.into(), self.work))
            }
        };
        self.allocations
            .release(retained_bytes)
            .map_err(|error| failed(error.into(), self.work))?;
        result
    }

    fn accept_decoded_page(
        &mut self,
        page: &BlobPage,
        expected_first: u64,
        expected_end: Option<u64>,
        height: u16,
    ) -> Result<(), BlobReadFailure> {
        if page.first_offset != expected_first
            || expected_end.is_some_and(|end| page.end_offset != end)
        {
            return Err(failed(BlobReadError::ChildBoundsMismatch, self.work));
        }
        if expected_end.is_none() && self.range_end > page.end_offset {
            return Err(failed(BlobReadError::InvalidRange, self.work));
        }
        match &page.node {
            BlobNode::Internal(children) => self.push_children(children, height)?,
            BlobNode::Leaf(chunks) => self.push_chunks(chunks)?,
        }
        Ok(())
    }

    fn push_children(
        &mut self,
        children: &[BlobChild],
        height: u16,
    ) -> Result<(), BlobReadFailure> {
        let child_height = height
            .checked_add(1)
            .ok_or_else(|| failed(BlobReadError::CycleOrHeight, self.work))?;
        for child in children.iter().rev() {
            self.examine_item()?;
            if child.first_offset < self.range_end && child.end_offset > self.range.offset {
                self.pending_capacity
                    .ensure_for_push(
                        &mut self.pending,
                        self.maximum_pending,
                        &mut self.allocations,
                        &mut self.work,
                        self.budget,
                    )
                    .map_err(|error| failed(error.into(), self.work))?;
                self.pending.push(PendingBlobRead::Page {
                    page: child.page,
                    expected_first: child.first_offset,
                    expected_end: Some(child.end_offset),
                    height: child_height,
                });
            } else if self.range.length != 0 && child.first_offset >= self.range_end {
                self.retain_successor(
                    child.first_offset,
                    ObjectReadRequest {
                        object_id: child.page,
                        maximum_bytes: self.limits.maximum_page_object_bytes(),
                    },
                );
            }
        }
        Ok(())
    }

    fn push_chunks(&mut self, chunks: &[BlobChunkRef]) -> Result<(), BlobReadFailure> {
        for chunk in chunks.iter().rev() {
            self.examine_item()?;
            if chunk.first_offset < self.range_end && chunk.end_offset > self.range.offset {
                self.pending_capacity
                    .ensure_for_push(
                        &mut self.pending,
                        self.maximum_pending,
                        &mut self.allocations,
                        &mut self.work,
                        self.budget,
                    )
                    .map_err(|error| failed(error.into(), self.work))?;
                self.pending.push(PendingBlobRead::Chunk(*chunk));
            } else if self.range.length != 0 && chunk.first_offset >= self.range_end {
                self.retain_successor(
                    chunk.first_offset,
                    ObjectReadRequest {
                        object_id: chunk.chunk,
                        maximum_bytes: chunk.end_offset - chunk.first_offset,
                    },
                );
            }
        }
        Ok(())
    }

    fn retain_successor(&mut self, offset: u64, request: ObjectReadRequest) {
        if self
            .successor
            .is_none_or(|(current_offset, _)| offset < current_offset)
        {
            self.successor = Some((
                offset,
                ResidencyHint {
                    request,
                    reason: ResidencyReason::SequentialRange,
                },
            ));
        }
    }

    fn examine_item(&mut self) -> Result<(), BlobReadFailure> {
        self.work = add_work(
            self.work,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            },
        )?;
        verify_work(self.work, self.budget)
    }

    fn accept_chunk(
        &mut self,
        receipt: &ObjectReceipt<ObjectRead>,
        chunk: BlobChunkRef,
    ) -> Result<(), BlobReadFailure> {
        let retained_bytes = match receipt.value.retention {
            ObjectReadRetention::Shared => 0,
            ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        self.allocations
            .claim_bytes(retained_bytes, 0, &mut self.work, self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        let chunk_length = chunk.end_offset - chunk.first_offset;
        if u64::try_from(receipt.value.len()).unwrap_or(u64::MAX) != chunk_length {
            return Err(failed(BlobReadError::ChunkLengthMismatch, self.work));
        }
        let start = self.range.offset.max(chunk.first_offset) - chunk.first_offset;
        let end = self.range_end.min(chunk.end_offset) - chunk.first_offset;
        let start =
            usize::try_from(start).map_err(|_| failed(BlobReadError::InvalidRange, self.work))?;
        let end =
            usize::try_from(end).map_err(|_| failed(BlobReadError::InvalidRange, self.work))?;
        self.output.extend_from_slice(&receipt.value[start..end]);
        self.work = add_work(
            self.work,
            WorkCounters {
                bytes_copied: u64::try_from(end - start).unwrap_or(u64::MAX),
                output_bytes: u64::try_from(end - start).unwrap_or(u64::MAX),
                items_returned: 1,
                ..WorkCounters::default()
            },
        )?;
        verify_work(self.work, self.budget)?;
        self.allocations
            .release(retained_bytes)
            .map_err(|error| failed(error.into(), self.work))
    }
}

impl frontier::Machine for BlobRangeMachine {
    type Output = BlobRead;
    type Failure = BlobReadFailure;

    fn complete(&mut self) -> Result<Option<Self::Output>, Self::Failure> {
        if self.pending.is_empty() && self.awaiting.is_none() {
            return self.finish().map(Some);
        }
        Ok(None)
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, Self::Failure> {
        BlobRangeMachine::prepare_read(self)
    }

    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<Self::Output>, Self::Failure> {
        self.accept_read(prospective, receipt)
    }

    fn storage_failure(
        &self,
        prospective: WorkCounters,
        failure: crate::storage::ObjectFailure,
    ) -> Self::Failure {
        match merge_blob_backend_work(prospective, *failure.work, self.allocations.live_bytes()) {
            Ok(combined) => failed(failure.error.into(), combined),
            Err(error) => failed(error.into(), prospective),
        }
    }

    fn cancelled(&self) -> Self::Failure {
        failed(BlobReadError::Cancelled, self.work)
    }
}

fn add_work(left: WorkCounters, right: WorkCounters) -> Result<WorkCounters, BlobReadFailure> {
    left.checked_add(right)
        .map_err(|error| failed(BlobReadError::Work(error), left))
}

fn verify_work(work: WorkCounters, budget: WorkBudget) -> Result<(), BlobReadFailure> {
    work.verify(budget)
        .map_err(|error| failed(BlobReadError::Work(error), work))
}

fn merge_blob_backend_work(
    prior: WorkCounters,
    mut backend: WorkCounters,
    live_bytes: u64,
) -> Result<WorkCounters, WorkError> {
    let simultaneous_peak = live_bytes
        .checked_add(backend.peak_allocation_bytes)
        .ok_or(WorkError::Overflow)?;
    backend.peak_allocation_bytes = 0;
    let mut merged = prior.checked_add(backend)?;
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    Ok(merged)
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
}

impl From<AllocationError> for BlobReadError {
    fn from(error: AllocationError) -> Self {
        match error {
            AllocationError::Work(error) => Self::Work(error),
            AllocationError::Overflow | AllocationError::ReleaseInvariant => {
                Self::Work(WorkError::Overflow)
            }
            AllocationError::InvalidCapacity
            | AllocationError::CapacityExceeded
            | AllocationError::AllocationFailed => Self::AllocationFailed,
        }
    }
}

fn count(value: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn capacity(value: u32) -> Result<usize, CanonicalDecodeError> {
    usize::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(message: &str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(message.to_owned())
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/blob.rs"]
mod tests;
