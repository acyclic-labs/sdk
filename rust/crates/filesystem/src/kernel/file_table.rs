//! Canonical path-independent file records and authenticated file-table pages.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::file_table_mutation::FileTableFormat;
use super::frontier;
use super::persistent_batch;
use super::types::{FileKind, digest_object};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::FileId;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectRead, ObjectReceipt, object_digest};
use crate::storage::{ObjectStore, ObjectStoreError};
use std::collections::HashSet;
use std::fmt;
use thiserror::Error;

const DOMAIN: &[u8; 8] = b"ACYFSFIL";
const VERSION: u16 = 2;

/// Maximum regular-file bytes stored directly in a file-table leaf record.
///
/// The fixed bound keeps decoded records allocation-free and prevents tiny
/// files from requiring separate extent, blob-index, and chunk objects.
pub const MAXIMUM_INLINE_FILE_BYTES: usize = 64;

/// Allocation-free canonical payload for one tiny regular file.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct InlineFileData {
    length: u8,
    bytes: [u8; MAXIMUM_INLINE_FILE_BYTES],
}

impl InlineFileData {
    /// Copies one admitted tiny-file payload into its canonical fixed-capacity
    /// representation.
    ///
    /// # Errors
    ///
    /// Rejects payloads larger than [`MAXIMUM_INLINE_FILE_BYTES`].
    pub fn new(bytes: &[u8]) -> Result<Self, InlineFileDataError> {
        let length = u8::try_from(bytes.len()).map_err(|_| InlineFileDataError::TooLarge)?;
        if bytes.len() > MAXIMUM_INLINE_FILE_BYTES {
            return Err(InlineFileDataError::TooLarge);
        }
        let mut stored = [0_u8; MAXIMUM_INLINE_FILE_BYTES];
        stored[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            length,
            bytes: stored,
        })
    }

    /// Returns the exact semantic file bytes without allocation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.length)]
    }

    pub(crate) fn replace_range(
        mut self,
        offset: usize,
        replacement: &[u8],
        logical_bytes: usize,
    ) -> Result<Self, InlineFileDataError> {
        let end = offset
            .checked_add(replacement.len())
            .ok_or(InlineFileDataError::TooLarge)?;
        if end > logical_bytes || logical_bytes > MAXIMUM_INLINE_FILE_BYTES {
            return Err(InlineFileDataError::TooLarge);
        }
        self.bytes[offset..end].copy_from_slice(replacement);
        self.bytes[logical_bytes..].fill(0);
        self.length = u8::try_from(logical_bytes).map_err(|_| InlineFileDataError::TooLarge)?;
        Ok(self)
    }

    pub(crate) fn truncate(mut self, logical_bytes: usize) -> Result<Self, InlineFileDataError> {
        if logical_bytes > self.as_bytes().len() {
            return Err(InlineFileDataError::TooLarge);
        }
        self.bytes[logical_bytes..].fill(0);
        self.length = u8::try_from(logical_bytes).map_err(|_| InlineFileDataError::TooLarge)?;
        Ok(self)
    }
}

impl fmt::Debug for InlineFileData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("InlineFileData")
            .field(&self.as_bytes())
            .finish()
    }
}

/// Tiny regular-file admission failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InlineFileDataError {
    /// Payload exceeds the canonical inline threshold.
    #[error("inline regular-file payload exceeds its canonical bound")]
    TooLarge,
}

/// Kind-specific immutable file payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilePayload {
    /// Tiny regular-file bytes embedded in the authenticated file-table leaf.
    InlineRegular(InlineFileData),
    /// Regular file size and sparse extent-tree root.
    Regular {
        /// Exact logical bytes, including holes.
        logical_bytes: u64,
        /// Authenticated sparse extent tree.
        extents: ObjectId,
    },
    /// Directory namespace-tree root.
    Directory {
        /// Authenticated directory tree.
        entries: ObjectId,
    },
    /// Symbolic-link target stored as an authenticated blob.
    SymbolicLink {
        /// Exact target representation bytes.
        target_bytes: u64,
        /// Authenticated blob-index root.
        target: ObjectId,
    },
    /// FIFO, socket, or mount-boundary entry with no payload.
    Empty,
    /// POSIX character or block device identity.
    Device {
        /// Device major number.
        major: u32,
        /// Device minor number.
        minor: u32,
    },
    /// Opaque Windows reparse payload.
    ReparsePoint {
        /// Exact payload bytes.
        payload_bytes: u64,
        /// Authenticated blob-index root.
        payload: ObjectId,
    },
}

/// One path-independent immutable file record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRecord {
    /// Stable identity reused by every hard link.
    pub file_id: FileId,
    /// Complete filesystem kind.
    pub kind: FileKind,
    /// Exact number of namespace bindings in this generation.
    pub link_count: u64,
    /// Canonical metadata object.
    pub metadata: ObjectId,
    /// Kind-specific content identity.
    pub payload: FilePayload,
}

impl FileRecord {
    pub(crate) fn validate(self) -> Result<(), FileTableError> {
        if self.link_count == 0 || self.metadata.kind != ObjectKind::Metadata {
            return Err(FileTableError::InvalidRecord);
        }
        let valid = match (self.kind, self.payload) {
            (FileKind::Regular, FilePayload::Regular { extents, .. }) => {
                extents.kind == ObjectKind::ExtentPage
            }
            (FileKind::Directory, FilePayload::Directory { entries }) => {
                entries.kind == ObjectKind::TreePage
            }
            (FileKind::SymbolicLink, FilePayload::SymbolicLink { target, .. })
            | (
                FileKind::ReparsePoint,
                FilePayload::ReparsePoint {
                    payload: target, ..
                },
            ) => target.kind == ObjectKind::Blob,
            (FileKind::Regular, FilePayload::InlineRegular(_))
            | (FileKind::CharacterDevice | FileKind::BlockDevice, FilePayload::Device { .. })
            | (FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary, FilePayload::Empty) => {
                true
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(FileTableError::InvalidRecord)
        }
    }
}

/// One lower-bound file-table child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileTableChild {
    /// Exact first file identity represented by the child.
    pub first_file_id: FileId,
    /// Authenticated child page.
    pub page: ObjectId,
}

/// One immutable authenticated file-table B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileTablePage {
    /// Strictly ordered file records.
    Leaf(Vec<FileRecord>),
    /// Strictly ordered lower-bound child references.
    Internal(Vec<FileTableChild>),
}

impl FileTablePage {
    fn validate(&self, maximum_items: u32) -> Result<(), FileTableError> {
        let count = match self {
            Self::Leaf(records) => {
                if records
                    .windows(2)
                    .any(|pair| pair[0].file_id >= pair[1].file_id)
                {
                    return Err(FileTableError::NotStrictlyOrdered);
                }
                for record in records {
                    record.validate()?;
                }
                records.len()
            }
            Self::Internal(children) => {
                if children.is_empty()
                    || children
                        .windows(2)
                        .any(|pair| pair[0].first_file_id >= pair[1].first_file_id)
                    || children
                        .iter()
                        .any(|child| child.page.kind != ObjectKind::FileTablePage)
                {
                    return Err(FileTableError::InvalidInternalPage);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(FileTableError::TooManyItems);
        }
        Ok(())
    }
}

/// Encodes one validated canonical file-table page.
///
/// # Errors
///
/// Rejects invalid records, ordering, bounds, or object classes.
pub fn encode_file_table_page(
    page: &FileTablePage,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    match page {
        FileTablePage::Leaf(records) => encode_file_table_leaf_records(records, maximum_items),
        FileTablePage::Internal(children) => encode_file_table_internal_children(
            children
                .iter()
                .map(|child| (child.first_file_id, child.page)),
            maximum_items,
        ),
    }
}

pub(crate) fn encode_file_table_leaf_records(
    records: &[FileRecord],
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError> {
    if u32::try_from(records.len()).unwrap_or(u32::MAX) > maximum_items {
        return Err(invariant(FileTableError::TooManyItems));
    }
    if records
        .windows(2)
        .any(|pair| pair[0].file_id >= pair[1].file_id)
    {
        return Err(invariant(FileTableError::NotStrictlyOrdered));
    }
    let mut encoded_length = DOMAIN
        .len()
        .checked_add(2 + 1 + 4)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    for record in records {
        record.validate().map_err(invariant)?;
        encoded_length = encoded_length
            .checked_add(file_record_encoded_length(*record)?)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
    }
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, encoded_length)?;
    encoder.u8(1);
    encoder.u32(count(records.len())?);
    for record in records {
        encode_record(&mut encoder, *record);
    }
    Ok(encoder.finish())
}

pub(crate) fn encode_file_table_internal_children<I>(
    children: I,
    maximum_items: u32,
) -> Result<Vec<u8>, CanonicalDecodeError>
where
    I: Clone + ExactSizeIterator<Item = (FileId, ObjectId)>,
{
    let length = children.len();
    if length == 0 {
        return Err(invariant(FileTableError::InvalidInternalPage));
    }
    if u32::try_from(length).unwrap_or(u32::MAX) > maximum_items {
        return Err(invariant(FileTableError::TooManyItems));
    }
    let mut prior = None;
    for (file_id, page) in children.clone() {
        if prior.is_some_and(|value| value >= file_id) || page.kind != ObjectKind::FileTablePage {
            return Err(invariant(FileTableError::InvalidInternalPage));
        }
        prior = Some(file_id);
    }
    let encoded_length = length
        .checked_mul(16 + 32)
        .and_then(|items| DOMAIN.len().checked_add(2 + 1 + 4 + items))
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, encoded_length)?;
    encoder.u8(2);
    encoder.u32(count(length)?);
    for (file_id, page) in children {
        encoder.fixed(&file_id.into_bytes());
        encoder.fixed(page.digest.as_bytes());
    }
    Ok(encoder.finish())
}

pub(crate) fn file_table_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let kind = match tag {
        1 => {
            for _ in 0..item_count {
                let _: [u8; 16] = decoder.fixed()?;
                decoder.u8()?;
                decoder.u64()?;
                let _: [u8; 32] = decoder.fixed()?;
                match decoder.u8()? {
                    1 | 3 | 6 => {
                        decoder.u64()?;
                        let _: [u8; 32] = decoder.fixed()?;
                    }
                    2 => {
                        let _: [u8; 32] = decoder.fixed()?;
                    }
                    4 => {}
                    5 => {
                        decoder.u32()?;
                        decoder.u32()?;
                    }
                    7 => {
                        let length = decoder.u8()?;
                        if usize::from(length) > MAXIMUM_INLINE_FILE_BYTES {
                            return Err(CanonicalDecodeError::FieldTooLarge {
                                observed: u32::from(length),
                                maximum: u32::try_from(MAXIMUM_INLINE_FILE_BYTES)
                                    .map_err(|_| CanonicalDecodeError::LengthOverflow)?,
                            });
                        }
                        decoder.take_exact(usize::from(length))?;
                    }
                    value => {
                        return Err(CanonicalDecodeError::UnknownTag {
                            field: "file_payload",
                            tag: value,
                        });
                    }
                }
            }
            DecodedPageKind::Leaf
        }
        2 => {
            for _ in 0..item_count {
                let _: [u8; 16] = decoder.fixed()?;
                let _: [u8; 32] = decoder.fixed()?;
            }
            DecodedPageKind::Internal
        }
        value => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "file_table_page",
                tag: value,
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

pub(crate) fn file_record_encoded_length(
    record: FileRecord,
) -> Result<usize, CanonicalDecodeError> {
    const PREFIX: usize = 16 + 1 + 8 + 32 + 1;
    let payload = match record.payload {
        FilePayload::InlineRegular(data) => 1_usize
            .checked_add(data.as_bytes().len())
            .ok_or(CanonicalDecodeError::LengthOverflow)?,
        FilePayload::Regular { .. }
        | FilePayload::SymbolicLink { .. }
        | FilePayload::ReparsePoint { .. } => 8 + 32,
        FilePayload::Directory { .. } => 32,
        FilePayload::Empty => 0,
        FilePayload::Device { .. } => 8,
    };
    PREFIX
        .checked_add(payload)
        .ok_or(CanonicalDecodeError::LengthOverflow)
}

fn encode_record(encoder: &mut Encoder, record: FileRecord) {
    encoder.fixed(&record.file_id.into_bytes());
    encoder.u8(record.kind.tag());
    encoder.u64(record.link_count);
    encoder.fixed(record.metadata.digest.as_bytes());
    match record.payload {
        FilePayload::InlineRegular(data) => {
            encoder.u8(7);
            encoder.u8(data.length);
            encoder.fixed(data.as_bytes());
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            encoder.u8(1);
            encoder.u64(logical_bytes);
            encoder.fixed(extents.digest.as_bytes());
        }
        FilePayload::Directory { entries } => {
            encoder.u8(2);
            encoder.fixed(entries.digest.as_bytes());
        }
        FilePayload::SymbolicLink {
            target_bytes,
            target,
        } => {
            encoder.u8(3);
            encoder.u64(target_bytes);
            encoder.fixed(target.digest.as_bytes());
        }
        FilePayload::Empty => encoder.u8(4),
        FilePayload::Device { major, minor } => {
            encoder.u8(5);
            encoder.u32(major);
            encoder.u32(minor);
        }
        FilePayload::ReparsePoint {
            payload_bytes,
            payload,
        } => {
            encoder.u8(6);
            encoder.u64(payload_bytes);
            encoder.fixed(payload.digest.as_bytes());
        }
    }
}

/// Decodes one bounded canonical file-table page.
///
/// # Errors
///
/// Fails closed on malformed bytes, unknown versions/tags, invalid records,
/// ordering, or bounds.
pub fn decode_file_table_page(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<FileTablePage, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes())?;
    let tag = decoder.u8()?;
    let item_count = decoder.u32()?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let mut records = Vec::new();
    let mut children = Vec::new();
    match tag {
        1 => {
            records
                .try_reserve_exact(capacity(item_count)?)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                records.push(decode_record(&mut decoder)?);
            }
        }
        2 => {
            children
                .try_reserve_exact(capacity(item_count)?)
                .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
            for _ in 0..item_count {
                children.push(FileTableChild {
                    first_file_id: FileId::from_bytes(decoder.fixed()?),
                    page: digest_object(ObjectKind::FileTablePage, decoder.fixed()?),
                });
            }
        }
        value => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "file_table_page",
                tag: value,
            });
        }
    }
    decoder.finish()?;
    let page = if tag == 1 {
        FileTablePage::Leaf(records)
    } else {
        FileTablePage::Internal(children)
    };
    page.validate(limits.maximum_page_items)
        .map_err(invariant)?;
    Ok(page)
}

fn decode_record(decoder: &mut Decoder<'_>) -> Result<FileRecord, CanonicalDecodeError> {
    let file_id = FileId::from_bytes(decoder.fixed()?);
    let kind = FileKind::from_tag(decoder.u8()?).map_err(invariant_tree)?;
    let link_count = decoder.u64()?;
    let metadata = digest_object(ObjectKind::Metadata, decoder.fixed()?);
    let payload = match decoder.u8()? {
        1 => FilePayload::Regular {
            logical_bytes: decoder.u64()?,
            extents: digest_object(ObjectKind::ExtentPage, decoder.fixed()?),
        },
        2 => FilePayload::Directory {
            entries: digest_object(ObjectKind::TreePage, decoder.fixed()?),
        },
        3 => FilePayload::SymbolicLink {
            target_bytes: decoder.u64()?,
            target: digest_object(ObjectKind::Blob, decoder.fixed()?),
        },
        4 => FilePayload::Empty,
        5 => FilePayload::Device {
            major: decoder.u32()?,
            minor: decoder.u32()?,
        },
        6 => FilePayload::ReparsePoint {
            payload_bytes: decoder.u64()?,
            payload: digest_object(ObjectKind::Blob, decoder.fixed()?),
        },
        7 => {
            let length = decoder.u8()?;
            if usize::from(length) > MAXIMUM_INLINE_FILE_BYTES {
                return Err(CanonicalDecodeError::FieldTooLarge {
                    observed: u32::from(length),
                    maximum: u32::try_from(MAXIMUM_INLINE_FILE_BYTES)
                        .map_err(|_| CanonicalDecodeError::LengthOverflow)?,
                });
            }
            let bytes = decoder.take_exact(usize::from(length))?;
            FilePayload::InlineRegular(InlineFileData::new(bytes).map_err(invariant_inline)?)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "file_payload",
                tag,
            });
        }
    };
    let record = FileRecord {
        file_id,
        kind,
        link_count,
        metadata,
        payload,
    };
    record.validate().map_err(invariant)?;
    Ok(record)
}

/// Computes one typed file-table page identity.
///
/// # Errors
///
/// Returns the same validation errors as [`encode_file_table_page`].
pub fn file_table_page_id(
    page: &FileTablePage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_file_table_page(page, maximum_items)?;
    Ok(ObjectId {
        kind: ObjectKind::FileTablePage,
        digest: object_digest(ObjectKind::FileTablePage, &bytes),
    })
}

/// Exact path-independent record lookup and measured authenticated work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRecordLookup {
    /// Matching record, or authenticated absence.
    pub record: Option<FileRecord>,
    /// Exact page/backend work.
    pub work: WorkCounters,
}

/// Original-order file records from one shared authenticated frontier batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRecordBatchLookup {
    /// One explicit present/absent result per requested stable file identity.
    pub records: Vec<Option<FileRecord>>,
    /// Exact shared traversal and backend work.
    pub work: WorkCounters,
}

/// Looks up file records while reading every distinct file-table frontier once.
///
/// # Errors
///
/// Rejects empty/oversized batches and returns the same authenticated routing,
/// storage, decode, allocation, and work failures as point lookup.
pub fn lookup_file_records<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    file_ids: &[FileId],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<FileRecordBatchLookup, FileRecordReadFailure> {
    persistent_batch::lookup::<S, FileTableFormat>(
        store,
        root,
        file_ids,
        maximum_queries,
        limits,
        budget,
    )
    .map(to_record_batch)
    .map_err(map_batch_failure)
}

/// Asynchronously executes the same batch as [`lookup_file_records`].
///
/// # Errors
///
/// Returns identical typed failures, including cooperative cancellation.
pub async fn lookup_file_records_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_ids: &[FileId],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<FileRecordBatchLookup, FileRecordReadFailure> {
    persistent_batch::lookup_async::<S, FileTableFormat>(
        store,
        root,
        file_ids,
        maximum_queries,
        limits,
        budget,
        cancellation,
    )
    .await
    .map(to_record_batch)
    .map_err(map_batch_failure)
}

fn to_record_batch(receipt: persistent_batch::Receipt<FileRecord>) -> FileRecordBatchLookup {
    FileRecordBatchLookup {
        records: receipt.values,
        work: receipt.work,
    }
}

fn map_batch_failure(failure: persistent_batch::Failure) -> FileRecordReadFailure {
    OperationFailure::new(map_batch_error(failure.error), *failure.work)
}

fn map_batch_error(error: persistent_batch::Error) -> FileRecordReadError {
    match error {
        persistent_batch::Error::Cancelled => FileRecordReadError::Cancelled,
        persistent_batch::Error::Empty => FileRecordReadError::EmptyBatch,
        persistent_batch::Error::TooManyQueries => FileRecordReadError::TooManyQueries,
        persistent_batch::Error::WrongRootKind => FileRecordReadError::WrongRootKind,
        persistent_batch::Error::InvalidLimits => FileRecordReadError::InvalidHeightLimit,
        persistent_batch::Error::HeightExceeded => FileRecordReadError::HeightExceeded,
        persistent_batch::Error::CycleOrAlias => FileRecordReadError::Cycle,
        persistent_batch::Error::ChildBoundsMismatch => FileRecordReadError::ChildBoundsMismatch,
        persistent_batch::Error::InvalidRouting => FileRecordReadError::InvalidRouting,
        persistent_batch::Error::AllocationFailed => FileRecordReadError::AllocationFailed,
        persistent_batch::Error::Storage(error) => error.into(),
        persistent_batch::Error::Decode(error) => error.into(),
        persistent_batch::Error::Work(error) => error.into(),
    }
}

/// Looks up one stable file identity through an authenticated file-table frontier.
///
/// # Errors
///
/// Fails on wrong object classes, routing forgery, cycles, excessive height,
/// malformed canonical pages, backend failure, or work-budget exhaustion.
pub fn lookup_file_record<S: ObjectStore>(
    store: &S,
    root: ObjectId,
    file_id: FileId,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<FileRecordLookup, FileRecordReadFailure> {
    let mut machine = FileLookupMachine::new(root, file_id, limits, budget)?;
    frontier::drive_sync(store, &mut machine)
}

/// Asynchronously looks up one file record through the same semantic machine
/// as [`lookup_file_record`].
///
/// # Errors
///
/// Returns the same typed semantic, storage, decode, work, and cancellation
/// failures as [`lookup_file_record`].
pub async fn lookup_file_record_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_id: FileId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<FileRecordLookup, FileRecordReadFailure> {
    let mut machine = FileLookupMachine::new(root, file_id, limits, budget)?;
    frontier::drive_async(store, &mut machine, cancellation).await
}

struct FileLookupMachine {
    file_id: FileId,
    limits: DecodeLimits,
    budget: WorkBudget,
    page: ObjectId,
    lower: Option<FileId>,
    upper: Option<FileId>,
    visited: HashSet<ObjectId>,
    work: WorkCounters,
}

impl FileLookupMachine {
    fn new(
        root: ObjectId,
        file_id: FileId,
        limits: DecodeLimits,
        budget: WorkBudget,
    ) -> Result<Self, FileRecordReadFailure> {
        if root.kind != ObjectKind::FileTablePage {
            return Err(file_read_failed(
                FileRecordReadError::WrongRootKind,
                WorkCounters::default(),
            ));
        }
        if !limits.page_limits_valid(1) {
            return Err(file_read_failed(
                FileRecordReadError::InvalidHeightLimit,
                WorkCounters::default(),
            ));
        }
        Ok(Self {
            file_id,
            limits,
            budget,
            page: root,
            lower: None,
            upper: None,
            visited: HashSet::new(),
            work: WorkCounters::default(),
        })
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, FileRecordReadFailure> {
        if self.visited.len() >= usize::from(self.limits.maximum_page_height) {
            return Err(file_read_failed(
                FileRecordReadError::HeightExceeded,
                self.work,
            ));
        }
        if !self.visited.insert(self.page) {
            return Err(file_read_failed(FileRecordReadError::Cycle, self.work));
        }
        let prospective = self
            .work
            .checked_add(WorkCounters {
                page_reads: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| file_read_failed(error.into(), self.work))?;
        let remaining = prospective
            .remaining(self.budget)
            .map_err(|error| file_read_failed(error.into(), self.work))?;
        Ok(frontier::ReadRequest {
            page: self.page,
            maximum_bytes: self.limits.maximum_page_object_bytes(),
            remaining,
            prospective,
        })
    }

    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<FileRecordLookup>, FileRecordReadFailure> {
        self.work = prospective
            .checked_add(receipt.work)
            .map_err(|error| file_read_failed(error.into(), prospective))?;
        self.work
            .verify(self.budget)
            .map_err(|error| file_read_failed(error.into(), self.work))?;
        match decode_file_table_page(&receipt.value, self.limits)
            .map_err(|error| file_read_failed(error.into(), self.work))?
        {
            FileTablePage::Leaf(records) => {
                validate_record_bounds(&records, self.lower, self.upper)
                    .map_err(|error| file_read_failed(error, self.work))?;
                let record = records
                    .binary_search_by_key(&self.file_id, |record| record.file_id)
                    .ok()
                    .and_then(|index| records.get(index).copied());
                Ok(Some(FileRecordLookup {
                    record,
                    work: self.work,
                }))
            }
            FileTablePage::Internal(children) => {
                validate_child_bounds(&children, self.lower, self.upper)
                    .map_err(|error| file_read_failed(error, self.work))?;
                let partition =
                    children.partition_point(|child| child.first_file_id <= self.file_id);
                let selected = partition.saturating_sub(1);
                let child = children.get(selected).copied().ok_or_else(|| {
                    file_read_failed(FileRecordReadError::InvalidRouting, self.work)
                })?;
                self.lower = Some(child.first_file_id);
                self.upper = children
                    .get(selected + 1)
                    .map(|next| next.first_file_id)
                    .or(self.upper);
                self.page = child.page;
                Ok(None)
            }
        }
    }
}

impl frontier::Machine for FileLookupMachine {
    type Output = FileRecordLookup;
    type Failure = FileRecordReadFailure;

    fn complete(&mut self) -> Result<Option<Self::Output>, Self::Failure> {
        Ok(None)
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, Self::Failure> {
        FileLookupMachine::prepare_read(self)
    }

    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<Self::Output>, Self::Failure> {
        FileLookupMachine::accept(self, prospective, receipt)
    }

    fn storage_failure(
        &self,
        prospective: WorkCounters,
        failure: crate::storage::ObjectFailure,
    ) -> Self::Failure {
        match prospective.checked_add(*failure.work) {
            Ok(spent) => file_read_failed(failure.error.into(), spent),
            Err(error) => file_read_failed(error.into(), prospective),
        }
    }

    fn cancelled(&self) -> Self::Failure {
        file_read_failed(FileRecordReadError::Cancelled, self.work)
    }
}

fn validate_record_bounds(
    records: &[FileRecord],
    lower: Option<FileId>,
    upper: Option<FileId>,
) -> Result<(), FileRecordReadError> {
    if lower.is_some() && records.first().map(|record| record.file_id) != lower {
        return Err(FileRecordReadError::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && records.last().is_some_and(|record| record.file_id >= upper)
    {
        return Err(FileRecordReadError::ChildBoundsMismatch);
    }
    Ok(())
}

fn validate_child_bounds(
    children: &[FileTableChild],
    lower: Option<FileId>,
    upper: Option<FileId>,
) -> Result<(), FileRecordReadError> {
    if lower.is_some() && children.first().map(|child| child.first_file_id) != lower {
        return Err(FileRecordReadError::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && children
            .last()
            .is_some_and(|child| child.first_file_id >= upper)
    {
        return Err(FileRecordReadError::ChildBoundsMismatch);
    }
    Ok(())
}

/// Sparse file-record lookup failure retaining exact spent work.
pub type FileRecordReadFailure = OperationFailure<FileRecordReadError>;

fn file_read_failed(error: FileRecordReadError, work: WorkCounters) -> FileRecordReadFailure {
    OperationFailure::new(error, work)
}

/// Authenticated file-table frontier failures.
#[derive(Debug, Error)]
pub enum FileRecordReadError {
    /// Cooperative cancellation occurred before the next storage boundary.
    #[error("file-record lookup was cancelled")]
    Cancelled,
    /// Batch lookup requires at least one identity.
    #[error("file-record lookup batch is empty")]
    EmptyBatch,
    /// Batch lookup exceeds its explicit query limit.
    #[error("file-record lookup batch exceeds its admitted query bound")]
    TooManyQueries,
    /// Root is not a file-table page.
    #[error("file-record lookup root is not a file-table page")]
    WrongRootKind,
    /// Page-height limit must be non-zero.
    #[error("file-table height limit must be non-zero")]
    InvalidHeightLimit,
    /// Child graph references an ancestor.
    #[error("file-table graph contains a cycle")]
    Cycle,
    /// Traversal did not reach a leaf within the admitted height.
    #[error("file-table height exceeds its admitted bound")]
    HeightExceeded,
    /// Internal routing could not select a child.
    #[error("file-table routing invariant failed")]
    InvalidRouting,
    /// Parent lower/upper routing bounds disagree with the selected child.
    #[error("file-table child bounds do not match its page")]
    ChildBoundsMismatch,
    /// A bounded batch scratch allocation failed.
    #[error("file-record lookup allocation failed")]
    AllocationFailed,
    /// Immutable-object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page failed decoding or validation.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// File-table semantic failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FileTableError {
    /// File identities must be strictly ordered and unique.
    #[error("file records are not strictly ordered")]
    NotStrictlyOrdered,
    /// Record kind, links, metadata, or payload is inconsistent.
    #[error("file record is inconsistent")]
    InvalidRecord,
    /// Internal page is empty, unordered, or points at the wrong object class.
    #[error("file-table internal page is invalid")]
    InvalidInternalPage,
    /// Page exceeds its admitted item bound.
    #[error("file-table page exceeds its item bound")]
    TooManyItems,
}

fn count(value: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn capacity(value: u32) -> Result<usize, CanonicalDecodeError> {
    usize::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

fn invariant(error: FileTableError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

fn invariant_tree(error: super::TreePageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

fn invariant_inline(error: InlineFileDataError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(error.to_string())
}

#[cfg(test)]
#[path = "tests/file_table.rs"]
mod tests;
