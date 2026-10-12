//! Canonical path-independent file records and authenticated file-table pages.

use crate::kernel::Child;
use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder, ReadAllocationStep};
use super::codec::{DecodedPageKind, DecodedPageShape};
use super::file_table_mutation::FileTableFormat;
use super::persistent_batch;
use super::persistent_point;
use super::types::{FileKind, digest_object};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{FileId, usize_to_u64};
use crate::heap_future::in_heap_measured;
use super::rebase::ProofDataCustody;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError, object_digest};
use std::fmt;
use strum::{AsRefStr, EnumDiscriminants, EnumIter};
use thiserror::Error;

/// Encodes one canonical page with before-effect admission.
pub fn encode_file_table_page<E>(
    page: &FileTablePage, maximum_items: u32,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where E: From<CanonicalDecodeError> + From<crate::WorkError> {
    match page {
        FileTablePage::Leaf(records) => encode_file_table_leaf_records(records, maximum_items, None, &mut observe, allocation_error),
        FileTablePage::Internal(children) => encode_file_table_internal_children(children.iter().map(|child| (child.first, child.page)), maximum_items, observe, allocation_error),
    }
}

const DOMAIN: &[u8; 8] = b"ACYFSFIL";
const VERSION: u16 = 1;
const RECORD_DOMAIN: &[u8; 8] = b"ACYFSREC";
const RECORD_VERSION: u16 = 1;

/// Maximum regular-file bytes stored directly in a file-table leaf record.
pub const MAXIMUM_INLINE_FILE_BYTES: usize = 64;

/// Allocation-free canonical payload for one tiny regular file.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct InlineFileData {
    length: u8,
    bytes: [u8; MAXIMUM_INLINE_FILE_BYTES],
}

impl InlineFileData {
    /// Copies one admitted tiny-file payload into its canonical fixed-capacity representation.
    pub fn new(bytes: &[u8]) -> Result<Self, InlineFileDataError> {
        let length = u8::try_from(bytes.len()).map_err(|_| InlineFileDataError::TooLarge)?;
        if bytes.len() > MAXIMUM_INLINE_FILE_BYTES {
            return Err(InlineFileDataError::TooLarge);
        }
        let mut stored = [0_u8; MAXIMUM_INLINE_FILE_BYTES];
        #[allow(
            clippy::indexing_slicing,
            reason = "bytes.len() <= MAXIMUM_INLINE_FILE_BYTES == stored.len() was just checked above (the `if bytes.len() > MAXIMUM_INLINE_FILE_BYTES` early return)"
        )]
        stored[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            length,
            bytes: stored,
        })
    }

    /// Returns the exact semantic file bytes without allocation.
    #[must_use]
    #[allow(
        clippy::indexing_slicing,
        reason = "self.length <= MAXIMUM_INLINE_FILE_BYTES == self.bytes.len() is the InlineFileData invariant maintained by every constructor: new() checks bytes.len() <= MAXIMUM_INLINE_FILE_BYTES before assigning length, and replace_range/truncate check logical_bytes against the same bound before assigning length"
    )]
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
        #[allow(
            clippy::indexing_slicing,
            reason = "end <= logical_bytes <= MAXIMUM_INLINE_FILE_BYTES == self.bytes.len() and offset <= end (end = offset + replacement.len() via checked_add) are both established by the check just above"
        )]
        self.bytes[offset..end].copy_from_slice(replacement);
        #[allow(
            clippy::indexing_slicing,
            reason = "logical_bytes <= MAXIMUM_INLINE_FILE_BYTES == self.bytes.len() is established by the check just above"
        )]
        self.bytes[logical_bytes..].fill(0);
        self.length = u8::try_from(logical_bytes).map_err(|_| InlineFileDataError::TooLarge)?;
        Ok(self)
    }

    pub(crate) fn truncate(mut self, logical_bytes: usize) -> Result<Self, InlineFileDataError> {
        if logical_bytes > self.as_bytes().len() {
            return Err(InlineFileDataError::TooLarge);
        }
        #[allow(
            clippy::indexing_slicing,
            reason = "logical_bytes <= self.as_bytes().len() <= self.bytes.len() is established by the check just above (as_bytes() never exceeds the fixed-capacity buffer)"
        )]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumDiscriminants)]
#[strum_discriminants(
    name(FilePayloadKind),
    derive(EnumIter, AsRefStr),
    cfg_attr(
        all(feature = "typescript", target_arch = "wasm32"),
        derive(serde::Serialize, tsify::Tsify),
        serde(rename_all = "kebab-case")
    ),
    cfg_attr(feature = "napi-types", napi_derive::napi(string_enum = "kebab-case")),
    strum(serialize_all = "kebab-case")
)]
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

impl FilePayload {
    /// Returns the finite boundary kind for this payload.
    #[must_use]
    pub fn kind(&self) -> FilePayloadKind {
        self.into()
    }
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

/// One immutable authenticated file-table B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileTablePage {
    /// Strictly ordered file records.
    Leaf(Vec<FileRecord>),
    /// Strictly ordered lower-bound child references.
    Internal(Vec<Child<FileId>>),
}

impl FileTablePage {
    fn validate(&self, maximum_items: u32) -> Result<(), FileTableError> {
        let count = match self {
            Self::Leaf(records) => {
                if records.windows(2).any(|pair| {
                    let [first, second] = pair else {
                        return false;
                    };
                    first.file_id >= second.file_id
                }) {
                    return Err(FileTableError::NotStrictlyOrdered);
                }
                for record in records {
                    record.validate()?;
                }
                records.len()
            }
            Self::Internal(children) => {
                if children.is_empty()
                    || children.windows(2).any(|pair| {
                        let [first, second] = pair else {
                            return false;
                        };
                        first.first >= second.first
                    })
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

pub(crate) fn encode_file_table_leaf_records<E>(
    records: &[FileRecord],
    maximum_items: u32,
    data_custody: Option<&ProofDataCustody>,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
{
    use super::codec::EncodeWorkStep;
    let mut observe = |step| super::codec::observe_encoding(&mut observe, step, data_custody);
    observe(EncodeWorkStep::Check)?;
    if count(records.len())? > maximum_items {
        return Err(invariant(FileTableError::TooManyItems).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut prior = None;
    let mut copied = 0_u64;
    for record in records {
        observe(EncodeWorkStep::Check)?;
        let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(examined))?;
        let validation = if prior.is_some_and(|prior| prior >= record.file_id) {
            Err(invariant(FileTableError::NotStrictlyOrdered))
        } else {
            record.validate().map_err(invariant)
        };
        observe(EncodeWorkStep::Completed(examined))?;
        validation?;
        length = length.checked_add(file_record_encoded_length(*record)?)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        copied = copied.checked_add(record_copied_bytes(*record)).ok_or(WorkError::Overflow)?;
        prior = Some(record.file_id);
    }
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: usize_to_u64(records.len()), bytes_encoded: usize_to_u64(length),
        bytes_copied: copied, ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let header = WorkCounters { bytes_encoded: 5, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(header))?;
    encoder.u8(1);
    encoder.u32(count(records.len())?);
    observe(EncodeWorkStep::Completed(header))?;
    for record in records {
        observe(EncodeWorkStep::Check)?;
        let encoded = WorkCounters {
            items_examined: 1, bytes_encoded: usize_to_u64(file_record_encoded_length(*record)?),
            bytes_copied: record_copied_bytes(*record), ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(encoded))?;
        encode_record(&mut encoder, *record);
        observe(EncodeWorkStep::Completed(encoded))?;
    }
    Ok(encoder.finish())
}

pub(crate) fn encode_file_table_internal_children<E, I>(
    children: I,
    maximum_items: u32,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
    I: Clone + ExactSizeIterator<Item = (FileId, ObjectId)>,
{
    use super::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    let item_count = count(children.len())?;
    if item_count == 0 {
        return Err(invariant(FileTableError::InvalidInternalPage).into());
    }
    if item_count > maximum_items {
        return Err(invariant(FileTableError::TooManyItems).into());
    }
    let mut prior = None;
    for (file_id, page) in children.clone() {
        observe(EncodeWorkStep::Check)?;
        let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(examined))?;
        let valid = !prior.is_some_and(|prior| prior >= file_id) && page.kind == ObjectKind::FileTablePage;
        observe(EncodeWorkStep::Completed(examined))?;
        if !valid {
            return Err(invariant(FileTableError::InvalidInternalPage).into());
        }
        prior = Some(file_id);
    }
    let item_bytes = children.len().checked_mul(16 + 32).ok_or(CanonicalDecodeError::LengthOverflow)?;
    let length = (DOMAIN.len() + 2 + 1 + 4).checked_add(item_bytes).ok_or(CanonicalDecodeError::LengthOverflow)?;
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: u64::from(item_count), bytes_encoded: usize_to_u64(length),
        bytes_copied: usize_to_u64(item_bytes), ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let header = WorkCounters { bytes_encoded: 5, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(header))?;
    encoder.u8(2);
    encoder.u32(item_count);
    observe(EncodeWorkStep::Completed(header))?;
    for (file_id, page) in children {
        observe(EncodeWorkStep::Check)?;
        let encoded = WorkCounters { items_examined: 1, bytes_encoded: 48, bytes_copied: 48, ..WorkCounters::default() };
        observe(EncodeWorkStep::Admit(encoded))?;
        encoder.fixed(&file_id.into_bytes());
        encoder.fixed(page.digest.as_bytes());
        observe(EncodeWorkStep::Completed(encoded))?;
    }
    Ok(encoder.finish())
}

fn record_copied_bytes(record: FileRecord) -> u64 {
    16 + 32 + match record.payload {
        FilePayload::InlineRegular(data) => usize_to_u64(data.as_bytes().len()),
        FilePayload::Regular { .. } | FilePayload::Directory { .. }
        | FilePayload::SymbolicLink { .. } | FilePayload::ReparsePoint { .. } => 32,
        FilePayload::Empty | FilePayload::Device { .. } => 0,
    }
}

pub(crate) fn file_table_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let tag = decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let item_count = decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        });
    }
    let kind = match tag {
        1 => {
            for _ in 0..item_count {
                let _: [u8; 16] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let _: [u8; 32] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                match decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? {
                    1 | 3 | 6 => {
                        decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                        let _: [u8; 32] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                    }
                    2 => {
                        let _: [u8; 32] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                    }
                    4 => {}
                    5 => {
                        decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                        decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                    }
                    7 => {
                        let length = decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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
                let _: [u8; 16] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let _: [u8; 32] = decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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

pub(crate) fn encode_file_record<E>(
    record: FileRecord,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
{
    use super::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(examined))?;
    let validation = record.validate().map_err(invariant);
    observe(EncodeWorkStep::Completed(examined))?;
    validation?;
    let item_length = file_record_encoded_length(record)?;
    let length = (RECORD_DOMAIN.len() + 2).checked_add(item_length).ok_or(CanonicalDecodeError::LengthOverflow)?;
    let copied = record_copied_bytes(record);
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: 1, bytes_encoded: usize_to_u64(length), bytes_copied: copied,
        ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        RECORD_DOMAIN, RECORD_VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    let encoded = WorkCounters {
        items_examined: 1, bytes_encoded: usize_to_u64(item_length), bytes_copied: copied,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Check)?;
    observe(EncodeWorkStep::Admit(encoded))?;
    encode_record(&mut encoder, record);
    observe(EncodeWorkStep::Completed(encoded))?;
    Ok(encoder.finish())
}

pub(crate) fn decode_file_record(bytes: &[u8]) -> Result<FileRecord, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, RECORD_DOMAIN, RECORD_VERSION, usize_to_u64(bytes.len()), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    let record = decode_record(&mut decoder, &mut |_| Ok::<_, CanonicalDecodeError>(()))?;
    decoder.finish()?;
    Ok(record)
}

/// Decodes one canonical object with before-effect admission.
pub fn decode_file_table_page<E>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<FileTablePage, E>
where
    E: From<CanonicalDecodeError>,
{
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_page_object_bytes(), observe)?;
    let tag = decoder.u8(observe)?;
    let item_count = decoder.u32(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }.into());
    }
    match tag {
        1 => {
            let mut records = Vec::new();
            let capacity = capacity(item_count)?;
            let admitted_bytes = usize_to_u64(capacity.checked_mul(std::mem::size_of::<FileRecord>())
                .ok_or(CanonicalDecodeError::LengthOverflow)?);
            observe(ReadAllocationStep::Allocate { bytes: admitted_bytes, operations: u64::from(capacity != 0) })?;
            if let Err(error) = records.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(records.capacity() * std::mem::size_of::<FileRecord>()),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                records.push(decode_record(&mut decoder, observe)?);
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            let page = FileTablePage::Leaf(records);
            page.validate(limits.maximum_page_items).map_err(invariant)?;
            Ok(page)
        }
        2 => {
            let mut children = Vec::new();
            let capacity = capacity(item_count)?;
            let admitted_bytes = usize_to_u64(capacity.checked_mul(std::mem::size_of::<Child<FileId>>())
                .ok_or(CanonicalDecodeError::LengthOverflow)?);
            observe(ReadAllocationStep::Allocate { bytes: admitted_bytes, operations: u64::from(capacity != 0) })?;
            if let Err(error) = children.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(children.capacity() * std::mem::size_of::<Child<FileId>>()),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let first = FileId::from_bytes(decoder.fixed::<16, E>(observe)?);
                let page = digest_object(ObjectKind::FileTablePage, decoder.fixed::<32, E>(observe)?);
                children.push(Child { first, page });
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            if children.is_empty()
                || children.windows(2).any(|pair| matches!(pair, [left, right] if left.first >= right.first))
            {
                return Err(invariant(FileTableError::InvalidInternalPage).into());
            }
            // Child identities have the canonical FileTablePage kind above.
            Ok(FileTablePage::Internal(children))
        }
        tag => Err(CanonicalDecodeError::UnknownTag { field: "file_table_page", tag }.into()),
    }
}

fn decode_record<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<FileRecord, E> {
    let file_id = FileId::from_bytes(decoder.fixed::<16, E>(observe)?);
    let kind = FileKind::from_tag(decoder.u8(observe)?).map_err(invariant_tree)?;
    let link_count = decoder.u64(observe)?;
    let metadata = digest_object(ObjectKind::Metadata, decoder.fixed::<32, E>(observe)?);
    let payload = match decoder.u8(observe)? {
        1 => FilePayload::Regular {
            logical_bytes: decoder.u64(observe)?,
            extents: digest_object(ObjectKind::ExtentPage, decoder.fixed::<32, E>(observe)?),
        },
        2 => FilePayload::Directory {
            entries: digest_object(ObjectKind::TreePage, decoder.fixed::<32, E>(observe)?),
        },
        3 => FilePayload::SymbolicLink {
            target_bytes: decoder.u64(observe)?,
            target: digest_object(ObjectKind::Blob, decoder.fixed::<32, E>(observe)?),
        },
        4 => FilePayload::Empty,
        5 => FilePayload::Device {
            major: decoder.u32(observe)?,
            minor: decoder.u32(observe)?,
        },
        6 => FilePayload::ReparsePoint {
            payload_bytes: decoder.u64(observe)?,
            payload: digest_object(ObjectKind::Blob, decoder.fixed::<32, E>(observe)?),
        },
        7 => {
            let length = decoder.u8(observe)?;
            if usize::from(length) > MAXIMUM_INLINE_FILE_BYTES {
                return Err(CanonicalDecodeError::FieldTooLarge {
                    observed: u32::from(length),
                    maximum: u32::try_from(MAXIMUM_INLINE_FILE_BYTES)
                        .map_err(|_| CanonicalDecodeError::LengthOverflow)?,
                }.into());
            }
            let bytes = decoder.take_exact(usize::from(length))?;
            observe(ReadAllocationStep::BeforeCopy(u64::from(length)))?;
            let inline = InlineFileData::new(bytes).map_err(invariant_inline)?;
            observe(ReadAllocationStep::Copied(u64::from(length)))?;
            FilePayload::InlineRegular(inline)
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "file_payload",
                tag,
            }.into());
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
pub fn file_table_page_id(
    page: &FileTablePage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_file_table_page(page, maximum_items, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
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
pub async fn lookup_file_records_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_ids: &[FileId],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<FileRecordBatchLookup, FileRecordReadFailure> {
    lookup_file_records_async_with_custody(
        store, root, file_ids, maximum_queries, limits, budget, cancellation, None,
    )
    .await
}

pub(crate) async fn lookup_file_records_async_with_custody<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_ids: &[FileId],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    data_custody: Option<&ProofDataCustody>,
) -> Result<FileRecordBatchLookup, FileRecordReadFailure> {
    let frame_observer = |event| data_custody.map_or(Ok(0), |owner| owner.record_heap_future_event(event));
    in_heap_measured(move |budget| async move {
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
        .map(|receipt| OperationReceipt {
            work: receipt.work,
            value: to_record_batch(receipt),
        })
        .map_err(map_batch_failure)
    }, budget, cancellation, FileRecordReadError::Cancelled,
        data_custody.map(|_| &frame_observer as &(dyn Fn(_) -> Result<u64, WorkError> + Sync)))
    .await
    .map(|receipt| FileRecordBatchLookup { work: receipt.work, ..receipt.value })
}

pub(crate) async fn lookup_file_records_async_with_accounting<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_ids: &[FileId],
    maximum_queries: u32,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&ProofDataCustody>,
) -> Result<(Vec<Option<FileRecord>>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    persistent_batch::lookup_async_with_accounting::<S, FileTableFormat>(
        store, root, file_ids, maximum_queries, limits, accounting, cancellation, data_custody,
    ).await.map(|(receipt, allocation)| (receipt.value, allocation))
}

fn to_record_batch(receipt: OperationReceipt<Vec<Option<FileRecord>>>) -> FileRecordBatchLookup {
    FileRecordBatchLookup {
        records: receipt.value,
        work: receipt.work,
    }
}

fn map_batch_failure(failure: persistent_batch::Failure) -> FileRecordReadFailure {
    OperationFailure::new(map_batch_error(failure.error), *failure.work)
}

pub(super) fn map_batch_error(error: persistent_batch::Error) -> FileRecordReadError {
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

pub(super) fn unmap_batch_error(error: FileRecordReadError) -> persistent_batch::Error {
    match error {
        FileRecordReadError::Cancelled => persistent_batch::Error::Cancelled,
        FileRecordReadError::EmptyBatch => persistent_batch::Error::Empty,
        FileRecordReadError::TooManyQueries => persistent_batch::Error::TooManyQueries,
        FileRecordReadError::WrongRootKind => persistent_batch::Error::WrongRootKind,
        FileRecordReadError::InvalidHeightLimit => persistent_batch::Error::InvalidLimits,
        FileRecordReadError::Cycle => persistent_batch::Error::CycleOrAlias,
        FileRecordReadError::HeightExceeded => persistent_batch::Error::HeightExceeded,
        FileRecordReadError::InvalidRouting => persistent_batch::Error::InvalidRouting,
        FileRecordReadError::ChildBoundsMismatch => persistent_batch::Error::ChildBoundsMismatch,
        FileRecordReadError::AllocationFailed => persistent_batch::Error::AllocationFailed,
        FileRecordReadError::Storage(error) => persistent_batch::Error::Storage(error),
        FileRecordReadError::Decode(error) => persistent_batch::Error::Decode(error),
        FileRecordReadError::Work(error) => persistent_batch::Error::Work(error),
    }
}

pub(crate) async fn lookup_file_record_async_with_accounting<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_id: FileId,
    limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&ProofDataCustody>,
) -> Result<(Option<FileRecord>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    persistent_point::lookup_async_with_accounting::<S, FileTableFormat>(
        store, root, &file_id, limits, accounting, cancellation, data_custody,
    ).await
}

/// Looks up one stable file identity through the authenticated file table.
pub fn lookup_file_record<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    file_id: FileId,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<FileRecordLookup, FileRecordReadFailure> {
    crate::async_storage::poll_immediate(lookup_file_record_async(
        store,
        root,
        file_id,
        limits,
        budget,
        &CancellationToken::new(),
    ))
}

/// Asynchronously looks up one file record, sharing every page another reader of `store` already decoded.
pub async fn lookup_file_record_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_id: FileId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<FileRecordLookup, FileRecordReadFailure> {
    lookup_file_record_async_with_custody(
        store, root, file_id, limits, budget, cancellation, None,
    )
    .await
}

pub(crate) async fn lookup_file_record_async_with_custody<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_id: FileId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    data_custody: Option<&ProofDataCustody>,
) -> Result<FileRecordLookup, FileRecordReadFailure> {
    let frame_observer = |event| data_custody.map_or(Ok(0), |owner| owner.record_heap_future_event(event));
    in_heap_measured(move |budget| async move {
        persistent_point::lookup_async::<S, FileTableFormat>(
            store,
            root,
            &file_id,
            limits,
            budget,
            cancellation,
            data_custody,
        )
        .await
        .map(|receipt| OperationReceipt {
            work: receipt.work,
            value: FileRecordLookup { record: receipt.value, work: receipt.work },
        })
        .map_err(map_batch_failure)
    }, budget, cancellation, FileRecordReadError::Cancelled,
        data_custody.map(|_| &frame_observer as &(dyn Fn(_) -> Result<u64, WorkError> + Sync)))
    .await
    .map(|receipt| FileRecordLookup { work: receipt.work, ..receipt.value })
}

/// Sparse file-record lookup failure retaining exact spent work.
pub type FileRecordReadFailure = OperationFailure<FileRecordReadError>;

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
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::FileTable(error))
}

fn invariant_tree(error: super::TreePageError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Tree(error))
}

fn invariant_inline(error: InlineFileDataError) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::InlineFileData(error))
}

#[cfg(test)]
#[path = "tests/file_table.rs"]
mod tests;
