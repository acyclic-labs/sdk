//! Canonical cross-profile metadata with explicit unavailable fields.

use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use super::types::digest_object;
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use thiserror::Error;

const DOMAIN: &[u8; 8] = b"ACYFSMET";
const VERSION: u16 = 1;

/// A metadata fact that is either unavailable in the source profile or exact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataField<T> {
    /// Source filesystem/profile cannot represent or did not observe the fact.
    Unavailable,
    /// Exact observed value, including zero.
    Value(T),
}

/// Complete profile-independent metadata record.
///
/// Platform-specific facts are never guessed. `Unavailable` is distinct from a
/// supported field whose exact value is zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileMetadata {
    /// POSIX permission/type bits when represented by the source.
    pub posix_mode: MetadataField<u32>,
    /// POSIX numeric owner identity.
    pub posix_uid: MetadataField<u32>,
    /// POSIX numeric group identity.
    pub posix_gid: MetadataField<u32>,
    /// POSIX inode flags where available.
    pub posix_flags: MetadataField<u64>,
    /// Windows file-attribute bitset.
    pub windows_attributes: MetadataField<u32>,
    /// Creation/birth time in signed Unix-epoch nanoseconds.
    pub created_ns: MetadataField<i64>,
    /// Last content-modification time in signed Unix-epoch nanoseconds.
    pub modified_ns: MetadataField<i64>,
    /// Last access time in signed Unix-epoch nanoseconds.
    pub accessed_ns: MetadataField<i64>,
    /// Last metadata-change time in signed Unix-epoch nanoseconds.
    pub changed_ns: MetadataField<i64>,
    /// Named xattrs, alternate streams, and resource forks.
    pub named_attributes: MetadataField<ObjectId>,
    /// Canonical ACL bytes when represented independently of a security descriptor.
    pub acl: MetadataField<ObjectId>,
    /// Canonical Windows security descriptor bytes.
    pub security_descriptor: MetadataField<ObjectId>,
}

impl Default for FileMetadata {
    fn default() -> Self {
        Self {
            posix_mode: MetadataField::Unavailable,
            posix_uid: MetadataField::Unavailable,
            posix_gid: MetadataField::Unavailable,
            posix_flags: MetadataField::Unavailable,
            windows_attributes: MetadataField::Unavailable,
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            named_attributes: MetadataField::Unavailable,
            acl: MetadataField::Unavailable,
            security_descriptor: MetadataField::Unavailable,
        }
    }
}

impl FileMetadata {
    /// This metadata without the timestamps that reads, writes, and captures
    /// move on their own.
    #[must_use]
    pub(crate) const fn without_timestamps(self) -> Self {
        Self {
            created_ns: MetadataField::Unavailable,
            modified_ns: MetadataField::Unavailable,
            accessed_ns: MetadataField::Unavailable,
            changed_ns: MetadataField::Unavailable,
            ..self
        }
    }

    /// Records a content change made at `at_ns`, in signed Unix-epoch
    /// nanoseconds: every represented modification and status-change time
    /// takes that instant. Returns whether any such time is represented.
    pub fn stamp_content_change(&mut self, at_ns: i64) -> bool {
        let mut stamped = false;
        for time in [&mut self.modified_ns, &mut self.changed_ns] {
            if let MetadataField::Value(time) = time {
                *time = at_ns;
                stamped = true;
            }
        }
        stamped
    }

    fn validate(self) -> Result<(), CanonicalDecodeError> {
        validate_object(
            self.named_attributes,
            ObjectKind::AttributePage,
            "named attributes",
        )?;
        validate_object(self.acl, ObjectKind::Blob, "acl")?;
        validate_object(
            self.security_descriptor,
            ObjectKind::Blob,
            "security descriptor",
        )
    }
}

/// Encodes one canonical metadata record.
///
/// # Errors
///
/// Rejects references whose typed object class does not match the field.
pub fn encode_file_metadata(metadata: FileMetadata) -> Result<Vec<u8>, CanonicalDecodeError> {
    let length = file_metadata_encoded_len(metadata)?;
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, length)?;
    encode_metadata_fields(metadata, &mut encoder);
    Ok(encoder.finish())
}

/// Canonical metadata encoding with admission before the one owned buffer.
///
/// # Errors
/// Returns validation, cancellation, allocation, or work-budget failures
/// together with the actual completed encoding prefix.
pub fn encode_file_metadata_measured(
    metadata: FileMetadata,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, FileMetadataEncodeError> {
    let mut work = WorkCounters::default();
    let mut allocations = super::allocation::AllocationLedger::default();
    let result = (|| {
        cancellation.check()?;
        let length = file_metadata_encoded_len(metadata)?;
        let encoded = WorkCounters { bytes_encoded: length as u64, ..WorkCounters::default() };
        work.admit(&encoded, &budget)?;
        let admitted = allocations.claim_elements::<u8>(length, &mut work, budget)
            .map_err(|error| error.into_work_or(FileMetadataEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
        let mut encoder = Encoder::with_exact_capacity(DOMAIN, VERSION, length)?;
        encode_metadata_fields(metadata, &mut encoder);
        work.charge(&encoded, &budget)?;
        let bytes = encoder.finish();
        if bytes.capacity() as u64 > admitted {
            allocations.claim_bytes(bytes.capacity() as u64 - admitted, 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(|error| error.into_work_or(FileMetadataEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
            work.verify(budget)?;
        }
        Ok(bytes)
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// A measured canonical metadata encoding failure.
#[derive(Debug, Error)]
pub enum FileMetadataEncodeError {
    /// Metadata fields or the canonical allocation are invalid.
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    /// An actual encoding boundary exceeded its permit.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Encoding was cancelled before work began.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
}

fn file_metadata_encoded_len(metadata: FileMetadata) -> Result<usize, CanonicalDecodeError> {
    metadata.validate()?;
    fn field<T>(value: MetadataField<T>, payload: usize) -> usize {
        match value { MetadataField::Unavailable => 1, MetadataField::Value(_) => 1 + payload }
    }
    fn object(value: MetadataField<ObjectId>) -> usize {
        match value { MetadataField::Unavailable => 1, MetadataField::Value(value) => 1 + value.digest.as_bytes().len() }
    }
    [
        field(metadata.posix_mode, std::mem::size_of::<u32>()),
        field(metadata.posix_uid, std::mem::size_of::<u32>()),
        field(metadata.posix_gid, std::mem::size_of::<u32>()),
        field(metadata.posix_flags, std::mem::size_of::<u64>()),
        field(metadata.windows_attributes, std::mem::size_of::<u32>()),
        field(metadata.created_ns, std::mem::size_of::<i64>()),
        field(metadata.modified_ns, std::mem::size_of::<i64>()),
        field(metadata.accessed_ns, std::mem::size_of::<i64>()),
        field(metadata.changed_ns, std::mem::size_of::<i64>()),
        object(metadata.named_attributes),
        object(metadata.acl),
        object(metadata.security_descriptor),
    ].into_iter().try_fold(DOMAIN.len() + std::mem::size_of::<u16>(), |bytes, field| {
        bytes.checked_add(field).ok_or(CanonicalDecodeError::LengthOverflow)
    })
}

fn encode_metadata_fields(metadata: FileMetadata, encoder: &mut Encoder) {
    encode_u32(encoder, metadata.posix_mode);
    encode_u32(encoder, metadata.posix_uid);
    encode_u32(encoder, metadata.posix_gid);
    encode_u64(encoder, metadata.posix_flags);
    encode_u32(encoder, metadata.windows_attributes);
    encode_i64(encoder, metadata.created_ns);
    encode_i64(encoder, metadata.modified_ns);
    encode_i64(encoder, metadata.accessed_ns);
    encode_i64(encoder, metadata.changed_ns);
    encode_object(encoder, metadata.named_attributes);
    encode_object(encoder, metadata.acl);
    encode_object(encoder, metadata.security_descriptor);
}

/// Decodes one bounded canonical metadata record.
///
/// # Errors
///
/// Fails closed on malformed bytes, non-canonical presence tags, wrong object
/// classes, unsupported versions, or trailing data.
pub fn decode_file_metadata(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<FileMetadata, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_object_bytes)?;
    let metadata = FileMetadata {
        posix_mode: decode_u32(&mut decoder, "posix_mode")?,
        posix_uid: decode_u32(&mut decoder, "posix_uid")?,
        posix_gid: decode_u32(&mut decoder, "posix_gid")?,
        posix_flags: decode_u64(&mut decoder, "posix_flags")?,
        windows_attributes: decode_u32(&mut decoder, "windows_attributes")?,
        created_ns: decode_i64(&mut decoder, "created_ns")?,
        modified_ns: decode_i64(&mut decoder, "modified_ns")?,
        accessed_ns: decode_i64(&mut decoder, "accessed_ns")?,
        changed_ns: decode_i64(&mut decoder, "changed_ns")?,
        named_attributes: decode_object(
            &mut decoder,
            ObjectKind::AttributePage,
            "named_attributes",
        )?,
        acl: decode_object(&mut decoder, ObjectKind::Blob, "acl")?,
        security_descriptor: decode_object(&mut decoder, ObjectKind::Blob, "security_descriptor")?,
    };
    decoder.finish()?;
    metadata.validate()?;
    Ok(metadata)
}

/// Computes the typed authenticated identity of canonical metadata.
///
/// # Errors
///
/// Returns the same validation errors as [`encode_file_metadata`].
pub fn file_metadata_id(metadata: FileMetadata) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_file_metadata(metadata)?;
    Ok(ObjectId {
        kind: ObjectKind::Metadata,
        digest: object_digest(ObjectKind::Metadata, &bytes),
    })
}

fn encode_u32(encoder: &mut Encoder, field: MetadataField<u32>) {
    match field {
        MetadataField::Unavailable => encoder.u8(0),
        MetadataField::Value(value) => {
            encoder.u8(1);
            encoder.u32(value);
        }
    }
}

fn encode_u64(encoder: &mut Encoder, field: MetadataField<u64>) {
    match field {
        MetadataField::Unavailable => encoder.u8(0),
        MetadataField::Value(value) => {
            encoder.u8(1);
            encoder.u64(value);
        }
    }
}

fn encode_i64(encoder: &mut Encoder, field: MetadataField<i64>) {
    match field {
        MetadataField::Unavailable => encoder.u8(0),
        MetadataField::Value(value) => {
            encoder.u8(1);
            encoder.i64(value);
        }
    }
}

fn encode_object(encoder: &mut Encoder, field: MetadataField<ObjectId>) {
    match field {
        MetadataField::Unavailable => encoder.u8(0),
        MetadataField::Value(object) => {
            encoder.u8(1);
            encoder.fixed(object.digest.as_bytes());
        }
    }
}

fn decode_u32(
    decoder: &mut Decoder<'_>,
    field: &'static str,
) -> Result<MetadataField<u32>, CanonicalDecodeError> {
    match decoder.u8()? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.u32()?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }),
    }
}

fn decode_u64(
    decoder: &mut Decoder<'_>,
    field: &'static str,
) -> Result<MetadataField<u64>, CanonicalDecodeError> {
    match decoder.u8()? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.u64()?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }),
    }
}

fn decode_i64(
    decoder: &mut Decoder<'_>,
    field: &'static str,
) -> Result<MetadataField<i64>, CanonicalDecodeError> {
    match decoder.u8()? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.i64()?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }),
    }
}

fn decode_object(
    decoder: &mut Decoder<'_>,
    kind: ObjectKind,
    field: &'static str,
) -> Result<MetadataField<ObjectId>, CanonicalDecodeError> {
    match decoder.u8()? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(digest_object(kind, decoder.fixed()?))),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }),
    }
}

fn validate_object(
    field: MetadataField<ObjectId>,
    kind: ObjectKind,
    name: &'static str,
) -> Result<(), CanonicalDecodeError> {
    if let MetadataField::Value(object) = field
        && object.kind != kind
    {
        return Err(CanonicalDecodeError::Invariant(format!(
            "{name} has the wrong object kind"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/metadata.rs"]
mod tests;
