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
    encode_file_metadata_engine(metadata, WorkBudget::UNBOUNDED, None)
        .map(|receipt| receipt.value)
        .map_err(|failure| match failure.error {
            FileMetadataEncodeError::Canonical(error) => error,
            FileMetadataEncodeError::AllocationFailed(_) => CanonicalDecodeError::AllocationFailed,
            FileMetadataEncodeError::Work(error) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Work(error)),
            FileMetadataEncodeError::Cancelled(_) => CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static("filesystem operation was cancelled")),
        })
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
    encode_file_metadata_engine(metadata, budget, Some(cancellation))
}

fn encode_file_metadata_engine(
    metadata: FileMetadata,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> MeasuredResult<OperationReceipt<Vec<u8>>, FileMetadataEncodeError> {
    let mut work = WorkCounters::default();
    let mut allocations = super::allocation::AllocationLedger::default();
    let result = encode_file_metadata_with_allocations(
        metadata, &mut allocations, &mut work, budget, cancellation,
    );
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

/// Encodes using the original invocation's ledger and physically owned custody.
/// The caller destroys the returned bytes before retiring the allocation guard.
pub(crate) fn encode_file_metadata_with_accounting(
    metadata: &FileMetadata,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let original = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let original = false;
    let mut funding = super::codec::EncodeAccounting::new(accounting);
    let result = encode_file_metadata_with_steps(
        *metadata, |step| funding.observe(step),
        |source| {
            #[cfg(not(target_arch = "wasm32"))]
            if original {
                return crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(source));
            }
            let _ = (source, original);
            CanonicalDecodeError::AllocationFailed.into()
        },
    );
    funding.finish(result)
}

fn encode_file_metadata_with_allocations(
    metadata: FileMetadata,
    allocations: &mut super::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, FileMetadataEncodeError> {
    let before = allocations.live_bytes();
    let result = encode_file_metadata_with_steps(
        metadata,
        |step| observe_metadata_encoding(step, allocations, work, budget, cancellation),
        FileMetadataEncodeError::AllocationFailed,
    );
    if result.is_err() {
        let owned = allocations.live_bytes().checked_sub(before).ok_or(crate::WorkError::Overflow)?;
        allocations.release(owned)
            .map_err(|error| error.into_work_or(FileMetadataEncodeError::Work(crate::WorkError::Overflow)))?;
    }
    result
}

/// The canonical metadata body; its allocators and field writes never run in a root loan.
pub(super) fn encode_file_metadata_with_steps<E>(
    metadata: FileMetadata,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError>,
{
    use super::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
    observe(EncodeWorkStep::Admit(examined))?;
    let length = file_metadata_encoded_len(metadata);
    observe(EncodeWorkStep::Completed(examined))?;
    let length = length?;
    let copied = [metadata.named_attributes, metadata.acl, metadata.security_descriptor]
        .into_iter().filter(|field| matches!(field, MetadataField::Value(_))).count() as u64 * 32;
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: 12, bytes_encoded: length as u64, bytes_copied: copied,
        ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity_with_steps(
        DOMAIN, VERSION, length,
        |step| observe(EncodeWorkStep::Allocation(step)), allocation_error,
    )?;
    encode_metadata_fields(metadata, &mut encoder, &mut observe)?;
    Ok(encoder.finish())
}

fn observe_metadata_encoding(
    step: super::codec::EncodeWorkStep,
    allocations: &mut super::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<(), FileMetadataEncodeError> {
    use super::codec::{EncodeAllocationStep, EncodeWorkStep};
    let completed = match step {
        EncodeWorkStep::Check => {
            cancellation.map_or(Ok(()), CancellationToken::check)?;
            return Ok(());
        }
        EncodeWorkStep::Admit(delta) => {
            work.admit(&delta, &budget)?;
            return Ok(());
        }
        EncodeWorkStep::Completed(delta) => delta,
        EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
            allocations.claim_elements::<u8>(capacity, work, budget)
                .map_err(|error| error.into_work_or(FileMetadataEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
            return Ok(());
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
            if actual_bytes > admitted_bytes {
                allocations.record_owned_bytes(actual_bytes - admitted_bytes, work)
                    .map_err(|error| error.into_work_or(FileMetadataEncodeError::Canonical(CanonicalDecodeError::AllocationFailed)))?;
            }
            work.verify(budget)?;
            return Ok(());
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) => {
            work.admit(&WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }, &budget)?;
            return Ok(());
        }
        EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) =>
            WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() },
    };
    work.try_add_assign(&completed)?;
    work.verify(budget)?;
    cancellation.map_or(Ok(()), CancellationToken::check)?;
    Ok(())
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
    /// The actual fallible canonical-buffer reservation failed.
    #[error("canonical metadata allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
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

fn encode_metadata_fields<E>(
    metadata: FileMetadata,
    encoder: &mut Encoder,
    observe: &mut impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
) -> Result<(), E> {
    macro_rules! field {
        ($value:expr, $payload:expr, $copied:expr, $encode:ident) => {{
            observe(super::codec::EncodeWorkStep::Check)?;
            let (encoded, copied) = match $value {
                MetadataField::Unavailable => (1, 0),
                MetadataField::Value(_) => (1 + $payload, $copied),
            };
            let completed = WorkCounters {
                items_examined: 1, bytes_encoded: encoded, bytes_copied: copied,
                ..WorkCounters::default()
            };
            observe(super::codec::EncodeWorkStep::Admit(completed))?;
            $encode(encoder, $value);
            observe(super::codec::EncodeWorkStep::Completed(completed))?;
        }};
    }
    field!(metadata.posix_mode, 4, 0, encode_u32);
    field!(metadata.posix_uid, 4, 0, encode_u32);
    field!(metadata.posix_gid, 4, 0, encode_u32);
    field!(metadata.posix_flags, 8, 0, encode_u64);
    field!(metadata.windows_attributes, 4, 0, encode_u32);
    field!(metadata.created_ns, 8, 0, encode_i64);
    field!(metadata.modified_ns, 8, 0, encode_i64);
    field!(metadata.accessed_ns, 8, 0, encode_i64);
    field!(metadata.changed_ns, 8, 0, encode_i64);
    field!(metadata.named_attributes, 32, 32, encode_object);
    field!(metadata.acl, 32, 32, encode_object);
    field!(metadata.security_descriptor, 32, 32, encode_object);
    Ok(())
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
    decode_file_metadata_with_steps(bytes, limits, &mut |_| Ok(()))
}

/// Decodes the same record with admission and actual primitive/item completions.
pub(crate) fn decode_file_metadata_with_steps<E: From<CanonicalDecodeError>>(
    bytes: &[u8], limits: DecodeLimits,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<FileMetadata, E> {
    let mut decoder = Decoder::new_with_steps(bytes, DOMAIN, VERSION, limits.maximum_object_bytes, observe)?;
    observe(super::codec::ReadAllocationStep::BeforeItem)?;
    let metadata = FileMetadata {
        posix_mode: decode_u32(&mut decoder, "posix_mode", observe)?,
        posix_uid: decode_u32(&mut decoder, "posix_uid", observe)?,
        posix_gid: decode_u32(&mut decoder, "posix_gid", observe)?,
        posix_flags: decode_u64(&mut decoder, "posix_flags", observe)?,
        windows_attributes: decode_u32(&mut decoder, "windows_attributes", observe)?,
        created_ns: decode_i64(&mut decoder, "created_ns", observe)?,
        modified_ns: decode_i64(&mut decoder, "modified_ns", observe)?,
        accessed_ns: decode_i64(&mut decoder, "accessed_ns", observe)?,
        changed_ns: decode_i64(&mut decoder, "changed_ns", observe)?,
        named_attributes: decode_object(
            &mut decoder,
            ObjectKind::AttributePage,
            "named_attributes",
            observe,
        )?,
        acl: decode_object(&mut decoder, ObjectKind::Blob, "acl", observe)?,
        security_descriptor: decode_object(&mut decoder, ObjectKind::Blob, "security_descriptor", observe)?,
    };
    decoder.finish()?;
    metadata.validate()?;
    observe(super::codec::ReadAllocationStep::Item)?;
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

fn decode_u32<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<u32>, E> {
    match decoder.u8_with_steps(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.u32_with_steps(observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn decode_u64<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<u64>, E> {
    match decoder.u8_with_steps(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.u64_with_steps(observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn decode_i64<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<i64>, E> {
    match decoder.u8_with_steps(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.i64_with_steps(observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn decode_object<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    kind: ObjectKind,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<ObjectId>, E> {
    match decoder.u8_with_steps(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(digest_object(kind, decoder.fixed_with_steps::<32, E>(observe)?))),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
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
        return Err(CanonicalDecodeError::Invariant(
            super::codec::CanonicalInvariant::MetadataReferenceKind { field: name },
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/metadata.rs"]
mod tests;
