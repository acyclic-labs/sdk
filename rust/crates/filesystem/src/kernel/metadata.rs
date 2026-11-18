//! Canonical cross-profile metadata with explicit unavailable fields.

use super::allocation::KernelAccounting;
use super::codec::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{AsyncObjectStore, CancellationToken, FsError, WorkCounters};
use acyclic_fs_format::metadata::{FileMetadata, MetadataField};

const DOMAIN: &[u8; 8] = b"ACYFSMET";
const VERSION: u16 = 1;

/// Encodes one canonical object with before-effect admission.
pub fn encode_file_metadata<E>(
    metadata: FileMetadata,
    mut observe: impl FnMut(super::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError>,
{
    use super::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    let examined = WorkCounters {
        items_examined: 1,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(examined))?;
    let length = file_metadata_encoded_len(metadata);
    observe(EncodeWorkStep::Completed(examined))?;
    let length = length?;
    let copied = [
        metadata.named_attributes,
        metadata.acl,
        metadata.security_descriptor,
    ]
    .into_iter()
    .map(object_payload_length)
    .sum();
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: 12,
        bytes_encoded: length as u64,
        bytes_copied: copied,
        ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN,
        VERSION,
        length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    encode_metadata_fields(metadata, &mut encoder, &mut observe)?;
    Ok(encoder.finish())
}

fn file_metadata_encoded_len(metadata: FileMetadata) -> Result<usize, CanonicalDecodeError> {
    validate(metadata)?;
    fn field<T>(value: &MetadataField<T>, payload: usize) -> usize {
        match value {
            MetadataField::Unavailable => 1,
            MetadataField::Value(_) => 1 + payload,
        }
    }
    fn object(value: MetadataField<ObjectId>) -> usize {
        match value {
            MetadataField::Unavailable => 1,
            MetadataField::Value(value) => 1 + value.encoded_len(),
        }
    }
    [
        field(&metadata.posix_mode, std::mem::size_of::<u32>()),
        field(&metadata.posix_uid, std::mem::size_of::<u32>()),
        field(&metadata.posix_gid, std::mem::size_of::<u32>()),
        field(&metadata.posix_flags, std::mem::size_of::<u64>()),
        field(&metadata.windows_attributes, std::mem::size_of::<u32>()),
        field(&metadata.created_ns, std::mem::size_of::<i64>()),
        field(&metadata.modified_ns, std::mem::size_of::<i64>()),
        field(&metadata.accessed_ns, std::mem::size_of::<i64>()),
        field(&metadata.changed_ns, std::mem::size_of::<i64>()),
        object(metadata.named_attributes),
        object(metadata.acl),
        object(metadata.security_descriptor),
    ]
    .into_iter()
    .try_fold(DOMAIN.len() + std::mem::size_of::<u16>(), |bytes, field| {
        bytes
            .checked_add(field)
            .ok_or(CanonicalDecodeError::LengthOverflow)
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
                items_examined: 1,
                bytes_encoded: encoded,
                bytes_copied: copied,
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
    field!(
        metadata.named_attributes,
        object_payload_length(metadata.named_attributes),
        object_payload_length(metadata.named_attributes),
        encode_object
    );
    field!(
        metadata.acl,
        object_payload_length(metadata.acl),
        object_payload_length(metadata.acl),
        encode_object
    );
    field!(
        metadata.security_descriptor,
        object_payload_length(metadata.security_descriptor),
        object_payload_length(metadata.security_descriptor),
        encode_object
    );
    Ok(())
}

/// Reads one canonical metadata object, retiring its original scratch on every exit.
pub(crate) fn read_file_metadata_with_accounting<'task, 'accounting, S: AsyncObjectStore>(
    store: &'task S,
    object: ObjectId,
    limits: DecodeLimits,
    accounting: &'task mut KernelAccounting<'accounting>,
    cancellation: &'task CancellationToken,
) -> impl std::future::Future<Output = Result<FileMetadata, FsError>>
+ acyclic_stream::ProviderTask
+ 'task {
    async move {
        let mut accounting = accounting.allocation_scope()?;
        let (read, _) = super::read_object_with_accounting(
            store,
            object,
            limits.maximum_object_bytes,
            &mut accounting,
            cancellation,
        )
        .await?;
        super::decode_owned(&mut accounting, |observe| {
            decode_file_metadata(&read.bytes, limits, &mut |step| observe(step))
        })
        .map(|(metadata, _)| metadata)
    }
}

/// Decodes the same record with admission and actual primitive/item completions.
pub fn decode_file_metadata<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<FileMetadata, E> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, limits.maximum_object_bytes, observe)?;
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
        security_descriptor: decode_object(
            &mut decoder,
            ObjectKind::Blob,
            "security_descriptor",
            observe,
        )?,
    };
    decoder.finish()?;
    validate(metadata)?;
    observe(super::codec::ReadAllocationStep::Item)?;
    Ok(metadata)
}

/// Computes the typed authenticated identity of canonical metadata.
pub fn file_metadata_id(metadata: FileMetadata) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_file_metadata(
        metadata,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    Ok(ObjectId {
        storage: None,
        kind: ObjectKind::Metadata,
        digest: object_digest(&bytes),
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
            encoder.object(object);
        }
    }
}

fn decode_u32<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<u32>, E> {
    match decoder.u8(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.u32(observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn decode_u64<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<u64>, E> {
    match decoder.u8(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.u64(observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn decode_i64<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<i64>, E> {
    match decoder.u8(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.i64(observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn decode_object<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    kind: ObjectKind,
    field: &'static str,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<MetadataField<ObjectId>, E> {
    match decoder.u8(observe)? {
        0 => Ok(MetadataField::Unavailable),
        1 => Ok(MetadataField::Value(decoder.object(kind, observe)?)),
        tag => Err(CanonicalDecodeError::UnknownTag { field, tag }.into()),
    }
}

fn validate(metadata: FileMetadata) -> Result<(), CanonicalDecodeError> {
    validate_object(
        metadata.named_attributes,
        ObjectKind::AttributePage,
        "named attributes",
    )?;
    validate_object(metadata.acl, ObjectKind::Blob, "acl")?;
    validate_object(
        metadata.security_descriptor,
        ObjectKind::Blob,
        "security descriptor",
    )
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

fn object_payload_length(field: MetadataField<ObjectId>) -> u64 {
    match field {
        MetadataField::Unavailable => 0,
        MetadataField::Value(object) => object.encoded_len() as u64,
    }
}
