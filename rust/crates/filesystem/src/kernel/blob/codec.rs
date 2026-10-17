use super::*;

/// Encodes one canonical page with before-effect admission.
pub fn encode_blob_page<E: From<CanonicalDecodeError>>(
    page: &BlobPage,
    maximum_items: u32,
    mut observe: impl FnMut(crate::kernel::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E> {
    use crate::kernel::codec::{EncodeAllocationStep, EncodeWorkStep};
    observe(EncodeWorkStep::Check)?;
    let encoded_length = blob_page_encoded_length(page, maximum_items)?;
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: encoded_length as u64,
        ..WorkCounters::UNCHARGED
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN,
        VERSION,
        encoded_length,
        |step| {
            if matches!(step, EncodeAllocationStep::BeforeHeader { .. }) {
                observe(EncodeWorkStep::Check)?;
            }
            observe(EncodeWorkStep::Allocation(step))
        },
        allocation_error,
    )?;
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
    observe(EncodeWorkStep::Completed(WorkCounters {
        bytes_encoded: (encoded_length - DOMAIN.len() - 2) as u64,
        ..WorkCounters::UNCHARGED
    }))?;
    Ok(encoder.finish())
}

pub(crate) fn blob_page_encoded_length(
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

pub(crate) fn decode_blob_page_with_accounting(
    bytes: &[u8],
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(BlobPage, u64, KernelOwnedAllocation), FsError> {
    let page = crate::kernel::file_read::decode_owned(accounting, |observe| {
        decode_blob_page(bytes, limits, &mut |step| observe(step))
    })
    .map_err(read_decode_error)?;
    let owner = ReadOwned {
        value: page.0,
        custody: page.1,
    };
    let bytes = match &owner.value.node {
        BlobNode::Leaf(items) => items.capacity() * size_of::<BlobChunkRef>(),
        BlobNode::Internal(items) => items.capacity() * size_of::<BlobChild>(),
    };
    Ok((owner.value, usize_to_u64(bytes), owner.custody))
}

/// Decodes one canonical page with physical allocation observation.
pub fn decode_blob_page<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<BlobPage, E> {
    let mut decoder = Decoder::new(
        bytes,
        DOMAIN,
        VERSION,
        limits.maximum_page_object_bytes(),
        observe,
    )?;
    let first_offset = decoder.u64(observe)?;
    let end_offset = decoder.u64(observe)?;
    let tag = decoder.u8(observe)?;
    let item_count = decoder.u32(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }
        .into());
    }
    let mut leaf = Vec::new();
    let mut internal = Vec::new();
    match tag {
        1 => {
            reserve_decoded_items(&mut leaf, capacity(item_count)?, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                leaf.push(BlobChunkRef {
                    first_offset: decoder.u64(observe)?,
                    end_offset: decoder.u64(observe)?,
                    chunk: ObjectId {
                        kind: ObjectKind::BlobChunk,
                        digest: crate::foundation::Digest::from_bytes(
                            decoder.fixed::<32, E>(observe)?,
                        ),
                    },
                });
                observe(ReadAllocationStep::Item)?;
            }
        }
        2 => {
            reserve_decoded_items(&mut internal, capacity(item_count)?, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                internal.push(BlobChild {
                    first_offset: decoder.u64(observe)?,
                    end_offset: decoder.u64(observe)?,
                    page: ObjectId {
                        kind: ObjectKind::Blob,
                        digest: crate::foundation::Digest::from_bytes(
                            decoder.fixed::<32, E>(observe)?,
                        ),
                    },
                });
                observe(ReadAllocationStep::Item)?;
            }
        }
        tag => {
            return Err(CanonicalDecodeError::UnknownTag {
                field: "blob_page",
                tag,
            }
            .into());
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

/// Computes the typed identity of one canonical blob-index page.
pub fn blob_page_id(page: &BlobPage, maximum_items: u32) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_blob_page(
        page,
        maximum_items,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    Ok(ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &bytes),
    })
}

pub(crate) fn encode_blob_page_with_accounting(
    page: &BlobPage,
    maximum_items: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<ReadOwned<Vec<u8>>, FsError> {
    blob_page_encoded_length(page, maximum_items).map_err(BlobBuildError::Encode)?;
    accounting.admit(WorkCounters {
        page_writes: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let (value, custody) = crate::kernel::codec::encode_owned(accounting, |observe, _| {
        encode_blob_page(
            page,
            maximum_items,
            |step| observe(page_write_step(step)),
            |error| FsError::BlobBuild(BlobBuildError::ReserveFailed(error)),
        )
    })
    .map_err(|error| match error {
        FsError::Decode(error) => FsError::BlobBuild(BlobBuildError::Encode(error)),
        error => error,
    })?;
    Ok(ReadOwned { value, custody })
}

pub(super) fn count(value: usize) -> Result<u32, CanonicalDecodeError> {
    u32::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

pub(super) fn capacity(value: u32) -> Result<usize, CanonicalDecodeError> {
    usize::try_from(value).map_err(|_| CanonicalDecodeError::LengthOverflow)
}

pub(super) fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(crate::kernel::codec::CanonicalInvariant::Static(message))
}

pub(crate) fn page_write_step(
    step: crate::kernel::codec::EncodeWorkStep,
) -> crate::kernel::codec::EncodeWorkStep {
    use crate::kernel::codec::EncodeWorkStep;
    match step {
        EncodeWorkStep::Admit(mut work) => {
            work.page_writes = 1;
            EncodeWorkStep::Admit(work)
        }
        EncodeWorkStep::Completed(mut work) => {
            work.page_writes = 1;
            EncodeWorkStep::Completed(work)
        }
        step => step,
    }
}
