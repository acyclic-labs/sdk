use super::*;

/// Encodes one canonical page with before-effect admission.
pub fn encode_file_table_page<E>(
    page: &FileTablePage,
    maximum_items: u32,
    mut observe: impl FnMut(crate::kernel::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<crate::WorkError>,
{
    match page {
        FileTablePage::Leaf(records) => encode_file_table_leaf_records(
            records,
            maximum_items,
            None,
            &mut observe,
            allocation_error,
        ),
        FileTablePage::Internal(children) => encode_file_table_internal_children(
            children.iter().map(|child| (child.first, child.page)),
            maximum_items,
            observe,
            allocation_error,
        ),
    }
}

pub(crate) fn encode_file_table_leaf_records<E>(
    records: &[FileRecord],
    maximum_items: u32,
    data_custody: Option<&ProofDataCustody>,
    mut observe: impl FnMut(crate::kernel::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
{
    use crate::kernel::codec::EncodeWorkStep;
    let mut observe =
        |step| crate::kernel::codec::observe_encoding(&mut observe, step, data_custody);
    observe(EncodeWorkStep::Check)?;
    if count(records.len())? > maximum_items {
        return Err(invariant(FileTableError::TooManyItems).into());
    }
    let mut length = DOMAIN.len() + 2 + 1 + 4;
    let mut prior = None;
    let mut copied = 0_u64;
    for record in records {
        observe(EncodeWorkStep::Check)?;
        let examined = WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(examined))?;
        let validation = if prior.is_some_and(|prior| prior >= record.file_id) {
            Err(invariant(FileTableError::NotStrictlyOrdered))
        } else {
            record.validate().map_err(invariant)
        };
        observe(EncodeWorkStep::Completed(examined))?;
        validation?;
        length = length
            .checked_add(file_record_encoded_length(*record)?)
            .ok_or(CanonicalDecodeError::LengthOverflow)?;
        copied = copied
            .checked_add(record_copied_bytes(*record))
            .ok_or(WorkError::Overflow)?;
        prior = Some(record.file_id);
    }
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: usize_to_u64(length),
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
    let header = WorkCounters {
        bytes_encoded: 5,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(header))?;
    encoder.u8(1);
    encoder.u32(count(records.len())?);
    observe(EncodeWorkStep::Completed(header))?;
    for record in records {
        observe(EncodeWorkStep::Check)?;
        let encoded = WorkCounters {
            bytes_encoded: usize_to_u64(file_record_encoded_length(*record)?),
            bytes_copied: record_copied_bytes(*record),
            ..WorkCounters::default()
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
    mut observe: impl FnMut(crate::kernel::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
    I: Clone + ExactSizeIterator<Item = (FileId, ObjectId)>,
{
    use crate::kernel::codec::EncodeWorkStep;
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
        let examined = WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(examined))?;
        let valid =
            !prior.is_some_and(|prior| prior >= file_id) && page.kind == ObjectKind::FileTablePage;
        observe(EncodeWorkStep::Completed(examined))?;
        if !valid {
            return Err(invariant(FileTableError::InvalidInternalPage).into());
        }
        prior = Some(file_id);
    }
    let item_bytes = children
        .len()
        .checked_mul(16 + 32)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let length = (DOMAIN.len() + 2 + 1 + 4)
        .checked_add(item_bytes)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    observe(EncodeWorkStep::Admit(WorkCounters {
        bytes_encoded: usize_to_u64(length),
        bytes_copied: usize_to_u64(item_bytes),
        ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        DOMAIN,
        VERSION,
        length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    let header = WorkCounters {
        bytes_encoded: 5,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(header))?;
    encoder.u8(2);
    encoder.u32(item_count);
    observe(EncodeWorkStep::Completed(header))?;
    for (file_id, page) in children {
        observe(EncodeWorkStep::Check)?;
        let encoded = WorkCounters {
            bytes_encoded: 48,
            bytes_copied: 48,
            ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(encoded))?;
        encoder.fixed(&file_id.into_bytes());
        encoder.fixed(page.digest.as_bytes());
        observe(EncodeWorkStep::Completed(encoded))?;
    }
    Ok(encoder.finish())
}

pub(super) fn record_copied_bytes(record: FileRecord) -> u64 {
    16 + 32
        + match record.payload {
            FilePayload::InlineRegular(data) => usize_to_u64(data.as_bytes().len()),
            FilePayload::Regular { .. }
            | FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::ReparsePoint { .. } => 32,
            FilePayload::Empty | FilePayload::Device { .. } => 0,
        }
}

pub(crate) fn file_table_page_decode_shape(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<DecodedPageShape, CanonicalDecodeError> {
    let mut decoder = Decoder::new(
        bytes,
        DOMAIN,
        VERSION,
        limits.maximum_page_object_bytes(),
        &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
    )?;
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
                let _: [u8; 16] =
                    decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let _: [u8; 32] =
                    decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                match decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? {
                    1 | 3 | 6 => {
                        decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                        let _: [u8; 32] = decoder
                            .fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                    }
                    2 => {
                        let _: [u8; 32] = decoder
                            .fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                    }
                    4 => {}
                    5 => {
                        decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                        decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                    }
                    7 => {
                        let length = decoder
                            .u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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
                let _: [u8; 16] =
                    decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let _: [u8; 32] =
                    decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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
    mut observe: impl FnMut(crate::kernel::codec::EncodeWorkStep) -> Result<(), E>,
    allocation_error: impl FnOnce(std::collections::TryReserveError) -> E,
) -> Result<Vec<u8>, E>
where
    E: From<CanonicalDecodeError> + From<WorkError>,
{
    use crate::kernel::codec::EncodeWorkStep;
    observe(EncodeWorkStep::Check)?;
    let examined = WorkCounters {
        items_examined: 1,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Admit(examined))?;
    let validation = record.validate().map_err(invariant);
    observe(EncodeWorkStep::Completed(examined))?;
    validation?;
    let item_length = file_record_encoded_length(record)?;
    let length = (RECORD_DOMAIN.len() + 2)
        .checked_add(item_length)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let copied = record_copied_bytes(record);
    observe(EncodeWorkStep::Admit(WorkCounters {
        items_examined: 1,
        bytes_encoded: usize_to_u64(length),
        bytes_copied: copied,
        ..WorkCounters::default()
    }))?;
    let mut encoder = Encoder::with_exact_capacity(
        RECORD_DOMAIN,
        RECORD_VERSION,
        length,
        |step| observe(EncodeWorkStep::Allocation(step)),
        allocation_error,
    )?;
    let encoded = WorkCounters {
        items_examined: 1,
        bytes_encoded: usize_to_u64(item_length),
        bytes_copied: copied,
        ..WorkCounters::default()
    };
    observe(EncodeWorkStep::Check)?;
    observe(EncodeWorkStep::Admit(encoded))?;
    encode_record(&mut encoder, record);
    observe(EncodeWorkStep::Completed(encoded))?;
    Ok(encoder.finish())
}

pub(crate) fn decode_file_record(bytes: &[u8]) -> Result<FileRecord, CanonicalDecodeError> {
    let mut decoder = Decoder::new(
        bytes,
        RECORD_DOMAIN,
        RECORD_VERSION,
        usize_to_u64(bytes.len()),
        &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
    )?;
    let record = decode_record(
        &mut decoder,
        &mut |_| Ok::<_, CanonicalDecodeError>(()),
        std::convert::identity,
    )?;
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
    decode_page::<true, E>(bytes, limits, observe)
}

fn decode_page<const CHECK_ORDER: bool, E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<FileTablePage, E> {
    let mut decoder = Decoder::new(
        bytes,
        DOMAIN,
        VERSION,
        limits.maximum_page_object_bytes(),
        observe,
    )?;
    let tag = decoder.u8(observe)?;
    let item_count = decoder.u32(observe)?;
    if item_count > limits.maximum_page_items {
        return Err(CanonicalDecodeError::FieldTooLarge {
            observed: item_count,
            maximum: limits.maximum_page_items,
        }
        .into());
    }
    match tag {
        1 => {
            let mut records = Vec::new();
            let capacity = capacity(item_count)?;
            let admitted_bytes = usize_to_u64(
                capacity
                    .checked_mul(std::mem::size_of::<FileRecord>())
                    .ok_or(CanonicalDecodeError::LengthOverflow)?,
            );
            observe(ReadAllocationStep::Allocate {
                bytes: admitted_bytes,
                operations: u64::from(capacity != 0),
            })?;
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
                decode_record(&mut decoder, observe, |record| records.push(record))?;
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            if CHECK_ORDER
                && records
                    .windows(2)
                    .any(|pair| matches!(pair, [left, right] if left.file_id >= right.file_id))
            {
                return Err(invariant(FileTableError::NotStrictlyOrdered).into());
            }
            // decode_record checked every record; the header admitted count.
            // Keep ordering after finish to preserve trailing-byte precedence.
            #[cfg(test)]
            #[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
            super::decode_probe::after_decode(&records, observe)?;
            Ok(FileTablePage::Leaf(records))
        }
        2 => {
            let mut children = Vec::new();
            let capacity = capacity(item_count)?;
            let admitted_bytes = usize_to_u64(
                capacity
                    .checked_mul(std::mem::size_of::<Child<FileId>>())
                    .ok_or(CanonicalDecodeError::LengthOverflow)?,
            );
            observe(ReadAllocationStep::Allocate {
                bytes: admitted_bytes,
                operations: u64::from(capacity != 0),
            })?;
            if let Err(error) = children.try_reserve_exact(capacity) {
                observe(ReadAllocationStep::ReserveFailed(error))?;
                return Err(CanonicalDecodeError::AllocationFailed.into());
            }
            observe(ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes: usize_to_u64(
                    children.capacity() * std::mem::size_of::<Child<FileId>>(),
                ),
            })?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let first = FileId::from_bytes(decoder.fixed::<16, E>(observe)?);
                let page =
                    digest_object(ObjectKind::FileTablePage, decoder.fixed::<32, E>(observe)?);
                children.push(Child { first, page });
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            if children.is_empty()
                || children
                    .windows(2)
                    .any(|pair| matches!(pair, [left, right] if left.first >= right.first))
            {
                return Err(invariant(FileTableError::InvalidInternalPage).into());
            }
            // Child identities have the canonical FileTablePage kind above.
            Ok(FileTablePage::Internal(children))
        }
        tag => Err(CanonicalDecodeError::UnknownTag {
            field: "file_table_page",
            tag,
        }
        .into()),
    }
}

// Let the page caller insert the record without returning its full aggregate.
fn decode_record<E: From<CanonicalDecodeError>, T>(
    decoder: &mut Decoder<'_>,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
    finish: impl FnOnce(FileRecord) -> T,
) -> Result<T, E> {
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
                }
                .into());
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
            }
            .into());
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
    Ok(finish(record))
}

/// Computes one typed file-table page identity.
pub fn file_table_page_id(
    page: &FileTablePage,
    maximum_items: u32,
) -> Result<ObjectId, CanonicalDecodeError> {
    let bytes = encode_file_table_page(
        page,
        maximum_items,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    Ok(ObjectId {
        kind: ObjectKind::FileTablePage,
        digest: object_digest(ObjectKind::FileTablePage, &bytes),
    })
}

#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
#[path = "../tests/file_table_decode.rs"]
mod tests;
