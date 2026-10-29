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
    let item_bytes = children.clone().try_fold(0usize, |bytes, (_, page)| {
        bytes
            .checked_add(16 + page.encoded_len())
            .ok_or(CanonicalDecodeError::LengthOverflow)
    })?;
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
            bytes_encoded: usize_to_u64(16 + page.encoded_len()),
            bytes_copied: usize_to_u64(16 + page.encoded_len()),
            ..WorkCounters::default()
        };
        observe(EncodeWorkStep::Admit(encoded))?;
        encoder.fixed(&file_id.into_bytes());
        encoder.object(page);
        observe(EncodeWorkStep::Completed(encoded))?;
    }
    Ok(encoder.finish())
}

pub(super) fn record_copied_bytes(record: FileRecord) -> u64 {
    16 + usize_to_u64(record.metadata.encoded_len())
        + match record.payload {
            FilePayload::InlineRegular(data) => usize_to_u64(data.as_bytes().len()),
            FilePayload::Regular {
                extents: object, ..
            }
            | FilePayload::Directory { entries: object }
            | FilePayload::SymbolicLink { target: object, .. }
            | FilePayload::ReparsePoint {
                payload: object, ..
            } => usize_to_u64(object.encoded_len()),
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
    let (tag, item_count) = decoder.page_header(limits.maximum_page_items, &mut |_| {
        Ok::<_, CanonicalDecodeError>(())
    })?;
    let kind = match tag {
        1 => {
            for _ in 0..item_count {
                let _: [u8; 16] =
                    decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                decoder.any_object(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                match decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? {
                    1 | 3 | 6 => {
                        decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                        decoder.any_object(&mut |_| {
                            Ok::<_, crate::kernel::CanonicalDecodeError>(())
                        })?;
                    }
                    2 => {
                        decoder.any_object(&mut |_| {
                            Ok::<_, crate::kernel::CanonicalDecodeError>(())
                        })?;
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
                decoder.any_object(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
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
    let prefix = 16 + 1 + 8 + record.metadata.encoded_len() + 1;
    let payload = match record.payload {
        FilePayload::InlineRegular(data) => 1_usize
            .checked_add(data.as_bytes().len())
            .ok_or(CanonicalDecodeError::LengthOverflow)?,
        FilePayload::Regular {
            extents: object, ..
        }
        | FilePayload::SymbolicLink { target: object, .. }
        | FilePayload::ReparsePoint {
            payload: object, ..
        } => 8 + object.encoded_len(),
        FilePayload::Directory { entries } => entries.encoded_len(),
        FilePayload::Empty => 0,
        FilePayload::Device { .. } => 8,
    };
    prefix
        .checked_add(payload)
        .ok_or(CanonicalDecodeError::LengthOverflow)
}

fn encode_record(encoder: &mut Encoder, record: FileRecord) {
    encoder.fixed(&record.file_id.into_bytes());
    encoder.u8(record.kind.tag());
    encoder.u64(record.link_count);
    encoder.object(record.metadata);
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
            encoder.object(extents);
        }
        FilePayload::Directory { entries } => {
            encoder.u8(2);
            encoder.object(entries);
        }
        FilePayload::SymbolicLink {
            target_bytes,
            target,
        } => {
            encoder.u8(3);
            encoder.u64(target_bytes);
            encoder.object(target);
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
            encoder.object(payload);
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
    let (tag, item_count) = decoder.page_header(limits.maximum_page_items, observe)?;
    match tag {
        1 => {
            let mut records = Vec::new();
            let capacity = capacity(item_count)?;
            crate::kernel::file_read::reserve_decoded_items(&mut records, capacity, observe)?;
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
            #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
            super::decode_probe::after_decode(&records, observe)?;
            Ok(FileTablePage::Leaf(records))
        }
        2 => {
            let mut children = Vec::new();
            let capacity = capacity(item_count)?;
            crate::kernel::file_read::reserve_decoded_items(&mut children, capacity, observe)?;
            for _ in 0..item_count {
                observe(ReadAllocationStep::BeforeItem)?;
                let first = FileId::from_bytes(decoder.fixed::<16, E>(observe)?);
                let page = decoder.object(ObjectKind::FileTablePage, observe)?;
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
    let metadata = decoder.object(ObjectKind::Metadata, observe)?;
    let payload = match decoder.u8(observe)? {
        1 => FilePayload::Regular {
            logical_bytes: decoder.u64(observe)?,
            extents: decoder.object(ObjectKind::ExtentPage, observe)?,
        },
        2 => FilePayload::Directory {
            entries: decoder.object(ObjectKind::TreePage, observe)?,
        },
        3 => FilePayload::SymbolicLink {
            target_bytes: decoder.u64(observe)?,
            target: decoder.object(ObjectKind::Blob, observe)?,
        },
        4 => FilePayload::Empty,
        5 => FilePayload::Device {
            major: decoder.u32(observe)?,
            minor: decoder.u32(observe)?,
        },
        6 => FilePayload::ReparsePoint {
            payload_bytes: decoder.u64(observe)?,
            payload: decoder.object(ObjectKind::Blob, observe)?,
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
        storage: None,
        kind: ObjectKind::FileTablePage,
        digest: object_digest(&bytes),
    })
}

#[cfg(test)]
#[cfg(not(loom))]
#[path = "../tests/file_table_decode.rs"]
mod tests;
