// Independent pre-optimization parser from main 07fa36415. Keep its
// parsing, semantic validation and observer order separate from the candidate.
use super::*;

pub(super) fn decode<E>(
    bytes: &[u8],
    limits: DecodeLimits,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), E>,
) -> Result<FileTablePage, E>
where
    E: From<CanonicalDecodeError>,
{
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
                records.push(decode_record(&mut decoder, observe)?);
                observe(ReadAllocationStep::Item)?;
            }
            decoder.finish()?;
            let page = FileTablePage::Leaf(records);
            validate_page(&page, limits.maximum_page_items).map_err(invariant)?;
            Ok(page)
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
    validate_record(record).map_err(invariant)?;
    Ok(record)
}

fn validate_record(record: FileRecord) -> Result<(), FileTableError> {
    if record.link_count == 0 || record.metadata.kind != ObjectKind::Metadata {
        return Err(FileTableError::InvalidRecord);
    }
    let valid = match (record.kind, record.payload) {
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
        | (FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary, FilePayload::Empty) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(FileTableError::InvalidRecord)
    }
}

fn validate_page(page: &FileTablePage, maximum_items: u32) -> Result<(), FileTableError> {
    let count = match page {
        FileTablePage::Leaf(records) => {
            if records.windows(2).any(|pair| {
                let [first, second] = pair else {
                    return false;
                };
                first.file_id >= second.file_id
            }) {
                return Err(FileTableError::NotStrictlyOrdered);
            }
            for record in records {
                validate_record(*record)?;
            }
            records.len()
        }
        FileTablePage::Internal(children) => {
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
