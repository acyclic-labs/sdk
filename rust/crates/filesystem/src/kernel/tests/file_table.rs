use super::*;
use crate::foundation::Digest;
#[cfg(feature = "memory")]
use crate::memory::MemoryObjectStore;
#[cfg(feature = "memory")]
use crate::storage::ObjectStore;
#[cfg(feature = "memory")]
use bytes::Bytes;
#[cfg(feature = "memory")]
use std::task::{Context, Poll, Waker};

#[cfg(feature = "memory")]
fn put_memory_page(
    store: &MemoryObjectStore,
    page: &FileTablePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, 8)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_file_table_page(page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

#[cfg(feature = "memory")]
fn fifo_record(byte: u8) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

#[cfg(feature = "memory")]
fn two_level_file_table(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let first = put_memory_page(store, &FileTablePage::Leaf(vec![fifo_record(1)]))?;
    let second = put_memory_page(store, &FileTablePage::Leaf(vec![fifo_record(2)]))?;
    put_memory_page(
        store,
        &FileTablePage::Internal(vec![
            FileTableChild {
                first_file_id: FileId::from_bytes([1; 16]),
                page: first,
            },
            FileTableChild {
                first_file_id: FileId::from_bytes([2; 16]),
                page: second,
            },
        ]),
    )
}

#[test]
fn kind_payload_mismatches_fail_before_identity() {
    let record = FileRecord {
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Directory,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::ZERO,
        },
        payload: FilePayload::Empty,
    };
    assert!(encode_file_table_page(&FileTablePage::Leaf(vec![record]), 8).is_err());
}

#[allow(clippy::too_many_lines)]
#[test]
fn every_file_record_payload_and_typed_reference_is_validated() {
    let metadata = ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::from_bytes([9; 32]),
    };
    let typed = |kind| ObjectId {
        kind,
        digest: Digest::from_bytes([10; 32]),
    };
    let base = FileRecord {
        file_id: FileId::from_bytes([9; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata,
        payload: FilePayload::Empty,
    };
    for invalid in [
        FileRecord {
            link_count: 0,
            ..base
        },
        FileRecord {
            metadata: typed(ObjectKind::Blob),
            ..base
        },
        FileRecord {
            kind: FileKind::Regular,
            payload: FilePayload::Regular {
                logical_bytes: 1,
                extents: typed(ObjectKind::Blob),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::Directory,
            payload: FilePayload::Directory {
                entries: typed(ObjectKind::Blob),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::SymbolicLink,
            payload: FilePayload::SymbolicLink {
                target_bytes: 1,
                target: typed(ObjectKind::TreePage),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::ReparsePoint,
            payload: FilePayload::ReparsePoint {
                payload_bytes: 1,
                payload: typed(ObjectKind::TreePage),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::Directory,
            payload: FilePayload::Empty,
            ..base
        },
    ] {
        assert!(matches!(
            invalid.validate(),
            Err(FileTableError::InvalidRecord)
        ));
    }
    for valid in [
        base,
        FileRecord {
            kind: FileKind::Regular,
            payload: FilePayload::Regular {
                logical_bytes: 1,
                extents: typed(ObjectKind::ExtentPage),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::Directory,
            payload: FilePayload::Directory {
                entries: typed(ObjectKind::TreePage),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::SymbolicLink,
            payload: FilePayload::SymbolicLink {
                target_bytes: 1,
                target: typed(ObjectKind::Blob),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::ReparsePoint,
            payload: FilePayload::ReparsePoint {
                payload_bytes: 1,
                payload: typed(ObjectKind::Blob),
            },
            ..base
        },
        FileRecord {
            kind: FileKind::CharacterDevice,
            payload: FilePayload::Device { major: 1, minor: 3 },
            ..base
        },
        FileRecord {
            kind: FileKind::MountBoundary,
            payload: FilePayload::Empty,
            ..base
        },
    ] {
        assert!(valid.validate().is_ok());
    }
}

#[test]
fn record_round_trip_is_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let page = FileTablePage::Leaf(vec![FileRecord {
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
        link_count: 2,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([2; 32]),
        },
        payload: FilePayload::Regular {
            logical_bytes: 99,
            extents: ObjectId {
                kind: ObjectKind::ExtentPage,
                digest: Digest::from_bytes([3; 32]),
            },
        },
    }]);
    let encoded = encode_file_table_page(&page, 8)?;
    assert_eq!(
        decode_file_table_page(&encoded, DecodeLimits::default())?,
        page
    );
    Ok(())
}

#[test]
fn inline_regular_payload_is_bounded_allocation_free_and_canonical()
-> Result<(), Box<dyn std::error::Error>> {
    let data = InlineFileData::new(&[0, 1, 2, 0xff])?;
    assert_eq!(data.as_bytes(), &[0, 1, 2, 0xff]);
    assert_eq!(
        InlineFileData::new(&[7; MAXIMUM_INLINE_FILE_BYTES + 1]),
        Err(InlineFileDataError::TooLarge)
    );
    let page = FileTablePage::Leaf(vec![FileRecord {
        file_id: FileId::from_bytes([4; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([5; 32]),
        },
        payload: FilePayload::InlineRegular(data),
    }]);
    let encoded = encode_file_table_page(&page, 1)?;
    assert_eq!(
        decode_file_table_page(&encoded, DecodeLimits::default())?,
        page
    );
    assert_eq!(encoded.len(), 78);
    assert_eq!(
        &encoded[encoded.len() - data.as_bytes().len()..],
        data.as_bytes()
    );
    assert_eq!(data.truncate(2)?.as_bytes(), &[0, 1]);
    assert_eq!(data.truncate(5), Err(InlineFileDataError::TooLarge));
    assert_eq!(
        data.replace_range(1, &[9, 8], 4)?.as_bytes(),
        &[0, 9, 8, 0xff]
    );
    assert_eq!(
        data.replace_range(usize::MAX, &[1], 4),
        Err(InlineFileDataError::TooLarge)
    );
    assert_eq!(
        data.replace_range(3, &[1, 2], 4),
        Err(InlineFileDataError::TooLarge)
    );
    assert_eq!(
        data.replace_range(0, &[], MAXIMUM_INLINE_FILE_BYTES + 1),
        Err(InlineFileDataError::TooLarge)
    );
    Ok(())
}

#[test]
fn batch_error_and_authenticated_child_bound_mapping_is_total() {
    use crate::kernel::persistent_batch::Error as BatchError;

    assert!(matches!(
        map_batch_error(BatchError::Cancelled),
        FileRecordReadError::Cancelled
    ));
    assert!(matches!(
        map_batch_error(BatchError::Empty),
        FileRecordReadError::EmptyBatch
    ));
    assert!(matches!(
        map_batch_error(BatchError::TooManyQueries),
        FileRecordReadError::TooManyQueries
    ));
    assert!(matches!(
        map_batch_error(BatchError::WrongRootKind),
        FileRecordReadError::WrongRootKind
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidLimits),
        FileRecordReadError::InvalidHeightLimit
    ));
    assert!(matches!(
        map_batch_error(BatchError::HeightExceeded),
        FileRecordReadError::HeightExceeded
    ));
    assert!(matches!(
        map_batch_error(BatchError::CycleOrAlias),
        FileRecordReadError::Cycle
    ));
    assert!(matches!(
        map_batch_error(BatchError::ChildBoundsMismatch),
        FileRecordReadError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidRouting),
        FileRecordReadError::InvalidRouting
    ));
    assert!(matches!(
        map_batch_error(BatchError::AllocationFailed),
        FileRecordReadError::AllocationFailed
    ));
    assert!(matches!(
        map_batch_error(BatchError::Storage(ObjectStoreError::Corrupt)),
        FileRecordReadError::Storage(ObjectStoreError::Corrupt)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Decode(CanonicalDecodeError::TrailingBytes)),
        FileRecordReadError::Decode(CanonicalDecodeError::TrailingBytes)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Work(WorkError::Overflow)),
        FileRecordReadError::Work(WorkError::Overflow)
    ));

    let first = FileId::from_bytes([1; 16]);
    let second = FileId::from_bytes([2; 16]);
    let child = FileTableChild {
        first_file_id: first,
        page: ObjectId {
            kind: ObjectKind::FileTablePage,
            digest: Digest::from_bytes([1; 32]),
        },
    };
    assert!(validate_child_bounds(&[child], Some(first), Some(second)).is_ok());
    assert!(matches!(
        validate_child_bounds(&[child], Some(second), None),
        Err(FileRecordReadError::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_child_bounds(&[child], Some(first), Some(first)),
        Err(FileRecordReadError::ChildBoundsMismatch)
    ));
    let record = FileRecord {
        file_id: first,
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([2; 32]),
        },
        payload: FilePayload::Empty,
    };
    assert!(validate_record_bounds(&[record], Some(first), Some(second)).is_ok());
    assert!(matches!(
        validate_record_bounds(&[record], Some(second), None),
        Err(FileRecordReadError::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_record_bounds(&[record], Some(first), Some(first)),
        Err(FileRecordReadError::ChildBoundsMismatch)
    ));
}

#[test]
fn every_payload_shape_round_trips_through_full_and_shape_decoders()
-> Result<(), Box<dyn std::error::Error>> {
    let typed = |kind, byte| ObjectId {
        kind,
        digest: Digest::from_bytes([byte; 32]),
    };
    let metadata = typed(ObjectKind::Metadata, 20);
    let record = |byte, kind, payload| FileRecord {
        file_id: FileId::from_bytes([byte; 16]),
        kind,
        link_count: 1,
        metadata,
        payload,
    };
    let inline = InlineFileData::new(b"inline")?;
    assert_eq!(
        format!("{inline:?}"),
        "InlineFileData([105, 110, 108, 105, 110, 101])"
    );
    let records = vec![
        record(1, FileKind::Regular, FilePayload::InlineRegular(inline)),
        record(
            2,
            FileKind::Regular,
            FilePayload::Regular {
                logical_bytes: 4_096,
                extents: typed(ObjectKind::ExtentPage, 21),
            },
        ),
        record(
            3,
            FileKind::Directory,
            FilePayload::Directory {
                entries: typed(ObjectKind::TreePage, 22),
            },
        ),
        record(
            4,
            FileKind::SymbolicLink,
            FilePayload::SymbolicLink {
                target_bytes: 17,
                target: typed(ObjectKind::Blob, 23),
            },
        ),
        record(5, FileKind::Fifo, FilePayload::Empty),
        record(
            6,
            FileKind::CharacterDevice,
            FilePayload::Device { major: 8, minor: 1 },
        ),
        record(
            7,
            FileKind::ReparsePoint,
            FilePayload::ReparsePoint {
                payload_bytes: 31,
                payload: typed(ObjectKind::Blob, 24),
            },
        ),
    ];
    for record in &records {
        assert!(file_record_encoded_length(*record)? > 0);
    }
    let page = FileTablePage::Leaf(records);
    let encoded = encode_file_table_page(&page, 8)?;
    let shape = file_table_page_decode_shape(&encoded, DecodeLimits::default())?;
    assert_eq!(shape.kind, DecodedPageKind::Leaf);
    assert_eq!(shape.items, 7);
    assert_eq!(shape.nested_bytes, 0);
    assert_eq!(
        decode_file_table_page(&encoded, DecodeLimits::default())?,
        page
    );

    let child = FileTableChild {
        first_file_id: FileId::from_bytes([1; 16]),
        page: typed(ObjectKind::FileTablePage, 25),
    };
    let internal = FileTablePage::Internal(vec![child]);
    let encoded = encode_file_table_page(&internal, 8)?;
    let shape = file_table_page_decode_shape(&encoded, DecodeLimits::default())?;
    assert_eq!(shape.kind, DecodedPageKind::Internal);
    assert_eq!(shape.items, 1);
    assert_eq!(
        decode_file_table_page(&encoded, DecodeLimits::default())?,
        internal
    );
    Ok(())
}

#[test]
fn file_table_page_invariants_fail_at_encoding_and_identity_boundaries() {
    let record = |byte| FileRecord {
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([byte; 32]),
        },
        payload: FilePayload::Empty,
    };
    let child = |byte, kind| FileTableChild {
        first_file_id: FileId::from_bytes([byte; 16]),
        page: ObjectId {
            kind,
            digest: Digest::from_bytes([byte; 32]),
        },
    };
    let invalid = [
        FileTablePage::Leaf(vec![record(2), record(1)]),
        FileTablePage::Internal(Vec::new()),
        FileTablePage::Internal(vec![child(1, ObjectKind::Blob)]),
        FileTablePage::Internal(vec![
            child(2, ObjectKind::FileTablePage),
            child(1, ObjectKind::FileTablePage),
        ]),
    ];
    for page in invalid {
        assert!(encode_file_table_page(&page, 8).is_err());
        assert!(file_table_page_id(&page, 8).is_err());
        assert!(page.validate(8).is_err());
    }
    let oversized_leaf = FileTablePage::Leaf(vec![record(1)]);
    assert!(encode_file_table_page(&oversized_leaf, 0).is_err());
    assert_eq!(
        oversized_leaf.validate(0),
        Err(FileTableError::TooManyItems)
    );
    let oversized_internal = FileTablePage::Internal(vec![child(1, ObjectKind::FileTablePage)]);
    assert!(encode_file_table_page(&oversized_internal, 0).is_err());
    assert_eq!(
        oversized_internal.validate(0),
        Err(FileTableError::TooManyItems)
    );
}

#[test]
fn malformed_file_table_tags_and_bounds_fail_in_shape_and_full_decoders()
-> Result<(), Box<dyn std::error::Error>> {
    const RECORD_PAYLOAD_OFFSET: usize = 15 + 16 + 1 + 8 + 32;

    let page = FileTablePage::Leaf(vec![FileRecord {
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::ZERO,
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(b"x")?),
    }]);
    let encoded = encode_file_table_page(&page, 1)?;
    let tight = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    for result in [
        file_table_page_decode_shape(&encoded, tight).map(|_| ()),
        decode_file_table_page(&encoded, tight).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::FieldTooLarge {
                observed: 1,
                maximum: 0,
            })
        ));
    }

    let mut unknown_page = encoded.clone();
    unknown_page[DOMAIN.len() + 2] = 9;
    for result in [
        file_table_page_decode_shape(&unknown_page, DecodeLimits::default()).map(|_| ()),
        decode_file_table_page(&unknown_page, DecodeLimits::default()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::UnknownTag {
                field: "file_table_page",
                tag: 9,
            })
        ));
    }

    let mut unknown_payload = encoded.clone();
    unknown_payload[RECORD_PAYLOAD_OFFSET] = 9;
    for result in [
        file_table_page_decode_shape(&unknown_payload, DecodeLimits::default()).map(|_| ()),
        decode_file_table_page(&unknown_payload, DecodeLimits::default()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::UnknownTag {
                field: "file_payload",
                tag: 9,
            })
        ));
    }

    let mut oversized_inline = encoded;
    oversized_inline[RECORD_PAYLOAD_OFFSET + 1] = u8::try_from(MAXIMUM_INLINE_FILE_BYTES + 1)?;
    for result in [
        file_table_page_decode_shape(&oversized_inline, DecodeLimits::default()).map(|_| ()),
        decode_file_table_page(&oversized_inline, DecodeLimits::default()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::FieldTooLarge { observed, maximum })
                if observed == u32::try_from(MAXIMUM_INLINE_FILE_BYTES + 1)?
                    && maximum == u32::try_from(MAXIMUM_INLINE_FILE_BYTES)?
        ));
    }
    Ok(())
}

#[cfg(feature = "memory")]
#[test]
fn lookup_reads_only_one_file_table_frontier() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_file_table(&store)?;
    let found = lookup_file_record(
        &store,
        root,
        FileId::from_bytes([2; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(found.record, Some(fifo_record(2)));
    assert_eq!(found.work.page_reads, 2);

    let cancellation = CancellationToken::new();
    let mut future = std::pin::pin!(lookup_file_record_async(
        &store,
        root,
        FileId::from_bytes([2; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory-backed asynchronous file lookup remained pending".into());
    };
    assert_eq!(asynchronous?, found);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(lookup_file_record_async(
        &store,
        root,
        FileId::from_bytes([2; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ));
    let Poll::Ready(result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous file lookup remained pending".into());
    };
    let failure = result
        .err()
        .ok_or("pre-cancelled asynchronous file lookup unexpectedly succeeded")?;
    assert!(matches!(failure.error, FileRecordReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[cfg(feature = "memory")]
#[test]
fn file_record_batch_shares_frontiers_and_preserves_order() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = two_level_file_table(&store)?;
    let batch = lookup_file_records(
        &store,
        root,
        &[
            FileId::from_bytes([2; 16]),
            FileId::from_bytes([1; 16]),
            FileId::from_bytes([2; 16]),
            FileId::from_bytes([3; 16]),
        ],
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        batch.records,
        vec![
            Some(fifo_record(2)),
            Some(fifo_record(1)),
            Some(fifo_record(2)),
            None,
        ]
    );
    assert_eq!(batch.work.page_reads, 3);
    assert_eq!(batch.work.backend_read_operations, 2);
    Ok(())
}

#[cfg(feature = "memory")]
#[test]
fn file_lookup_limits_kinds_and_authenticated_bounds_fail_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_file_table(&store)?;
    let wrong = lookup_file_record(
        &store,
        ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([55; 32]),
        },
        FileId::from_bytes([1; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("wrong file-table root kind unexpectedly succeeded")?;
    assert!(matches!(wrong.error, FileRecordReadError::WrongRootKind));
    assert_eq!(*wrong.work, WorkCounters::default());

    let invalid_limits = DecodeLimits {
        maximum_page_height: 0,
        ..DecodeLimits::default()
    };
    let invalid = lookup_file_record(
        &store,
        root,
        FileId::from_bytes([1; 16]),
        invalid_limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid file lookup limits unexpectedly succeeded")?;
    assert!(matches!(
        invalid.error,
        FileRecordReadError::InvalidHeightLimit
    ));
    assert_eq!(*invalid.work, WorkCounters::default());
    let height_one = DecodeLimits {
        maximum_page_height: 1,
        ..DecodeLimits::default()
    };
    let height = lookup_file_record(
        &store,
        root,
        FileId::from_bytes([1; 16]),
        height_one,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("file lookup height limit unexpectedly succeeded")?;
    assert!(matches!(height.error, FileRecordReadError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);

    let empty = lookup_file_records(
        &store,
        root,
        &[],
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("empty file lookup batch unexpectedly succeeded")?;
    assert!(matches!(empty.error, FileRecordReadError::EmptyBatch));
    assert_eq!(*empty.work, WorkCounters::default());
    let excessive = lookup_file_records(
        &store,
        root,
        &[FileId::from_bytes([1; 16]), FileId::from_bytes([2; 16])],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("excessive file lookup batch unexpectedly succeeded")?;
    assert!(matches!(
        excessive.error,
        FileRecordReadError::TooManyQueries
    ));
    assert_eq!(*excessive.work, WorkCounters::default());

    let leaf = put_memory_page(&store, &FileTablePage::Leaf(vec![fifo_record(1)]))?;
    let forged = put_memory_page(
        &store,
        &FileTablePage::Internal(vec![FileTableChild {
            first_file_id: FileId::from_bytes([0; 16]),
            page: leaf,
        }]),
    )?;
    let failure = lookup_file_record(
        &store,
        forged,
        FileId::from_bytes([1; 16]),
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("forged file-table bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FileRecordReadError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}
