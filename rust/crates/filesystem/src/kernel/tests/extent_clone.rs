use super::*;
use crate::foundation::Digest;
use crate::kernel::{
    Extent, ExtentKind, ExtentPage, decode_extent_page, encode_extent_page, extent_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectStore;
use bytes::Bytes;

fn put(
    store: &MemoryObjectStore,
    extents: Vec<Extent>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let page = ExtentPage::Leaf(extents);
    let root = extent_page_id(&page, 16)?;
    store.put(
        root,
        Bytes::from(encode_extent_page(&page, 16)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(root)
}

fn extents(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = store.read(root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    let ExtentPage::Leaf(extents) = decode_extent_page(&bytes, DecodeLimits::default())? else {
        return Err("clone result is not a leaf".into());
    };
    Ok(extents)
}

#[test]
fn clone_reuses_content_and_preserves_sparse_kinds_without_blob_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([4; 32]),
    };
    let source = put(
        &store,
        vec![
            Extent {
                offset: 0,
                length: 4,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 20,
                },
            },
            Extent {
                offset: 4,
                length: 4,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 8,
                length: 4,
                kind: ExtentKind::AllocatedZero,
            },
        ],
    )?;
    let destination = put(
        &store,
        vec![Extent {
            offset: 0,
            length: 16,
            kind: ExtentKind::Hole,
        }],
    )?;
    let receipt = clone_extent_range(
        &store,
        ExtentCloneRequest {
            source_root: source,
            source_logical_bytes: 12,
            source_range: ByteRange {
                offset: 2,
                length: 8,
            },
            destination_root: destination,
            destination_logical_bytes: 16,
            destination_offset: 6,
            maximum_spans: 3,
            maximum_mutations: 3,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.logical_bytes, 16);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 1);
    assert_eq!(
        extents(&store, receipt.root)?,
        vec![
            Extent {
                offset: 0,
                length: 6,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 6,
                length: 2,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 22,
                },
            },
            Extent {
                offset: 8,
                length: 4,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 12,
                length: 2,
                kind: ExtentKind::AllocatedZero,
            },
            Extent {
                offset: 14,
                length: 2,
                kind: ExtentKind::Hole,
            },
        ]
    );
    Ok(())
}

#[test]
fn same_file_overlap_plans_source_before_destination_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        vec![
            Extent {
                offset: 0,
                length: 4,
                kind: ExtentKind::AllocatedZero,
            },
            Extent {
                offset: 4,
                length: 4,
                kind: ExtentKind::Hole,
            },
        ],
    )?;
    let receipt = clone_extent_range(
        &store,
        ExtentCloneRequest {
            source_root: root,
            source_logical_bytes: 8,
            source_range: ByteRange {
                offset: 0,
                length: 6,
            },
            destination_root: root,
            destination_logical_bytes: 8,
            destination_offset: 2,
            maximum_spans: 2,
            maximum_mutations: 2,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        extents(&store, receipt.root)?,
        vec![
            Extent {
                offset: 0,
                length: 6,
                kind: ExtentKind::AllocatedZero,
            },
            Extent {
                offset: 6,
                length: 2,
                kind: ExtentKind::Hole,
            },
        ]
    );
    Ok(())
}

#[test]
fn invalid_decode_limits_reject_before_source_reads() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }],
    )?;
    let failure = clone_extent_range(
        &store,
        ExtentCloneRequest {
            source_root: root,
            source_logical_bytes: 8,
            source_range: ByteRange {
                offset: 0,
                length: 1,
            },
            destination_root: root,
            destination_logical_bytes: 8,
            destination_offset: 1,
            maximum_spans: 1,
            maximum_mutations: 1,
        },
        DecodeLimits {
            maximum_page_items: 1,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid decode limits unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentCloneError::InvalidLimits));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn async_and_sync_clone_share_exact_results_and_work() -> Result<(), Box<dyn std::error::Error>> {
    let sync_store = MemoryObjectStore::default();
    let async_store = MemoryObjectStore::default();
    let source_extents = vec![
        Extent {
            offset: 0,
            length: 4,
            kind: ExtentKind::AllocatedZero,
        },
        Extent {
            offset: 4,
            length: 4,
            kind: ExtentKind::Hole,
        },
    ];
    let destination_extents = vec![Extent {
        offset: 0,
        length: 12,
        kind: ExtentKind::Hole,
    }];
    let sync_source = put(&sync_store, source_extents.clone())?;
    let async_source = put(&async_store, source_extents)?;
    let sync_destination = put(&sync_store, destination_extents.clone())?;
    let async_destination = put(&async_store, destination_extents)?;
    let request = ExtentCloneRequest {
        source_root: sync_source,
        source_logical_bytes: 8,
        source_range: ByteRange {
            offset: 1,
            length: 6,
        },
        destination_root: sync_destination,
        destination_logical_bytes: 12,
        destination_offset: 3,
        maximum_spans: 2,
        maximum_mutations: 2,
    };
    let synchronous = clone_extent_range(
        &sync_store,
        request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(clone_extent_range_async(
        &async_store,
        ExtentCloneRequest {
            source_root: async_source,
            destination_root: async_destination,
            ..request
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous range clone unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);
    Ok(())
}

#[test]
fn pre_cancelled_async_clone_performs_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }],
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(clone_extent_range_async(
        &store,
        ExtentCloneRequest {
            source_root: root,
            source_logical_bytes: 8,
            source_range: ByteRange {
                offset: 0,
                length: 1,
            },
            destination_root: root,
            destination_logical_bytes: 8,
            destination_offset: 1,
            maximum_spans: 1,
            maximum_mutations: 1,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory-backed asynchronous range clone unexpectedly blocked")?
    .err()
    .ok_or("cancelled range clone unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentCloneError::Source(super::super::ExtentReadError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn request_validation_rejects_every_shape_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let extent = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: Digest::ZERO,
    };
    let valid = ExtentCloneRequest {
        source_root: extent,
        source_logical_bytes: 1,
        source_range: ByteRange {
            offset: 0,
            length: 1,
        },
        destination_root: extent,
        destination_logical_bytes: 1,
        destination_offset: 0,
        maximum_spans: 1,
        maximum_mutations: 1,
    };
    let cases = [
        (
            ExtentCloneRequest {
                source_root: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: Digest::ZERO,
                },
                ..valid
            },
            ExtentCloneError::WrongRootKind,
        ),
        (
            ExtentCloneRequest {
                destination_root: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: Digest::ZERO,
                },
                ..valid
            },
            ExtentCloneError::WrongRootKind,
        ),
        (
            ExtentCloneRequest {
                source_range: ByteRange {
                    offset: 0,
                    length: 0,
                },
                ..valid
            },
            ExtentCloneError::EmptyRange,
        ),
        (
            ExtentCloneRequest {
                source_range: ByteRange {
                    offset: u64::MAX,
                    length: 1,
                },
                ..valid
            },
            ExtentCloneError::RangeOverflow,
        ),
        (
            ExtentCloneRequest {
                destination_offset: u64::MAX,
                ..valid
            },
            ExtentCloneError::RangeOverflow,
        ),
        (
            ExtentCloneRequest {
                maximum_spans: 0,
                maximum_mutations: 0,
                ..valid
            },
            ExtentCloneError::InvalidLimits,
        ),
        (
            ExtentCloneRequest {
                maximum_spans: 2,
                maximum_mutations: 1,
                ..valid
            },
            ExtentCloneError::InvalidLimits,
        ),
        (valid, ExtentCloneError::InvalidLimits),
    ];
    let case_count = cases.len();
    for (index, (request, expected)) in cases.into_iter().enumerate() {
        let limits = if index + 1 == case_count {
            DecodeLimits {
                maximum_page_height: 0,
                ..DecodeLimits::default()
            }
        } else {
            DecodeLimits::default()
        };
        let Err(failure) = validate_request(request, limits) else {
            return Err("invalid clone request unexpectedly succeeded".into());
        };
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
    }
    Ok(())
}
