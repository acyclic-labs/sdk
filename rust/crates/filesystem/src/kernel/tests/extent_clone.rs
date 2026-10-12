use super::*;
use crate::foundation::Digest;
use crate::kernel::{
    Extent, ExtentKind, ExtentPage, decode_extent_page, encode_extent_page, extent_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectStore;
use acyclic_stream::OwnedBytes;

fn put(
    store: &MemoryObjectStore,
    extents: Vec<Extent>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let page = ExtentPage::Leaf(extents);
    let root = extent_page_id(&page, 16)?;
    store.put(
        root,
        OwnedBytes::from(encode_extent_page(&page, 16, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(root)
}

fn extents(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = store.read(root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    let ExtentPage::Leaf(extents) = decode_extent_page(&bytes, DecodeLimits::default(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? else {
        return Err("clone result is not a leaf".into());
    };
    Ok(extents)
}

#[tokio::test]
async fn clone_reuses_content_and_preserves_sparse_kinds_without_blob_reads()
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
        &CancellationToken::new(),
    ).await?;
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

#[tokio::test]
async fn same_file_overlap_plans_source_before_destination_writes()
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
        &CancellationToken::new(),
    ).await?;
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

#[tokio::test]
async fn invalid_decode_limits_reject_before_source_reads() -> Result<(), Box<dyn std::error::Error>> {
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("invalid decode limits unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentCloneError::InvalidLimits));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_clone_performs_zero_work() -> Result<(), Box<dyn std::error::Error>> {
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
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
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
        assert_eq!(
            std::mem::discriminant(&failure),
            std::mem::discriminant(&expected)
        );
    }
    Ok(())
}
