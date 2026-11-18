use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, Extent, ExtentCloneError, ExtentCloneRequest, ExtentKind,
    ExtentMutationError, ExtentPage, clone_extent_range, decode_extent_page,
};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, ByteRange, CancellationToken, MemoryObjectStore,
    ObjectStoreError,
};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

use super::super::extent_objects::put_extent_page;
use super::super::memory::memory_objects;

async fn put(
    store: &MemoryObjectStore,
    extents: Vec<Extent>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    put_extent_page(store, &ExtentPage::Leaf(extents), 16).await
}

async fn extents(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = store
        .read(
            root,
            u64::MAX,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    let ExtentPage::Leaf(extents) =
        decode_extent_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, CanonicalDecodeError>(())
        })?
    else {
        return Err("clone result is not a leaf".into());
    };
    Ok(extents)
}

#[tokio::test]
async fn clone_reuses_content_and_preserves_sparse_kinds_without_blob_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let blob = ObjectId {
        storage: None,
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
    )
    .await?;
    let destination = put(
        &store,
        vec![Extent {
            offset: 0,
            length: 16,
            kind: ExtentKind::Hole,
        }],
    )
    .await?;
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
            maximum_spans: Some(3),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.logical_bytes, 16);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 1);
    assert_eq!(
        extents(&store, receipt.root).await?,
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
    let store = memory_objects();
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
    )
    .await?;
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
            maximum_spans: Some(2),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        extents(&store, receipt.root).await?,
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
async fn invalid_decode_limits_reject_before_source_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let root = put(
        &store,
        vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }],
    )
    .await?;
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
            maximum_spans: Some(1),
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
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_clone_performs_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put(
        &store,
        vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }],
    )
    .await?;
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
            maximum_spans: Some(1),
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
        ExtentCloneError::Destination(ExtentMutationError::Storage(ObjectStoreError::Cancelled))
    ));
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn request_validation_rejects_every_shape_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    let extent = ObjectId {
        storage: None,
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
        maximum_spans: Some(1),
    };
    let cases = [
        (
            ExtentCloneRequest {
                source_root: ObjectId {
                    storage: None,
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
                    storage: None,
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
                maximum_spans: Some(0),

                ..valid
            },
            ExtentCloneError::InvalidLimits,
        ),
        (valid, ExtentCloneError::InvalidLimits),
    ];
    let store = memory_objects();
    let cancellation = CancellationToken::new();
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
        let Err(failure) = clone_extent_range(
            &store,
            request,
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        else {
            return Err("invalid clone request unexpectedly succeeded".into());
        };
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(*failure.work()?, WorkCounters::UNCHARGED);
    }
    Ok(())
}
