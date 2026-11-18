use acyclic_fs::kernel::{
    DecodeLimits, Extent, ExtentKind, ExtentPage, ExtentRangeRequest, ExtentReadError,
    ExtentSeekRequest, ExtentSeekTarget, plan_extent_range, seek_extent,
};
use acyclic_fs::{ByteRange, CancellationToken};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

use super::super::extent_objects::put_extent_page;
use super::super::memory::memory_objects;

#[tokio::test]
async fn sparse_seek_distinguishes_holes_allocated_data_content_and_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let content = ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([19; 32]),
    };
    let root = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![
            Extent {
                offset: 0,
                length: 10,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 10,
                length: 10,
                kind: ExtentKind::AllocatedZero,
            },
            Extent {
                offset: 20,
                length: 10,
                kind: ExtentKind::Content {
                    object: content,
                    object_offset: 7,
                },
            },
        ]),
        16,
    )
    .await?;
    let cancellation = CancellationToken::new();
    let seek = |offset, target| {
        seek_extent(
            &store,
            ExtentSeekRequest {
                root,
                file_size: 30,
                offset,
                target,
                limits: DecodeLimits::default(),
                budget: WorkBudget::UNBOUNDED,
            },
            &cancellation,
        )
    };
    assert_eq!(seek(5, ExtentSeekTarget::Hole).await?.value, Some(5));
    assert_eq!(seek(5, ExtentSeekTarget::Data).await?.value, Some(10));
    assert_eq!(seek(12, ExtentSeekTarget::Data).await?.value, Some(12));
    assert_eq!(seek(12, ExtentSeekTarget::Hole).await?.value, Some(30));
    assert_eq!(seek(22, ExtentSeekTarget::Data).await?.value, Some(22));
    assert_eq!(seek(30, ExtentSeekTarget::Data).await?.value, None);
    assert_eq!(seek(30, ExtentSeekTarget::Hole).await?.value, Some(30));

    let invalid = seek(31, ExtentSeekTarget::Data)
        .await
        .err()
        .ok_or("seek beyond EOF succeeded")?;
    assert!(matches!(invalid.error, ExtentReadError::InvalidRange));
    assert_eq!(*invalid.work()?, WorkCounters::default());

    Ok(())
}

#[tokio::test]
async fn empty_and_all_hole_files_have_exact_seek_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 10,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    for (file_size, target, expected) in [
        (10, ExtentSeekTarget::Data, None),
        (10, ExtentSeekTarget::Hole, Some(0)),
        (0, ExtentSeekTarget::Data, None),
        (0, ExtentSeekTarget::Hole, Some(0)),
    ] {
        let receipt = seek_extent(
            &store,
            ExtentSeekRequest {
                root,
                file_size,
                offset: 0,
                target,
                limits: DecodeLimits::default(),
                budget: WorkBudget::UNBOUNDED,
            },
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(receipt.value, expected);
        if file_size == 0 {
            assert_eq!(receipt.work.page_reads, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn span_bound_stops_alternating_sparse_output() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![
            Extent {
                offset: 0,
                length: 1,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 1,
                length: 1,
                kind: ExtentKind::AllocatedZero,
            },
        ]),
        16,
    )
    .await?;
    let failure = plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 2,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
            maximum_spans: Some(1),
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("one-span bound unexpectedly admitted two spans")?;
    assert!(matches!(failure.error, ExtentReadError::TooManySpans));
    assert_eq!(failure.work()?.page_reads, 1);
    assert_eq!(failure.work()?.items_returned, 1);

    let before_work = plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 2,
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            maximum_spans: Some(0),
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero span limit unexpectedly succeeded")?;
    assert!(matches!(
        before_work.error,
        ExtentReadError::InvalidSpanLimit
    ));
    assert_eq!(*before_work.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn range_and_seek_admission_reject_every_invalid_request_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 4,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    let request = ExtentRangeRequest {
        root,
        file_size: 4,
        range: ByteRange {
            offset: 0,
            length: 1,
        },
        maximum_spans: Some(1),
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let invalid = [
        ExtentRangeRequest {
            root: ObjectId {
                storage: None,
                kind: ObjectKind::Blob,
                digest: root.digest,
            },
            ..request
        },
        ExtentRangeRequest {
            range: ByteRange {
                offset: u64::MAX,
                length: 1,
            },
            ..request
        },
        ExtentRangeRequest {
            range: ByteRange {
                offset: 4,
                length: 1,
            },
            ..request
        },
        ExtentRangeRequest {
            limits: DecodeLimits {
                maximum_page_height: 0,
                ..DecodeLimits::default()
            },
            ..request
        },
    ];
    let expected = [
        ExtentReadError::WrongRootKind,
        ExtentReadError::InvalidRange,
        ExtentReadError::InvalidRange,
        ExtentReadError::HeightExceeded,
    ];
    for (request, expected) in invalid.into_iter().zip(expected) {
        let failure = plan_extent_range(&store, request, &CancellationToken::new())
            .await
            .err()
            .ok_or("invalid range request succeeded")?;
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(*failure.work()?, WorkCounters::default());
    }

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = (seek_extent(
        &store,
        ExtentSeekRequest {
            root,
            file_size: 4,
            offset: 0,
            target: ExtentSeekTarget::Data,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &cancellation,
    ))
    .await
    .err()
    .ok_or("pre-cancelled sparse seek succeeded")?;
    assert!(matches!(failure.error, ExtentReadError::Cancelled));
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}
