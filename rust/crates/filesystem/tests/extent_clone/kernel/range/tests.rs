use acyclic_fs::kernel::{
    DecodeLimits, Extent, ExtentChild, ExtentKind, ExtentPage, ExtentRangeRequest, ExtentReadError,
    ExtentSeekRequest, ExtentSeekTarget, ExtentSlice, plan_extent_range, seek_extent,
};
use acyclic_fs::{ByteRange, CancellationToken};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

use std::task::{Context, Poll, Waker};

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

#[tokio::test]
async fn range_reads_only_intersecting_leaf_pages() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let first = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    let second = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 100,
            length: 100,
            kind: ExtentKind::AllocatedZero,
        }]),
        16,
    )
    .await?;
    let root = put_extent_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first: 0,
                page: first,
            },
            ExtentChild {
                first: 100,
                page: second,
            },
        ]),
        16,
    )
    .await?;
    let request = ExtentRangeRequest {
        root,
        file_size: 200,
        range: ByteRange {
            offset: 150,
            length: 10,
        },
        maximum_spans: Some(1),
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let plan = plan_extent_range(&store, request, &CancellationToken::new()).await?;
    assert_eq!(plan.work.page_reads, 2);
    assert_eq!(
        plan.spans,
        vec![ExtentSlice {
            offset: 150,
            length: 10,
            source_end: 200,
            kind: ExtentKind::AllocatedZero,
        }]
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(plan_extent_range(&store, request, &cancelled));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous extent planning remained pending".into());
    };
    let failure = result
        .err()
        .ok_or("pre-cancelled asynchronous extent planning unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentReadError::Cancelled));
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn sparse_seek_traverses_each_forward_frontier_once() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let first = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    let second = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 100,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    let content = ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([9; 32]),
    };
    let third = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 200,
            length: 100,
            kind: ExtentKind::Content {
                object: content,
                object_offset: 0,
            },
        }]),
        16,
    )
    .await?;
    let root = put_extent_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first: 0,
                page: first,
            },
            ExtentChild {
                first: 100,
                page: second,
            },
            ExtentChild {
                first: 200,
                page: third,
            },
        ]),
        16,
    )
    .await?;
    let data = seek_extent(
        &store,
        ExtentSeekRequest {
            root,
            file_size: 300,
            offset: 10,
            target: ExtentSeekTarget::Data,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(data.value, Some(200));
    assert_eq!(data.work.page_reads, 4);
    assert_eq!(data.work.backend_read_operations, 4);
    let hole = seek_extent(
        &store,
        ExtentSeekRequest {
            root,
            file_size: 300,
            offset: 250,
            target: ExtentSeekTarget::Hole,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(hole.value, Some(300));
    assert_eq!(hole.work.page_reads, 2);
    let eof = seek_extent(
        &store,
        ExtentSeekRequest {
            root,
            file_size: 300,
            offset: 300,
            target: ExtentSeekTarget::Hole,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(eof.value, Some(300));
    assert_eq!(eof.work.page_reads, 0);
    Ok(())
}

#[tokio::test]
async fn nested_extent_pages_reject_forged_parent_bounds() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let leaf = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 1,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    let nested = put_extent_page(
        &store,
        &ExtentPage::Internal(vec![ExtentChild {
            first: 0,
            page: leaf,
        }]),
        16,
    )
    .await?;
    let root = put_extent_page(
        &store,
        &ExtentPage::Internal(vec![ExtentChild {
            first: 0,
            page: nested,
        }]),
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
            maximum_spans: Some(2),
            limits: DecodeLimits {
                maximum_page_height: 3,
                ..DecodeLimits::default()
            },
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("forged nested child bounds succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentReadError::ChildBoundsMismatch
    ));
    // Children carry no end of their own: the leaf under the forged
    // parent is what disagrees.
    assert_eq!(failure.work()?.page_reads, 3);
    Ok(())
}

#[tokio::test]
async fn authenticated_range_traversal_rejects_height_alias_and_child_bound_forgery()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let leaf = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 1,
            kind: ExtentKind::Hole,
        }]),
        16,
    )
    .await?;
    let one_child = put_extent_page(
        &store,
        &ExtentPage::Internal(vec![ExtentChild {
            first: 0,
            page: leaf,
        }]),
        16,
    )
    .await?;
    let request = |root, limits| ExtentRangeRequest {
        root,
        file_size: 2,
        range: ByteRange {
            offset: 0,
            length: 2,
        },
        maximum_spans: Some(2),
        limits,
        budget: WorkBudget::UNBOUNDED,
    };
    let bounds = plan_extent_range(
        &store,
        request(one_child, DecodeLimits::default()),
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("forged child bounds succeeded")?;
    assert!(matches!(bounds.error, ExtentReadError::ChildBoundsMismatch));
    assert_eq!(bounds.work()?.page_reads, 2);

    let height = plan_extent_range(
        &store,
        request(
            one_child,
            DecodeLimits {
                maximum_page_height: 1,
                ..DecodeLimits::default()
            },
        ),
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("height-limited internal traversal succeeded")?;
    assert!(matches!(height.error, ExtentReadError::HeightExceeded));
    assert_eq!(height.work()?.page_reads, 1);

    let alias_root = put_extent_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first: 0,
                page: leaf,
            },
            ExtentChild {
                first: 1,
                page: leaf,
            },
        ]),
        16,
    )
    .await?;
    let alias = plan_extent_range(
        &store,
        request(alias_root, DecodeLimits::default()),
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("aliased extent page succeeded")?;
    assert!(matches!(alias.error, ExtentReadError::Cycle));
    assert_eq!(alias.work()?.page_reads, 2);

    Ok(())
}
