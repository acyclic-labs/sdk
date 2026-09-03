use super::*;
use crate::kernel::{Extent, ExtentChild, encode_extent_page, extent_page_id};
use crate::memory::MemoryObjectStore;
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::ObjectReadRequest;
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

fn unlimited() -> WorkBudget {
    WorkBudget::UNBOUNDED
}

fn put_page(
    store: &MemoryObjectStore,
    page: &ExtentPage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = extent_page_id(page, 16)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_extent_page(page, 16)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

#[test]
fn range_reads_only_intersecting_leaf_pages() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let second = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 100,
            length: 100,
            kind: ExtentKind::AllocatedZero,
        }]),
    )?;
    let root = put_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first_offset: 0,
                end_offset: 100,
                page: first,
            },
            ExtentChild {
                first_offset: 100,
                end_offset: 200,
                page: second,
            },
        ]),
    )?;
    let request = ExtentRangeRequest {
        root,
        file_size: 200,
        range: ByteRange {
            offset: 150,
            length: 10,
        },
        maximum_spans: 1,
        limits: DecodeLimits::default(),
        budget: unlimited(),
    };
    let plan = plan_extent_range(&store, request)?;
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

    let cancellation = CancellationToken::new();
    let mut future = std::pin::pin!(plan_extent_range_async(&store, request, &cancellation,));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory-backed asynchronous extent planning remained pending".into());
    };
    assert_eq!(asynchronous?, plan);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(plan_extent_range_async(&store, request, &cancelled));
    let Poll::Ready(result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous extent planning remained pending".into());
    };
    let failure = result
        .err()
        .ok_or("pre-cancelled asynchronous extent planning unexpectedly succeeded")?;
    assert!(matches!(failure.error, ExtentReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn owned_extent_pages_preserve_sparse_results_and_exact_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let root = put_page(
        &store.inner,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::AllocatedZero,
        }]),
    )?;
    let request = ExtentRangeRequest {
        root,
        file_size: 8,
        range: ByteRange {
            offset: 2,
            length: 4,
        },
        maximum_spans: 1,
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let plan = crate::async_storage::poll_ready(plan_extent_range_async(
        &store,
        request,
        &CancellationToken::new(),
    ))
    .ok_or("owned extent planning blocked")??;
    assert_eq!(plan.spans.len(), 1);
    assert_eq!(plan.spans[0].offset, 2);
    assert_eq!(plan.spans[0].length, 4);
    assert!(plan.work.bytes_copied > 0);
    Ok(())
}

#[test]
fn owned_extent_page_releases_retention_when_decoded_cache_rejects_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let root = put_page(
        &store.inner,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::AllocatedZero,
        }]),
    )?;
    store
        .reject_decoded_admission
        .store(true, Ordering::Relaxed);
    let failure = crate::async_storage::poll_ready(plan_extent_range_async(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 8,
            range: ByteRange {
                offset: 0,
                length: 8,
            },
            maximum_spans: 1,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .ok_or("owned extent planning blocked")?
    .err()
    .ok_or("decoded-cache rejection unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentReadError::Storage(ObjectStoreError::Corrupt)
    ));
    assert!(failure.work.peak_allocation_bytes > 0);
    Ok(())
}

#[test]
fn repeated_extent_plans_reuse_decoded_pages_without_backend_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let leaf = put_page(
        &backend,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 200,
            kind: ExtentKind::AllocatedZero,
        }]),
    )?;
    let root = put_page(
        &backend,
        &ExtentPage::Internal(vec![ExtentChild {
            first_offset: 0,
            end_offset: 200,
            page: leaf,
        }]),
    )?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    let request = ExtentRangeRequest {
        root,
        file_size: 200,
        range: ByteRange {
            offset: 64,
            length: 32,
        },
        maximum_spans: 1,
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let cancellation = CancellationToken::new();
    let first =
        crate::async_storage::poll_ready(plan_extent_range_async(&store, request, &cancellation))
            .ok_or("cached extent planning blocked")??;
    assert_eq!(first.work.backend_read_operations, 2);
    assert_eq!(store.stats()?.resident_decoded_pages, 2);

    let second =
        crate::async_storage::poll_ready(plan_extent_range_async(&store, request, &cancellation))
            .ok_or("warm cached extent planning blocked")??;
    assert_eq!(second.spans, first.spans);
    assert_eq!(second.work.page_reads, 2);
    assert_eq!(second.work.backend_read_operations, 0);
    assert_eq!(second.work.object_bytes_read, 0);
    assert_eq!(store.stats()?.decoded_hits, 2);
    Ok(())
}

#[test]
fn range_plan_exposes_nearest_authenticated_unvisited_page()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let second = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 100,
            length: 100,
            kind: ExtentKind::AllocatedZero,
        }]),
    )?;
    let root = put_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first_offset: 0,
                end_offset: 100,
                page: first,
            },
            ExtentChild {
                first_offset: 100,
                end_offset: 200,
                page: second,
            },
        ]),
    )?;
    let limits = DecodeLimits::default();
    let plan = plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 200,
            range: ByteRange {
                offset: 90,
                length: 10,
            },
            maximum_spans: 1,
            limits,
            budget: unlimited(),
        },
    )?;
    assert_eq!(plan.work.page_reads, 2);
    assert_eq!(
        plan.next_residency,
        Some(ResidencyHint {
            request: ObjectReadRequest {
                object_id: second,
                maximum_bytes: limits.maximum_page_object_bytes(),
            },
            reason: ResidencyReason::SequentialRange,
        })
    );
    Ok(())
}

#[test]
fn sparse_seek_traverses_each_forward_frontier_once() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let second = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 100,
            length: 100,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let content = ObjectId {
        kind: ObjectKind::Blob,
        digest: crate::foundation::Digest::from_bytes([9; 32]),
    };
    let third = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 200,
            length: 100,
            kind: ExtentKind::Content {
                object: content,
                object_offset: 0,
            },
        }]),
    )?;
    let root = put_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first_offset: 0,
                end_offset: 100,
                page: first,
            },
            ExtentChild {
                first_offset: 100,
                end_offset: 200,
                page: second,
            },
            ExtentChild {
                first_offset: 200,
                end_offset: 300,
                page: third,
            },
        ]),
    )?;
    let data = seek_extent(
        &store,
        ExtentSeekRequest {
            root,
            file_size: 300,
            offset: 10,
            target: ExtentSeekTarget::Data,
            limits: DecodeLimits::default(),
            budget: unlimited(),
        },
    )?;
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
            budget: unlimited(),
        },
    )?;
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
            budget: unlimited(),
        },
    )?;
    assert_eq!(eof.value, Some(300));
    assert_eq!(eof.work.page_reads, 0);
    Ok(())
}

#[test]
fn sparse_seek_distinguishes_holes_allocated_data_content_and_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = ObjectId {
        kind: ObjectKind::Blob,
        digest: crate::foundation::Digest::from_bytes([19; 32]),
    };
    let root = put_page(
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
    )?;
    let seek = |offset, target| {
        seek_extent(
            &store,
            ExtentSeekRequest {
                root,
                file_size: 30,
                offset,
                target,
                limits: DecodeLimits::default(),
                budget: unlimited(),
            },
        )
    };
    assert_eq!(seek(5, ExtentSeekTarget::Hole)?.value, Some(5));
    assert_eq!(seek(5, ExtentSeekTarget::Data)?.value, Some(10));
    assert_eq!(seek(12, ExtentSeekTarget::Data)?.value, Some(12));
    assert_eq!(seek(12, ExtentSeekTarget::Hole)?.value, Some(30));
    assert_eq!(seek(22, ExtentSeekTarget::Data)?.value, Some(22));
    assert_eq!(seek(30, ExtentSeekTarget::Data)?.value, None);
    assert_eq!(seek(30, ExtentSeekTarget::Hole)?.value, Some(30));

    let invalid = seek(31, ExtentSeekTarget::Data)
        .err()
        .ok_or("seek beyond EOF succeeded")?;
    assert!(matches!(invalid.error, ExtentReadError::InvalidRange));
    assert_eq!(*invalid.work, WorkCounters::default());

    let asynchronous = crate::async_storage::poll_ready(seek_extent_async(
        &store,
        ExtentSeekRequest {
            root,
            file_size: 30,
            offset: 5,
            target: ExtentSeekTarget::Data,
            limits: DecodeLimits::default(),
            budget: unlimited(),
        },
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous seek remained pending")??;
    assert_eq!(asynchronous, seek(5, ExtentSeekTarget::Data)?);
    Ok(())
}

#[test]
fn empty_and_all_hole_files_have_exact_seek_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 10,
            kind: ExtentKind::Hole,
        }]),
    )?;
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
                budget: unlimited(),
            },
        )?;
        assert_eq!(receipt.value, expected);
        if file_size == 0 {
            assert_eq!(receipt.work.page_reads, 0);
        }
    }
    Ok(())
}

#[test]
fn span_bound_stops_alternating_sparse_output() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(
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
    )?;
    let failure = plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 2,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
            maximum_spans: 1,
            limits: DecodeLimits::default(),
            budget: unlimited(),
        },
    )
    .err()
    .ok_or("one-span bound unexpectedly admitted two spans")?;
    assert!(matches!(failure.error, ExtentReadError::TooManySpans));
    assert_eq!(failure.work.page_reads, 1);
    assert_eq!(failure.work.items_returned, 1);

    let before_work = plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 2,
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            maximum_spans: 0,
            limits: DecodeLimits::default(),
            budget: unlimited(),
        },
    )
    .err()
    .ok_or("zero span limit unexpectedly succeeded")?;
    assert!(matches!(
        before_work.error,
        ExtentReadError::InvalidSpanLimit
    ));
    assert_eq!(*before_work.work, WorkCounters::default());
    Ok(())
}

#[test]
fn range_terminal_and_lower_layer_error_translation_is_total()
-> Result<(), Box<dyn std::error::Error>> {
    for error in [AllocationError::Overflow, AllocationError::ReleaseInvariant] {
        assert!(matches!(
            ExtentReadError::from(error),
            ExtentReadError::Work(WorkError::Overflow)
        ));
    }
    for error in [
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        assert!(matches!(
            ExtentReadError::from(error),
            ExtentReadError::AllocationFailed
        ));
    }

    let root = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: crate::foundation::Digest::from_bytes([91; 32]),
    };
    let machine = RangeMachine::new(
        root,
        PlanInput {
            file_size: 4,
            range: ByteRange {
                offset: 0,
                length: 4,
            },
            maximum_spans: 1,
        },
        TraversalMode::Plan,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let incomplete = {
        let mut machine = machine;
        machine
            .finish()
            .err()
            .ok_or("incomplete range machine produced a plan")?
    };
    assert!(matches!(
        incomplete.error,
        ExtentReadError::IncompleteCoverage
    ));

    let machine = RangeMachine::new(
        root,
        PlanInput {
            file_size: 4,
            range: ByteRange {
                offset: 0,
                length: 4,
            },
            maximum_spans: 1,
        },
        TraversalMode::Plan,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let cancelled = frontier::Machine::cancelled(&machine);
    assert!(matches!(cancelled.error, ExtentReadError::Cancelled));

    let prospective = WorkCounters {
        object_probes: u64::MAX,
        ..WorkCounters::default()
    };
    let backend = crate::storage::ObjectFailure::new(
        ObjectStoreError::Missing,
        WorkCounters {
            object_probes: 1,
            ..WorkCounters::default()
        },
    );
    let overflow = frontier::Machine::storage_failure(&machine, prospective, backend);
    assert!(matches!(
        overflow.error,
        ExtentReadError::Work(WorkError::Overflow)
    ));
    assert_eq!(*overflow.work, prospective);
    Ok(())
}

#[test]
fn decoded_pages_and_output_spans_are_admitted_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(
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
            Extent {
                offset: 2,
                length: 1,
                kind: ExtentKind::Hole,
            },
        ]),
    )?;
    let limits = DecodeLimits {
        maximum_page_height: 1,
        maximum_visited_pages: 2,
        ..DecodeLimits::default()
    };
    let request = ExtentRangeRequest {
        root,
        file_size: 3,
        range: ByteRange {
            offset: 0,
            length: 3,
        },
        maximum_spans: 3,
        limits,
        budget: WorkBudget::UNBOUNDED,
    };
    let baseline = plan_extent_range(&store, request)?;
    assert_eq!(
        baseline.retained_allocation_bytes,
        u64::try_from(3 * size_of::<ExtentSlice>())?
    );
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = plan_extent_range(
        &store,
        ExtentRangeRequest {
            budget: WorkBudget {
                peak_allocation_bytes: maximum,
                ..WorkBudget::UNBOUNDED
            },
            ..request
        },
    )
    .err()
    .ok_or("one-byte-short range allocation budget unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentReadError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum: observed_maximum,
        }) if observed == baseline.work.peak_allocation_bytes && observed_maximum == maximum
    ));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn range_and_seek_admission_reject_every_invalid_request_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 4,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let request = ExtentRangeRequest {
        root,
        file_size: 4,
        range: ByteRange {
            offset: 0,
            length: 1,
        },
        maximum_spans: 1,
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let invalid = [
        ExtentRangeRequest {
            root: ObjectId {
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
        let failure = plan_extent_range(&store, request)
            .err()
            .ok_or("invalid range request succeeded")?;
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(seek_extent_async(
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
    .ok_or("pre-cancelled sparse seek remained pending")?
    .err()
    .ok_or("pre-cancelled sparse seek succeeded")?;
    assert!(matches!(failure.error, ExtentReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn nested_extent_pages_reject_forged_parent_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 1,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let nested = put_page(
        &store,
        &ExtentPage::Internal(vec![ExtentChild {
            first_offset: 0,
            end_offset: 1,
            page: leaf,
        }]),
    )?;
    let root = put_page(
        &store,
        &ExtentPage::Internal(vec![ExtentChild {
            first_offset: 0,
            end_offset: 2,
            page: nested,
        }]),
    )?;
    let failure = plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 2,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
            maximum_spans: 2,
            limits: DecodeLimits {
                maximum_page_height: 3,
                ..DecodeLimits::default()
            },
            budget: WorkBudget::UNBOUNDED,
        },
    )
    .err()
    .ok_or("forged nested child bounds succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentReadError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}

#[test]
fn authenticated_range_traversal_rejects_height_alias_and_child_bound_forgery()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = put_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 1,
            kind: ExtentKind::Hole,
        }]),
    )?;
    let one_child = put_page(
        &store,
        &ExtentPage::Internal(vec![ExtentChild {
            first_offset: 0,
            end_offset: 2,
            page: leaf,
        }]),
    )?;
    let request = |root, limits| ExtentRangeRequest {
        root,
        file_size: 2,
        range: ByteRange {
            offset: 0,
            length: 2,
        },
        maximum_spans: 2,
        limits,
        budget: WorkBudget::UNBOUNDED,
    };
    let bounds = plan_extent_range(&store, request(one_child, DecodeLimits::default()))
        .err()
        .ok_or("forged child bounds succeeded")?;
    assert!(matches!(bounds.error, ExtentReadError::ChildBoundsMismatch));
    assert_eq!(bounds.work.page_reads, 2);

    let height = plan_extent_range(
        &store,
        request(
            one_child,
            DecodeLimits {
                maximum_page_height: 1,
                ..DecodeLimits::default()
            },
        ),
    )
    .err()
    .ok_or("height-limited internal traversal succeeded")?;
    assert!(matches!(height.error, ExtentReadError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);

    let alias_root = put_page(
        &store,
        &ExtentPage::Internal(vec![
            ExtentChild {
                first_offset: 0,
                end_offset: 1,
                page: leaf,
            },
            ExtentChild {
                first_offset: 1,
                end_offset: 2,
                page: leaf,
            },
        ]),
    )?;
    let alias = plan_extent_range(&store, request(alias_root, DecodeLimits::default()))
        .err()
        .ok_or("aliased extent page succeeded")?;
    assert!(matches!(alias.error, ExtentReadError::Cycle));
    assert_eq!(alias.work.page_reads, 2);

    let cancellation = CancellationToken::new();
    for (request, expected) in [
        (request(one_child, DecodeLimits::default()), "child-bounds"),
        (
            request(
                one_child,
                DecodeLimits {
                    maximum_page_height: 1,
                    ..DecodeLimits::default()
                },
            ),
            "height",
        ),
        (request(alias_root, DecodeLimits::default()), "cycle"),
    ] {
        let failure = crate::async_storage::poll_ready(plan_extent_range_async(
            &store,
            request,
            &cancellation,
        ))
        .ok_or("malformed async range unexpectedly suspended")?
        .err()
        .ok_or("malformed async range unexpectedly succeeded")?;
        assert!(match expected {
            "child-bounds" => matches!(failure.error, ExtentReadError::ChildBoundsMismatch),
            "height" => matches!(failure.error, ExtentReadError::HeightExceeded),
            "cycle" => matches!(failure.error, ExtentReadError::Cycle),
            _ => false,
        });
        assert!(failure.work.page_reads > 0);
    }
    Ok(())
}
