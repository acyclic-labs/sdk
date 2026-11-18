use acyclic_fs::kernel::{
    BlobBuildError, BlobBuildOptions, BlobChild, BlobChunkRef, BlobNode, BlobPage, BlobReadError,
    BlobReadFailure, CanonicalDecodeError, DecodeLimits, build_blob, decode_blob_page,
    read_blob_range,
};
use acyclic_fs::speculation::{ResidencyHint, ResidencyReason};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, ByteRange, CancellationToken, MemoryObjectStore,
    ObjectReadRequest, ObjectStoreError,
};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use acyclic_stream::OwnedBytes;
use std::task::{Context, Poll, Waker};

use super::super::blob_objects::put_blob_page;
use super::super::put_object;

#[tokio::test]
async fn empty_source_builds_one_authenticated_empty_root() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new(OwnedBytes::new());
    let built = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 4,
            page_items: 2,
            page_bytes: 129,
            maximum_blob_bytes: Some(1),
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(built.logical_bytes, 0);
    assert_eq!(built.work.source_bytes_read, 0);
    assert_eq!(built.work.backend_write_operations, 1);
    let encoded = AsyncObjectStore::read(
        &store,
        built.root,
        129,
        &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
    )
    .await?;
    assert_eq!(
        decode_blob_page(&encoded, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?,
        BlobPage {
            first_offset: 0,
            end_offset: 0,
            node: BlobNode::Leaf(Vec::new()),
        }
    );
    let read = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 0,
            length: 0,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert!(read.bytes.is_empty());
    Ok(())
}

#[tokio::test]
async fn oversized_source_consumes_only_one_detection_byte_past_bound() {
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new([7_u8; 100]);
    let result = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 64,
            page_items: 4,
            page_bytes: 227,
            maximum_blob_bytes: Some(3),
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        result,
        Err(OperationFailure {
            error: BlobBuildError::TooLarge,
            ..
        })
    ));
    assert_eq!(source.position(), 4);
}

#[tokio::test]
async fn pre_cancelled_async_blob_build_performs_zero_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new(b"abc");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = (build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 2,
            page_items: 2,
            page_bytes: 129,
            maximum_blob_bytes: Some(3),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled blob build unexpectedly succeeded")?;
    assert!(matches!(failure.error, BlobBuildError::Cancelled));
    assert_eq!(*failure.work()?, WorkCounters::default());
    assert_eq!(source.position(), 0);
    Ok(())
}

#[tokio::test]
async fn parent_child_bound_forgery_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunk = put_object(
        &store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"abcd"),
    )
    .await?;
    let leaf = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk,
        }]),
    };
    let leaf_id = put_blob_page(&store, &leaf, 8).await?;
    let root = BlobPage {
        first_offset: 1,
        end_offset: 4,
        node: BlobNode::Internal(vec![BlobChild {
            first_offset: 1,
            end_offset: 4,
            page: leaf_id,
        }]),
    };
    let root_id = put_blob_page(&store, &root, 8).await?;
    assert!(matches!(
        read_blob_range(
            &store,
            root_id,
            ByteRange {
                offset: 1,
                length: 1
            },
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(BlobReadFailure {
            error: BlobReadError::ChildBoundsMismatch,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn blob_read_exposes_nearest_authenticated_unvisited_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, second) = adjacent_chunks(&store, 8).await?;
    let read = read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 0,
            length: 4,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(read.bytes.as_ref(), b"abcd");
    assert_eq!(read.work.object_probes, 2);
    assert_eq!(
        read.next_residency,
        Some(ResidencyHint {
            request: ObjectReadRequest {
                object_id: second,
                maximum_bytes: 4,
            },
            reason: ResidencyReason::SequentialRange,
        })
    );
    Ok(())
}

async fn adjacent_chunks(
    store: &MemoryObjectStore,
    maximum_items: u32,
) -> Result<(ObjectId, ObjectId), Box<dyn std::error::Error>> {
    let first = put_object(
        store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"abcd"),
    )
    .await?;
    let second = put_object(
        store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"efgh"),
    )
    .await?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 8,
        node: BlobNode::Leaf(vec![
            BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk: first,
            },
            BlobChunkRef {
                first_offset: 4,
                end_offset: 8,
                chunk: second,
            },
        ]),
    };
    let root = put_blob_page(store, &page, maximum_items).await?;
    Ok((root, second))
}

#[tokio::test]
async fn tiny_range_reads_only_one_intersecting_chunk() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = adjacent_chunks(&store, 8).await?;
    let result = read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 5,
            length: 2,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(result.bytes.as_ref(), b"fg");
    assert_eq!(result.work.page_reads, 1);
    assert_eq!(result.work.object_probes, 2);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 5,
            length: 2,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(cancelled_result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous blob read remained pending".into());
    };
    let failure = cancelled_result
        .err()
        .ok_or("pre-cancelled asynchronous blob read unexpectedly succeeded")?;
    assert!(matches!(failure.error, BlobReadError::Cancelled));
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn blob_range_rejects_missing_short_invalid_and_wrongly_typed_inputs()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let missing_chunk = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([88; 32]),
    };
    let missing_page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: missing_chunk,
        }]),
    };
    let missing_root = put_blob_page(&store, &missing_page, 8).await?;
    let missing = read_blob_range(
        &store,
        missing_root,
        ByteRange {
            offset: 0,
            length: 4,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("missing blob chunk unexpectedly succeeded")?;
    assert!(matches!(
        missing.error,
        BlobReadError::Storage(ObjectStoreError::Missing)
    ));
    assert_eq!(missing.work()?.page_reads, 1);

    let short_chunk = put_object(
        &store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"ab"),
    )
    .await?;
    let short_page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: short_chunk,
        }]),
    };
    let short_root = put_blob_page(&store, &short_page, 8).await?;
    let short = read_blob_range(
        &store,
        short_root,
        ByteRange {
            offset: 0,
            length: 4,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("short blob chunk unexpectedly succeeded")?;
    assert!(matches!(short.error, BlobReadError::ChunkLengthMismatch));
    assert_eq!(short.work()?.object_probes, 2);

    for range in [
        ByteRange {
            offset: 4,
            length: 1,
        },
        ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        let failure = read_blob_range(
            &store,
            short_root,
            range,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("invalid blob range unexpectedly succeeded")?;
        assert!(matches!(failure.error, BlobReadError::InvalidRange));
    }
    let empty = read_blob_range(
        &store,
        short_root,
        ByteRange {
            offset: 0,
            length: 0,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert!(empty.bytes.is_empty());
    assert_eq!(empty.work.page_reads, 1);
    let wrong = read_blob_range(
        &store,
        short_chunk,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("wrong blob root kind unexpectedly succeeded")?;
    assert!(matches!(wrong.error, BlobReadError::WrongRootKind));
    assert_eq!(*wrong.work()?, WorkCounters::default());
    let invalid_limits = DecodeLimits {
        maximum_page_height: 0,
        ..DecodeLimits::default()
    };
    let invalid = read_blob_range(
        &store,
        short_root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        invalid_limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("invalid blob limits unexpectedly succeeded")?;
    assert!(matches!(invalid.error, BlobReadError::InvalidLimits));
    assert_eq!(*invalid.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn blob_read_allocations_are_admitted_before_decode_queue_and_output_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = adjacent_chunks(&store, 2).await?;
    let limits = DecodeLimits {
        maximum_page_items: 2,
        maximum_page_height: 1,
        ..DecodeLimits::default()
    };
    let range = ByteRange {
        offset: 1,
        length: 6,
    };
    let baseline = read_blob_range(
        &store,
        root,
        range,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = read_blob_range(
        &store,
        root,
        range,
        limits,
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("one-byte-short blob read allocation budget unexpectedly succeeded")?;
    assert!(
        matches!(
            failure.error,
            BlobReadError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed,
                maximum: observed_maximum,
            }) if observed > observed_maximum && observed_maximum <= maximum
        ),
        "{failure:?}"
    );
    assert!(failure.work()?.peak_allocation_bytes <= maximum);
    assert_eq!(failure.work()?.backend_read_operations, 1);
    assert_eq!(failure.work()?.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn streaming_builder_round_trips_a_multilevel_index() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new(b"abcdefghi");
    let built = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 2,
            page_items: 2,
            page_bytes: 129,
            maximum_blob_bytes: Some(9),
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(built.logical_bytes, 9);
    assert_eq!(built.work.source_bytes_read, 9);
    let read = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 1,
            length: 7,
        },
        DecodeLimits {
            maximum_page_items: 2,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(read.bytes.as_ref(), b"bcdefgh");
    assert!(read.work.page_reads >= 3);
    let narrow = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 4,
            length: 1,
        },
        DecodeLimits {
            maximum_page_items: 2,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(narrow.bytes.as_ref(), b"e");
    assert!(narrow.work.object_probes < read.work.object_probes);

    let shallow = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits {
            maximum_page_items: 2,
            maximum_page_height: 1,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("multilevel blob unexpectedly fit a one-page height bound")?;
    assert!(matches!(shallow.error, BlobReadError::CycleOrHeight));
    assert!(shallow.work()?.page_reads > 0);
    Ok(())
}

#[tokio::test]
async fn page_byte_bound_and_peak_budget_reject_before_unadmitted_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    for options in [
        BlobBuildOptions {
            chunk_bytes: 0,
            page_items: 2,
            page_bytes: 129,
            maximum_blob_bytes: Some(1),
        },
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 1,
            page_bytes: 129,
            maximum_blob_bytes: Some(1),
        },
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 2,
            page_bytes: 129,
            maximum_blob_bytes: Some(0),
        },
    ] {
        let mut source = std::io::Cursor::new(b"x");
        let failure = build_blob(
            &store,
            &mut source,
            options,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("invalid blob options unexpectedly succeeded")?;
        assert!(matches!(failure.error, BlobBuildError::InvalidOptions));
        assert_eq!(*failure.work()?, WorkCounters::default());
        assert_eq!(source.position(), 0);
    }
    let mut invalid_source = std::io::Cursor::new(b"abc");
    let invalid = build_blob(
        &store,
        &mut invalid_source,
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 2,
            page_bytes: 128,
            maximum_blob_bytes: Some(3),
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("undersized blob page unexpectedly succeeded")?;
    assert!(matches!(invalid.error, BlobBuildError::InvalidOptions));
    assert_eq!(invalid_source.position(), 0);
    assert_eq!(*invalid.work()?, WorkCounters::default());

    let options = BlobBuildOptions {
        chunk_bytes: 2,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(8),
    };
    let mut baseline_source = std::io::Cursor::new(b"abcdefgh");
    let baseline = build_blob(
        &store,
        &mut baseline_source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let mut limited_source = std::io::Cursor::new(b"abcdefgh");
    let failure = build_blob(
        &MemoryObjectStore::default(),
        &mut limited_source,
        options,
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("one-byte-short blob peak budget unexpectedly succeeded")?;
    assert!(
        matches!(
            failure.error,
            BlobBuildError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed,
                maximum: observed_maximum,
            }) if observed > observed_maximum && observed_maximum <= maximum
        ),
        "{failure:?}"
    );
    assert!(failure.work()?.peak_allocation_bytes <= maximum);
    Ok(())
}
