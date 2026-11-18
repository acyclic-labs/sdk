use acyclic_fs::kernel::{
    BlobBuildError, BlobBuildOptions, BlobChild, BlobChunkRef, BlobNode, BlobPage, BlobReadError,
    BlobReadFailure, CanonicalDecodeError, DecodeLimits, build_blob, decode_blob_page,
    read_blob_range,
};
use acyclic_fs::speculation::{ResidencyHint, ResidencyReason};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, ByteRange, CancellationToken, MemoryObjectBackend,
    ObjectReadRequest,
};
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget, WorkCounters};
use acyclic_stream::OwnedBytes;
use std::task::{Context, Poll, Waker};

use super::super::blob_objects::put_blob_page;
use super::super::memory::memory_objects;
use super::super::put_object;

#[tokio::test]
async fn empty_source_builds_one_authenticated_empty_root() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
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
    let store = memory_objects();
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
    let store = memory_objects();
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
    let store = memory_objects();
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
    let store = memory_objects();
    let (root, second) = adjacent_chunks(&store).await?;
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
    store: &MemoryObjectBackend,
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
    let root = put_blob_page(store, &page, 8).await?;
    Ok((root, second))
}

#[tokio::test]
async fn tiny_range_reads_only_one_intersecting_chunk() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let (root, _) = adjacent_chunks(&store).await?;
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
