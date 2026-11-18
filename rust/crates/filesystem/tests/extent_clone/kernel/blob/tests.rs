use acyclic_fs::kernel::{
    BlobBuildError, BlobBuildOptions, BlobNode, BlobPage, CanonicalDecodeError, DecodeLimits,
    build_blob, decode_blob_page, read_blob_range,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, ByteRange, CancellationToken};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget, WorkCounters};
use acyclic_stream::OwnedBytes;

use super::super::memory::memory_objects;

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
