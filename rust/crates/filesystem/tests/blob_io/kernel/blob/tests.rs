use acyclic_fs::kernel::{
    AsyncBlobSource, BlobBuildError, BlobBuildOptions, BlobChild, BlobChunkRef, BlobNode, BlobPage,
    BlobReadError, BlobReadFailure, CanonicalDecodeError, DecodeLimits, OwnedSourceChunk,
    build_blob, decode_blob_page, read_blob_range,
};
use acyclic_fs::speculation::{ResidencyHint, ResidencyReason};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, ByteRange, CancellationToken, LogicalObjectStore,
    MemoryObjectStore, ObjectReadRequest, ObjectStoreError,
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

#[path = "../../../support/blob_source.rs"]
mod blob_source;
use blob_source::ScriptedSource;

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

#[tokio::test]
async fn asynchronous_blob_ranges_batch_intersecting_leaf_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut chunks = Vec::new();
    for bytes in [b"abc".as_slice(), b"def".as_slice(), b"ghi".as_slice()] {
        chunks.push(
            put_object(
                &store,
                ObjectKind::BlobChunk,
                OwnedBytes::from(bytes.to_vec()),
            )
            .await?,
        );
    }
    let [first, second, third] = chunks.as_slice() else {
        return Err("expected exactly three chunks".into());
    };
    let page = BlobPage {
        first_offset: 0,
        end_offset: 9,
        node: BlobNode::Leaf(vec![
            BlobChunkRef {
                first_offset: 0,
                end_offset: 3,
                chunk: *first,
            },
            BlobChunkRef {
                first_offset: 3,
                end_offset: 6,
                chunk: *second,
            },
            BlobChunkRef {
                first_offset: 6,
                end_offset: 9,
                chunk: *third,
            },
        ]),
    };
    let root = put_blob_page(&store, &page, 8).await?;
    let read = (read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 1,
            length: 7,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(read.bytes.as_ref(), b"bcdefgh");
    assert_eq!(read.work.backend_read_operations, 2);
    assert_eq!(read.work.object_probes, 4);
    Ok(())
}

#[tokio::test]
async fn streaming_index_memory_is_logarithmic_in_chunk_count()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = acyclic_objects::MemoryObjects::memory();
    let store = LogicalObjectStore::new(std::sync::Arc::new(objects));
    let content = vec![3_u8; 4_096];
    let mut source = std::io::Cursor::new(&content);
    let built = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 1_024,
            page_bytes: 129,
            maximum_blob_bytes: Some(4_096),
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let linear_reference_bytes = u64::try_from(4_096 * size_of::<BlobChunkRef>())?;
    assert!(built.work.peak_allocation_bytes < linear_reference_bytes / 8);
    let read = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 2_047,
            length: 3,
        },
        DecodeLimits {
            maximum_page_items: 1_024,
            maximum_page_bytes: 129,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(read.bytes, OwnedBytes::from_static(&[3, 3, 3]));
    Ok(())
}

#[tokio::test]
async fn measured_async_source_work_replaces_inferred_bytes_and_obeys_budget()
-> Result<(), Box<dyn std::error::Error>> {
    struct MeasuredSource {
        returned: bool,
        work: Option<WorkCounters>,
    }

    impl AsyncBlobSource for MeasuredSource {
        async fn read<'a>(
            &'a mut self,
            _destination: &'a mut [u8],
            _budget: WorkBudget,
            _cancellation: &'a CancellationToken,
        ) -> std::io::Result<usize> {
            Err(std::io::Error::other("owned source should not be copied"))
        }

        async fn read_owned(
            &mut self,
            _maximum: usize,
            _budget: WorkBudget,
            _cancellation: &CancellationToken,
        ) -> std::io::Result<Option<OwnedSourceChunk>> {
            if self.returned {
                self.work = Some(WorkCounters::default());
                return Ok(Some(OwnedSourceChunk::claimed(
                    acyclic_stream::OwnedBytes::new(),
                    0,
                )));
            }
            self.returned = true;
            self.work = Some(WorkCounters {
                source_bytes_read: 3,
                source_path_components: 7,
                ..WorkCounters::default()
            });
            Ok(Some(OwnedSourceChunk::claimed(
                acyclic_stream::OwnedBytes::from_static(b"abc"),
                0,
            )))
        }

        fn take_work(&mut self) -> Option<WorkCounters> {
            self.work.take()
        }
    }

    let options = BlobBuildOptions {
        chunk_bytes: 3,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(3),
    };
    let mut source = MeasuredSource {
        returned: false,
        work: None,
    };
    let built = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(built.work.source_bytes_read, 3);
    assert_eq!(built.work.source_path_components, 7);

    let mut source = MeasuredSource {
        returned: false,
        work: None,
    };
    let mut budget = WorkBudget::UNBOUNDED;
    budget.source_path_components = 6;
    let failed = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("underbudgeted measured source unexpectedly built")?;
    assert!(matches!(failed.error, BlobBuildError::Work(_)));
    assert_eq!(failed.work()?.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn owned_chunk_retains_actual_backing_capacity_not_visible_length()
-> Result<(), Box<dyn std::error::Error>> {
    struct RetainedSource(Option<OwnedSourceChunk>);
    impl AsyncBlobSource for RetainedSource {
        async fn read<'a>(
            &'a mut self,
            _destination: &'a mut [u8],
            _budget: WorkBudget,
            _cancellation: &'a CancellationToken,
        ) -> std::io::Result<usize> {
            Err(std::io::Error::other("the owned source must not be copied"))
        }

        async fn read_owned(
            &mut self,
            _maximum: usize,
            _budget: WorkBudget,
            _cancellation: &CancellationToken,
        ) -> std::io::Result<Option<OwnedSourceChunk>> {
            Ok(Some(self.0.take().unwrap_or(OwnedSourceChunk::claimed(
                acyclic_stream::OwnedBytes::new(),
                0,
            ))))
        }
    }

    // The producer transfers an existing allocation, including spare capacity.
    // Constructing it outside the callback is intentional: this is an input
    // ownership/retention control, not a claim of newly allocated source work.
    let make_source = || {
        let mut bytes = Vec::with_capacity(65_536);
        bytes.extend_from_slice(b"abc");
        let capacity = usize_to_u64(bytes.capacity());
        (
            RetainedSource(Some(OwnedSourceChunk::claimed(bytes.into(), capacity))),
            capacity,
        )
    };
    let options = BlobBuildOptions {
        chunk_bytes: 3,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(3),
    };
    let (mut source, capacity) = make_source();
    let built = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(built.logical_bytes, 3);
    assert_eq!(built.work.source_bytes_read, 3);
    assert!(built.work.peak_allocation_bytes >= capacity);

    let (mut source, capacity) = make_source();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = capacity - 1;
    let failed = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("spare-capacity input escaped its hard bound")?;
    assert!(matches!(failed.error, BlobBuildError::Work(_)));
    assert_eq!(failed.work()?.source_bytes_read, 3);
    assert_eq!(failed.work()?.backend_write_operations, 0);
    assert!(failed.work()?.peak_allocation_bytes >= capacity);
    Ok(())
}

#[tokio::test]
async fn partial_async_source_failure_and_midstream_cancellation_preserve_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    let options = BlobBuildOptions {
        chunk_bytes: 4,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(6),
    };
    let store = MemoryObjectStore::default();
    let mut partial = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: None,
    };
    let built = (build_blob(
        &store,
        &mut partial,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(built.logical_bytes, 6);
    assert_eq!(built.work.source_bytes_read, 6);
    assert_eq!(
        read_blob_range(
            &store,
            built.root,
            ByteRange {
                offset: 0,
                length: 6,
            },
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .bytes,
        OwnedBytes::from_static(b"abcdef")
    );

    let mut failing = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: Some(1),
        cancel_at: None,
    };
    let failure = (build_blob(
        &MemoryObjectStore::default(),
        &mut failing,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("failing source unexpectedly built")?;
    assert!(matches!(failure.error, BlobBuildError::Source(_)));
    assert_eq!(failure.work()?.source_bytes_read, 1);
    assert_eq!(failure.work()?.backend_write_operations, 0);

    let cancellation = CancellationToken::new();
    let mut cancelling = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: Some(1),
    };
    let cancelled = (build_blob(
        &MemoryObjectStore::default(),
        &mut cancelling,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("midstream-cancelled source unexpectedly built")?;
    assert!(matches!(cancelled.error, BlobBuildError::Cancelled));
    assert_eq!(cancelled.work()?.source_bytes_read, 1);
    assert_eq!(cancelled.work()?.backend_write_operations, 0);

    let cancellation = CancellationToken::new();
    let mut queued = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: Some(5),
    };
    let cancelled = (build_blob(
        &MemoryObjectStore::default(),
        &mut queued,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("queued midstream cancellation unexpectedly built")?;
    assert!(matches!(cancelled.error, BlobBuildError::Cancelled));
    assert_eq!(cancelled.work()?.source_bytes_read, 5);
    assert_eq!(cancelled.work()?.backend_write_operations, 0);

    let mut tight_budget = WorkBudget::UNBOUNDED;
    tight_budget.peak_allocation_bytes = cancelled.work()?.peak_allocation_bytes.saturating_sub(1);
    let mut bounded = std::io::Cursor::new(OwnedBytes::from_static(b"abcdef"));
    let rejected = (build_blob(
        &MemoryObjectStore::default(),
        &mut bounded,
        options,
        tight_budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("one-byte-short retained-buffer budget unexpectedly built")?;
    assert!(matches!(rejected.error, BlobBuildError::Work(_)));
    assert_eq!(rejected.work()?.source_bytes_read, 4);
    assert_eq!(rejected.work()?.backend_write_operations, 0);
    Ok(())
}
