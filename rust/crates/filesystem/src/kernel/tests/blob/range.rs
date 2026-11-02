#[tokio::test]
async fn tiny_range_reads_only_one_intersecting_chunk() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(
        &store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"abcd"),
    )
    .await?;
    let second = put(
        &store,
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
    let bytes = encode_blob_page(
        &page,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let root = put(&store, ObjectKind::Blob, OwnedBytes::from(bytes)).await?;
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
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn owned_blob_pages_and_chunks_preserve_exact_range_results()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let chunk = put(
        &store.inner,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"owned-range"),
    )
    .await?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 11,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 11,
            chunk,
        }]),
    };
    let root = put(
        &store.inner,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &page,
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
    let read = (read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 1,
            length: 5,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(&read.bytes[..], b"wned-");
    assert_eq!(read.logical_owned_bytes(), 5);
    let shared = read.bytes.slice(1..2);
    assert_eq!(shared.as_ref(), b"n");
    assert_eq!(
        read.logical_owned_bytes(),
        5,
        "shared slices retain the controlled output buffer"
    );
    assert_eq!(read.work.backend_read_operations, 2);
    assert!(read.work.bytes_copied >= 5);
    Ok(())
}

#[tokio::test]
async fn asynchronous_blob_ranges_batch_intersecting_leaf_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut chunks = Vec::new();
    for bytes in [b"abc".as_slice(), b"def".as_slice(), b"ghi".as_slice()] {
        chunks.push(
            put(
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
    let root = put(
        &store,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &page,
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
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
async fn blob_batch_retains_completed_write_on_withdrawal_and_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    enum Finish {
        Withdraw,
        LoseProducer,
        Complete,
    }
    for finish in [Finish::Withdraw, Finish::LoseProducer, Finish::Complete] {
        let inner = std::sync::Arc::new(MemoryObjectStore::default());
        let (release, receiver) = tokio::sync::oneshot::channel();
        let store = crate::test_support::PausedStore {
            inner: std::sync::Arc::clone(&inner),
            release: std::sync::Mutex::new(Some(receiver)),
        };
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            backend_write_operations: 1,
            object_bytes_written: 7,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let mut accounting = KernelAccounting::standalone(budget, &cancellation);
        accounting.charge_items(1)?;
        let mut batch = BlobBatchStore::new(&store, &mut accounting)?;
        let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"payload"));
        let id = batch.put(object, 0, &mut accounting, &cancellation).await?;
        let mut write = Box::pin(batch.flush(&mut accounting, &cancellation));
        // The real store writes before the original completion event pauses.
        assert!(futures::poll!(write.as_mut()).is_pending());
        match finish {
            Finish::Withdraw => {
                cancellation.cancel();
                drop(write);
                drop(release);
            }
            Finish::LoseProducer => {
                drop(release);
                assert!(matches!(
                    write.await,
                    Err(FsError::Object(ObjectStoreError::Rejected(message)))
                        if message == "cache fill producer lost"
                ));
            }
            Finish::Complete => {
                assert!(release.send(()).is_ok());
                write.await?;
            }
        }
        // The replaced remaining-budget frame loses the completed write here.
        assert_eq!(accounting.spent().backend_write_operations, 1);
        assert_eq!(accounting.spent().object_bytes_written, 7);
        assert_eq!(
            accounting
                .spent()
                .remaining(budget)?
                .backend_write_operations,
            0
        );
        batch.release(&mut accounting)?;
        let inspection = CancellationToken::new();
        let read = inner
            .read(id, 7, &mut crate::AuthorityWork::new(budget, &inspection))
            .await?;
        assert_eq!(read.bytes.as_ref(), b"payload");
        drop(read);
        drop(store);
        let retained = accounting.standalone_parts().unwrap().0.live_bytes();
        assert!(retained > 0, "the original frame funds the stored index");
        drop(inner);
        assert!(accounting.standalone_parts().unwrap().0.live_bytes() < retained);
    }
    Ok(())
}

#[tokio::test]
async fn owned_blob_page_releases_retention_when_decoded_cache_rejects_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let chunk = put(
        &store.inner,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"x"),
    )
    .await?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk,
        }]),
    };
    let root = put(
        &store.inner,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &page,
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
    store
        .reject_decoded_admission
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let failure = (read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("decoded-cache rejection unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        BlobReadError::Storage(ObjectStoreError::Corrupt)
    ));
    assert!(failure.work.backend_read_operations > 0);
    Ok(())
}

#[tokio::test]
async fn blob_read_exposes_nearest_authenticated_unvisited_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(
        &store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"abcd"),
    )
    .await?;
    let second = put(
        &store,
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
    let root = put(
        &store,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &page,
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
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
    let missing_root = put(
        &store,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &missing_page,
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
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
    assert_eq!(missing.work.page_reads, 1);

    let short_chunk = put(
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
    let short_root = put(
        &store,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &short_page,
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
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
    assert_eq!(short.work.object_probes, 2);

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
    assert_eq!(*wrong.work, WorkCounters::default());
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
    assert_eq!(*invalid.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn blob_read_allocations_are_admitted_before_decode_queue_and_output_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(
        &store,
        ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"abcd"),
    )
    .await?;
    let second = put(
        &store,
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
    let root = put(
        &store,
        ObjectKind::Blob,
        OwnedBytes::from(encode_blob_page(
            &page,
            2,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?),
    )
    .await?;
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
    assert!(failure.work.peak_allocation_bytes <= maximum);
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn parent_child_bound_forgery_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunk = put(
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
    let leaf_bytes = encode_blob_page(
        &leaf,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let leaf_id = put(&store, ObjectKind::Blob, OwnedBytes::from(leaf_bytes)).await?;
    let root = BlobPage {
        first_offset: 1,
        end_offset: 4,
        node: BlobNode::Internal(vec![BlobChild {
            first_offset: 1,
            end_offset: 4,
            page: leaf_id,
        }]),
    };
    let root_bytes = encode_blob_page(
        &root,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let root_id = put(&store, ObjectKind::Blob, OwnedBytes::from(root_bytes)).await?;
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
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(source.position(), 0);
    Ok(())
}
