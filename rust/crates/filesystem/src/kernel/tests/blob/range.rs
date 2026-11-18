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
    let root = put_blob_page(&store.inner, &page, 8).await?;
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
        let store =
            crate::test_support::PausedStore::new(std::sync::Arc::clone(&inner), Some(receiver));
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
        assert_eq!(accounting.spent().unwrap().backend_write_operations, 1);
        assert_eq!(accounting.spent().unwrap().object_bytes_written, 7);
        assert_eq!(
            accounting
                .spent()
                .unwrap()
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
    let root = put_blob_page(&store.inner, &page, 8).await?;
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
    assert!(failure.work.as_deref().unwrap().backend_read_operations > 0);
    Ok(())
}
