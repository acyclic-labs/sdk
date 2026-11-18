#[tokio::test]
async fn blob_page_publication_rejects_the_exact_encoded_byte_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunk = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([91; 32]),
    };
    let page = BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk,
        }]),
    };
    let options = BlobBuildOptions {
        chunk_bytes: 1,
        page_items: 2,
        page_bytes: 1,
        maximum_blob_bytes: Some(1),
    };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut batch = BlobBatchStore::new(&store, &mut accounting)?;
    let before = accounting.spent().unwrap();
    assert_eq!(before.allocation_operations, 1);
    let failure = (put_blob_page(&mut batch, &page, options, &mut accounting, &cancellation))
        .await
        .err()
        .ok_or("oversized blob page unexpectedly published")?;
    assert!(matches!(
        failure,
        FsError::BlobBuild(BlobBuildError::PageTooLarge {
            observed,
            maximum: 1,
        }) if observed > 1
    ));
    assert_eq!(accounting.spent().unwrap(), before);
    batch.release(&mut accounting)?;
    Ok(())
}
