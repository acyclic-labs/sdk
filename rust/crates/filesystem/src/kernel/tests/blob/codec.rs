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

#[tokio::test]
async fn decoded_blob_shape_and_sync_source_cancellation_fail_before_hidden_work()
-> Result<(), Box<dyn std::error::Error>> {
    let page = BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk: ObjectId {
                storage: None,
                kind: ObjectKind::BlobChunk,
                digest: Digest::from_bytes([77; 32]),
            },
        }]),
    };
    let encoded = encode_blob_page(
        &page,
        1,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let limits = DecodeLimits {
        maximum_page_items: 1,
        ..DecodeLimits::default()
    };
    let decoded = decode_blob_page(&encoded, limits, &mut |_| Ok::<_, CanonicalDecodeError>(()))?;
    assert_eq!(decoded, page);

    let zero_items = DecodeLimits {
        maximum_page_items: 0,
        ..limits
    };
    assert!(matches!(
        decode_blob_page(
            &encoded,
            zero_items,
            &mut |_| Ok::<_, CanonicalDecodeError>(())
        ),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));
    let mut unknown_tag = encoded;
    unknown_tag[26] = 99;
    assert!(matches!(
        decode_blob_page(
            &unknown_tag,
            limits,
            &mut |_| Ok::<_, CanonicalDecodeError>(())
        ),
        Err(CanonicalDecodeError::UnknownTag {
            field: "blob_page",
            tag: 99,
        })
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut source = std::io::Cursor::new(b"x");
    let mut destination = [0_u8; 1];
    let failure = (AsyncBlobSource::read(
        &mut source,
        &mut destination,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled synchronous blob source read succeeded")?;
    assert_eq!(failure.kind(), std::io::ErrorKind::Interrupted);
    assert_eq!(source.position(), 0);
    Ok(())
}
