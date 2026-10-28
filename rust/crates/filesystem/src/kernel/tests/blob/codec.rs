#[test]
fn blob_leaf_encoding_has_a_locked_golden_vector() -> Result<(), Box<dyn std::error::Error>> {
    let page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: Digest::from_bytes([7; 32]),
            },
        }]),
    };
    let expected = hex::decode(concat!(
        "4143594653424c420100",
        "0000000000000000",
        "0400000000000000",
        "01",
        "01000000",
        "0000000000000000",
        "0400000000000000",
        "07070707070707070707070707070707",
        "07070707070707070707070707070707"
    ))?;
    assert_eq!(
        encode_blob_page(
            &page,
            1,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        )?,
        expected
    );
    assert_eq!(
        decode_blob_page(&expected, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))?,
        page
    );
    let page_id = blob_page_id(&page, 1)?;
    assert_eq!(page_id.kind, ObjectKind::Blob);
    assert_eq!(page_id.digest, object_digest(&expected));

    let internal = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Internal(vec![BlobChild {
            first_offset: 0,
            end_offset: 4,
            page: page_id,
        }]),
    };
    let internal_bytes = encode_blob_page(
        &internal,
        1,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_blob_page(&internal_bytes, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(
            ()
        ))?,
        internal
    );
    assert_eq!(
        blob_page_id(&internal, 1)?.digest,
        object_digest(&internal_bytes)
    );
    Ok(())
}

#[test]
fn blob_page_codec_rejects_every_noncanonical_shape() -> Result<(), Box<dyn std::error::Error>> {
    let chunk = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([7; 32]),
    };
    let valid = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk,
        }]),
    };
    for invalid in [
        BlobPage {
            first_offset: 1,
            end_offset: 1,
            node: BlobNode::Leaf(Vec::new()),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![BlobChunkRef {
                first_offset: 0,
                end_offset: 0,
                chunk,
            }]),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![
                BlobChunkRef {
                    first_offset: 0,
                    end_offset: 2,
                    chunk,
                },
                BlobChunkRef {
                    first_offset: 3,
                    end_offset: 4,
                    chunk,
                },
            ]),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: chunk.digest,
                },
            }]),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Internal(vec![BlobChild {
                first_offset: 0,
                end_offset: 4,
                page: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: chunk.digest,
                },
            }]),
        },
    ] {
        assert!(
            encode_blob_page(
                &invalid,
                8,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed
            )
            .is_err()
        );
        assert!(blob_page_id(&invalid, 8).is_err());
    }
    assert!(matches!(
        encode_blob_page(
            &valid,
            0,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        ),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));

    let encoded = encode_blob_page(
        &valid,
        1,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let mut unknown_tag = encoded.clone();
    unknown_tag[26] = 99;
    assert!(matches!(
        decode_blob_page(&unknown_tag, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::UnknownTag {
            field: "blob_page",
            tag: 99,
        })
    ));
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(
        decode_blob_page(&trailing, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))
        .is_err()
    );
    assert!(
        decode_blob_page(
            &encoded[..encoded.len() - 1],
            DecodeLimits::default(),
            &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())
        )
        .is_err()
    );
    let limited = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        decode_blob_page(&encoded, limited, &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn blob_page_publication_rejects_the_exact_encoded_byte_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunk = ObjectId {
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
        maximum_blob_bytes: 1,
    };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut batch = BlobBatchStore::new(&store, &mut accounting)?;
    let before = accounting.spent();
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
    assert_eq!(accounting.spent(), before);
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
