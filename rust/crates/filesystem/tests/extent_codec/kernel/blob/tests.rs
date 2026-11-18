use acyclic_fs::kernel::{
    BlobChild, BlobChunkRef, BlobNode, BlobPage, CanonicalDecodeError, DecodeLimits, blob_page_id,
    decode_blob_page, encode_blob_page,
};
use acyclic_fs::object_digest;
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};

#[test]
fn blob_leaf_encoding_has_a_locked_golden_vector() -> Result<(), Box<dyn std::error::Error>> {
    let page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: ObjectId {
                storage: None,
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
        "02",
        "07070707070707070707070707070707",
        "07070707070707070707070707070707"
    ))?;
    assert_eq!(
        encode_blob_page(
            &page,
            1,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )?,
        expected
    );
    assert_eq!(
        decode_blob_page(&expected, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
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
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_blob_page(&internal_bytes, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
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
        storage: None,
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
                    storage: None,
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
                    storage: None,
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
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed
            )
            .is_err()
        );
        assert!(blob_page_id(&invalid, 8).is_err());
    }
    assert!(matches!(
        encode_blob_page(
            &valid,
            0,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        ),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));

    let encoded = encode_blob_page(
        &valid,
        1,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    let mut unknown_tag = encoded.clone();
    *unknown_tag.get_mut(26).ok_or("missing blob node tag")? = 99;
    assert!(matches!(
        decode_blob_page(&unknown_tag, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
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
            CanonicalDecodeError,
        >(()))
        .is_err()
    );
    assert!(
        decode_blob_page(
            encoded.split_last().ok_or("missing blob page bytes")?.1,
            DecodeLimits::default(),
            &mut |_| Ok::<_, CanonicalDecodeError>(())
        )
        .is_err()
    );
    let limited = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        decode_blob_page(
            &encoded,
            limited,
            &mut |_| Ok::<_, CanonicalDecodeError>(())
        ),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));
    Ok(())
}
