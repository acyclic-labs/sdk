use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, Extent, ExtentChild, ExtentKind, ExtentPage,
    decode_extent_page, encode_extent_page, extent_page_id,
};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};

#[test]
fn holes_allocated_zero_and_content_remain_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let page = ExtentPage::Leaf(vec![
        Extent {
            offset: 0,
            length: 4,
            kind: ExtentKind::Hole,
        },
        Extent {
            offset: 4,
            length: 4,
            kind: ExtentKind::AllocatedZero,
        },
        Extent {
            offset: 8,
            length: 4,
            kind: ExtentKind::Content {
                object: ObjectId {
                    storage: None,
                    kind: ObjectKind::Blob,
                    digest: Digest::from_bytes([9; 32]),
                },
                object_offset: 2,
            },
        },
    ]);
    let encoded = encode_extent_page(
        &page,
        16,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_extent_page(&encoded, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?,
        page
    );
    Ok(())
}

#[test]
fn overlapping_and_overflowing_extents_fail_closed() {
    let overlap = ExtentPage::Leaf(vec![
        Extent {
            offset: 0,
            length: 2,
            kind: ExtentKind::Hole,
        },
        Extent {
            offset: 1,
            length: 1,
            kind: ExtentKind::AllocatedZero,
        },
    ]);
    assert!(
        encode_extent_page(
            &overlap,
            16,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
    let overflow = ExtentPage::Leaf(vec![Extent {
        offset: u64::MAX,
        length: 1,
        kind: ExtentKind::Hole,
    }]);
    assert!(
        encode_extent_page(
            &overflow,
            16,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
}

#[test]
fn leaf_internal_shape_length_and_identity_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let leaf = ExtentPage::Leaf(vec![Extent {
        offset: 0,
        length: 4,
        kind: ExtentKind::Content {
            object: ObjectId {
                storage: None,
                kind: ObjectKind::Blob,
                digest: Digest::from_bytes([1; 32]),
            },
            object_offset: 7,
        },
    }]);
    let leaf_id = extent_page_id(&leaf, 1)?;
    let leaf_bytes = encode_extent_page(
        &leaf,
        1,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(leaf_bytes.len(), extent_page_encoded_length(&leaf, 1)?);
    assert_eq!(leaf_id.kind, ObjectKind::ExtentPage);
    assert_eq!(leaf_id.digest, object_digest(&leaf_bytes));

    let internal = ExtentPage::Internal(vec![ExtentChild {
        first: 0,
        page: leaf_id,
    }]);
    let internal_bytes = encode_extent_page(
        &internal,
        1,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        internal_bytes.len(),
        extent_page_encoded_length(&internal, 1)?
    );

    assert_eq!(
        decode_extent_page(&internal_bytes, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
        >(
            ()
        ))?,
        internal
    );
    Ok(())
}

#[test]
fn every_invalid_extent_page_shape_rejects_before_identity() {
    let typed = |kind| ObjectId {
        storage: None,
        kind,
        digest: Digest::ZERO,
    };
    let invalid = [
        ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 1,
            kind: ExtentKind::Content {
                object: typed(ObjectKind::Metadata),
                object_offset: 0,
            },
        }]),
        ExtentPage::Internal(Vec::new()),
        ExtentPage::Internal(vec![
            ExtentChild {
                first: 1,
                page: typed(ObjectKind::ExtentPage),
            },
            ExtentChild {
                first: 1,
                page: typed(ObjectKind::ExtentPage),
            },
        ]),
        ExtentPage::Internal(vec![ExtentChild {
            first: 0,
            page: typed(ObjectKind::Blob),
        }]),
    ];
    for page in invalid {
        assert!(
            encode_extent_page(
                &page,
                8,
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed
            )
            .is_err()
        );
        assert!(extent_page_id(&page, 8).is_err());
    }
    assert!(
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: 1,
                kind: ExtentKind::Hole,
            }]),
            0,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
}
