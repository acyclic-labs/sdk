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

#[test]
fn malformed_extent_tags_and_item_bounds_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    const PAGE_KIND_OFFSET: usize = 8 + 2;
    const FIRST_EXTENT_KIND_OFFSET: usize = PAGE_KIND_OFFSET + 1 + 4 + 8 + 8;

    let page = ExtentPage::Leaf(vec![Extent {
        offset: 0,
        length: 1,
        kind: ExtentKind::Hole,
    }]);
    let encoded = encode_extent_page(
        &page,
        1,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    let tight = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    {
        let result =
            decode_extent_page(&encoded, tight, &mut |_| Ok::<_, CanonicalDecodeError>(()))
                .map(|_| ());
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::FieldTooLarge {
                observed: 1,
                maximum: 0,
            })
        ));
    }

    let mut unknown_page = encoded.clone();
    *unknown_page
        .get_mut(PAGE_KIND_OFFSET)
        .ok_or("missing page kind")? = 9;
    {
        let result = decode_extent_page(&unknown_page, DecodeLimits::default(), &mut |_| {
            Ok::<_, CanonicalDecodeError>(())
        })
        .map(|_| ());
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::UnknownTag {
                field: "extent_page",
                tag: 9,
            })
        ));
    }

    let mut unknown_kind = encoded;
    *unknown_kind
        .get_mut(FIRST_EXTENT_KIND_OFFSET)
        .ok_or("missing extent kind")? = 9;
    {
        let result = decode_extent_page(&unknown_kind, DecodeLimits::default(), &mut |_| {
            Ok::<_, CanonicalDecodeError>(())
        })
        .map(|_| ());
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::UnknownTag {
                field: "extent_kind",
                tag: 9,
            })
        ));
    }
    Ok(())
}

/// Unknown versions refuse before decoding the strict v1 page layout.
#[test]
fn unknown_extent_versions_refuse_before_page_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let child = |first, byte| ExtentChild {
        first,
        page: ObjectId {
            storage: None,
            kind: ObjectKind::ExtentPage,
            digest: Digest::from_bytes([byte; 32]),
        },
    };
    let pages = [
        ExtentPage::Internal(vec![child(0, 1), child(8, 2)]),
        ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }]),
    ];
    for page in pages {
        let mut stored = encode_extent_page(
            &page,
            2,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed,
        )?;
        assert_eq!(
            decode_extent_page(&stored, DecodeLimits::default(), &mut |_| Ok::<
                _,
                CanonicalDecodeError,
            >(()))?,
            page
        );
        stored
            .get_mut(8..10)
            .ok_or("missing extent page version")?
            .copy_from_slice(&2_u16.to_le_bytes());
        let mut allocations = 0;
        let result = decode_extent_page(&stored, DecodeLimits::default(), &mut |_| {
            allocations += 1;
            Ok::<_, CanonicalDecodeError>(())
        });
        assert_eq!(
            result,
            Err(CanonicalDecodeError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        );
        assert_eq!(allocations, 0);
    }
    Ok(())
}
