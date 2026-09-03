use super::*;
use crate::foundation::Digest;

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
                    kind: ObjectKind::Blob,
                    digest: Digest::from_bytes([9; 32]),
                },
                object_offset: 2,
            },
        },
    ]);
    let encoded = encode_extent_page(&page, 16)?;
    assert_eq!(decode_extent_page(&encoded, DecodeLimits::default())?, page);
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
    assert!(encode_extent_page(&overlap, 16).is_err());
    let overflow = ExtentPage::Leaf(vec![Extent {
        offset: u64::MAX,
        length: 1,
        kind: ExtentKind::Hole,
    }]);
    assert!(encode_extent_page(&overflow, 16).is_err());
}

#[test]
fn leaf_internal_shape_length_and_identity_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let leaf = ExtentPage::Leaf(vec![Extent {
        offset: 0,
        length: 4,
        kind: ExtentKind::Content {
            object: ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::from_bytes([1; 32]),
            },
            object_offset: 7,
        },
    }]);
    let leaf_id = extent_page_id(&leaf, 1)?;
    let leaf_bytes = encode_extent_page(&leaf, 1)?;
    assert_eq!(leaf_bytes.len(), extent_page_encoded_length(&leaf, 1)?);
    assert_eq!(leaf_id.kind, ObjectKind::ExtentPage);
    assert_eq!(
        leaf_id.digest,
        object_digest(ObjectKind::ExtentPage, &leaf_bytes)
    );
    assert_eq!(
        extent_page_decode_shape(&leaf_bytes, DecodeLimits::default())?,
        DecodedPageShape {
            kind: DecodedPageKind::Leaf,
            items: 1,
            nested_bytes: 0,
        }
    );

    let internal = ExtentPage::Internal(vec![ExtentChild {
        first_offset: 0,
        end_offset: 4,
        page: leaf_id,
    }]);
    let internal_bytes = encode_extent_page(&internal, 1)?;
    assert_eq!(
        internal_bytes.len(),
        extent_page_encoded_length(&internal, 1)?
    );
    assert_eq!(
        extent_page_decode_shape(&internal_bytes, DecodeLimits::default())?,
        DecodedPageShape {
            kind: DecodedPageKind::Internal,
            items: 1,
            nested_bytes: 0,
        }
    );
    assert_eq!(
        decode_extent_page(&internal_bytes, DecodeLimits::default())?,
        internal
    );
    Ok(())
}

#[test]
fn malformed_extent_tags_and_item_bounds_fail_in_both_decoders()
-> Result<(), Box<dyn std::error::Error>> {
    const FIRST_EXTENT_KIND_OFFSET: usize = 15 + 8 + 8;

    let page = ExtentPage::Leaf(vec![Extent {
        offset: 0,
        length: 1,
        kind: ExtentKind::Hole,
    }]);
    let encoded = encode_extent_page(&page, 1)?;
    let tight = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    for result in [
        extent_page_decode_shape(&encoded, tight).map(|_| ()),
        decode_extent_page(&encoded, tight).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::FieldTooLarge {
                observed: 1,
                maximum: 0,
            })
        ));
    }

    let mut unknown_page = encoded.clone();
    unknown_page[DOMAIN.len() + 2] = 9;
    for result in [
        extent_page_decode_shape(&unknown_page, DecodeLimits::default()).map(|_| ()),
        decode_extent_page(&unknown_page, DecodeLimits::default()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::UnknownTag {
                field: "extent_page",
                tag: 9,
            })
        ));
    }

    let mut unknown_kind = encoded;
    unknown_kind[FIRST_EXTENT_KIND_OFFSET] = 9;
    for result in [
        extent_page_decode_shape(&unknown_kind, DecodeLimits::default()).map(|_| ()),
        decode_extent_page(&unknown_kind, DecodeLimits::default()).map(|_| ()),
    ] {
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

#[test]
fn every_invalid_extent_page_shape_rejects_before_identity() {
    let typed = |kind| ObjectId {
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
        ExtentPage::Internal(vec![ExtentChild {
            first_offset: 1,
            end_offset: 1,
            page: typed(ObjectKind::ExtentPage),
        }]),
        ExtentPage::Internal(vec![ExtentChild {
            first_offset: 0,
            end_offset: 1,
            page: typed(ObjectKind::Blob),
        }]),
    ];
    for page in invalid {
        assert!(encode_extent_page(&page, 8).is_err());
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
        )
        .is_err()
    );
}
