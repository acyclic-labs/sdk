use super::*;

fn name(value: &[u8]) -> Result<LogicalName, TreePageError> {
    LogicalName::new(NameEncoding::Utf8, value.to_vec(), 255)
}

#[test]
fn leaf_encoding_has_a_locked_golden_vector() -> Result<(), Box<dyn std::error::Error>> {
    let page = TreePage::Leaf(vec![TreeEntry {
        name: name(b"a")?,
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
    }]);
    let encoded = encode_tree_page(&page, 16)?;
    assert_eq!(
        hex::encode(&encoded),
        "4143594653545245010001010000000101000000610707070707070707070707070707070701"
    );
    assert_eq!(decode_tree_page(&encoded, DecodeLimits::default())?, page);
    Ok(())
}

#[test]
fn leaf_ordering_is_raw_byte_order_not_locale_aware() -> Result<(), Box<dyn std::error::Error>> {
    // Byte order, not locale-aware or case-insensitive collation: 'Z' (0x5A)
    // sorts before 'a' (0x61), and 'e' (0x65) sorts before 'é' (UTF-8 0xC3 0xA9)
    // purely because its leading byte is smaller, not because of any
    // accent-folding rule. This pins the tree page's on-disk ordering
    // invariant so a future locale-aware or case-insensitive comparator
    // regression is caught immediately.
    let byte_ordered = TreePage::Leaf(vec![
        entry(b"Z", 1)?,
        entry(b"a", 2)?,
        entry(b"e", 3)?,
        entry("é".as_bytes(), 4)?,
    ]);
    encode_tree_page(&byte_ordered, 16)?;

    // The order a real locale-aware, case-insensitive collation would
    // produce ("a" < "e" < "é" < "Z") is rejected: it is not byte-ascending.
    let locale_ordered = TreePage::Leaf(vec![
        entry(b"a", 2)?,
        entry(b"e", 3)?,
        entry("é".as_bytes(), 4)?,
        entry(b"Z", 1)?,
    ]);
    assert!(matches!(
        encode_tree_page(&locale_ordered, 16),
        Err(CanonicalDecodeError::Invariant(message)) if message.contains("not strictly ordered")
    ));
    Ok(())
}

fn entry(value: &[u8], seed: u8) -> Result<TreeEntry, TreePageError> {
    Ok(TreeEntry {
        name: name(value)?,
        file_id: FileId::from_bytes([seed; 16]),
        kind: FileKind::Regular,
    })
}

#[test]
fn malformed_and_noncanonical_pages_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let duplicate = TreePage::Leaf(vec![
        TreeEntry {
            name: name(b"a")?,
            file_id: FileId::from_bytes([1; 16]),
            kind: FileKind::Regular,
        },
        TreeEntry {
            name: name(b"a")?,
            file_id: FileId::from_bytes([2; 16]),
            kind: FileKind::Regular,
        },
    ]);
    assert!(encode_tree_page(&duplicate, 16).is_err());

    let mut empty = encode_tree_page(&TreePage::Leaf(Vec::new()), 16)?;
    empty.push(0);
    assert_eq!(
        decode_tree_page(&empty, DecodeLimits::default()),
        Err(CanonicalDecodeError::TrailingBytes)
    );
    Ok(())
}

#[test]
fn internal_children_require_tree_page_ids() -> Result<(), Box<dyn std::error::Error>> {
    let invalid = TreePage::Internal(vec![TreeChild {
        first_name: name(b"a")?,
        page: ObjectId {
            kind: ObjectKind::Blob,
            digest: crate::foundation::Digest::ZERO,
        },
    }]);
    assert!(encode_tree_page(&invalid, 16).is_err());
    Ok(())
}

#[test]
fn tree_encoder_rejects_every_item_order_and_bound_violation()
-> Result<(), Box<dyn std::error::Error>> {
    let entry = |value: &[u8], byte| {
        Ok::<_, TreePageError>(TreeEntry {
            name: name(value)?,
            file_id: FileId::from_bytes([byte; 16]),
            kind: FileKind::Regular,
        })
    };
    assert!(encode_tree_page(&TreePage::Leaf(vec![entry(b"a", 1)?]), 0).is_err());
    assert!(encode_tree_page(&TreePage::Internal(Vec::new()), 8).is_err());
    let child = |value: &[u8], byte| {
        Ok::<_, TreePageError>(TreeChild {
            first_name: name(value)?,
            page: ObjectId {
                kind: ObjectKind::TreePage,
                digest: crate::foundation::Digest::from_bytes([byte; 32]),
            },
        })
    };
    assert!(encode_tree_page(&TreePage::Internal(vec![child(b"a", 1)?]), 0).is_err());
    assert!(
        encode_tree_page(
            &TreePage::Internal(vec![child(b"b", 1)?, child(b"a", 2)?]),
            2,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn malformed_tree_page_tags_and_bounds_fail_in_shape_and_full_decoders()
-> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_tree_page(
        &TreePage::Leaf(vec![TreeEntry {
            name: name(b"a")?,
            file_id: FileId::from_bytes([1; 16]),
            kind: FileKind::Regular,
        }]),
        1,
    )?;
    let tight = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    for result in [
        tree_page_decode_shape(&encoded, tight).map(|_| ()),
        decode_tree_page(&encoded, tight).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::FieldTooLarge {
                observed: 1,
                maximum: 0,
            })
        ));
    }
    let mut unknown = encoded;
    unknown[DOMAIN.len() + 2] = 9;
    for result in [
        tree_page_decode_shape(&unknown, DecodeLimits::default()).map(|_| ()),
        decode_tree_page(&unknown, DecodeLimits::default()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(CanonicalDecodeError::UnknownTag {
                field: "tree_page",
                tag: 9,
            })
        ));
    }
    Ok(())
}
