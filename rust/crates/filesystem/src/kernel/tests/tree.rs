use super::*;

#[test]
fn malformed_tree_page_tags_and_bounds_fail_in_shape_and_full_decoders()
-> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_tree_page(
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"a".to_vec())?,
            file_id: FileId::from_bytes([1; 16]),
            kind: FileKind::Regular,
        }]),
        1,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let tight = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    for result in [
        tree_page_decode_shape(&encoded, tight).map(|_| ()),
        decode_tree_page(&encoded, tight, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })
        .map(|_| ()),
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
        decode_tree_page(&unknown, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })
        .map(|_| ()),
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
