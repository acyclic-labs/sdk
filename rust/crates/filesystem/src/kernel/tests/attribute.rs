use super::*;
use crate::foundation::Digest;

#[test]
fn classes_and_raw_names_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let page = AttributePage::Leaf(vec![AttributeEntry {
        name: AttributeName::new(AttributeClass::PosixXattr, b"user.test".to_vec(), 255)?,
        value_bytes: 4,
        value: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([9; 32]),
        },
    }]);
    let bytes = encode_attribute_page(&page, 8)?;
    assert_eq!(
        decode_attribute_page(&bytes, DecodeLimits::default())?,
        page
    );
    assert_attribute_encoder_limits(&page)?;
    Ok(())
}

#[test]
fn every_attribute_class_and_name_boundary_is_canonical() -> Result<(), Box<dyn std::error::Error>>
{
    for (class, tag) in [
        (AttributeClass::PosixXattr, 1),
        (AttributeClass::WindowsStream, 2),
        (AttributeClass::MacResourceFork, 3),
    ] {
        assert_eq!(class.tag(), tag);
        assert_eq!(AttributeClass::from_tag(tag)?, class);
    }
    assert_eq!(
        AttributeClass::from_tag(0),
        Err(AttributeError::UnknownClass(0))
    );
    assert_eq!(
        AttributeName::new(AttributeClass::PosixXattr, Vec::new(), 8),
        Err(AttributeError::InvalidName)
    );
    assert_eq!(
        AttributeName::new(AttributeClass::PosixXattr, b"a\0b".to_vec(), 8),
        Err(AttributeError::InvalidName)
    );
    assert_eq!(
        AttributeName::new(AttributeClass::PosixXattr, b"abc".to_vec(), 2),
        Err(AttributeError::InvalidName)
    );
    Ok(())
}

#[test]
fn every_attribute_page_shape_and_identity_is_canonical() -> Result<(), Box<dyn std::error::Error>>
{
    let first = AttributeName::new(AttributeClass::PosixXattr, b"a".to_vec(), 8)?;
    let second = AttributeName::new(AttributeClass::WindowsStream, b"b".to_vec(), 8)?;
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([1; 32]),
    };
    let attribute_page = ObjectId {
        kind: ObjectKind::AttributePage,
        digest: Digest::from_bytes([2; 32]),
    };
    let leaf = AttributePage::Leaf(vec![
        AttributeEntry {
            name: first.clone(),
            value_bytes: 3,
            value: blob,
        },
        AttributeEntry {
            name: second.clone(),
            value_bytes: 0,
            value: ObjectId {
                kind: ObjectKind::Blob,
                digest: Digest::from_bytes([3; 32]),
            },
        },
    ]);
    let internal = AttributePage::Internal(vec![
        AttributeChild {
            first_name: first.clone(),
            page: attribute_page,
        },
        AttributeChild {
            first_name: second.clone(),
            page: ObjectId {
                kind: ObjectKind::AttributePage,
                digest: Digest::from_bytes([4; 32]),
            },
        },
    ]);
    let limits = DecodeLimits {
        maximum_page_items: 2,
        maximum_name_bytes: 8,
        ..DecodeLimits::default()
    };
    for (page, kind) in [
        (leaf.clone(), DecodedPageKind::Leaf),
        (internal.clone(), DecodedPageKind::Internal),
    ] {
        let encoded = encode_attribute_page(&page, 2)?;
        let shape = attribute_page_decode_shape(&encoded, limits)?;
        assert_eq!(shape.kind, kind);
        assert_eq!(shape.items, 2);
        assert_eq!(shape.nested_bytes, 2);
        assert_eq!(decode_attribute_page(&encoded, limits)?, page);
        assert_attribute_encoder_limits(&page)?;
        let object_id = attribute_page_id(&page, 2)?;
        assert_eq!(object_id.kind, ObjectKind::AttributePage);
        assert_eq!(
            object_id.digest,
            object_digest(ObjectKind::AttributePage, &encoded)
        );
    }
    assert_eq!(
        attribute_leaf_page_encoded_length(
            match &leaf {
                AttributePage::Leaf(v) => v,
                AttributePage::Internal(_) => unreachable!(),
            },
            2
        )?,
        encode_attribute_page(&leaf, 2)?.len()
    );
    assert_eq!(
        attribute_internal_page_encoded_length(
            match &internal {
                AttributePage::Internal(v) => v.iter().map(|child| (&child.first_name, child.page)),
                AttributePage::Leaf(_) => unreachable!(),
            },
            2
        )?,
        encode_attribute_page(&internal, 2)?.len()
    );
    Ok(())
}

#[test]
fn malformed_attribute_pages_fail_at_every_public_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let name = AttributeName::new(AttributeClass::PosixXattr, b"a".to_vec(), 8)?;
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([1; 32]),
    };
    let wrong = ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::from_bytes([2; 32]),
    };
    let entry = AttributeEntry {
        name: name.clone(),
        value_bytes: 1,
        value: blob,
    };
    let duplicate = AttributePage::Leaf(vec![entry.clone(), entry.clone()]);
    let wrong_leaf = AttributePage::Leaf(vec![AttributeEntry {
        value: wrong,
        ..entry.clone()
    }]);
    let empty_internal = AttributePage::Internal(Vec::new());
    let wrong_internal = AttributePage::Internal(vec![AttributeChild {
        first_name: name.clone(),
        page: wrong,
    }]);
    for page in [&duplicate, &wrong_leaf, &empty_internal, &wrong_internal] {
        assert!(encode_attribute_page(page, 8).is_err());
        assert_eq!(page.validate(8), Err(AttributeError::InvalidPage));
    }
    assert_eq!(
        AttributePage::Leaf(vec![entry.clone()]).validate(0),
        Err(AttributeError::TooManyItems)
    );
    assert_eq!(
        AttributePage::Internal(vec![AttributeChild {
            first_name: name,
            page: ObjectId {
                kind: ObjectKind::AttributePage,
                digest: Digest::from_bytes([3; 32]),
            },
        }])
        .validate(0),
        Err(AttributeError::TooManyItems)
    );
    assert!(encode_attribute_page(&AttributePage::Leaf(vec![entry.clone()]), 0).is_err());
    assert!(attribute_page_id(&duplicate, 8).is_err());

    let valid = encode_attribute_page(&AttributePage::Leaf(vec![entry]), 1)?;
    let mut unknown_page = valid.clone();
    unknown_page[DOMAIN.len() + 2] = 9;
    assert!(matches!(
        attribute_page_decode_shape(&unknown_page, DecodeLimits::default()),
        Err(CanonicalDecodeError::UnknownTag {
            field: "attribute_page",
            tag: 9
        })
    ));
    assert!(matches!(
        decode_attribute_page(&unknown_page, DecodeLimits::default()),
        Err(CanonicalDecodeError::UnknownTag {
            field: "attribute_page",
            tag: 9
        })
    ));

    let mut unknown_class = valid.clone();
    unknown_class[DOMAIN.len() + 2 + 1 + 4] = 9;
    assert!(matches!(
        decode_attribute_page(&unknown_class, DecodeLimits::default()),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let tight_items = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        attribute_page_decode_shape(&valid, tight_items),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0
        })
    ));
    assert!(matches!(
        decode_attribute_page(&valid, tight_items),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0
        })
    ));
    Ok(())
}

fn assert_attribute_encoder_limits(page: &AttributePage) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let receipt = encode_attribute_page_measured(page, 8, WorkBudget::UNBOUNDED, &cancellation)?;
    assert_eq!(receipt.value, encode_attribute_page(page, 8)?);
    assert_eq!(receipt.work.bytes_encoded, usize_to_u64(receipt.value.len()));
    assert_eq!(receipt.work.allocation_operations, 1);
    assert_eq!(receipt.work.peak_allocation_bytes, usize_to_u64(receipt.value.capacity()));
    let (count, copied) = match page {
        AttributePage::Leaf(entries) => (entries.len(), entries.iter().map(|entry| usize_to_u64(entry.name.as_bytes().len()) + 32).sum::<u64>()),
        AttributePage::Internal(children) => (children.len(), children.iter().map(|child| usize_to_u64(child.first_name.as_bytes().len()) + 32).sum::<u64>()),
    };
    assert_eq!(receipt.work.items_examined, usize_to_u64(count) * 2);
    assert_eq!(receipt.work.bytes_copied, copied);
    assert_eq!(encode_attribute_page_measured(page, 8, receipt.work, &cancellation)?.value, receipt.value);
    for budget in [
        WorkBudget { bytes_encoded: receipt.work.bytes_encoded - 1, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { peak_allocation_bytes: usize_to_u64(receipt.value.len()) - 1, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = encode_attribute_page_measured(page, 8, budget, &cancellation).expect_err("permit must reject before allocation");
        assert!(matches!(failure.error, AttributeEncodeError::Work(_)));
        assert_eq!(failure.work.items_examined, usize_to_u64(count));
        assert_eq!(failure.work.bytes_encoded, 0);
        assert_eq!(failure.work.allocation_operations, 0);
    }
    if copied != 0 {
        let failure = encode_attribute_page_measured(
            page, 8, WorkBudget { bytes_copied: copied - 1, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).expect_err("copy permit must reject before allocation");
        assert!(matches!(failure.error, AttributeEncodeError::Work(_)));
        assert_eq!(failure.work.items_examined, usize_to_u64(count));
        assert_eq!(failure.work.allocation_operations, 0);
    }
    if count != 0 {
        let failure = encode_attribute_page_measured(
            page, 8, WorkBudget { items_examined: usize_to_u64(count), ..WorkBudget::UNBOUNDED }, &cancellation,
        ).expect_err("encoding pass needs its item permit");
        assert_eq!(failure.work.items_examined, usize_to_u64(count));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
    }
    cancellation.cancel();
    let failure = encode_attribute_page_measured(page, 8, WorkBudget::UNBOUNDED, &cancellation).expect_err("pre-cancelled page must reject");
    assert!(matches!(failure.error, AttributeEncodeError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn measured_attribute_empty_and_all_classes_use_the_canonical_encoder()
-> Result<(), Box<dyn std::error::Error>> {
    assert_attribute_encoder_limits(&AttributePage::Leaf(Vec::new()))?;
    for class in [AttributeClass::PosixXattr, AttributeClass::WindowsStream, AttributeClass::MacResourceFork] {
        assert_attribute_encoder_limits(&AttributePage::Leaf(vec![AttributeEntry {
            name: AttributeName::new(class, b"stream".to_vec(), 8)?,
            value_bytes: 0,
            value: ObjectId { kind: ObjectKind::Blob, digest: Digest::from_bytes([7; 32]) },
        }]))?;
    }
    Ok(())
}
