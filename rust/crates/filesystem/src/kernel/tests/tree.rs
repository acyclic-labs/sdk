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
    assert_eq!(
        encode_tree_page(&locale_ordered, 16).err(),
        Some(invariant(TreePageError::NamesNotStrictlyOrdered)),
    );
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

#[test]
fn measured_tree_encoding_preserves_each_name_encoding_and_internal_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let names = [
        LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
        LogicalName::new(NameEncoding::PosixBytes, vec![0xFF], 255)?,
        LogicalName::new(NameEncoding::WindowsUtf16Le, vec![b'z', 0], 255)?,
    ];
    for name in names {
        for page in [
            TreePage::Leaf(vec![TreeEntry { name: name.clone(), file_id: FileId::from_bytes([7; 16]), kind: FileKind::Regular }]),
            TreePage::Internal(vec![TreeChild { first_name: name.clone(), page: ObjectId { kind: ObjectKind::TreePage, digest: crate::foundation::Digest::from_bytes([8; 32]) } }]),
        ] {
            let baseline = encode_tree_page(&page, 16)?;
            let receipt = encode_tree_page_measured(&page, 16, WorkBudget::UNBOUNDED, &cancellation, 23)?;
            assert_eq!(receipt.value, baseline);
            assert_eq!(decode_tree_page(&receipt.value, DecodeLimits::default())?, page);
            assert_eq!(receipt.work.items_examined, 1);
            assert_eq!(receipt.work.bytes_encoded, receipt.value.len() as u64);
            let identity_bytes = if matches!(page, TreePage::Leaf(_)) { 16 } else { 32 };
            assert_eq!(receipt.work.bytes_copied, name.as_bytes().len() as u64 + identity_bytes);
            assert_eq!(receipt.work.allocation_operations, 1);
            assert_eq!(receipt.work.peak_allocation_bytes, 23 + receipt.value.capacity() as u64);
            let permit = WorkBudget {
                items_examined: 1,
                bytes_encoded: receipt.work.bytes_encoded,
                bytes_copied: receipt.work.bytes_copied,
                allocation_operations: 1,
                peak_allocation_bytes: receipt.work.peak_allocation_bytes,
                ..WorkBudget::UNBOUNDED
            };
            let finite = encode_tree_page_measured(&page, 16, permit, &cancellation, 23)?;
            assert_eq!(finite, receipt);
            assert_eq!(object_digest(ObjectKind::TreePage, &finite.value), tree_page_id(&page, 16)?.digest);
        }
    }
    Ok(())
}

#[test]
fn measured_tree_refusals_and_invalid_pages_preserve_completed_prefixes()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let page = TreePage::Leaf(vec![entry(b"a", 1)?, entry(b"b", 2)?]);
    let failure = encode_tree_page_measured(&page, 16, WorkBudget { items_examined: 1, ..WorkBudget::UNBOUNDED }, &cancellation, 0).expect_err("second validation item refused");
    assert!(matches!(failure.error, TreePageEncodeError::Work(_)));
    assert_eq!(failure.work.items_examined, 1);
    assert_eq!(failure.work.allocation_operations, 0);
    for permit in [
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { peak_allocation_bytes: 1, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = encode_tree_page_measured(&page, 16, permit, &cancellation, 0).expect_err("finite encoder must refuse");
        assert!(matches!(failure.error, TreePageEncodeError::Work(_)));
        assert_eq!(failure.work.items_examined, 2);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
    }
    let duplicate = TreePage::Leaf(vec![entry(b"a", 1)?, entry(b"a", 2)?]);
    let failure = encode_tree_page_measured(&duplicate, 16, WorkBudget::UNBOUNDED, &cancellation, 23).expect_err("duplicate name");
    assert!(matches!(failure.error, TreePageEncodeError::Canonical(CanonicalDecodeError::Invariant(_))));
    assert_eq!(failure.work.items_examined, 2);
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.peak_allocation_bytes, 23);
    for invalid in [TreePage::Internal(Vec::new()), TreePage::Leaf(vec![entry(b"a", 1)?])] {
        let failure = encode_tree_page_measured(&invalid, 0, WorkBudget::UNBOUNDED, &cancellation, 0).expect_err("invalid page shape or item bound");
        assert!(matches!(failure.error, TreePageEncodeError::Canonical(CanonicalDecodeError::Invariant(_))));
        assert_eq!(failure.work.items_examined, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
    }
    let wrong_child = TreePage::Internal(vec![TreeChild { first_name: name(b"a")?, page: ObjectId { kind: ObjectKind::Blob, digest: crate::foundation::Digest::from_bytes([9; 32]) } }]);
    let failure = encode_tree_page_measured(&wrong_child, 16, WorkBudget::UNBOUNDED, &cancellation, 0).expect_err("wrong child kind");
    assert!(matches!(failure.error, TreePageEncodeError::Canonical(CanonicalDecodeError::Invariant(_))));
    assert_eq!(failure.work.items_examined, 1);
    assert_eq!(failure.work.bytes_encoded, 0);
    cancellation.cancel();
    let failure = encode_tree_page_measured(&wrong_child, 16, WorkBudget::UNBOUNDED, &cancellation, 91).expect_err("cancelled before validation");
    assert!(matches!(failure.error, TreePageEncodeError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_tree_encoder_rejects_on_original_budget_before_capacity_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&work, &cancellation);
    let before = work.receipt();
    let live_before = work.live_allocation_bytes();
    let result = encode_tree_page_with_accounting(&TreePage::Leaf(vec![entry(b"a", 1)?]), 16, &mut accounting, &cancellation);
    assert!(matches!(result, Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)))));
    assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
    assert_eq!(work.receipt().bytes_encoded, before.bytes_encoded);
    assert_eq!(work.receipt().items_examined, before.items_examined + 1);
    assert_eq!(work.live_allocation_bytes(), live_before);
    cancellation.cancel();
    let before_cancelled = work.receipt();
    let result = encode_tree_page_with_accounting(&TreePage::Leaf(Vec::new()), 16, &mut accounting, &cancellation);
    assert!(matches!(result, Err(crate::FsError::Cancelled(_))));
    assert_eq!(work.receipt(), before_cancelled);
    Ok(())
}

#[test]
fn measured_tree_allocation_error_retains_the_actual_reservation_cause() {
    let source = Vec::<u8>::new().try_reserve_exact(usize::MAX)
        .expect_err("byte capacity exceeds Vec's addressable allocation bound");
    let error = TreePageEncodeError::AllocationFailed(source);
    assert!(std::error::Error::source(&error)
        .is_some_and(|cause| cause.downcast_ref::<std::collections::TryReserveError>().is_some()));
}
