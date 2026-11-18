use super::*;
use crate::foundation::Digest;

#[test]
fn malformed_extent_tags_and_item_bounds_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    const FIRST_EXTENT_KIND_OFFSET: usize = 15 + 8 + 8;

    let page = ExtentPage::Leaf(vec![Extent {
        offset: 0,
        length: 1,
        kind: ExtentKind::Hole,
    }]);
    let encoded = encode_extent_page(
        &page,
        1,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let tight = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    {
        let result = decode_extent_page(&encoded, tight, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })
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
    unknown_page[DOMAIN.len() + 2] = 9;
    {
        let result = decode_extent_page(&unknown_page, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
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
    unknown_kind[FIRST_EXTENT_KIND_OFFSET] = 9;
    {
        let result = decode_extent_page(&unknown_kind, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
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

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn original_extent_decode_records_only_the_completed_item_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let page = ExtentPage::Leaf(vec![
        Extent {
            offset: 0,
            length: 1,
            kind: ExtentKind::Hole,
        },
        Extent {
            offset: 1,
            length: 1,
            kind: ExtentKind::AllocatedZero,
        },
    ]);
    let mut bytes = encode_extent_page(
        &page,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let _ = bytes.pop();
    let cancellation = crate::CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes().unwrap();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let error =
        decode_extent_page_with_accounting(&bytes, DecodeLimits::default(), &mut accounting)
            .err()
            .ok_or("truncated extent item was accepted")?;
    assert!(matches!(
        error,
        crate::FsError::FileRead(super::super::FileRangeReadError::Extent(
            super::super::ExtentReadError::Decode(_)
        ))
    ));
    assert_eq!(original.receipt().unwrap().items_examined, 1);
    assert_eq!(original.receipt().unwrap().bytes_copied, 40);
    assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
    Ok(())
}

/// Unknown versions refuse before decoding the strict v1 page layout.
#[test]
fn unknown_extent_versions_refuse_before_page_allocation() {
    use crate::kernel::codec::Encoder;
    let child = |byte| ObjectId {
        storage: None,
        kind: ObjectKind::ExtentPage,
        digest: Digest::from_bytes([byte; 32]),
    };
    let mut internal = Encoder::new(b"ACYFSEXT", 2);
    internal.u8(2);
    internal.u32(2);
    for (first, end, byte) in [(0_u64, 8_u64, 1_u8), (8, 16, 2)] {
        internal.u64(first);
        internal.u64(end);
        internal.object(child(byte));
    }
    let mut leaf = Encoder::new(b"ACYFSEXT", 2);
    leaf.u8(1);
    leaf.u32(1);
    leaf.u64(0);
    leaf.u64(8);
    leaf.u8(1);
    for stored in [internal.finish(), leaf.finish()] {
        let result = decode_extent_page(&stored, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        });
        assert_eq!(
            result,
            Err(crate::kernel::CanonicalDecodeError::UnsupportedVersion {
                found: 2,
                expected: 1,
            })
        );
        let mut current = stored.clone();
        current[8..10].copy_from_slice(&1_u16.to_le_bytes());
        let reread = decode_extent_page(&current, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        });
        assert_ne!(reread.as_ref().err(), result.as_ref().err());
    }
}
