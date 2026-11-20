use super::*;

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
    let original = crate::NativeMaterializationWork::root(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
        &crate::services::ServicesInitialized::for_kernel_tests(),
    )
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
