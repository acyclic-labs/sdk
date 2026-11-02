use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn windows_projection_preserves_one_component_and_exact_native_units(
        text in "[a-zé中𝄞]{1,32}",
        separator in any::<bool>(),
        raw in prop::collection::vec(1_u16..=u16::MAX, 1..32),
    ) {
        for encoding in [NameEncoding::Utf8, NameEncoding::PosixBytes] {
            let text = if separator { format!("{text}\\sibling") } else { text.clone() };
            let name = crate::kernel::LogicalName::new(encoding, text.as_bytes().to_vec()).unwrap();
            let result = crate::native_name::windows_name_units(&name);
            if separator {
                // Encoding the text directly would silently introduce a sibling path.
                prop_assert!(result.is_none());
            } else {
                let units = result.unwrap();
                let expected = text.encode_utf16().collect::<Vec<_>>();
                prop_assert_eq!(units.clone().collect::<Vec<_>>(), expected.clone());
                prop_assert_eq!(units.collect::<Vec<_>>(), expected);
            }
        }
        // Native UTF-16 permits unpaired surrogates and must retain them exactly.
        let units = [u16::from(b'x'), 0xd800, u16::from(b'a')].into_iter().chain(raw.into_iter().map(|unit| {
            if unit == u16::from(b'/') || unit == u16::from(b'\\') { u16::from(b'x') } else { unit }
        })).collect::<Vec<_>>();
        let name = crate::kernel::LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            units.iter().copied().flat_map(u16::to_le_bytes).collect(),
        ).unwrap();
        prop_assert_eq!(crate::native_name::windows_name_units(&name).unwrap().collect::<Vec<_>>(), units);
        let opaque = crate::kernel::LogicalName::new(NameEncoding::PosixBytes, vec![0xff]).unwrap();
        prop_assert!(crate::native_name::windows_name_units(&opaque).is_none());
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn finite_name_budget() -> WorkBudget {
    WorkBudget {
        bytes_copied: 1_024,
        bytes_encoded: 1_024,
        items_examined: 1_024,
        allocation_operations: 16,
        peak_allocation_bytes: 1_024,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_name_zero_and_exact_budgets_preserve_real_allocations()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    for profile in [
        FilesystemProfile::Portable,
        FilesystemProfile::Browser,
        FilesystemProfile::Posix,
        FilesystemProfile::Windows,
    ] {
        for text in ["native", "é𝄞"] {
            let expected = if profile == FilesystemProfile::Windows {
                text.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                text.as_bytes().to_vec()
            };
            let mut zero = DemandWork::new(WorkBudget::default(), &cancellation);
            assert!(matches!(
                host_name_bytes_measured(OsStr::new(text), profile, &mut zero),
                Err(DemandError::Work(_))
            ));
            assert_eq!(zero.work, WorkCounters::default());
            let mut measured = DemandWork::new(finite_name_budget(), &cancellation);
            let (encoding, bytes) =
                host_name_bytes_measured(OsStr::new(text), profile, &mut measured)?;
            assert_eq!(encoding, NameEncoding::native_for_profile(profile));
            assert_eq!(bytes, expected);
            assert_eq!(measured.live_allocation_bytes(), bytes.capacity() as u64);
            let exact_budget = measured.work;
            drop(bytes);
            let mut exact = DemandWork::new(exact_budget, &cancellation);
            let (_, bytes) = host_name_bytes_measured(OsStr::new(text), profile, &mut exact)?;
            assert_eq!(bytes, expected);
            assert_eq!(exact.work, exact_budget);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_name_uses_original_allocation_custody() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 64 * 1024,
            ..finite_name_budget()
        },
        &cancellation,
    )
    .map_err(|(error, _work)| error)?;
    original.admit(WorkCounters {
        items_examined: 3,
        ..WorkCounters::UNCHARGED
    })?;
    let baseline = original.live_allocation_bytes();
    let prior = original.receipt();
    let ((encoding, bytes), custody) = host_name_bytes_with_native_work(
        OsStr::new("native"),
        FilesystemProfile::Portable,
        &original,
    )?;
    assert_eq!(encoding, NameEncoding::Utf8);
    assert_eq!(bytes, b"native");
    assert!(original.shares_ledger(custody.work()));
    assert_eq!(custody.bytes(), bytes.capacity() as u64);
    assert_eq!(original.receipt().items_examined, prior.items_examined);
    assert_eq!(
        original.receipt().allocation_operations,
        prior.allocation_operations + 1
    );
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + bytes.capacity() as u64,
    );
    drop(bytes);
    drop(custody);
    assert_eq!(original.live_allocation_bytes(), baseline,);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_name_cancel_and_short_unit_budget_keep_actual_prefix() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut cancelled = DemandWork::new(finite_name_budget(), &cancellation);
    assert!(matches!(
        host_name_bytes_measured(
            OsStr::new("native"),
            FilesystemProfile::Portable,
            &mut cancelled,
        ),
        Err(DemandError::Cancelled)
    ));
    assert_eq!(cancelled.work, WorkCounters::default());
    let mut short = finite_name_budget();
    short.items_examined = 1;
    let mut measured = DemandWork::new(short, &CancellationToken::new());
    assert!(matches!(
        host_name_bytes_measured(OsStr::new("ab"), FilesystemProfile::Windows, &mut measured,),
        Err(DemandError::Work(_))
    ));
    assert_eq!(measured.work.items_examined, 1);
    assert_eq!(measured.work.allocation_operations, 0);
    assert_eq!(measured.work.peak_allocation_bytes, 0);
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
fn original_name_preserves_non_utf8_bytes_and_capacity() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::ffi::OsStrExt as _;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 64 * 1024,
            ..finite_name_budget()
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let raw = [b'x', 0xff];
    let ((encoding, bytes), allocation) = host_name_bytes_with_native_work(
        OsStr::from_bytes(&raw),
        FilesystemProfile::Posix,
        &original,
    )?;
    assert_eq!(encoding, NameEncoding::PosixBytes);
    assert_eq!(bytes, raw);
    assert_eq!(allocation.bytes(), bytes.capacity() as u64);
    drop(bytes);
    drop(allocation);
    assert!(matches!(
        host_name_bytes_with_native_work(
            OsStr::from_bytes(&raw),
            FilesystemProfile::Portable,
            &original,
        ),
        Err(crate::NativeTreeMaterializationError::InvalidLayout)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_utf16_quota_keeps_completed_prefix_and_retires_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_encoded: 2,
            peak_allocation_bytes: 64 * 1024,
            ..finite_name_budget()
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let prior = original.receipt();
    assert!(matches!(
        host_name_bytes_with_native_work(OsStr::new("ab"), FilesystemProfile::Windows, &original,),
        Err(crate::NativeTreeMaterializationError::Work(_))
    ));
    let completed = original.receipt();
    assert_eq!(completed.bytes_encoded, prior.bytes_encoded + 2);
    assert_eq!(completed.items_examined, prior.items_examined + 3);
    assert_eq!(
        completed.allocation_operations,
        prior.allocation_operations + 1
    );
    assert_eq!(original.live_allocation_bytes(), baseline);
    cancellation.cancel();
    let before = original.receipt();
    assert!(matches!(
        host_name_bytes_with_native_work(OsStr::new("c"), FilesystemProfile::Portable, &original,),
        Err(crate::NativeTreeMaterializationError::Cancelled)
    ));
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[cfg(windows)]
#[test]
fn original_wide_name_preserves_unpaired_surrogates() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 64 * 1024,
            ..finite_name_budget()
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let ((encoding, bytes), allocation) = host_wide_name_bytes_with_source_work(
        &[0xd800, 0x0061],
        FilesystemProfile::Windows,
        &mut original.clone(),
    )?;
    assert_eq!(encoding, NameEncoding::WindowsUtf16Le);
    assert_eq!(bytes, [0, 0xd8, 0x61, 0]);
    let allocation = allocation.expect("native name allocation");
    assert_eq!(allocation.bytes(), bytes.capacity() as u64);
    drop(bytes);
    drop(allocation);
    assert!(matches!(
        host_wide_name_bytes_with_source_work(
            &[0xd800],
            FilesystemProfile::Portable,
            &mut original.clone(),
        ),
        Err(DemandError::InvalidRequest)
    ));
    Ok(())
}
