use super::*;

#[test]
fn encoding_rejects_a_component_before_allocating_beyond_its_bound() {
    assert!(host_name_bytes(OsStr::new("ab"), FilesystemProfile::Portable, 1).is_err());
    assert!(host_name_bytes(OsStr::new("ab"), FilesystemProfile::Windows, 3).is_err());
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
    let mut zero = DemandWork::new(WorkBudget::default(), &cancellation);
    assert!(matches!(
        host_name_bytes_measured(
            OsStr::new("native"),
            FilesystemProfile::Portable,
            255,
            &mut zero,
        ),
        Err(DemandError::Work(_))
    ));
    assert_eq!(zero.work, WorkCounters::default());
    let mut measured = DemandWork::new(finite_name_budget(), &cancellation);
    let (_, bytes) = host_name_bytes_measured(
        OsStr::new("native"),
        FilesystemProfile::Portable,
        255,
        &mut measured,
    )?;
    assert_eq!(bytes, b"native");
    let exact_budget = measured.work;
    drop(bytes);
    let mut exact = DemandWork::new(exact_budget, &cancellation);
    let (_, bytes) = host_name_bytes_measured(
        OsStr::new("native"),
        FilesystemProfile::Portable,
        255,
        &mut exact,
    )?;
    assert_eq!(bytes, b"native");
    assert_eq!(exact.work, exact_budget);
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
        255,
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
            255,
            &mut cancelled,
        ),
        Err(DemandError::Cancelled)
    ));
    assert_eq!(cancelled.work, WorkCounters::default());
    let mut short = finite_name_budget();
    short.items_examined = 1;
    let mut measured = DemandWork::new(short, &CancellationToken::new());
    assert!(matches!(
        host_name_bytes_measured(
            OsStr::new("ab"),
            FilesystemProfile::Windows,
            255,
            &mut measured,
        ),
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
        255,
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
            255,
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
        host_name_bytes_with_native_work(
            OsStr::new("ab"),
            FilesystemProfile::Windows,
            255,
            &original,
        ),
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
        host_name_bytes_with_native_work(
            OsStr::new("c"),
            FilesystemProfile::Portable,
            255,
            &original,
        ),
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
        255,
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
            255,
            &mut original.clone(),
        ),
        Err(DemandError::InvalidRequest)
    ));
    Ok(())
}
