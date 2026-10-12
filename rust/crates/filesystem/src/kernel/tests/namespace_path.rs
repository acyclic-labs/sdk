use super::*;

#[cfg(not(target_arch = "wasm32"))]
mod original_namespace_controls {
    use super::*;
    use crate::kernel::allocation::KernelAccounting;

    fn is_budget_error(error: &crate::FsError) -> bool {
        matches!(error, crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { .. })
        ))
    }

    #[test]
    fn original_positive_allocation_budget_exhausts_before_path_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let controller_operations = {
            let probe = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            probe.receipt().allocation_operations
        };
        let budget = WorkBudget {
            allocation_operations: controller_operations.checked_add(2).ok_or("allocation quota overflow")?,
            ..WorkBudget::UNBOUNDED
        };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let (paid_path, paid_guard) = NamespacePath::parse_in_profile_with_accounting(
            "/paid", FilesystemProfile::Portable, VolumeLimits::default(),
            &mut accounting, &cancellation,
        )?;
        drop(paid_path);
        drop(paid_guard);
        assert_eq!(original.receipt().allocation_operations, budget.allocation_operations);
        assert_eq!(original.live_allocation_bytes(), baseline);
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let error = NamespacePath::parse_in_profile_with_accounting(
            "/a/b", FilesystemProfile::Portable, VolumeLimits::default(),
            &mut accounting, &cancellation,
        ).err().ok_or("exhausted original allocated a path")?;
        assert!(is_budget_error(&error));
        let after = original.receipt();
        assert_eq!(after.allocation_operations, before.allocation_operations);
        assert_eq!(after.peak_allocation_bytes, before.peak_allocation_bytes);
        assert_eq!(after.bytes_copied, before.bytes_copied);
        assert_eq!(after.bytes_encoded, before.bytes_encoded);
        assert_eq!(after.source_path_components - before.source_path_components, 2);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_cancellation_precedes_parse_and_clone_allocations()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = NamespacePath::from_portable(
            &PortablePath::parse("/valid", VolumeLimits::default())?, VolumeLimits::default(),
        )?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        cancellation.cancel();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        for result in [
            NamespacePath::parse_in_profile_with_accounting(
                "/valid", FilesystemProfile::Portable, VolumeLimits::default(),
                &mut accounting, &cancellation,
            ),
            source.clone_with_accounting(&mut accounting, &cancellation),
        ] {
            let error = result.err().ok_or("cancelled original produced a path")?;
            assert!(matches!(error, crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::Cancelled
            )));
            assert_eq!(original.receipt(), before);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

}

#[test]
fn portable_raw_posix_and_windows_names_remain_exact() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let portable = PortablePath::parse("/workspace/src", limits)?;
    let converted = NamespacePath::from_portable(&portable, limits)?;
    assert_eq!(converted.depth(), 2);
    assert_eq!(converted.encoded_bytes(), 14);
    let parent = converted
        .parent()
        .ok_or_else(|| std::io::Error::other("workspace parent is missing"))?;
    assert_eq!(parent.depth(), 1);
    assert_eq!(parent.encoded_bytes(), 10);
    let root = parent
        .parent()
        .ok_or_else(|| std::io::Error::other("namespace root is missing"))?;
    assert!(root.is_root());
    assert_eq!(root.encoded_bytes(), 1);
    assert!(root.parent().is_none());

    let posix = LogicalName::new(NameEncoding::PosixBytes, vec![0xff, b'a'], 255)?;
    let windows = LogicalName::new(
        NameEncoding::WindowsUtf16Le,
        "file".encode_utf16().flat_map(u16::to_le_bytes).collect(),
        255,
    )?;
    let native = NamespacePath::new(vec![posix.clone(), windows], limits)?;
    assert_eq!(
        native.split_last().and_then(|(parent, _)| parent.first()),
        Some(&posix)
    );
    assert!(native.is_within(&NamespacePath::new(vec![posix], limits)?));
    Ok(())
}

#[test]
fn string_paths_use_the_volume_name_encoding() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let portable = PortablePath::parse("/é/𝄞", limits)?;
    for (profile, encoding) in [
        (FilesystemProfile::Portable, NameEncoding::Utf8),
        (FilesystemProfile::Posix, NameEncoding::PosixBytes),
        (FilesystemProfile::Windows, NameEncoding::WindowsUtf16Le),
    ] {
        let path = NamespacePath::from_portable_in_profile(&portable, profile, limits)?;
        assert_eq!(
            path.components()
                .first()
                .and_then(LogicalName::unicode_text)
                .as_deref(),
            Some("é")
        );
        assert!(
            path.components()
                .iter()
                .all(|name| name.encoding() == encoding)
        );
        assert_eq!(
            path.components().first().map(LogicalName::as_bytes),
            Some(match profile {
                FilesystemProfile::Windows => &[0xe9, 0x00][..],
                _ => "é".as_bytes(),
            })
        );
        if profile == FilesystemProfile::Windows {
            assert_eq!(
                path.components().get(1).map(LogicalName::as_bytes),
                Some(&[0x34, 0xd8, 0x1e, 0xdd][..])
            );
        }
    }
    Ok(())
}

#[test]
fn bounds_are_checked_without_storage() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits {
        maximum_path_bytes: 4,
        maximum_component_bytes: 2,
        maximum_path_depth: 1,
        ..VolumeLimits::default()
    };
    let name = LogicalName::new(NameEncoding::Utf8, b"ab".to_vec(), 2)?;
    assert!(NamespacePath::new(vec![name.clone()], limits).is_ok());
    assert_eq!(
        NamespacePath::new(vec![name.clone(), name], limits),
        Err(NamespacePathError::TooDeep)
    );

    let wide_limits = VolumeLimits {
        maximum_path_bytes: 64,
        maximum_component_bytes: 1,
        maximum_path_depth: 4,
        ..VolumeLimits::default()
    };
    let wide = LogicalName::new(NameEncoding::Utf8, b"ab".to_vec(), 2)?;
    assert_eq!(
        NamespacePath::new(vec![wide], wide_limits),
        Err(NamespacePathError::ComponentTooLong)
    );

    let short_path_limits = VolumeLimits {
        maximum_path_bytes: 2,
        maximum_component_bytes: 2,
        maximum_path_depth: 4,
        ..VolumeLimits::default()
    };
    let first = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 2)?;
    let second = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 2)?;
    assert_eq!(
        NamespacePath::new(vec![first, second], short_path_limits),
        Err(NamespacePathError::PathTooLong)
    );
    Ok(())
}

#[test]
fn invalid_and_cancelled_paths_do_not_allocate() {
    let cancellation = CancellationToken::new();
    for path in ["relative", "/a//b", "/.", "/..", "/a/", "/nul\0"] {
        let failure = NamespacePath::parse_in_profile_measured(
            path, FilesystemProfile::Windows, VolumeLimits::default(),
            WorkBudget::UNBOUNDED, &cancellation, 0,
        ).expect_err("invalid spelling must fail");
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.peak_allocation_bytes, 0);
    }
    cancellation.cancel();
    let failure = NamespacePath::parse_in_profile_measured(
        "/valid", FilesystemProfile::Portable, VolumeLimits::default(),
        WorkBudget::UNBOUNDED, &cancellation, 99,
    ).expect_err("cancelled parser must fail");
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(failure.error, NamespacePathError::Cancelled);
}

#[test]
fn native_encoded_bounds_reject_before_owned_path_allocation() {
    let cancellation = CancellationToken::new();
    for (path, limits) in [
        ("/ab", VolumeLimits { maximum_component_bytes: 3, ..VolumeLimits::default() }),
        ("/ab/c", VolumeLimits { maximum_path_bytes: 7, ..VolumeLimits::default() }),
        ("/a/b", VolumeLimits { maximum_path_depth: 1, ..VolumeLimits::default() }),
    ] {
        let failure = NamespacePath::parse_in_profile_measured(
            path, FilesystemProfile::Windows, limits, WorkBudget::UNBOUNDED, &cancellation, 0,
        ).expect_err("native profile bounds must fail");
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
    }
}

#[test]
fn allocation_denial_and_late_copy_denial_preserve_actual_prefix() {
    let cancellation = CancellationToken::new();
    let limits = VolumeLimits::default();
    let denied = NamespacePath::parse_in_profile_measured(
        "/a/b", FilesystemProfile::Portable, limits,
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation, 0,
    ).expect_err("vector admission must fail");
    assert_eq!(denied.work.allocation_operations, 0);
    assert_eq!(denied.work.source_path_components, 2);
    let failure = NamespacePath::parse_in_profile_measured(
        "/a/bb", FilesystemProfile::Portable, limits,
        WorkBudget { bytes_copied: 1, ..WorkBudget::UNBOUNDED }, &cancellation, 0,
    ).expect_err("second name copy must fail");
    assert_eq!(failure.work.bytes_copied, 1);
    assert_eq!(failure.work.allocation_operations, 2);
    assert_eq!(failure.work.peak_allocation_bytes,
        2 * std::mem::size_of::<LogicalName>() as u64 + 1);
}

const CURSOR_DOMAIN: &[u8] = b"bounded-path-cursor-test";

fn cursor_path_bytes(path: &NamespacePath) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let capacity = CURSOR_DOMAIN.len() + 2 + path.cursor_encoded_len()?;
    let mut encoder = Encoder::with_exact_capacity(CURSOR_DOMAIN, 1, capacity, |_| Ok::<_, CanonicalDecodeError>(()), |_| CanonicalDecodeError::AllocationFailed)?;
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    path.encode_cursor_into_with_accounting(&mut encoder, &mut accounting, &cancellation)?;
    assert_eq!(accounting.spent().bytes_encoded, path.cursor_encoded_len()? as u64);
    Ok(encoder.finish())
}

#[test]
fn cursor_codec_preserves_opaque_names_and_actual_shared_live_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    for (profile, encoding, bytes) in [
        (FilesystemProfile::Windows, NameEncoding::WindowsUtf16Le, vec![0, 0xd8]),
        (FilesystemProfile::Posix, NameEncoding::PosixBytes, vec![0xff]),
        (FilesystemProfile::Portable, NameEncoding::Utf8, "é".as_bytes().to_vec()),
        (FilesystemProfile::Browser, NameEncoding::Utf8, b"browser".to_vec()),
    ] {
        let path = NamespacePath::new(vec![LogicalName::new(encoding, bytes, limits.maximum_component_bytes)?], limits)?;
        let encoded = cursor_path_bytes(&path)?;
        let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        ledger.claim_bytes(37, 0, &mut work, WorkBudget::UNBOUNDED)?;
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        );
        let (decoded, mut allocation) = NamespacePath::decode_cursor_with_accounting(
            &mut decoder, profile, limits, &mut accounting, &cancellation,
        )?;
        decoder.finish()?;
        assert_eq!(decoded, path);
        assert_eq!(accounting.spent().bytes_copied, path.components()[0].as_bytes().len() as u64);
        assert_eq!(accounting.spent().allocation_operations, 2);
        let retained = decoded.retained_bytes();
        drop(decoded);
        accounting.release_owned(&mut allocation, retained)?;
        drop(accounting);
        assert_eq!(work.peak_allocation_bytes, 37 + retained);
        assert_eq!(ledger.live_bytes(), 37);
    }
    Ok(())
}

#[test]
fn cursor_codec_rejects_profile_and_whole_path_bounds_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let native = NamespacePath::new(vec![LogicalName::new(
        NameEncoding::WindowsUtf16Le, vec![0, 0xd8], limits.maximum_component_bytes,
    )?], limits)?;
    let portable = NamespacePath::parse_in_profile_measured(
        "/a/b", FilesystemProfile::Portable, limits, WorkBudget::UNBOUNDED, &CancellationToken::new(), 0,
    )?.value;
    for (encoded, profile, bounded, expected) in [
        (cursor_path_bytes(&native)?, FilesystemProfile::Portable, limits, NamespacePathError::Name(TreePageError::InvalidName)),
        (cursor_path_bytes(&portable)?, FilesystemProfile::Portable,
            VolumeLimits { maximum_path_depth: 1, ..limits }, NamespacePathError::TooDeep),
        (cursor_path_bytes(&portable)?, FilesystemProfile::Portable,
            VolumeLimits { maximum_path_bytes: 3, ..limits }, NamespacePathError::PathTooLong),
        (cursor_path_bytes(&native)?, FilesystemProfile::Windows,
            VolumeLimits { maximum_component_bytes: 1, ..limits }, NamespacePathError::Name(TreePageError::NameTooLarge)),
    ] {
        let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        );
        let error = NamespacePath::decode_cursor_with_accounting(
            &mut decoder, profile, bounded, &mut accounting, &cancellation,
        ).expect_err("invalid borrowed shape must fail");
        assert!(matches!(error, crate::FsError::Path(super::super::PathLookupError::NamespacePath(error)) if error == expected));
        drop(accounting);
        assert_eq!(work.allocation_operations, 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), 0);
    }
    Ok(())
}

#[test]
fn canonical_lowercase_matches_std_contextual_unicode_model()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::types::lowercase_unicode;

    let cancellation = CancellationToken::new();
    let contexts = [
        "", "A", "a", "ǅ", "Σ", "ʰ", "\u{0345}", "\u{200c}", "\u{200d}",
        "\u{2060}", "\u{fe0f}", "'", ".", ":", " ", "1",
        "A\u{0345}\u{200d}ʰ", "\u{0345}\u{200d}ʰA",
    ];
    let mut cases = vec![
        "ΟΣ".to_owned(), "ΟΣΑ".to_owned(), "ΣΟΣ".to_owned(),
        "İKẞ𐐀".to_owned(), "AΣ\u{0345}".to_owned(),
        "AΣ\u{0345}B".to_owned(), "ǅʰΣ\u{200d}".to_owned(),
    ];
    for before in contexts {
        for after in contexts {
            cases.push(format!("{before}Σ{after}"));
        }
    }
    for text in cases {
        let expected = text.to_lowercase().into_bytes();
        let name = LogicalName::new(NameEncoding::Utf8, text.as_bytes().to_vec(), u32::MAX)?;
        assert_eq!(name.case_fold_key(), expected, "ordinary key: {text:?}");
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        let held = lowercase_unicode("already-held-root", &mut ledger, &mut work, WorkBudget::UNBOUNDED, Some(&cancellation))?;
        let held_bytes = held.capacity() as u64;
        let before = work;
        let folded = lowercase_unicode(&text, &mut ledger, &mut work, WorkBudget::UNBOUNDED, Some(&cancellation))?;
        assert_eq!(folded, expected, "measured key: {text:?}");
        let bytes = folded.capacity() as u64;
        assert_eq!(ledger.live_bytes(), held_bytes + bytes);
        assert_eq!(work.peak_allocation_bytes, held_bytes + bytes);
        assert_eq!(work.allocation_operations, before.allocation_operations + 1);
        assert_eq!(work.bytes_encoded, before.bytes_encoded + folded.len() as u64);
        assert_eq!(work.bytes_copied, before.bytes_copied);
        assert!(work.items_examined > before.items_examined);
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        let (bridged, allocation) = crate::kernel::types::lowercase_unicode_with_accounting(
            &text, &mut accounting, &cancellation,
        )?;
        assert_eq!(bridged, expected, "accounting bridge: {text:?}");
        let bridged_work = accounting.spent();
        assert_eq!(bridged_work.items_examined, work.items_examined - before.items_examined);
        assert_eq!(bridged_work.bytes_encoded, work.bytes_encoded - before.bytes_encoded);
        assert_eq!(bridged_work.bytes_copied, 0);
        let bridged_bytes = bridged.capacity() as u64;
        drop(bridged);
        crate::kernel::persistent_point::retire_allocation(
            allocation, bridged_bytes, &mut accounting,
        )?;
        drop(folded);
        ledger.release(bytes)?;
        assert_eq!(ledger.live_bytes(), held_bytes);
        drop(held);
        ledger.release(held_bytes)?;
        assert_eq!(ledger.live_bytes(), 0);
    }
    Ok(())
}

#[test]
fn canonical_case_keys_preserve_native_byte_grammar()
-> Result<(), Box<dyn std::error::Error>> {
    for text in ["ΟΣ", "ǅΣ", "İ𝄞", "A\u{200d}Σ"] {
        let bytes: Vec<u8> = text.encode_utf16().flat_map(u16::to_le_bytes).collect();
        let name = LogicalName::new(NameEncoding::WindowsUtf16Le, bytes.clone(), u32::MAX)?;
        assert_eq!(name.case_fold_key(), bytes);
    }
    for (encoding, bytes) in [
        (NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8, b'A', 0x00]),
        (NameEncoding::PosixBytes, vec![0xff, b'A', b'S']),
    ] {
        let name = LogicalName::new(encoding, bytes.clone(), u32::MAX)?;
        assert_eq!(name.case_fold_key(), bytes);
    }
    for invalid in ["", ".", "..", "a/b", "a\0b"] {
        assert!(LogicalName::new(
            NameEncoding::Utf8, invalid.as_bytes().to_vec(), u32::MAX,
        ).is_err());
    }
    Ok(())
}
