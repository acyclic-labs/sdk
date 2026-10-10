use super::*;

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
fn measured_profiles_charge_exact_owned_names() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let cancellation = CancellationToken::new();
    for (profile, name_bytes, copied, encoded) in [
        (FilesystemProfile::Portable, 6, 6, 0),
        (FilesystemProfile::Browser, 6, 6, 0),
        (FilesystemProfile::Posix, 6, 6, 0),
        (FilesystemProfile::Windows, 6, 0, 6),
    ] {
        let receipt = NamespacePath::parse_in_profile_measured(
            "/é/𝄞", profile, limits, WorkBudget::UNBOUNDED, &cancellation, 17,
        )?;
        assert_eq!(receipt.value.depth(), 2);
        assert_eq!(receipt.work.source_path_components, 2);
        assert_eq!(receipt.work.items_examined, 2);
        assert_eq!(receipt.work.allocation_operations, 3);
        assert_eq!(receipt.work.bytes_copied, copied);
        assert_eq!(receipt.work.bytes_encoded, encoded);
        assert_eq!(receipt.value.retained_bytes(),
            2 * std::mem::size_of::<LogicalName>() as u64 + name_bytes);
        assert_eq!(receipt.work.peak_allocation_bytes, 17 + receipt.value.retained_bytes());
    }
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

#[test]
fn shared_path_ledger_keeps_overlap_and_retires_failed_copies()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let first = NamespacePath::parse_in_profile_with_ledger(
        "/a", FilesystemProfile::Portable, VolumeLimits::default(), &mut ledger,
        &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let retained = ledger.live_bytes();
    let budget = WorkBudget { bytes_copied: work.bytes_copied,
        ..WorkBudget::UNBOUNDED };
    let failure = NamespacePath::parse_in_profile_with_ledger(
        "/b", FilesystemProfile::Portable, VolumeLimits::default(), &mut ledger,
        &mut work, budget, &cancellation,
    );
    assert!(matches!(failure, Err(NamespacePathError::Work(_))));
    assert_eq!(ledger.live_bytes(), retained);
    assert_eq!(work.peak_allocation_bytes,
        retained + std::mem::size_of::<LogicalName>() as u64);
    ledger.release(first.retained_bytes())?;
    assert_eq!(ledger.live_bytes(), 0);
    let next = NamespacePath::parse_in_profile_with_ledger(
        "/c", FilesystemProfile::Portable, VolumeLimits::default(), &mut ledger,
        &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(ledger.live_bytes(), next.retained_bytes());
    Ok(())
}

#[test]
fn measured_copy_and_child_preserve_opaque_native_units()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let cancellation = CancellationToken::new();
    let name = LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0, 0xd8], 255)?;
    let source = NamespacePath::new(vec![name.clone()], limits)?;
    let copied = source.clone_measured(WorkBudget::UNBOUNDED, &cancellation, source.retained_bytes())?;
    assert_eq!(copied.value, source);
    assert_eq!(copied.work.bytes_copied, 2);
    assert_eq!(copied.work.peak_allocation_bytes, 2 * source.retained_bytes());
    let child = source.child_measured(
        &name, limits, WorkBudget::UNBOUNDED, &cancellation, source.retained_bytes(),
    )?;
    assert_eq!(child.value.components(), &[name.clone(), name]);
    let denied = source.clone_measured(
        WorkBudget { items_examined: 0, ..WorkBudget::UNBOUNDED }, &cancellation, 0,
    ).expect_err("zero work budget must deny before copy allocation");
    assert_eq!(denied.work.allocation_operations, 0);
    Ok(())
}

#[test]
fn shared_native_child_copy_preserves_live_scope_and_failure_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let cancellation = CancellationToken::new();
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let parent = NamespacePath::parse_in_profile_with_ledger(
        "/a", FilesystemProfile::Windows, limits, &mut ledger, &mut work,
        WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let name = LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0, 0xd8], 255)?;
    ledger.claim_bytes(name.retained_bytes() as u64, 0, &mut work, WorkBudget::UNBOUNDED)?;
    let before_child = ledger.live_bytes();
    let child = parent.child_with_ledger(
        &name, limits, &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(child.components().last(), Some(&name));
    assert_eq!(ledger.live_bytes(), before_child + child.retained_bytes());
    let before_failure = work;
    let live = ledger.live_bytes();
    let budget = WorkBudget {
        bytes_copied: work.bytes_copied + 2, ..WorkBudget::UNBOUNDED
    };
    let denied = child.clone_with_ledger(
        &mut ledger, &mut work, budget, &cancellation,
    );
    assert!(matches!(denied, Err(NamespacePathError::Work(_))));
    assert_eq!(ledger.live_bytes(), live);
    assert_eq!(work.bytes_copied, before_failure.bytes_copied + 2);
    assert_eq!(work.allocation_operations, before_failure.allocation_operations + 2);
    ledger.release(child.retained_bytes())?;
    assert_eq!(ledger.live_bytes(), before_child);
    Ok(())
}

const CURSOR_DOMAIN: &[u8] = b"bounded-path-cursor-test";

fn cursor_path_bytes(path: &NamespacePath) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let capacity = CURSOR_DOMAIN.len() + 2 + path.cursor_encoded_len()?;
    let mut encoder = Encoder::with_exact_capacity(CURSOR_DOMAIN, 1, capacity)?;
    let mut work = WorkCounters::default();
    path.encode_cursor_into(&mut encoder, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert_eq!(work.bytes_encoded, path.cursor_encoded_len()? as u64);
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
        let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX)?;
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        ledger.claim_bytes(37, 0, &mut work, WorkBudget::UNBOUNDED)?;
        let decoded = NamespacePath::decode_cursor_with_ledger(
            &mut decoder, profile, limits, &mut ledger, &mut work,
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        )?;
        decoder.finish()?;
        assert_eq!(decoded, path);
        assert_eq!(work.bytes_copied, path.components()[0].as_bytes().len() as u64);
        assert_eq!(work.allocation_operations, 2);
        assert_eq!(ledger.live_bytes(), 37 + decoded.retained_bytes());
        assert_eq!(work.peak_allocation_bytes, ledger.live_bytes());
        ledger.release(decoded.retained_bytes())?;
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
        let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX)?;
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let error = NamespacePath::decode_cursor_with_ledger(
            &mut decoder, profile, bounded, &mut ledger, &mut work,
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).expect_err("invalid borrowed shape must fail");
        assert_eq!(error, expected);
        assert_eq!(work.allocation_operations, 0);
        assert_eq!(work.bytes_copied, 0);
        assert_eq!(ledger.live_bytes(), 0);
    }
    Ok(())
}

#[test]
fn cursor_codec_denial_preserves_performed_prefix_and_retires_only_owned_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let path = NamespacePath::parse_in_profile_measured(
        "/a/bb", FilesystemProfile::Portable, limits, WorkBudget::UNBOUNDED, &CancellationToken::new(), 0,
    )?.value;
    let encoded = cursor_path_bytes(&path)?;
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    ledger.claim_bytes(19, 0, &mut work, WorkBudget::UNBOUNDED)?;
    let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX)?;
    let denied = NamespacePath::decode_cursor_with_ledger(
        &mut decoder, FilesystemProfile::Portable, limits, &mut ledger, &mut work,
        WorkBudget { bytes_copied: 1, ..WorkBudget::UNBOUNDED }, &CancellationToken::new(),
    );
    assert!(matches!(denied, Err(NamespacePathError::Work(_))));
    assert_eq!(work.bytes_copied, 1);
    assert_eq!(work.allocation_operations, 2);
    assert_eq!(ledger.live_bytes(), 19);
    assert!(work.peak_allocation_bytes > 19);
    let before = work;
    let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX)?;
    let denied = NamespacePath::decode_cursor_with_ledger(
        &mut decoder, FilesystemProfile::Portable, limits, &mut ledger, &mut work,
        WorkBudget { peak_allocation_bytes: 19, ..WorkBudget::UNBOUNDED }, &CancellationToken::new(),
    );
    assert!(matches!(denied, Err(NamespacePathError::Work(_))));
    assert_eq!(work.allocation_operations, before.allocation_operations);
    assert_eq!(ledger.live_bytes(), 19);
    Ok(())
}

#[test]
fn cursor_codec_does_not_grow_unadmitted_output_or_allocate_on_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let path = NamespacePath::parse_in_profile_measured(
        "/a", FilesystemProfile::Portable, VolumeLimits::default(), WorkBudget::UNBOUNDED, &CancellationToken::new(), 0,
    )?.value;
    let mut encoder = Encoder::with_exact_capacity(CURSOR_DOMAIN, 1, CURSOR_DOMAIN.len() + 2)?;
    for _ in 0..encoder.remaining_capacity() {
        encoder.u8(0);
    }
    let capacity = encoder.capacity();
    let mut work = WorkCounters::default();
    assert_eq!(path.encode_cursor_into(
        &mut encoder, &mut work, WorkBudget::UNBOUNDED, &CancellationToken::new(),
    ), Err(NamespacePathError::AllocationFailed));
    assert_eq!(encoder.capacity(), capacity);
    assert_eq!(work, WorkCounters::default());
    let encoded = cursor_path_bytes(&path)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut decoder = Decoder::new(&encoded, CURSOR_DOMAIN, 1, u64::MAX)?;
    let mut ledger = AllocationLedger::default();
    assert_eq!(NamespacePath::decode_cursor_with_ledger(
        &mut decoder, FilesystemProfile::Portable, VolumeLimits::default(), &mut ledger,
        &mut work, WorkBudget::UNBOUNDED, &cancellation,
    ), Err(NamespacePathError::Cancelled));
    assert_eq!(work, WorkCounters::default());
    assert_eq!(ledger.live_bytes(), 0);
    Ok(())
}
