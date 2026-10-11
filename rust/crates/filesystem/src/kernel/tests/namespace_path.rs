use crate::kernel::PathLookupError;
use super::*;

#[test]
fn borrowed_name_constructor_charges_only_the_invalid_bounds_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::namespace_path::NamespacePathStep;
    let names = [
        LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 2)?,
        LogicalName::new(NameEncoding::Utf8, b"bb".to_vec(), 2)?,
        LogicalName::new(NameEncoding::Utf8, b"c".to_vec(), 2)?,
    ];
    let limits = VolumeLimits { maximum_component_bytes: 1, ..VolumeLimits::default() };
    let cancellation = CancellationToken::new();
    let mut work = WorkCounters::UNCHARGED;
    let mut allocations = 0;
    let result = NamespacePath::names_with_steps::<NamespacePathError>(
        &names, limits, &cancellation, &mut |step| {
            match step {
                NamespacePathStep::Charge(delta) =>
                    work.charge(delta, &WorkBudget::UNBOUNDED).map_err(NamespacePathError::Work)?,
                NamespacePathStep::Allocation(_) => allocations += 1,
                NamespacePathStep::Admit(_) | NamespacePathStep::Performed(_) => {}
            }
            Ok(())
        },
    );
    assert_eq!(result, Err(NamespacePathError::ComponentTooLong));
    assert_eq!(work.items_examined, 2);
    assert_eq!(work.source_path_components, 2);
    assert_eq!(work.bytes_copied, 0);
    assert_eq!(allocations, 0);
    Ok(())
}

#[test]
fn borrowed_name_constructor_cancellation_precedes_any_accounting_step()
-> Result<(), Box<dyn std::error::Error>> {
    let names = [LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], 2)?];
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut steps = 0;
    let result = NamespacePath::names_with_steps::<NamespacePathError>(
        &names, VolumeLimits::default(), &cancellation, &mut |_| {
            steps += 1;
            Ok(())
        },
    );
    assert_eq!(result, Err(NamespacePathError::Cancelled));
    assert_eq!(steps, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
mod original_namespace_controls {
    use super::*;
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};

    fn is_budget_error(error: &crate::FsError) -> bool {
        matches!(error, crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { .. })
        ))
    }

    #[test]
    fn original_fork_path_preflight_does_not_construct_owned_root_or_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let limits = VolumeLimits::default();
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        NamespacePath::validate_in_profile_with_accounting(
            "/", FilesystemProfile::Windows, limits, &mut accounting, &cancellation,
        )?;
        assert_eq!(original.receipt(), before);
        NamespacePath::validate_in_profile_with_accounting(
            "/é/𝄞", FilesystemProfile::Windows, limits, &mut accounting, &cancellation,
        )?;
        let after = original.receipt();
        assert_eq!(after.allocation_operations, before.allocation_operations);
        assert_eq!(after.bytes_copied, before.bytes_copied);
        assert_eq!(after.bytes_encoded, before.bytes_encoded);
        assert_eq!(after.source_path_components - before.source_path_components, 2);
        assert_eq!(original.live_allocation_bytes(), baseline);
        NamespacePath::validate_in_profile_with_accounting(
            "/a", FilesystemProfile::Portable,
            VolumeLimits { maximum_component_bytes: 1, ..limits },
            &mut accounting, &cancellation,
        )?;
        let error = NamespacePath::validate_in_profile_with_accounting(
            "/a", FilesystemProfile::Windows,
            VolumeLimits { maximum_component_bytes: 1, ..limits },
            &mut accounting, &cancellation,
        ).err().ok_or("UTF16 bound was validated as UTF8")?;
        assert!(matches!(error, crate::FsError::Path(crate::kernel::PathLookupError::NamespacePath(
            NamespacePathError::Name(TreePageError::NameTooLarge),
        ))));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_fork_prefix_copies_only_retained_selected_components()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let source = NamespacePath::new(vec![
            LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], 256)?,
            LogicalName::new(NameEncoding::WindowsUtf16Le, vec![b'z', 0], 256)?,
        ], limits)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let before = original.receipt();
        let (root, root_guard) = source.prefix_with_accounting(0, limits, &mut accounting, &cancellation)?;
        assert!(root.is_root());
        assert_eq!(original.receipt(), before);
        drop(root);
        drop(root_guard);
        let (prefix, guard) = source.prefix_with_accounting(1, limits, &mut accounting, &cancellation)?;
        assert_eq!(prefix.components(), &source.components()[..1]);
        assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 2);
        assert_eq!(original.receipt().source_path_components - before.source_path_components, 1);
        let KernelOwnedAllocation::NativeOriginal(guard) = guard
        else { return Err("selected prefix lost original claim".into()); };
        assert_eq!(guard.bytes(), prefix.retained_bytes());
        assert_eq!(original.live_allocation_bytes(), baseline + guard.bytes());
        drop(prefix);
        drop(guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        let before = original.receipt();
        assert!(source.prefix_with_accounting(3, limits, &mut accounting, &cancellation).is_err());
        assert_eq!(original.receipt(), before);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_cursor_codec_uses_funded_encoder_and_retained_input_overlap()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let path = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], limits.maximum_component_bytes,
        )?], limits)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let capacity = CURSOR_DOMAIN.len() + 2 + path.cursor_encoded_len()?;
        let mut encoded_guard = accounting.claim_owned(capacity as u64, 1)?;
        let header = WorkCounters { bytes_encoded: (CURSOR_DOMAIN.len() + 2) as u64,
            bytes_copied: (CURSOR_DOMAIN.len() + 2) as u64, ..WorkCounters::UNCHARGED };
        accounting.admit(header)?;
        let mut encoder = Encoder::with_exact_capacity(CURSOR_DOMAIN, 1, capacity)?;
        accounting.reconcile_owned(&mut encoded_guard, capacity as u64, encoder.capacity() as u64)?;
        accounting.record_completed(header)?;
        let before = original.receipt();
        path.encode_cursor_into_with_accounting(&mut encoder, &mut accounting, &cancellation)?;
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().bytes_encoded - before.bytes_encoded, path.cursor_encoded_len()? as u64);
        let bytes = encoder.finish();
        let mut decoder = Decoder::new(&bytes, CURSOR_DOMAIN, 1, u64::MAX)?;
        let (decoded, decoded_guard) = NamespacePath::decode_cursor_with_accounting(
            &mut decoder, FilesystemProfile::Windows, limits, &mut accounting, &cancellation,
        )?;
        decoder.finish()?;
        assert_eq!(decoded, path);
        let (KernelOwnedAllocation::NativeOriginal(encoded_guard),
            KernelOwnedAllocation::NativeOriginal(decoded_guard)) = (encoded_guard, decoded_guard)
        else { return Err("cursor backing lost original accounting".into()); };
        assert_eq!(encoded_guard.bytes(), bytes.capacity() as u64);
        assert_eq!(decoded_guard.bytes(), decoded.retained_bytes());
        assert!(decoded_guard.shares_ledger(&encoded_guard));
        assert_eq!(original.live_allocation_bytes(),
            baseline + encoded_guard.bytes() + decoded_guard.bytes());
        drop(decoded);
        drop(decoded_guard);
        assert_eq!(original.live_allocation_bytes(), baseline + encoded_guard.bytes());
        drop(bytes);
        drop(encoded_guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_cursor_decoder_preserves_full_shape_and_partial_quota_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let opaque = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], limits.maximum_component_bytes,
        )?], limits)?;
        let opaque_bytes = cursor_path_bytes(&opaque)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut decoder = Decoder::new(&opaque_bytes, CURSOR_DOMAIN, 1, u64::MAX)?;
        assert!(NamespacePath::decode_cursor_with_accounting(
            &mut decoder, FilesystemProfile::Portable, limits, &mut accounting, &cancellation,
        ).is_err());
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(original.live_allocation_bytes(), baseline);

        let source = NamespacePath::from_portable(&PortablePath::parse("/a/b", limits)?, limits)?;
        let bytes = cursor_path_bytes(&source)?;
        let controller_operations = {
            let probe = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            probe.receipt().allocation_operations
        };
        let original = crate::NativeMaterializationWork::new(WorkBudget {
            allocation_operations: controller_operations.checked_add(2).ok_or("allocation quota overflow")?,
            ..WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut decoder = Decoder::new(&bytes, CURSOR_DOMAIN, 1, u64::MAX)?;
        let error = NamespacePath::decode_cursor_with_accounting(
            &mut decoder, FilesystemProfile::Portable, limits, &mut accounting, &cancellation,
        ).err().ok_or("partial cursor quota completed")?;
        assert!(is_budget_error(&error));
        assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 2);
        assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 1);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_name_clone_preserves_raw_units_and_denies_copy_before_reserve()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let name = LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], 256)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let before = original.receipt();
        let (copied, guard) = name.clone_with_accounting(&mut accounting, &cancellation)?;
        assert_eq!(copied, name);
        let KernelOwnedAllocation::NativeOriginal(guard) = guard
        else { return Err("name clone lost original claim".into()); };
        assert_eq!(guard.bytes(), copied.retained_bytes() as u64);
        assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 1);
        assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 2);
        drop(copied);
        drop(guard);
        assert_eq!(original.live_allocation_bytes(), baseline);

        let original = crate::NativeMaterializationWork::new(WorkBudget {
            bytes_copied: 0, ..WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(error, _)| error)?;
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let error = name.clone_with_accounting(&mut accounting, &cancellation)
            .err().ok_or("zero copy quota reserved a name buffer")?;
        assert!(is_budget_error(&error));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[test]
    fn funded_name_unicode_projection_matches_exact_original_encodings()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        for encoding in [NameEncoding::Utf8, NameEncoding::PosixBytes, NameEncoding::WindowsUtf16Le] {
            let bytes = if encoding == NameEncoding::WindowsUtf16Le {
                "é𝄞".encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else { "é𝄞".as_bytes().to_vec() };
            let name = LogicalName::new(encoding, bytes, 256)?;
            let expected = name.unicode_text().ok_or("valid Unicode name was rejected")?;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let before = original.receipt();
            let (text, guard) = name.unicode_text_with_native_work(&original, &cancellation)?;
            assert_eq!(text, expected.as_ref());
            assert_eq!(guard.bytes(), text.capacity() as u64);
            assert_eq!(original.live_allocation_bytes(), baseline + guard.bytes());
            assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 1);
            assert_eq!(original.receipt().output_bytes - before.output_bytes, text.len() as u64);
            if encoding == NameEncoding::WindowsUtf16Le {
                assert_eq!(original.receipt().bytes_encoded - before.bytes_encoded, text.len() as u64);
                assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
            } else {
                assert_eq!(original.receipt().bytes_copied - before.bytes_copied, text.len() as u64);
                assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
            }
            drop(text);
            drop(guard);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[test]
    fn funded_name_projection_exhausts_positive_quota_with_actual_owned_string()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let controller_operations = {
            let probe = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            probe.receipt().allocation_operations
        };
        let original = crate::NativeMaterializationWork::new(WorkBudget {
            allocation_operations: controller_operations.checked_add(1).ok_or("allocation quota overflow")?,
            ..WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(error, _)| error)?;
        let name = LogicalName::new(NameEncoding::WindowsUtf16Le,
            vec![0xe9, 0x00, 0x34, 0xd8, 0x1e, 0xdd], 256)?;
        let baseline = original.live_allocation_bytes();
        let (text, guard) = name.unicode_text_with_native_work(&original, &cancellation)?;
        let before = original.receipt();
        let live = original.live_allocation_bytes();
        let error = name.unicode_text_with_native_work(&original, &cancellation)
            .err().ok_or("exhausted name quota allocated another string")?;
        assert!(is_budget_error(&error));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
        assert_eq!(original.receipt().output_bytes, before.output_bytes);
        assert_eq!(original.receipt().peak_allocation_bytes, before.peak_allocation_bytes);
        assert_eq!(original.live_allocation_bytes(), live);
        drop(text);
        drop(guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn funded_name_projection_keeps_opaque_and_cancelled_causes_before_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let names = [
            LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 256)?,
            LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], 256)?,
        ];
        for name in &names {
            let before = original.receipt();
            let live = original.live_allocation_bytes();
            let error = name.unicode_text_with_native_work(&original, &cancellation)
                .err().ok_or("opaque name was projected with replacement characters")?;
            assert!(matches!(error, crate::FsError::Path(crate::kernel::PathLookupError::NamespacePath(
                NamespacePathError::NonUnicodeProjection,
            ))));
            assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
            assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
            assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
            assert_eq!(original.live_allocation_bytes(), live);
        }
        let valid = LogicalName::new(NameEncoding::Utf8, b"valid".to_vec(), 256)?;
        let before = original.receipt();
        cancellation.cancel();
        let error = valid.unicode_text_with_native_work(&original, &cancellation)
            .err().ok_or("cancelled name projection allocated")?;
        assert!(matches!(error,
            crate::FsError::Cancelled(_)
            | crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled)
        ));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[test]
    fn original_child_and_unicode_projection_keep_all_live_original_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let limits = VolumeLimits::default();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let (path, path_guard) = NamespacePath::parse_in_profile_with_accounting(
            "/é/𝄞", FilesystemProfile::Windows, limits, &mut accounting, &cancellation,
        )?;
        let name = LogicalName::new(NameEncoding::WindowsUtf16Le, vec![b'n', 0],
            limits.maximum_component_bytes)?;
        let (child, child_guard) = path.child_with_accounting(
            &name, limits, &mut accounting, &cancellation,
        )?;
        let before = original.receipt();
        let (text, text_guard) = child.unicode_text_with_accounting(&mut accounting, &cancellation)?;
        let (KernelOwnedAllocation::NativeOriginal(path_guard),
            KernelOwnedAllocation::NativeOriginal(child_guard),
            KernelOwnedAllocation::NativeOriginal(text_guard)) = (path_guard, child_guard, text_guard)
        else { return Err("output is not attached to original accounting".into()); };
        assert_eq!(text, "/é/𝄞/n");
        assert_eq!(path_guard.bytes(), path.retained_bytes());
        assert_eq!(child_guard.bytes(), child.retained_bytes());
        assert_eq!(text_guard.bytes(), text.capacity() as u64);
        assert!(text_guard.shares_ledger(&path_guard));
        assert!(child_guard.shares_ledger(&path_guard));
        assert_eq!(original.live_allocation_bytes(),
            baseline + path_guard.bytes() + child_guard.bytes() + text_guard.bytes());
        assert_eq!(original.receipt().bytes_encoded - before.bytes_encoded, text.len() as u64);
        assert_eq!(original.receipt().output_bytes - before.output_bytes, text.len() as u64);
        assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 1);
        drop(text);
        drop(text_guard);
        drop(child);
        drop(child_guard);
        assert_eq!(original.live_allocation_bytes(), baseline + path_guard.bytes());
        drop(path);
        drop(path_guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_projection_rejects_opaque_units_and_output_quota_before_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let opaque = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8], limits.maximum_component_bytes,
        )?], limits)?;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let error = opaque.unicode_text_with_accounting(&mut accounting, &cancellation)
            .err().ok_or("opaque UTF16 was projected through replacement Unicode")?;
        assert!(matches!(error, crate::FsError::Path(crate::kernel::PathLookupError::NamespacePath(
            NamespacePathError::NonUnicodeProjection,
        ))));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
        assert_eq!(original.live_allocation_bytes(), baseline);

        let original = crate::NativeMaterializationWork::new(
            WorkBudget { output_bytes: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(error, _)| error)?;
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let (path, path_guard) = NamespacePath::parse_in_profile_with_accounting(
            "/a/b", FilesystemProfile::Portable, limits, &mut accounting, &cancellation,
        )?;
        let before = original.receipt();
        let live = original.live_allocation_bytes();
        let error = path.unicode_text_with_accounting(&mut accounting, &cancellation)
            .err().ok_or("output quota allocated Unicode projection")?;
        assert!(is_budget_error(&error));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
        assert_eq!(original.receipt().output_bytes, before.output_bytes);
        assert_eq!(original.live_allocation_bytes(), live);
        drop(path);
        drop(path_guard);
        Ok(())
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

    #[test]
    fn original_invalid_grammar_and_profile_bounds_allocate_and_copy_nothing()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let limits = VolumeLimits::default();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        for (text, bounded) in [
            ("relative", limits), ("/a//b", limits), ("/.", limits),
            ("/..", limits), ("/a/", limits), ("/nul\0", limits),
            ("/ab", VolumeLimits { maximum_component_bytes: 3, ..limits }),
            ("/ab/c", VolumeLimits { maximum_path_bytes: 7, ..limits }),
            ("/a/b", VolumeLimits { maximum_path_depth: 1, ..limits }),
        ] {
            let expected = NamespacePath::parse_in_profile_measured(
                text, FilesystemProfile::Windows, bounded, WorkBudget::UNBOUNDED,
                &cancellation, 0,
            ).err().ok_or("ordinary engine accepted invalid path")?;
            let before = original.receipt();
            let error = NamespacePath::parse_in_profile_with_accounting(
                text, FilesystemProfile::Windows, bounded, &mut accounting, &cancellation,
            ).err().ok_or("original engine accepted invalid path")?;
            assert!(matches!(&error, crate::FsError::Path(
                PathLookupError::NamespacePath(cause)
            ) if cause == &expected.error));
            let after = original.receipt();
            assert_eq!(after.allocation_operations, before.allocation_operations);
            assert_eq!(after.peak_allocation_bytes, before.peak_allocation_bytes);
            assert_eq!(after.bytes_copied, before.bytes_copied);
            assert_eq!(after.bytes_encoded, before.bytes_encoded);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[test]
    fn original_all_profiles_preserve_exact_capacity_utf16_and_decomposed_unicode()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        for profile in [
            FilesystemProfile::Portable, FilesystemProfile::Browser,
            FilesystemProfile::Posix, FilesystemProfile::Windows,
        ] {
            let text = "/e\u{301}/𝄞";
            let expected = NamespacePath::parse_in_profile_measured(
                text, profile, limits, WorkBudget::UNBOUNDED, &cancellation, 0,
            )?;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let before = original.receipt();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let (path, guard) = NamespacePath::parse_in_profile_with_accounting(
                text, profile, limits, &mut accounting, &cancellation,
            )?;
            let KernelOwnedAllocation::NativeOriginal(guard) = guard else {
                return Err("path guard does not retain the original ledger".into());
            };
            assert_eq!(path, expected.value);
            assert_eq!(path.components()[0].unicode_text().as_deref(), Some("e\u{301}"));
            if profile == FilesystemProfile::Windows {
                assert_eq!(path.components()[0].as_bytes(), &[0x65, 0, 1, 3]);
                assert_eq!(path.components()[1].as_bytes(), &[0x34, 0xd8, 0x1e, 0xdd]);
            }
            assert_eq!(guard.bytes(), path.retained_bytes());
            let after = original.receipt();
            assert_eq!(after.allocation_operations - before.allocation_operations, 3);
            assert_eq!(after.bytes_copied - before.bytes_copied, expected.work.bytes_copied);
            assert_eq!(after.bytes_encoded - before.bytes_encoded, expected.work.bytes_encoded);
            assert_eq!(after.source_path_components - before.source_path_components, 2);
            assert_eq!(after.items_examined - before.items_examined, 2);
            assert_eq!(after.peak_allocation_bytes, baseline + guard.bytes());
            assert_eq!(original.live_allocation_bytes(), baseline + guard.bytes());
            drop(path);
            drop(guard);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[test]
    fn original_clone_preserves_opaque_native_units_and_name_constraints()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let cancellation = CancellationToken::new();
        for (encoding, bytes) in [
            (NameEncoding::WindowsUtf16Le, vec![0, 0xd8]),
            (NameEncoding::WindowsUtf16Le, vec![0, 0xdc]),
            (NameEncoding::PosixBytes, vec![0xff, b'a']),
        ] {
            let source = NamespacePath::new(vec![
                LogicalName::new(encoding, bytes, limits.maximum_component_bytes)?,
            ], limits)?;
            let expected = source.clone_measured(WorkBudget::UNBOUNDED, &cancellation, 0)?;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let before = original.receipt();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let (path, guard) = source.clone_with_accounting(&mut accounting, &cancellation)?;
            let KernelOwnedAllocation::NativeOriginal(guard) = guard else {
                return Err("clone guard does not retain the original ledger".into());
            };
            assert_eq!(path, source);
            assert_eq!(path, expected.value);
            assert_eq!(guard.bytes(), path.retained_bytes());
            assert_eq!(original.receipt().bytes_copied - before.bytes_copied, 2);
            assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
            assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 2);
            assert_eq!(original.receipt().peak_allocation_bytes, baseline + guard.bytes());
            drop(path);
            drop(guard);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        for bytes in [vec![0xd8], vec![0, 0], vec![b'/' , 0], vec![b'\\', 0]] {
            assert!(LogicalName::new(NameEncoding::WindowsUtf16Le, bytes, 255).is_err());
        }
        Ok(())
    }

    #[test]
    fn original_two_owned_paths_share_live_ledger_and_retire_after_outputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let (first, first_guard) = NamespacePath::parse_in_profile_with_accounting(
            "/e\u{301}/𝄞", FilesystemProfile::Windows, VolumeLimits::default(),
            &mut accounting, &cancellation,
        )?;
        let (second, second_guard) = first.clone_with_accounting(&mut accounting, &cancellation)?;
        let KernelOwnedAllocation::NativeOriginal(first_guard) = first_guard else {
            return Err("first guard is not original".into());
        };
        let KernelOwnedAllocation::NativeOriginal(second_guard) = second_guard else {
            return Err("second guard is not original".into());
        };
        assert_eq!(first, second);
        assert!(first_guard.shares_ledger(&second_guard));
        assert_eq!(first_guard.bytes(), first.retained_bytes());
        assert_eq!(second_guard.bytes(), second.retained_bytes());
        let overlap = baseline + first_guard.bytes() + second_guard.bytes();
        assert_eq!(original.live_allocation_bytes(), overlap);
        assert_eq!(original.receipt().peak_allocation_bytes, overlap);
        let peak = original.receipt().peak_allocation_bytes;
        drop(second);
        drop(second_guard);
        assert_eq!(original.live_allocation_bytes(), baseline + first_guard.bytes());
        drop(first);
        drop(first_guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(original.receipt().peak_allocation_bytes, peak);
        Ok(())
    }

    #[test]
    fn original_partial_quota_failures_preserve_actual_prefix_and_retire_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let source = NamespacePath::parse_in_profile_measured(
            "/a/bb", FilesystemProfile::Portable, limits, WorkBudget::UNBOUNDED,
            &cancellation, 0,
        )?.value;
        let component_capacity = source.retained_bytes()
            - source.components().iter().map(|name| name.retained_bytes() as u64).sum::<u64>();
        let first_capacity = source.components()[0].retained_bytes() as u64;
        let controller_operations = {
            let probe = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| error)?;
            probe.receipt().allocation_operations
        };
        for clone in [false, true] {
            for allocation_quota in [false, true] {
                let budget = WorkBudget {
                    allocation_operations: if allocation_quota {
                        controller_operations.checked_add(2).ok_or("allocation quota overflow")?
                    } else { u64::MAX },
                    bytes_copied: if allocation_quota { u64::MAX } else { 1 },
                    ..WorkBudget::UNBOUNDED
                };
                let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, _)| error)?;
                let baseline = original.live_allocation_bytes();
                let before = original.receipt();
                let mut accounting = KernelAccounting::native(&original, &cancellation);
                let result = if clone {
                    source.clone_with_accounting(&mut accounting, &cancellation)
                } else {
                    NamespacePath::parse_in_profile_with_accounting(
                        "/a/bb", FilesystemProfile::Portable, limits,
                        &mut accounting, &cancellation,
                    )
                };
                let error = result.err().ok_or("partial quota unexpectedly completed")?;
                assert!(is_budget_error(&error));
                let after = original.receipt();
                assert_eq!(after.bytes_copied - before.bytes_copied, 1);
                assert_eq!(after.bytes_encoded, before.bytes_encoded);
                assert_eq!(after.allocation_operations - before.allocation_operations, 2);
                assert_eq!(after.source_path_components - before.source_path_components, 2);
                assert_eq!(after.peak_allocation_bytes, baseline + component_capacity + first_capacity);
                assert_eq!(original.live_allocation_bytes(), baseline);
            }
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

#[test]
fn canonical_lowercase_matches_std_contextual_unicode_model()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::types::lowercase_unicode_with_ledger;

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
        let held = lowercase_unicode_with_ledger(
            "already-held-root", &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        let held_bytes = held.capacity() as u64;
        let before = work;
        let folded = lowercase_unicode_with_ledger(
            &text, &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        )?;
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

#[test]
fn canonical_lowercase_denial_preserves_performed_prefix_and_owned_overlap()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::types::lowercase_unicode_with_ledger;

    let cancellation = CancellationToken::new();
    let text = "AΣ\u{200d}";
    for (mut budget, items, allocations, encoded, peak) in [
        (WorkBudget { items_examined: 5, ..WorkBudget::UNBOUNDED }, 5, 0, 0, 17),
        (WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, 6, 0, 0, 17),
        (WorkBudget { peak_allocation_bytes: 22, ..WorkBudget::UNBOUNDED }, 6, 0, 0, 17),
        (WorkBudget { bytes_encoded: 1, ..WorkBudget::UNBOUNDED }, 11, 1, 1, 23),
        (WorkBudget { items_examined: 10, ..WorkBudget::UNBOUNDED }, 10, 1, 1, 23),
    ] {
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        let held = lowercase_unicode_with_ledger(
            "already-held-root", &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        let held_bytes = held.capacity() as u64;
        assert_eq!(held_bytes, 17);
        let before = work;
        budget.items_examined = budget.items_examined.saturating_add(before.items_examined);
        budget.allocation_operations = budget.allocation_operations.saturating_add(before.allocation_operations);
        budget.bytes_encoded = budget.bytes_encoded.saturating_add(before.bytes_encoded);
        assert!(matches!(lowercase_unicode_with_ledger(
            text, &mut ledger, &mut work, budget, &cancellation,
        ), Err(NamespacePathError::Work(_))));
        assert_eq!(work.items_examined, before.items_examined + items);
        assert_eq!(work.allocation_operations, before.allocation_operations + allocations);
        assert_eq!(work.bytes_encoded, before.bytes_encoded + encoded);
        assert_eq!(work.bytes_copied, before.bytes_copied);
        assert_eq!(work.peak_allocation_bytes, peak);
        assert_eq!(ledger.live_bytes(), held_bytes);
        drop(held);
        ledger.release(held_bytes)?;
        assert_eq!(ledger.live_bytes(), 0);
    }
    Ok(())
}

#[test]
fn canonical_lowercase_cancellation_and_empty_keys_do_not_allocate()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::types::lowercase_unicode_with_ledger;

    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    let cancellation = CancellationToken::new();
    let held = lowercase_unicode_with_ledger(
        "already-held-root", &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let held_bytes = held.capacity() as u64;
    let before = work;
    let empty = lowercase_unicode_with_ledger(
        "", &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert!(empty.is_empty());
    assert_eq!(empty.capacity(), 0);
    assert_eq!(work, before);
    assert_eq!(ledger.live_bytes(), held_bytes);
    cancellation.cancel();
    for text in ["", "ǅʰΣ\u{200d}", "İKẞ𐐀"] {
        assert_eq!(lowercase_unicode_with_ledger(
            text, &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        ), Err(NamespacePathError::Cancelled));
        assert_eq!(work, before);
        assert_eq!(ledger.live_bytes(), held_bytes);
    }
    drop(held);
    ledger.release(held_bytes)?;
    assert_eq!(ledger.live_bytes(), 0);
    Ok(())
}
