use super::*;
use proptest::prelude::*;

fn empty_stage_matches(
    ranges: &[(HostDataRange, StagedContent)],
    staged: u64,
    before: u64,
    read: u64,
) -> proptest::test_runner::TestCaseResult {
    prop_assert!(ranges.is_empty());
    prop_assert_eq!(staged, before);
    prop_assert_eq!(read, 0);
    Ok(())
}

fn finalization_matches(accepted: bool, expected: bool) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(accepted, expected);
    Ok(())
}

#[test]
fn empty_staging_preserves_bytes_and_retires_custody() -> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancellation = CancellationToken::new();
    let workspace = runtime
        .block_on(crate::Fs::memory().create_workspace(
            "empty-stage",
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value;
    let checkout = runtime
        .block_on(workspace.checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // No content ranges means no source bytes, no staged-byte increment, and
    // no retained allocation after the returned empty vector retires.
    runner.run(
        &(
            0_u64..1024,
            prop_oneof![Just(0_u64), 1_u64..65537],
            any::<bool>(),
        ),
        |(before, length, cancelled)| {
            let source = tempfile::tempdir().unwrap();
            let file = std::fs::File::create(source.path().join("file")).unwrap();
            #[cfg(windows)]
            acyclic_native_runtime::device_control_in_place(
                &file,
                windows::Win32::System::Ioctl::FSCTL_SET_SPARSE,
                &[],
                &mut [],
                None,
            )
            .unwrap();
            file.set_len(length).unwrap();
            drop(file);
            let root = HostRoot::open(source.path(), &mut crate::native_host::ordinary_work())
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            let snapshot = test_host_stat(&root, Path::new("file")).unwrap();
            let cancellation = CancellationToken::new();
            let original =
                NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
            let initial = original.live_allocation_bytes();
            if cancelled {
                cancellation.cancel();
            }
            let mut staged = before;
            let result = runtime.block_on(stage_regular_body(
                &checkout,
                &root,
                &test_host_path(Path::new("file")),
                &snapshot,
                8,
                &original,
                &mut staged,
            ));
            if cancelled {
                prop_assert!(result.is_err());
            } else {
                let ranges = result.unwrap();
                empty_stage_matches(
                    &ranges,
                    staged,
                    before,
                    original.receipt().source_bytes_read,
                )?;
                // A deliberately broken staging result increments bytes without data.
                prop_assert!(empty_stage_matches(&ranges, staged + 1, before, 0).is_err());
                drop(ranges);
            }
            prop_assert_eq!(original.live_allocation_bytes(), initial);
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn empty_finalization_rejects_growth_replacement_removal_and_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // The same held-file and current-path proof is required for every outcome,
    // including files for which no content reader or worker is necessary.
    runner.run(&(1_usize..17), |length| {
        for edit in 0..5 {
            let source = tempfile::tempdir().unwrap();
            let path = source.path().join("file");
            std::fs::write(&path, []).unwrap();
            let root = HostRoot::open(source.path(), &mut crate::native_host::ordinary_work())
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            let snapshot = test_host_stat(&root, Path::new("file")).unwrap();
            let held = test_open_file(&root, Path::new("file")).unwrap().into_std();
            let cancellation = CancellationToken::new();
            let original =
                NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
            match edit {
                1 => std::fs::write(&path, vec![b'x'; length]).unwrap(),
                2 => {
                    let replacement = source.path().join("replacement");
                    std::fs::write(&replacement, []).unwrap();
                    std::fs::rename(&path, source.path().join("displaced")).unwrap();
                    std::fs::rename(replacement, &path).unwrap();
                }
                3 => std::fs::remove_file(&path).unwrap(),
                4 => cancellation.cancel(),
                _ => {}
            }
            let accepted = finish_regular_capture(
                &root,
                &test_host_path(Path::new("file")),
                &snapshot,
                &held,
                &original,
            )
            .is_ok();
            let expected = edit == 0;
            finalization_matches(accepted, expected)?;
            // Deliberately omit final validation: it must fail for every edit.
            if !expected {
                prop_assert!(finalization_matches(true, expected).is_err());
            }
        }
        Ok(())
    })?;
    Ok(())
}
