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
            sparse_file(&source.path().join("file"), length);
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

#[derive(Default)]
struct StageProbe {
    force_import: bool,
    omit_finish: bool,
    edit: Option<Box<dyn FnOnce()>>,
}

thread_local! {
    static PROBE: std::cell::RefCell<StageProbe> = const {
        std::cell::RefCell::new(StageProbe {
            force_import: false, omit_finish: false, edit: None,
        })
    };
}

pub(super) fn after_ranges(empty: bool) -> bool {
    #[cfg(target_os = "linux")]
    crate::native_host::stat_probe::after_ranges();
    #[cfg(windows)]
    super::windows_dense_tests::after_ranges();
    PROBE.with_borrow_mut(|probe| {
        if let Some(edit) = probe.edit.take() {
            assert!(empty, "fixture must reach the empty-range branch");
            edit();
        }
        empty && !probe.force_import
    })
}

pub(super) fn omit_finish() -> bool {
    #[cfg(windows)]
    if super::windows_dense_tests::omit_finish() {
        return true;
    }
    PROBE.with_borrow(|probe| probe.omit_finish)
}

struct Reset;

impl Drop for Reset {
    fn drop(&mut self) {
        PROBE.set(StageProbe::default());
    }
}

pub(super) fn sparse_file(path: &Path, length: u64) {
    let file = std::fs::File::create(path).unwrap();
    #[cfg(windows)]
    acyclic_native_runtime::device_control_in_place(
        &file,
        windows::Win32::System::Ioctl::FSCTL_SET_SPARSE,
        &[],
        &mut [],
        None,
    )
    .unwrap();
    #[cfg(windows)]
    {
        // Punch while nonresident: NTFS can report small resident zeros as data.
        let grown = length.max(65537);
        file.set_len(grown).unwrap();
        let mut zero = [0; 16];
        zero[8..].copy_from_slice(&grown.to_le_bytes());
        acyclic_native_runtime::device_control_in_place(
            &file,
            windows::Win32::System::Ioctl::FSCTL_SET_ZERO_DATA,
            &zero,
            &mut [],
            None,
        )
        .unwrap();
    }
    file.set_len(length).unwrap();
}

fn side_data(path: &Path, bytes: &[u8]) {
    #[cfg(target_os = "linux")]
    match rustix::fs::setxattr(
        path,
        c"user.empty729",
        bytes,
        rustix::fs::XattrFlags::empty(),
    ) {
        Ok(()) | Err(rustix::io::Errno::NOTSUP) => {}
        Err(error) => panic!("setxattr failed: {error}"),
    }
    #[cfg(windows)]
    std::fs::write(path.with_file_name("file:empty729"), bytes).unwrap();
    #[cfg(not(any(target_os = "linux", windows)))]
    let _ = (path, bytes);
}

fn acceptance_matches(accepted: bool, edit: u8) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(accepted, edit == 0);
    Ok(())
}

async fn run_capture(
    initial: &crate::facade::MemoryCheckout,
    path: &NamespacePath,
    options: &CaptureOptions,
    setup: &NativeMaterializationWork,
    variant: (bool, bool),
    edit: (u8, usize),
    budget: WorkBudget,
) -> (
    crate::facade::MemoryCheckout,
    Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>>,
    NativeMaterializationWork,
    WorkCounters,
) {
    let (work, cancellation) = setup
        .native_volume_work(budget)
        .unwrap()
        .invocation_view()
        .unwrap();
    let mut checkout = initial
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(&work, &cancellation),
            &cancellation,
        )
        .unwrap();
    let before = work.receipt();
    let cancel = cancellation.clone();
    let file = options.source_root.join("file");
    PROBE.set(StageProbe {
        force_import: variant.0,
        omit_finish: variant.1,
        edit: Some(Box::new(move || match edit.0 {
            1 => std::fs::write(&file, vec![b'x'; edit.1]).unwrap(),
            2 => {
                let replacement = file.with_file_name("replacement");
                std::fs::write(&replacement, []).unwrap();
                std::fs::rename(&file, file.with_file_name("displaced")).unwrap();
                std::fs::rename(replacement, &file).unwrap();
            }
            3 => std::fs::remove_file(&file).unwrap(),
            4 => cancel.cancel(),
            _ => {}
        })),
    });
    let _reset = Reset;
    let result = capture_subtrees_with_native_work(
        &mut checkout,
        std::slice::from_ref(path),
        options,
        &CapturePolicy::default(),
        &cancellation,
        None,
        &work,
    )
    .await;
    (checkout, result, work, before)
}

pub(super) async fn encoded_record(
    checkout: &mut crate::facade::MemoryCheckout,
    path: &NamespacePath,
    setup: &NativeMaterializationWork,
) -> Vec<u8> {
    let (lookup, _custody) = checkout
        .lookup_batch_no_follow(std::slice::from_ref(path), setup, setup.cancellation())
        .await
        .unwrap();
    crate::kernel::encode_file_record(
        lookup.entries[0].record.unwrap(),
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )
    .unwrap()
}

#[test]
fn empty_finalization_rejects_growth_replacement_removal_and_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let initial = runtime.block_on(async {
        crate::Fs::memory()
            .create_workspace(
                "empty-differential",
                WorkBudget::UNBOUNDED,
                setup.cancellation(),
            )
            .await
            .unwrap()
            .value
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                setup.cancellation(),
            )
            .await
            .unwrap()
            .value
    });
    let path = NamespacePath::from_portable(
        &crate::path::PortablePath::parse("/file", initial.volume_config().limits)?,
        initial.volume_config().limits,
    )?;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    runner.run(
        &(1_usize..17, prop::collection::vec(any::<u8>(), 0..17)),
        |(growth, attribute)| {
            runtime.block_on(async {
                for length in [0, 65537] {
                    let source = tempfile::tempdir().unwrap();
                    let file = source.path().join("file");
                    sparse_file(&file, length);
                    side_data(&file, &attribute);
                    let options = CaptureOptions {
                        source_root: source.path().to_owned(),
                        expected_root_identity: capture_root_identity(source.path()).unwrap(),
                        maximum_paths: 8,
                        maximum_extent_spans: 8,
                    };
                    let (mut baseline, seeded, _, _) = run_capture(
                        &initial,
                        &path,
                        &options,
                        &setup,
                        (true, false),
                        (0, growth),
                        WorkBudget::UNBOUNDED,
                    )
                    .await;
                    seeded.unwrap();
                    // Compare admission at the same decoded-cache state: the first
                    // metadata lookup otherwise pays a cold read shared by later clones.
                    baseline
                        .read_metadata(&path, &setup, setup.cancellation())
                        .await
                        .unwrap();
                    for edit in 0..5 {
                        let mut outcomes = Vec::new();
                        for force_import in [true, false] {
                            if edit != 0 {
                                if file.exists() {
                                    std::fs::remove_file(&file).unwrap();
                                }
                                sparse_file(&file, length);
                                side_data(&file, &attribute);
                            }
                            let (mut checkout, result, _work, before) = run_capture(
                                &baseline,
                                &path,
                                &options,
                                &setup,
                                (force_import, false),
                                (edit, growth),
                                WorkBudget::UNBOUNDED,
                            )
                            .await;
                            acceptance_matches(result.is_ok(), edit)?;
                            if edit == 0 {
                                let receipt = result.unwrap();
                                prop_assert_eq!(receipt.value.staged_file_bytes, 0);
                                outcomes.push((
                                    checkout.root().clone(),
                                    encoded_record(&mut checkout, &path, &setup).await,
                                    receipt.work.backend_read_operations
                                        - before.backend_read_operations,
                                    receipt.work.source_bytes_read - before.source_bytes_read,
                                ));
                            } else {
                                prop_assert_eq!(checkout.root(), baseline.root());
                            }
                        }
                        if edit == 0 {
                            prop_assert_eq!(&outcomes[0].0, &outcomes[1].0);
                            prop_assert_eq!(&outcomes[0].1, &outcomes[1].1);
                            prop_assert_eq!(outcomes[0].3, outcomes[1].3);
                            prop_assert!(outcomes[0].2 > outcomes[1].2);
                            for force_import in [true, false] {
                                let index = usize::from(!force_import);
                                let reads = outcomes[index].2;
                                for limit in [reads - 1, reads] {
                                    let budget = WorkBudget {
                                        backend_read_operations: limit,
                                        ..WorkBudget::UNBOUNDED
                                    };
                                    let (mut bounded, result, _work, before) = run_capture(
                                        &baseline,
                                        &path,
                                        &options,
                                        &setup,
                                        (force_import, false),
                                        (0, growth),
                                        budget,
                                    )
                                    .await;
                                    let work = match result {
                                        Ok(receipt) => {
                                            prop_assert_eq!(limit, reads);
                                            prop_assert_eq!(bounded.root(), &outcomes[index].0);
                                            prop_assert_eq!(
                                                encoded_record(&mut bounded, &path, &setup).await,
                                                outcomes[index].1.clone()
                                            );
                                            receipt.work
                                        }
                                        Err(failure) => {
                                            prop_assert_eq!(limit + 1, reads);
                                            prop_assert_eq!(bounded.root(), baseline.root());
                                            *failure.work
                                        }
                                    };
                                    prop_assert!(
                                        work.backend_read_operations
                                            - before.backend_read_operations
                                            <= limit
                                    );
                                }
                            }
                        } else if edit != 4 {
                            // The identical production capture, deliberately omitting its
                            // final proof, must be rejected by the acceptance oracle.
                            if file.exists() {
                                std::fs::remove_file(&file).unwrap();
                            }
                            sparse_file(&file, length);
                            side_data(&file, &attribute);
                            let (_, broken, _, _) = run_capture(
                                &baseline,
                                &path,
                                &options,
                                &setup,
                                (false, true),
                                (edit, growth),
                                WorkBudget::UNBOUNDED,
                            )
                            .await;
                            prop_assert!(acceptance_matches(broken.is_ok(), edit).is_err());
                        }
                    }
                }
                Ok(())
            })
        },
    )?;
    Ok(())
}
