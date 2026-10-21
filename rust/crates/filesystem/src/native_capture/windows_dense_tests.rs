use super::*;
use proptest::prelude::*;

#[derive(Default)]
struct Probe {
    query: bool,
    omit_finish: bool,
    edit: Option<Box<dyn FnOnce()>>,
}

thread_local! {
    static PROBE: std::cell::RefCell<Probe> = std::cell::RefCell::new(Probe::default());
}

pub(super) fn force_query() -> bool {
    PROBE.with_borrow(|probe| probe.query)
}

pub(super) fn omit_finish() -> bool {
    PROBE.with_borrow(|probe| probe.omit_finish)
}

pub(super) fn after_ranges() {
    let edit = PROBE.with_borrow_mut(|probe| probe.edit.take());
    if let Some(edit) = edit {
        edit();
    }
}

struct Reset;

impl Drop for Reset {
    fn drop(&mut self) {
        PROBE.set(Probe::default());
    }
}

async fn capture(
    initial: &crate::facade::MemoryCheckout,
    path: &NamespacePath,
    options: &CaptureOptions,
    setup: &NativeMaterializationWork,
    probe: Probe,
    budget: WorkBudget,
) -> (
    crate::facade::MemoryCheckout,
    Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>>,
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
    PROBE.set(probe);
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
    (checkout, result, before)
}

fn fixture(path: &Path, kind: u8, length: usize, byte: u8) {
    use std::io::{Seek as _, Write as _};
    if matches!(kind, 1 | 2) {
        super::empty_stage_tests::sparse_file(path, length as u64);
        if kind == 2 {
            let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            file.write_all(&[byte]).unwrap();
            file.seek(std::io::SeekFrom::End(-1)).unwrap();
            file.write_all(&[byte]).unwrap();
        }
    } else {
        std::fs::write(path, vec![byte; length]).unwrap();
        if kind == 3 {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            acyclic_native_runtime::device_control_in_place(
                &file,
                windows::Win32::System::Ioctl::FSCTL_SET_COMPRESSION,
                &2_u16.to_le_bytes(),
                &mut [],
                None,
            )
            .unwrap();
        } else if kind == 4 {
            std::fs::write(path.with_file_name("file:dense"), [byte; 3]).unwrap();
        }
    }
}

#[test]
fn dense_capture_equals_queried_capture_with_bounded_admission()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let initial = runtime.block_on(async {
        crate::Fs::memory()
            .create_workspace("dense-ranges", WorkBudget::UNBOUNDED, setup.cancellation())
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
        cases: 16,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Every generated length/content covers dense, fully sparse, sparse with data,
    // compressed and alternate-stream fixtures through the production transaction.
    runner.run(
        &(
            prop_oneof![
                Just(1_usize),
                Just(80),
                Just(4096),
                Just(65537),
                1_usize..8193
            ],
            any::<u8>(),
        ),
        |(length, byte)| {
            runtime.block_on(async {
                for kind in 0..5 {
                    let source = if kind == 3 {
                        crate::windows_test_filesystems::ntfs_tempdir().unwrap()
                    } else {
                        tempfile::tempdir().unwrap()
                    };
                    fixture(&source.path().join("file"), kind, length, byte);
                    let options = CaptureOptions {
                        source_root: source.path().to_owned(),
                        expected_root_identity: capture_root_identity(source.path()).unwrap(),
                        maximum_paths: 8,
                        maximum_extent_spans: 8,
                    };
                    // Share the authored identity; independent creation deliberately mints
                    // different FileIds, unrelated to allocation-range discovery.
                    let (mut baseline, seeded, _) = capture(
                        &initial,
                        &path,
                        &options,
                        &setup,
                        Probe {
                            query: true,
                            ..Probe::default()
                        },
                        WorkBudget::UNBOUNDED,
                    )
                    .await;
                    seeded.unwrap();
                    baseline
                        .read_metadata(&path, &setup, setup.cancellation())
                        .await
                        .unwrap();
                    // Reuse compares the prior extent plan too. Prime that same
                    // immutable page so the two variants pay only their own work.
                    let (lookup, _custody) = baseline
                        .lookup_batch_no_follow(
                            std::slice::from_ref(&path),
                            &setup,
                            setup.cancellation(),
                        )
                        .await
                        .unwrap();
                    let record = lookup.entries[0].record.unwrap();
                    if matches!(record.payload, crate::kernel::FilePayload::Regular { .. }) {
                        let mut accounting =
                            crate::kernel::KernelAccounting::native(&setup, setup.cancellation());
                        drop(
                            baseline
                                .plan_record_extents(
                                    record,
                                    crate::ByteRange {
                                        offset: 0,
                                        length: length as u64,
                                    },
                                    8,
                                    &mut accounting,
                                    setup.cancellation(),
                                )
                                .await
                                .unwrap(),
                        );
                    }
                    let mut outcomes = Vec::new();
                    for query in [true, false] {
                        let (mut checkout, result, before) = capture(
                            &baseline,
                            &path,
                            &options,
                            &setup,
                            Probe {
                                query,
                                ..Probe::default()
                            },
                            WorkBudget::UNBOUNDED,
                        )
                        .await;
                        let receipt = result.unwrap();
                        let reads =
                            receipt.work.backend_read_operations - before.backend_read_operations;
                        let record =
                            super::empty_stage_tests::encoded_record(&mut checkout, &path, &setup)
                                .await;
                        outcomes.push((
                            checkout.root().clone(),
                            record,
                            reads,
                            receipt.value.staged_file_bytes,
                        ));
                    }
                    prop_assert_eq!(&outcomes[0].0, &outcomes[1].0);
                    prop_assert_eq!(&outcomes[0].1, &outcomes[1].1);
                    prop_assert_eq!(outcomes[0].3, outcomes[1].3);
                    prop_assert_eq!(
                        outcomes[0].2 - outcomes[1].2,
                        if matches!(kind, 0 | 4) { 2 } else { 0 }
                    );
                    // Interior I/O admission prefixes must fail atomically, or produce the
                    // exact authenticated result at the measured boundary.
                    if kind == 0 {
                        for (index, query) in [true, false].into_iter().enumerate() {
                            let reads = outcomes[index].2;
                            for limit in [0, reads / 2, reads - 1, reads] {
                                let (checkout, result, before) = capture(
                                    &baseline,
                                    &path,
                                    &options,
                                    &setup,
                                    Probe {
                                        query,
                                        ..Probe::default()
                                    },
                                    WorkBudget {
                                        backend_read_operations: limit,
                                        ..WorkBudget::UNBOUNDED
                                    },
                                )
                                .await;
                                let completed = match result {
                                    Ok(receipt) => {
                                        prop_assert_eq!(limit, reads);
                                        prop_assert_eq!(checkout.root(), &outcomes[index].0);
                                        receipt.work
                                    }
                                    Err(failure) => {
                                        prop_assert!(limit < reads);
                                        prop_assert_eq!(checkout.root(), baseline.root());
                                        *failure.work
                                    }
                                };
                                prop_assert!(
                                    completed.backend_read_operations
                                        - before.backend_read_operations
                                        <= limit
                                );
                            }
                        }
                    }
                }
                // A growth after the range decision must reject both implementations.
                // Omitting production finalization is the deliberately broken control.
                for (query, omit_finish) in [(true, false), (false, false), (false, true)] {
                    let source = tempfile::tempdir().unwrap();
                    let file = source.path().join("file");
                    fixture(&file, 0, length, byte);
                    let options = CaptureOptions {
                        source_root: source.path().to_owned(),
                        expected_root_identity: capture_root_identity(source.path()).unwrap(),
                        maximum_paths: 8,
                        maximum_extent_spans: 8,
                    };
                    let (checkout, result, _) = capture(
                        &initial,
                        &path,
                        &options,
                        &setup,
                        Probe {
                            query,
                            omit_finish,
                            edit: Some(Box::new(move || {
                                std::fs::write(file, vec![byte; length + 1]).unwrap()
                            })),
                        },
                        WorkBudget::UNBOUNDED,
                    )
                    .await;
                    prop_assert_eq!(result.is_ok(), omit_finish);
                    if !omit_finish {
                        prop_assert_eq!(checkout.root(), initial.root());
                    }
                }
                Ok(())
            })
        },
    )?;
    Ok(())
}

#[test]
fn dense_range_classification_rejects_every_unfamiliar_attribute() {
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    runner
        .run(&any::<u32>(), |attributes| {
            let ordinary = 0x01 | 0x02 | 0x04 | 0x20 | 0x80 | 0x100 | 0x2000;
            prop_assert_eq!(dense_attributes(attributes), attributes & !ordinary == 0);
            for bit in 0..32 {
                let flag = 1_u32 << bit;
                if flag & ordinary == 0 {
                    let marked = attributes & ordinary | flag;
                    prop_assert!(!dense_attributes(marked));
                    // A broken classifier ignoring this unknown flag must be caught.
                    prop_assert_ne!(dense_attributes(marked & !flag), dense_attributes(marked));
                }
            }
            Ok(())
        })
        .unwrap();
}
