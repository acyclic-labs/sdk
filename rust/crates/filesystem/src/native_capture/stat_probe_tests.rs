use super::*;
use crate::native_host::stat_probe::Probe;
use proptest::prelude::*;
use std::os::unix::fs::PermissionsExt as _;

fn metadata_bytes(stat: &HostStat) -> Vec<u8> {
    crate::kernel::encode_file_metadata(
        capture_metadata(stat),
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )
    .unwrap()
}

#[test]
fn capture_metadata_queries_match_frozen_facts_without_generation_work() {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    let check = |(bytes, mode, kind, admitted, cancelled): (Vec<u8>, u32, u8, bool, bool)| {
        let mut outcomes = Vec::new();
        for old in [false, true] {
            // Separate files, descriptors, roots, cancellation and original ledgers.
            let source = tempfile::tempdir().unwrap();
            let path = source.path().join("file");
            std::fs::write(&path, &bytes).unwrap();
            let file = std::fs::File::open(&path).unwrap();
            let _link_root = if mode & 1 != 0 {
                let links = tempfile::tempdir().unwrap();
                std::fs::hard_link(&path, links.path().join("link")).unwrap();
                Some(links)
            } else {
                None
            };
            file.set_permissions(std::fs::Permissions::from_mode(mode))
                .unwrap();
            let root =
                HostRoot::open(source.path(), &mut crate::native_host::ordinary_work()).unwrap();
            let expected = if kind == 3 {
                crate::native_host::stat_probe::baseline_at(
                    root.directory_fd(),
                    c"",
                    &mut crate::native_host::ordinary_work(),
                )
            } else {
                crate::native_host::stat_probe::baseline(
                    &file,
                    &mut crate::native_host::ordinary_work(),
                )
            }
            .unwrap();
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(
                WorkBudget {
                    backend_read_operations: if kind == 2 {
                        2 + 2 * u64::from(admitted)
                    } else {
                        u64::from(admitted)
                    },
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .unwrap();
            let mut reader = if kind == 2 {
                Some(root.read_dir_stats(Path::new(""), &mut &work).unwrap())
            } else {
                None
            };
            let before = work.receipt();
            let live = work.live_allocation_bytes();
            if cancelled {
                cancellation.cancel();
            }
            let probe = Probe::new(old, None);
            let mut result = None;
            let heap = acyclic_test_support::measure(|| {
                result = Some(match kind {
                    0 => root
                        .stat_file_metadata(&file, &mut work.clone())
                        .map_err(capture_demand_error),
                    1 => root
                        .stat_source_metadata(Path::new("file"), &mut work.clone())
                        .map_err(capture_demand_error),
                    2 => reader
                        .as_mut()
                        .unwrap()
                        .0
                        .next_metadata(&mut &work)
                        .map(|entry| {
                            let (mut entry, allocation) = entry.expect("fixture has one file");
                            assert!(entry.is_named("file"));
                            let stat = entry.stat.take().unwrap();
                            drop(entry);
                            drop(allocation);
                            stat
                        })
                        .map_err(CaptureError::from),
                    _ => root
                        .stat_source_metadata(Path::new(""), &mut work.clone())
                        .map_err(capture_demand_error),
                });
            });
            let trace = probe.trace();
            drop(probe);
            let result = result
                .unwrap()
                .map(|stat| {
                    assert_eq!(metadata_bytes(&stat), metadata_bytes(&expected));
                    assert_eq!(stat.identity(), expected.identity());
                    assert_eq!(
                        (stat.len(), stat.nlink()),
                        (expected.len(), expected.nlink())
                    );
                    assert_eq!(stat.file_type(), expected.file_type());
                })
                .map_err(|error| format!("{error:?}"));
            let success = admitted && !cancelled;
            prop_assert_eq!(result.is_ok(), success);
            let allocated = !cancelled && (kind == 1 || (kind == 2 && success));
            let expected_work = before
                .checked_add(WorkCounters {
                    backend_read_operations: if success {
                        if kind == 2 { 2 } else { 1 }
                    } else {
                        0
                    },
                    allocation_operations: u64::from(allocated),
                    bytes_copied: if allocated { 4 } else { 0 },
                    items_examined: if allocated {
                        if kind == 2 { 3 } else { 4 }
                    } else {
                        0
                    },
                    peak_allocation_bytes: live
                        + if allocated {
                            if kind == 2 { 4 } else { 5 }
                        } else {
                            0
                        },
                    ..WorkCounters::UNCHARGED
                })
                .unwrap();
            prop_assert_eq!(work.receipt(), expected_work);
            prop_assert_eq!(work.live_allocation_bytes(), live);
            let expected_heap = u64::from(allocated) + u64::from(!success);
            prop_assert_eq!(heap.count_total, expected_heap);
            prop_assert_eq!(trace, if success { if old { 5 } else { 2 } } else { 1 });
            let minimal =
                |trace, heap| trace == if success { 2 } else { 1 } && heap == expected_heap;
            prop_assert_eq!(minimal(trace, heap.count_total), !(old && success));
            outcomes.push((result, work.receipt()));
            drop(reader);
            prop_assert_eq!(
                work.live_allocation_bytes(),
                NativeMaterializationWork::accounting_state_bytes()
            );
        }
        prop_assert_eq!(&outcomes[0], &outcomes[1]);
        Ok(())
    };
    for kind in 0..4 {
        for admitted in [false, true] {
            for cancelled in [false, true] {
                check((vec![0, 255, 42], 0o600, kind, admitted, cancelled)).unwrap();
            }
        }
    }
    runner
        .run(
            &(
                prop::collection::vec(any::<u8>(), 0..96),
                0_u32..0o1000,
                0_u8..4,
                any::<bool>(),
                any::<bool>(),
            ),
            check,
        )
        .unwrap();
}

#[test]
fn production_staging_preserves_bytes_errors_charges_and_final_path_checks()
-> Result<(), Box<dyn std::error::Error>> {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let stagers = [
        runtime.block_on(super::capture_policy_tests::native_checkout(&setup))?,
        runtime.block_on(super::capture_policy_tests::native_checkout(&setup))?,
    ];
    let check = |(sparse, limit, edit, growth, cancelled): (bool, u64, u8, u64, bool)| {
        let mut outcomes = Vec::new();
        for old in [false, true] {
            let source = tempfile::tempdir().unwrap();
            let path = source.path().join("file");
            let file = std::fs::File::create(&path).unwrap();
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .unwrap();
            file.set_len(if sparse { 65537 } else { 0 }).unwrap();
            let root =
                HostRoot::open(source.path(), &mut crate::native_host::ordinary_work()).unwrap();
            let snapshot = test_host_stat(&root, Path::new("file")).unwrap();
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(
                WorkBudget {
                    backend_read_operations: limit,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .unwrap();
            let scope = CaptureWorkScope::new(&work).unwrap();
            let before = work.receipt();
            let live = work.live_allocation_bytes();
            let cancel = cancellation.clone();
            let action: Option<(u64, Box<dyn FnOnce()>)> = match edit {
                1 => Some((0, Box::new(move || file.set_len(65538 + growth).unwrap()))),
                2 => Some((
                    0,
                    Box::new(move || {
                        file.set_permissions(std::fs::Permissions::from_mode(0o400))
                            .unwrap()
                    }),
                )),
                3 => Some((0, Box::new(move || cancel.cancel()))),
                4 => {
                    let replacement = source.path().join("replacement");
                    std::fs::write(&replacement, []).unwrap();
                    // Replace after the held-descriptor check, before the final path query.
                    Some((
                        3,
                        Box::new(move || std::fs::rename(replacement, path).unwrap()),
                    ))
                }
                _ => None,
            };
            if cancelled {
                cancellation.cancel();
            }
            let probe = Probe::new(old, action);
            let host_path = test_host_path(Path::new("file"));
            let mut staged_bytes = 17;
            let mut result = None;
            let heap = {
                // Pass a pointer-sized future to block_on: Tokio otherwise boxes
                // this large staging future inside the allocation measurement.
                let mut future = std::pin::pin!(stage_regular_body(
                    &stagers[usize::from(old)],
                    &root,
                    &host_path,
                    &snapshot,
                    8,
                    &scope.native,
                    &mut staged_bytes,
                ));
                acyclic_test_support::measure(|| result = Some(runtime.block_on(future.as_mut())))
            };
            let trace = probe.trace();
            drop(probe);
            let result = result.unwrap().map_err(|error| format!("{error:?}"));
            let extra = u64::from(sparse);
            let needed = match edit {
                1 | 2 => 3 + extra,
                3 => 2 + extra,
                _ => 4 + extra,
            };
            let success = !cancelled && edit == 0 && limit >= needed;
            prop_assert_eq!(result.is_ok(), success);
            if let Ok(ranges) = &result {
                prop_assert!(ranges.is_empty());
            }
            prop_assert_eq!(staged_bytes, 17);
            let path_check = !cancelled && limit >= 3 + extra && matches!(edit, 0 | 4);
            let strings = if cancelled {
                0
            } else {
                1 + u64::from(path_check)
            };
            let expected = before
                .checked_add(WorkCounters {
                    backend_read_operations: if cancelled { 0 } else { limit.min(needed) },
                    allocation_operations: strings,
                    bytes_copied: strings * 4,
                    items_examined: strings * 4,
                    peak_allocation_bytes: live + if strings != 0 { 5 } else { 0 },
                    ..WorkCounters::UNCHARGED
                })
                .unwrap();
            prop_assert_eq!(work.receipt(), expected);
            prop_assert_eq!(work.live_allocation_bytes(), live);
            let oracle = |old| {
                let mut bits = 1;
                for done in [
                    !cancelled && limit >= 2,
                    !cancelled && limit >= 3 + extra && edit != 3,
                ] {
                    if done {
                        bits *= 2;
                        if old {
                            bits = bits * 2 + 1;
                        }
                    }
                }
                if !cancelled && limit >= 4 + extra && matches!(edit, 0 | 4) {
                    bits *= 2;
                    if old {
                        bits = bits * 2 + 1;
                    }
                }
                bits
            };
            prop_assert_eq!(trace, oracle(old));
            let expected_heap = strings + u64::from(!success);
            prop_assert_eq!(
                heap.count_total,
                expected_heap,
                "sparse={} limit={} edit={} cancelled={} old={}",
                sparse,
                limit,
                edit,
                cancelled,
                old
            );
            let minimal = |trace, count| trace == oracle(false) && count == expected_heap;
            prop_assert_eq!(
                minimal(trace, heap.count_total),
                !(old && !cancelled && limit >= 2)
            );
            outcomes.push((result, expected));
            drop(scope);
            prop_assert_eq!(
                work.live_allocation_bytes(),
                NativeMaterializationWork::accounting_state_bytes()
            );
        }
        prop_assert_eq!(&outcomes[0], &outcomes[1]);
        Ok(())
    };
    for sparse in [false, true] {
        for limit in 0..=5 {
            for edit in 0..5 {
                check((sparse, limit, edit, 1, false)).unwrap();
            }
        }
        check((sparse, 5, 0, 1, true)).unwrap();
    }
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    runner.run(
        &(any::<bool>(), 0_u64..6, 0_u8..5, 1_u64..33, any::<bool>()),
        check,
    )?;
    Ok(())
}
