use super::*;
use crate::native_host::stat_probe::Probe;
use acyclic_test_support::{AllocationInfo, differential, measure};

struct Fixture {
    _source: tempfile::TempDir,
    checkout: crate::facade::MemoryCheckout,
    setup: NativeMaterializationWork,
    options: CaptureOptions,
    paths: Vec<NamespacePath>,
    _health: Option<acyclic_native_runtime::NativeFile>,
}

fn hold_health(root: &Path) -> acyclic_native_runtime::NativeFile {
    acyclic_native_runtime::NativeFile::from_file(
        std::fs::File::open(root.join("d/body")).unwrap(),
        None,
    )
    .map_err(|(error, _)| error)
    .unwrap()
}

fn fixture(runtime: &tokio::runtime::Runtime) -> Fixture {
    fixture_in(runtime, None)
}

fn fixture_in(runtime: &tokio::runtime::Runtime, parent: Option<&Path>) -> Fixture {
    let source = match parent {
        Some(parent) => tempfile::tempdir_in(parent),
        None => tempfile::tempdir(),
    }
    .unwrap();
    std::fs::create_dir(source.path().join("d")).unwrap();
    std::fs::write(source.path().join("empty"), []).unwrap();
    std::fs::write(source.path().join("d/body"), [0x5a; 96]).unwrap();
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let mut checkout = runtime
        .block_on(super::capture_policy_tests::native_checkout(&setup))
        .unwrap();
    let limits = checkout.volume_config().limits;
    let paths = ["/empty", "/d/body"]
        .map(|text| {
            NamespacePath::from_portable(
                &crate::path::PortablePath::parse(text, limits).unwrap(),
                limits,
            )
            .unwrap()
        })
        .to_vec();
    let options = CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: capture_root_identity(source.path()).unwrap(),
        maximum_paths: Some(8),
        maximum_extent_spans: Some(8),
    };
    let health = hold_health(source.path());
    for _ in 0..2 {
        runtime
            .block_on(capture_subtrees_with_native_work(
                &mut checkout,
                &[NamespacePath::new(Vec::new(), limits).unwrap()],
                &options,
                &CapturePolicy::default(),
                setup.cancellation(),
                None,
                &setup,
            ))
            .unwrap();
    }
    Fixture {
        _source: source,
        checkout,
        setup,
        options,
        paths,
        _health: Some(health),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Output {
    result: Result<CaptureReceipt, String>,
    bytes: Vec<Vec<u8>>,
    unchanged_root: bool,
    work: serde_json::Value,
    health_items: u64,
    live_before: u64,
    live_after: u64,
    root_path_units: u64,
}

#[cfg(target_os = "linux")]
const RESCAN_CALLS: &[&str] = &[
    "openat",
    "fstat",
    "statx",
    "openat",
    "getdents64",
    "statx",
    "statx",
    "getdents64",
    "openat",
    "openat",
    "getdents64",
    "statx",
    "getdents64",
    "openat",
    "statx",
    "statx",
    "statx",
    "openat",
    "openat",
    "statx",
    "statx",
    "openat",
    "statx",
];

#[cfg(windows)]
const RESCAN_CALLS: &[&str] = &[
    "GetFullPathNameW",
    "GetFullPathNameW",
    "CreateFileW",
    "GetFileInformationByHandle",
    "NtQueryInformationFile",
    "GetFileInformationByHandle",
    "NtCreateFile",
    "GetFileInformationByHandleEx",
    "NtQueryInformationByName",
    "NtQueryInformationByName",
    "GetFileInformationByHandleEx",
    "NtCreateFile",
    "GetFileInformationByHandle",
    "NtCreateFile",
    "GetFileInformationByHandleEx",
    "NtQueryInformationByName",
    "GetFileInformationByHandleEx",
    "NtCreateFile",
    "NtQueryInformationFile",
    "NtQueryInformationFile",
    "NtQueryInformationByName",
    "NtCreateFile",
    "NtCreateFile",
    "NtQueryInformationFile",
    "NtQueryInformationFile",
    "NtCreateFile",
    "NtQueryInformationByName",
];

// One exact native trace and the historical same-cut heap measurement are the
// oracle for both production and every restored-work control. Storage-dependent
// allocations belong to that cut, rather than a fixed fixture-wide constant.
fn minimal_rescan(
    heap: AllocationInfo,
    reference: AllocationInfo,
    trace: &[&str],
    fallback: bool,
) -> bool {
    let expected = RESCAN_CALLS.iter().copied().flat_map(|call| {
        [Some(call), (fallback && call == "statx").then_some("fstat")]
            .into_iter()
            .flatten()
    });
    heap == reference && trace.iter().copied().eq(expected)
}

fn run(
    runtime: &tokio::runtime::Runtime,
    fixture: &mut Fixture,
    old: bool,
    fault: u8,
    limit: u64,
    cancelled: bool,
) -> (Output, AllocationInfo) {
    #[cfg(windows)]
    let root_path_units = {
        use std::os::windows::ffi::OsStrExt as _;
        fixture
            .options
            .source_root
            .as_os_str()
            .encode_wide()
            .count() as u64
    };
    #[cfg(target_os = "linux")]
    let root_path_units = {
        use std::os::unix::ffi::OsStrExt as _;
        fixture.options.source_root.as_os_str().as_bytes().len() as u64
    };
    let (work, cancellation) = fixture
        .setup
        .native_volume_work(WorkBudget {
            backend_read_operations: limit,
            ..WorkBudget::UNBOUNDED
        })
        .unwrap()
        .invocation_view()
        .unwrap();
    let root = fixture.checkout.root().clone();
    let path = NamespacePath::new(Vec::new(), fixture.checkout.volume_config().limits).unwrap();
    let baseline = NativeViewBaseline::new(fixture.options.expected_root_identity, true);
    if cancelled {
        cancellation.cancel();
    }
    let live_before = work.live_allocation_bytes();
    let before = work.receipt();
    let probe = Probe::rescan(old);
    if fault == 1 {
        probe.restore_enumeration();
    }
    if fault == 2 {
        probe.duplicate_queries();
    }
    if fault == 4 {
        probe.restore_birth_time_errors();
    }
    let _owner = (fault == 3).then(super::owner_allocation_tests::restore_owner);
    let mut result = None;
    let roots = [path];
    let policy = CapturePolicy::default();
    let heap = {
        let mut future = std::pin::pin!(capture_subtrees_with_native_work(
            &mut fixture.checkout,
            &roots,
            &fixture.options,
            &policy,
            &cancellation,
            Some(&baseline),
            &work,
        ));
        measure(|| result = Some(runtime.block_on(future.as_mut())))
    };
    probe.publish();
    // This is the actual admitted search work, observed while the registry held
    // its lock, rather than a calibration that parallel imports could invalidate.
    let health_items = probe.examinations();
    drop(probe);
    let result = result
        .unwrap()
        .map(|receipt| receipt.value)
        .map_err(|failure| format!("{:?}", failure.error));
    let mut receipt = serde_json::to_value(work.receipt()).unwrap();
    let before = serde_json::to_value(before).unwrap();
    for (name, value) in receipt.as_object_mut().unwrap() {
        if name == "peakAllocationBytes" {
            // A high-water mark is not additive. Compare its actual final state
            // on the same ledger; measure() separately checks each physical peak.
            assert!(value.as_u64().unwrap() >= before[name].as_u64().unwrap());
        } else {
            *value = serde_json::json!(value.as_u64().unwrap() - before[name].as_u64().unwrap());
        }
    }
    let live_after = work.live_allocation_bytes();
    let unchanged_root = fixture.checkout.root() == &root;
    let bytes = fixture
        .paths
        .iter()
        .zip([0, 96])
        .map(|(path, length)| {
            runtime
                .block_on(fixture.checkout.read_file_range(
                    path,
                    crate::ByteRange { offset: 0, length },
                    &fixture.setup,
                    fixture.setup.cancellation(),
                ))
                .unwrap()
                .bytes
                .as_ref()
                .to_vec()
        })
        .collect();
    (
        Output {
            result,
            bytes,
            unchanged_root,
            work: receipt,
            health_items,
            live_before,
            live_after,
            root_path_units,
        },
        heap,
    )
}

#[test]
fn complete_rescans_have_exact_native_calls_heap_and_unchanged_outputs()
-> Result<(), Box<dyn std::error::Error>> {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    for (forced, without_birth_time) in [(false, false), (false, true), (true, false)] {
        #[cfg(windows)]
        if forced || without_birth_time {
            continue;
        }
        #[cfg(target_os = "linux")]
        let _statx =
            crate::native_host::stat_probe::StatxOptions::enter(forced, without_birth_time);
        #[cfg(target_os = "linux")]
        let fallback = crate::native_host::stat_probe::uses_fallback(&std::fs::File::open(".")?);
        #[cfg(windows)]
        let fallback = false;
        for (limit, cancelled) in [(u64::MAX, false), (0, false), (4, false), (u64::MAX, true)] {
            // Both variants observe the same source and authenticated cut. Each
            // run still receives fresh resource credit and cancellation custody.
            let mut state = fixture(&runtime);
            // Warm both capture and the output reads before either measurement.
            run(&runtime, &mut state, false, 0, u64::MAX, false);
            let paired = [true, false].map(|old| {
                differential::observe::<_, _, &'static str>(
                    || (),
                    |()| run(&runtime, &mut state, old, 0, limit, cancelled),
                )
            });
            for ((output, heap), trace) in &paired {
                assert_eq!(output.bytes, [vec![], vec![0x5a; 96]]);
                assert!(output.unchanged_root);
                assert_eq!(output.result.is_ok(), limit == u64::MAX && !cancelled);
                assert_eq!(output.live_after, output.live_before);
                eprintln!(
                    "RESCAN limit={limit} cancel={cancelled} heap={heap:?} work={:?} health={} trace={trace:?}",
                    output.work, output.health_items
                );
            }
            let [
                ((mut before, before_heap), old_trace),
                ((mut after, after_heap), trace),
            ] = paired;
            before.work["itemsExamined"] = serde_json::json!(
                before.work["itemsExamined"].as_u64().unwrap() - before.health_items
            );
            after.work["itemsExamined"] = serde_json::json!(
                after.work["itemsExamined"].as_u64().unwrap() - after.health_items
            );
            if after.result.is_ok() {
                assert!(before.health_items > 0 && after.health_items > 0);
            }
            assert_eq!(before.result, after.result);
            assert_eq!(before.work, after.work);
            assert_eq!(before_heap, after_heap);
            #[cfg(windows)]
            if limit == u64::MAX && !cancelled {
                assert!(
                    minimal_rescan(after_heap, before_heap, &trace, fallback),
                    "heap={after_heap:?}; trace={trace:?}"
                );
                assert!(minimal_rescan(
                    before_heap,
                    before_heap,
                    &old_trace,
                    fallback
                ));
            }
            #[cfg(target_os = "linux")]
            {
                if limit == u64::MAX && !cancelled {
                    assert!(minimal_rescan(after_heap, before_heap, &trace, fallback));
                    assert_eq!(
                        minimal_rescan(before_heap, before_heap, &old_trace, fallback),
                        fallback
                    );
                    assert_eq!(
                        old_trace.iter().filter(|event| **event == "handle").count(),
                        if fallback { 0 } else { 10 }
                    );
                } else if !cancelled {
                    assert_eq!(
                        trace,
                        if limit == 0 {
                            vec![]
                        } else if fallback {
                            vec!["openat", "fstat", "statx", "fstat"]
                        } else {
                            vec!["openat", "fstat", "statx"]
                        }
                    );
                }
            }
            assert_eq!(
                old_trace
                    .iter()
                    .filter(|event| **event != "handle")
                    .collect::<Vec<_>>(),
                trace.iter().collect::<Vec<_>>()
            );
            if limit == u64::MAX && !cancelled {
                for fault in [1, 2, 3, 4] {
                    if fault == 4 && !(forced || without_birth_time) {
                        continue; // The old conversion allocates only for absent birth times.
                    }
                    if cfg!(windows) && fault == 1 {
                        continue; // next_metadata already forwards to next on Windows.
                    }
                    let ((broken, broken_heap), broken_trace) =
                        differential::observe::<_, _, &'static str>(
                            || (),
                            |()| run(&runtime, &mut state, false, fault, limit, false),
                        );
                    assert_eq!(broken.bytes, after.bytes);
                    assert_eq!(broken.result, after.result);
                    if (cfg!(target_os = "linux") && fault == 1 && !fallback) || fault == 2 {
                        assert_ne!(broken_trace, trace);
                    }
                    if fault == 3 {
                        assert_eq!(broken_heap.count_total, after_heap.count_total + 1);
                    }
                    if fault == 4 {
                        assert_eq!(broken_heap.count_total, after_heap.count_total + 48);
                        assert_eq!(broken_trace, trace);
                        eprintln!(
                            "BIRTH_TIME fallback={fallback} masked={without_birth_time} old={} optional={}",
                            broken_heap.count_total, after_heap.count_total
                        );
                    }
                    assert_eq!(
                        minimal_rescan(broken_heap, before_heap, &broken_trace, fallback),
                        fallback && fault == 1
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn exact_rescan_work_tracks_native_root_names() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // Keep the physical source, authored root, store and warmed proofs identical.
    // Renaming an ancestor changes only the root spelling: the captured directory
    // itself and all its descendants retain their identities and metadata.
    let parent = tempfile::tempdir().unwrap();
    let short = parent.path().join("rescan");
    let long = parent.path().join("longer-rescan-Ω-🦀");
    std::fs::create_dir(&short).unwrap();
    let mut fixture = fixture_in(&runtime, Some(&short));
    let source_name = fixture._source.path().file_name().unwrap().to_owned();
    let mut observations = Vec::new();
    for ancestor in [&short, &long] {
        // Windows requires descendant handles to close before an ancestor moves.
        // Reopen the same health registration before either measured capture.
        drop(fixture._health.take());
        if ancestor == &long {
            std::fs::rename(&short, &long).unwrap();
            fixture.options.source_root = long.join(&source_name);
        }
        fixture._health = Some(hold_health(&fixture.options.source_root));
        let (output, heap) = run(&runtime, &mut fixture, false, 0, u64::MAX, false);
        assert!(output.result.is_ok());
        assert!(output.unchanged_root);
        assert_eq!(output.bytes, [vec![], vec![0x5a; 96]]);
        let items = output.work["itemsExamined"].as_u64().unwrap() - output.health_items;
        observations.push((output.root_path_units, items, heap.count_total));
    }
    drop(fixture._health.take());
    std::fs::rename(&long, &short).unwrap();
    assert_eq!(observations[0].2, observations[1].2);
    assert_eq!(
        observations[1].1 - observations[0].1,
        (if cfg!(windows) { 2 } else { 1 }) * (observations[1].0 - observations[0].0)
    );
    assert_ne!(
        observations[0].1, observations[1].1,
        "fixed-count control accepted both roots"
    );
}
