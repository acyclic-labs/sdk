use super::*;
use crate::native_host::stat_probe::Probe;
use acyclic_test_support::{AllocationInfo, differential, measure};

struct Fixture {
    _source: tempfile::TempDir,
    checkout: crate::facade::MemoryCheckout,
    setup: NativeMaterializationWork,
    options: CaptureOptions,
    paths: Vec<NamespacePath>,
    _health: acyclic_native_runtime::NativeFile,
}

fn fixture(runtime: &tokio::runtime::Runtime) -> Fixture {
    fixture_with_prefix(runtime, ".tmp")
}

fn fixture_with_prefix(runtime: &tokio::runtime::Runtime, prefix: &str) -> Fixture {
    let source = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
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
        maximum_paths: 8,
        maximum_extent_spans: 8,
    };
    let health = acyclic_native_runtime::NativeFile::from_file(
        std::fs::File::open(source.path().join("d/body")).unwrap(),
        None,
    )
    .map_err(|(error, _)| error)
    .unwrap();
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
        _health: health,
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

// One oracle for the candidate and every restored-work control. Literal heap
// counts include the complete production rescan, excluding fixture construction
// and trace publication. They are independent of work receipts and the baseline.
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

fn minimal_rescan(heap: AllocationInfo, trace: &[&str], fallback: bool) -> bool {
    // Main's lazy append-error change removes one discarded error allocation
    // for each of the three host names and three checkout names in this fixture.
    let expected = RESCAN_CALLS.iter().copied().flat_map(|call| {
        [Some(call), (fallback && call == "statx").then_some("fstat")]
            .into_iter()
            .flatten()
    });
    heap.count_total == if cfg!(windows) { 324 } else { 331 } && trace.iter().copied().eq(expected)
}

fn run(
    runtime: &tokio::runtime::Runtime,
    mut fixture: Fixture,
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
    let _index = (fault == 5).then(crate::logical_objects::restore_eager_index);
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
        // The original ledger includes fixture work. For its monotone peak this
        // is the increase over the prior peak; physical peak storage is also
        // compared independently by measure().
        *value = serde_json::json!(value.as_u64().unwrap() - before[name].as_u64().unwrap());
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
            // Separate host roots, stores, caches, checkouts, cancellations and ledgers.
            let paired = differential::pair::<_, _, &'static str>(
                || fixture(&runtime),
                |state, old| run(&runtime, state, old, 0, limit, cancelled),
            );
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
                assert_eq!(after.work["itemsExamined"], expected_rescan_items(&after));
            }
            assert_eq!(before.result, after.result);
            assert_eq!(before.work, after.work);
            assert_eq!(before_heap, after_heap);
            #[cfg(windows)]
            if limit == u64::MAX && !cancelled {
                assert!(
                    minimal_rescan(after_heap, &trace, fallback),
                    "heap={after_heap:?}; trace={trace:?}"
                );
                assert!(minimal_rescan(before_heap, &old_trace, fallback));
            }
            #[cfg(target_os = "linux")]
            {
                if limit == u64::MAX && !cancelled {
                    assert!(minimal_rescan(after_heap, &trace, fallback));
                    assert_eq!(minimal_rescan(before_heap, &old_trace, fallback), fallback);
                    assert_eq!(
                        old_trace.iter().filter(|event| **event == "handle").count(),
                        if fallback { 0 } else { 10 }
                    );
                    assert_eq!(
                        after.work["backendReadOperations"],
                        if fallback { 52 } else { 42 }
                    );
                } else if !cancelled {
                    assert_eq!(after_heap.count_total, if limit == 0 { 11 } else { 15 });
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
                for fault in [1, 2, 3, 4, 5] {
                    if fault == 4 && !(forced || without_birth_time) {
                        continue; // The old conversion allocates only for absent birth times.
                    }
                    if cfg!(windows) && fault == 1 {
                        continue; // next_metadata already forwards to next on Windows.
                    }
                    let [_, ((broken, broken_heap), broken_trace)] =
                        differential::pair::<_, _, &'static str>(
                            || fixture(&runtime),
                            |state, old| {
                                run(
                                    &runtime,
                                    state,
                                    old,
                                    if old { 0 } else { fault },
                                    limit,
                                    false,
                                )
                            },
                        );
                    assert_eq!(broken.bytes, after.bytes);
                    assert_eq!(broken.result, after.result);
                    if (cfg!(target_os = "linux") && fault == 1 && !fallback) || fault == 2 {
                        assert_ne!(broken_trace, trace);
                    }
                    if fault == 3 {
                        assert_eq!(broken_heap.count_total, after_heap.count_total + 1);
                    }
                    if fault == 5 {
                        assert_eq!(broken_heap.count_total, after_heap.count_total + 1);
                        assert_eq!(broken_trace, trace);
                        assert!(
                            broken.work["itemsExamined"].as_u64().unwrap() - broken.health_items
                                > after.work["itemsExamined"].as_u64().unwrap()
                        );
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
                        minimal_rescan(broken_heap, &broken_trace, fallback),
                        fallback && fault == 1
                    );
                }
            }
        }
    }
    Ok(())
}

fn expected_rescan_items(output: &Output) -> u64 {
    #[cfg(windows)]
    {
        // with_root_name counts and encodes each original UTF-16 unit once.
        // The remaining 348 examinations belong to the fixed rescan fixture.
        348 + 2 * output.root_path_units
    }
    #[cfg(target_os = "linux")]
    {
        // prepare_unix_name examines each source-root byte once. The fixed
        // fixture contributes the other 316 examinations, independent of TMPDIR.
        316 + output.root_path_units
    }
}

#[test]
fn exact_rescan_work_tracks_native_root_names() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut observations = Vec::new();
    for prefix in ["rescan-", "longer-rescan-Ω-🦀-"] {
        let (output, heap) = run(
            &runtime,
            fixture_with_prefix(&runtime, prefix),
            false,
            0,
            u64::MAX,
            false,
        );
        assert!(output.result.is_ok());
        assert!(output.unchanged_root);
        assert_eq!(heap.count_total, if cfg!(windows) { 324 } else { 331 });
        let items = output.work["itemsExamined"].as_u64().unwrap() - output.health_items;
        assert_eq!(items, expected_rescan_items(&output));
        observations.push((output.root_path_units, items));
    }
    assert_eq!(
        observations[1].1 - observations[0].1,
        (if cfg!(windows) { 2 } else { 1 }) * (observations[1].0 - observations[0].0)
    );
    assert_ne!(
        observations[0].1, observations[1].1,
        "fixed-count control accepted both roots"
    );
}
