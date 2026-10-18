use super::*;
use acyclic_test_support::differential;
use acyclic_test_support::{AllocationInfo, measure};
use std::cell::Cell;

/// How a rescan treats a regular file whose prior record and metadata match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Reuse {
    /// Production: authenticate the content root before staging anything.
    Verified,
    /// Restored previous work: stage, writing every object, then compare.
    PersistFirst,
    /// Unsound control: trust matching metadata without reading a byte.
    MetadataOnly,
    /// Restores the rejected verify-then-stage path for sparse layouts.
    VerifySparse,
    /// A query confined to the large-file path, outside the old work oracle.
    RedundantLargeStat,
    /// Omits the large-file held-handle and final-path validation.
    OmitLargeValidation,
    /// Restores speculative verification followed by a second staging read.
    ReadTwice,
    /// Corrupts an observed metadata field despite preserving the body.
    WrongAccessed,
    /// Restores one unnecessary owner allocation on the regular-file path.
    ExtraAllocation,
}

thread_local! {
    static MODE: Cell<Reuse> = const { Cell::new(Reuse::Verified) };
}

pub(super) fn mode() -> Reuse {
    MODE.with(Cell::get)
}

pub(super) fn repeat_read(sparse: bool) -> bool {
    mode() == Reuse::ReadTwice || (sparse && mode() == Reuse::VerifySparse)
}

struct Mode(Reuse);

impl Mode {
    fn enter(mode: Reuse) -> Self {
        Self(MODE.with(|cell| cell.replace(mode)))
    }
}

impl Drop for Mode {
    fn drop(&mut self) {
        MODE.with(|cell| cell.set(self.0));
    }
}

pub(super) fn single_read() -> impl Drop {
    Mode::enter(Reuse::Verified)
}

const FILES: [(&str, usize); 3] = [("/empty", 0), ("/d/body", 96), ("/d/large", 70_000)];

fn body(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

struct Fixture {
    source: tempfile::TempDir,
    checkout: crate::facade::MemoryCheckout,
    setup: NativeMaterializationWork,
    options: CaptureOptions,
    paths: Vec<NamespacePath>,
    lengths: [u64; 3],
    budget: WorkBudget,
}

fn fixture(runtime: &tokio::runtime::Runtime) -> Fixture {
    fixture_with_sparse(runtime, None)
}

fn fixture_with_sparse(
    runtime: &tokio::runtime::Runtime,
    sparse: Option<(u64, usize, bool)>,
) -> Fixture {
    let source = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("d")).unwrap();
    for (name, length) in FILES {
        std::fs::write(
            source.path().join(name.trim_start_matches('/')),
            body(length, 7),
        )
        .unwrap();
    }
    if let Some((offset, length, split)) = sparse {
        use std::io::{Seek, SeekFrom, Write};
        // Isolate sparse admission: the other paths have empty bodies.
        std::fs::write(source.path().join("d/body"), []).unwrap();
        let mut file = std::fs::File::create(source.path().join("d/large")).unwrap();
        file.set_len(FILES[2].1 as u64).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&body(length, 7)).unwrap();
        if split {
            file.seek(SeekFrom::Start(offset + length as u64 + 4096))
                .unwrap();
            file.write_all(&body(length, 9)).unwrap();
        }
    }
    let setup =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let lengths = FILES.map(|(name, _)| {
        std::fs::metadata(source.path().join(name.trim_start_matches('/')))
            .unwrap()
            .len()
    });
    let mut checkout = runtime
        .block_on(super::capture_policy_tests::native_checkout(&setup))
        .unwrap();
    checkout.bind_authored_operation(crate::OperationId::from_bytes([23; 16]));
    let limits = checkout.volume_config().limits;
    let paths = FILES
        .map(|(text, _)| {
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
    Fixture {
        source,
        checkout,
        setup,
        options,
        paths,
        lengths,
        budget: WorkBudget::UNBOUNDED,
    }
}

#[derive(Debug)]
struct Output {
    result: Result<CaptureReceipt, String>,
    bytes: Vec<Vec<u8>>,
    metadata: Vec<FileMetadata>,
    unchanged_root: bool,
    live_before: u64,
    live_after: u64,
}

fn rescan(
    runtime: &tokio::runtime::Runtime,
    fixture: &mut Fixture,
    reuse: Reuse,
    cancelled: bool,
) -> (
    Output,
    serde_json::Value,
    AllocationInfo,
    crate::kernel::GenerationRoot,
) {
    rescan_with_baseline(runtime, fixture, reuse, cancelled, true)
}

fn rescan_with_baseline(
    runtime: &tokio::runtime::Runtime,
    fixture: &mut Fixture,
    reuse: Reuse,
    cancelled: bool,
    read_only: bool,
) -> (
    Output,
    serde_json::Value,
    AllocationInfo,
    crate::kernel::GenerationRoot,
) {
    let (work, cancellation) = fixture
        .setup
        .native_volume_work(fixture.budget)
        .unwrap()
        .invocation_view()
        .unwrap();
    if cancelled {
        cancellation.cancel();
    }
    let prior = fixture.checkout.root().clone();
    let baseline = NativeViewBaseline::new(fixture.options.expected_root_identity, read_only);
    let roots = [NamespacePath::new(Vec::new(), fixture.checkout.volume_config().limits).unwrap()];
    let policy = CapturePolicy::default();
    let live_before = work.live_allocation_bytes();
    let before = serde_json::to_value(work.receipt()).unwrap();
    let mut result = None;
    let heap = {
        let _mode = Mode::enter(reuse);
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
    let mut receipt = serde_json::to_value(work.receipt()).unwrap();
    for (name, value) in receipt.as_object_mut().unwrap() {
        *value = serde_json::json!(value.as_u64().unwrap() - before[name].as_u64().unwrap());
    }
    let live_after = work.live_allocation_bytes();
    #[cfg(target_os = "macos")]
    eprintln!(
        "NATIVE_REUSE heap={} copied={} work={receipt}",
        heap.count_total,
        crate::native_host::stat_probe::copied_bytes()
    );
    let bytes = fixture
        .paths
        .iter()
        .zip(fixture.lengths)
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
    let root = fixture.checkout.root().clone();
    let metadata = read_final_metadata(runtime, fixture);
    let output = Output {
        result: result
            .unwrap()
            .map(|receipt| receipt.value)
            .map_err(|failure| format!("{:?}", failure.error)),
        bytes,
        metadata,
        unchanged_root: root == prior,
        live_before,
        live_after,
    };
    (output, receipt, heap, root)
}

fn read_final_metadata(
    runtime: &tokio::runtime::Runtime,
    fixture: &mut Fixture,
) -> Vec<FileMetadata> {
    fixture
        .paths
        .iter()
        .map(|path| {
            let (lookup, _custody) = runtime
                .block_on(fixture.checkout.lookup_batch_no_follow(
                    std::slice::from_ref(path),
                    &fixture.setup,
                    fixture.setup.cancellation(),
                ))
                .unwrap();
            runtime
                .block_on(fixture.checkout.read_metadata(
                    lookup.entries[0].record.unwrap().file_id,
                    &fixture.setup,
                    fixture.setup.cancellation(),
                ))
                .unwrap()
        })
        .collect()
}

fn expected_metadata(
    runtime: &tokio::runtime::Runtime,
    fixture: &mut Fixture,
) -> Vec<FileMetadata> {
    let prior = read_final_metadata(runtime, fixture);
    let host = HostRoot::open(
        fixture.source.path(),
        &mut crate::native_host::ordinary_work(),
    )
    .unwrap();
    FILES
        .into_iter()
        .zip(prior)
        .map(|((name, _), prior)| {
            let stat = test_host_stat(&host, Path::new(name.trim_start_matches('/'))).unwrap();
            let mut metadata = preserve_unobserved_metadata(capture_metadata(&stat), prior);
            NativeViewBaseline::new(fixture.options.expected_root_identity, false)
                .restore_canonical_stamps(host.identity(), prior, &mut metadata);
            metadata
        })
        .collect()
}

/// Replaces a record's bytes with same-length content and restores its prior
/// metadata: the host's stamps then match although its bytes differ, as after
/// a second write through a shared mapping.
fn tamper(runtime: &tokio::runtime::Runtime, fixture: &mut Fixture, index: usize) {
    let work = &fixture.setup;
    let cancellation = work.cancellation();
    let path = fixture.paths[index].clone();
    let checkout = &mut fixture.checkout;
    runtime.block_on(async {
        let (lookup, _custody) = checkout
            .lookup_batch_no_follow(std::slice::from_ref(&path), work, cancellation)
            .await
            .unwrap();
        let record = lookup.entries[0].record.unwrap();
        let metadata = checkout
            .read_metadata(record.file_id, work, cancellation)
            .await
            .unwrap();
        let replacement = body(FILES[index].1, 99);
        let content = checkout
            .stage_content(
                &mut std::io::Cursor::new(replacement.as_slice()),
                replacement.len() as u64,
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .unwrap()
            .value;
        let changes = vec![
            AuthoredMutation::WriteFromContent {
                path: path.clone(),
                offset: 0,
                content,
            },
            AuthoredMutation::SetMetadata { path, metadata },
        ];
        let slots = work
            .claim(
                (changes.capacity() * std::mem::size_of::<AuthoredMutation>()) as u64,
                1,
                WorkCounters::UNCHARGED,
            )
            .unwrap();
        checkout
            .apply_borrowed_authored_transaction_with_native_work(
                &changes,
                &slots,
                work,
                cancellation,
            )
            .await
            .unwrap();
    });
}

fn candidate(fixture: &Fixture) -> crate::facade::MemoryCheckout {
    let work = &fixture.setup;
    fixture
        .checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )
        .unwrap()
}

fn set_prior_access_time(runtime: &tokio::runtime::Runtime, fixture: &mut Fixture, nanos: i64) {
    let work = &fixture.setup;
    let cancellation = work.cancellation();
    let host = HostRoot::open(
        fixture.source.path(),
        &mut crate::native_host::ordinary_work(),
    )
    .unwrap();
    let accessed = FILES.map(|(name, _)| {
        let stat = test_host_stat(&host, Path::new(name.trim_start_matches('/'))).unwrap();
        capture_metadata(&stat).accessed_ns
    });
    runtime.block_on(async {
        let mut changes = Vec::new();
        for (index, path) in fixture.paths.iter().cloned().enumerate() {
            let (lookup, _custody) = fixture
                .checkout
                .lookup_batch_no_follow(std::slice::from_ref(&path), work, cancellation)
                .await
                .unwrap();
            let mut metadata = fixture
                .checkout
                .read_metadata(
                    lookup.entries[0].record.unwrap().file_id,
                    work,
                    cancellation,
                )
                .await
                .unwrap();
            // Other files start from their current access time, so this property
            // isolates one metadata edit even when capture itself moves atime.
            metadata.accessed_ns = if index == 2 {
                MetadataField::Value(nanos)
            } else {
                accessed[index]
            };
            changes.push(AuthoredMutation::SetMetadata { path, metadata });
        }
        let slots = work
            .claim(
                (changes.capacity() * std::mem::size_of::<AuthoredMutation>()) as u64,
                1,
                WorkCounters::UNCHARGED,
            )
            .unwrap();
        fixture
            .checkout
            .apply_borrowed_authored_transaction_with_native_work(
                &changes,
                &slots,
                work,
                cancellation,
            )
            .await
            .unwrap();
    });
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn metadata_changes_reuse_the_authenticated_body(accessed in 1_i64..32) {
        let _subscriber = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let runtime = setup();
        for cancelled in [false, true] {
        let [(((old, old_work, old_heap, _), old_expected), old_trace), (((new, work, heap, _), expected), trace)] =
            differential::pair::<_, _, &'static str>(|| {
                let mut state = fixture(&runtime);
                set_prior_access_time(&runtime, &mut state, accessed);
                let expected = expected_metadata(&runtime, &mut state);
                (state, expected)
            }, |(mut state, expected), old| {
                #[cfg(any(target_os = "linux", target_os = "macos", windows))]
                let probe = crate::native_host::stat_probe::Probe::rescan(false);
                let observed = rescan_with_baseline(&runtime, &mut state,
                    if old { Reuse::PersistFirst } else { Reuse::Verified }, cancelled, false);
                #[cfg(any(target_os = "linux", target_os = "macos", windows))]
                probe.publish();
                (observed, expected)
            });
        proptest::prop_assert_eq!((&old.result, &old.bytes), (&new.result, &new.bytes));
        proptest::prop_assert_eq!(new.result.is_ok(), !cancelled);
        proptest::prop_assert_eq!(old_trace, trace);
        proptest::prop_assert_eq!(work["sourceBytesRead"].as_u64(), Some(if cancelled { 0 } else { 70_117 }));
        // The old metadata update cloned the six-byte namespace path for
        // resize-to-zero, resize-to-length and write. Both still read all bytes.
        proptest::prop_assert_eq!(old_work["sourceBytesRead"].as_u64().unwrap(),
            work["sourceBytesRead"].as_u64().unwrap() + if cancelled { 0 } else { 18 });
        if !cancelled {
            proptest::prop_assert_eq!(&old.metadata, &old_expected);
            proptest::prop_assert_eq!(&new.metadata, &expected);
            exact_metadata(heap, &work).unwrap();
            proptest::prop_assert!(!new.unchanged_root && !old.unchanged_root);
            proptest::prop_assert_eq!(new.bytes, FILES.map(|(_, length)| body(length, 7)).to_vec());
            proptest::prop_assert!(work["objectBytesWritten"].as_u64() < old_work["objectBytesWritten"].as_u64());
            eprintln!("METADATA_REUSE heap={} old_heap={} work={work} old_work={old_work}", heap.count_total, old_heap.count_total);
        }
        }
    }
}

fn exact_metadata(heap: AllocationInfo, work: &serde_json::Value) -> Result<(), String> {
    let expected = if cfg!(windows) {
        485
    } else if cfg!(target_os = "macos") {
        513
    } else {
        497
    };
    let counters = [
        "objectBytesWritten",
        "bytesHashed",
        "bytesEncoded",
        "bytesCopied",
        "objectProbes",
        "allocationOperations",
    ];
    let expected_work = if cfg!(windows) {
        [500, 73_654, 1_245, 19_871, 17, 466]
    } else if cfg!(target_os = "macos") {
        [516, 73_346, 1_286, 22_188, 15, 480]
    } else {
        [516, 73_846, 1_158, 20_678, 17, 478]
    };
    let actual_work = counters.map(|name| work[name].as_u64().unwrap());
    let observed = (
        heap.count_total,
        work["sourceBytesRead"].as_u64(),
        actual_work,
    );
    if observed == (expected, Some(70_117), expected_work) {
        Ok(())
    } else {
        Err(format!(
            "metadata capture {observed:?}, expected heap={expected}, source=70117, work={expected_work:?}"
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[test]
fn metadata_oracle_rejects_wrong_atime_extra_heap_and_query_controls() {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = setup();
    for reuse in [
        Reuse::Verified,
        Reuse::WrongAccessed,
        Reuse::ExtraAllocation,
        Reuse::RedundantLargeStat,
        Reuse::OmitLargeValidation,
    ] {
        let [
            (((baseline, _, _, _), _), baseline_trace),
            (((output, work, heap, _), expected), trace),
        ] = differential::pair::<_, _, &'static str>(
            || {
                let mut state = fixture(&runtime);
                set_prior_access_time(&runtime, &mut state, 1);
                let expected = expected_metadata(&runtime, &mut state);
                (state, expected)
            },
            |(mut state, expected), old| {
                let probe = crate::native_host::stat_probe::Probe::rescan(false);
                let result = rescan_with_baseline(
                    &runtime,
                    &mut state,
                    if old { Reuse::PersistFirst } else { reuse },
                    false,
                    false,
                );
                probe.publish();
                (result, expected)
            },
        );
        assert_eq!(
            (&baseline.result, &baseline.bytes),
            (&output.result, &output.bytes)
        );
        let valid = output.metadata == expected
            && exact_metadata(heap, &work).is_ok()
            && trace == baseline_trace;
        assert_eq!(
            valid,
            reuse == Reuse::Verified,
            "{reuse:?}: heap={heap:?} work={work}"
        );
    }
}

fn exact_changed(
    index: usize,
    heap: AllocationInfo,
    work: &serde_json::Value,
) -> Result<(), String> {
    let large = index == 2;
    let (requests, expected) = if cfg!(windows) {
        (
            687,
            if large {
                [70_135, 70_698, 215_497, 2_329, 29_829, 22, 664]
            } else {
                [70_132, 794, 75_689, 2_329, 29_818, 22, 664]
            },
        )
    } else {
        let (requests, copied) = if cfg!(target_os = "macos") {
            (714, 31_401)
        } else {
            (699, 30_625)
        };
        (
            requests,
            if large {
                [70_135, 70_714, 215_673, 2_242, copied + 11, 22, 676]
            } else {
                [70_132, 810, 75_865, 2_242, copied, 22, 676]
            },
        )
    };
    let actual = [
        "sourceBytesRead",
        "objectBytesWritten",
        "bytesHashed",
        "bytesEncoded",
        "bytesCopied",
        "objectProbes",
        "allocationOperations",
    ]
    .map(|name| work[name].as_u64().unwrap());
    if (heap.count_total, actual) == (requests, expected) {
        Ok(())
    } else {
        Err(format!(
            "changed capture heap={heap:?}, work={actual:?}; expected heap={requests}, work={expected:?}"
        ))
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn ordinary_same_length_edits_read_once_at_the_old_exact_byte_budget(seed in 8u8..99) {
        let _subscriber = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let runtime = setup();
        for index in [1, 2] {
            // Collection reads 21 path bytes. Three mutation clones cost
            // 15 bytes for d/body or 18 bytes for d/large.
            let exact = if index == 1 { 70_132 } else { 70_135 };
            for budget in [exact, u64::MAX] {
                let [((old, old_work, old_heap, _), old_trace), ((new, work, heap, _), trace)] = differential::pair::<_, _, &'static str>(|| {
                    let mut state = fixture(&runtime);
                    std::fs::write(state.source.path().join(FILES[index].0.trim_start_matches('/')), body(FILES[index].1, seed)).unwrap();
                    state.budget.source_bytes_read = budget;
                    state
                }, |mut state, old| {
                    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
                    let probe = crate::native_host::stat_probe::Probe::rescan(false);
                    let result = rescan(&runtime, &mut state, if old { Reuse::PersistFirst } else { Reuse::Verified }, false);
                    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
                    probe.publish();
                    result
                });
                proptest::prop_assert!(new.result.is_ok());
                proptest::prop_assert_eq!((&old.result, &old.bytes), (&new.result, &new.bytes));
                proptest::prop_assert_eq!(&new.bytes[index], &body(FILES[index].1, seed));
                proptest::prop_assert_eq!(old_trace, trace);
                proptest::prop_assert_eq!(work["sourceBytesRead"].as_u64(), Some(exact));
                proptest::prop_assert_eq!(old_work["sourceBytesRead"].as_u64(), Some(exact));
                exact_changed(index, heap, &work).unwrap();
                if budget == u64::MAX {
                    eprintln!("CHANGED_REUSE index={index} heap={heap:?} old_heap={old_heap:?} work={work} old_work={old_work}");
                }
            }
            let mut broken = fixture(&runtime);
            std::fs::write(broken.source.path().join(FILES[index].0.trim_start_matches('/')), body(FILES[index].1, seed)).unwrap();
            let (output, work, heap, _) = rescan(&runtime, &mut broken, Reuse::ReadTwice, false);
            proptest::prop_assert!(output.result.is_ok());
            proptest::prop_assert_eq!(work["sourceBytesRead"].as_u64(), Some(exact + 70_096));
            proptest::prop_assert!(exact_changed(index, heap, &work).is_err());
            broken.budget.source_bytes_read = exact;
            let (output, _, _, _) = rescan(&runtime, &mut broken, Reuse::ReadTwice, false);
            proptest::prop_assert!(output.result.is_err());
        }
    }
}

fn setup() -> tokio::runtime::Runtime {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

#[test]
fn matching_stamps_never_hide_changed_bytes() {
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = setup();
    for index in [1, 2] {
        let host = body(FILES[index].1, 7);
        let mut fixture = fixture(&runtime);
        tamper(&runtime, &mut fixture, index);
        let tampered = candidate(&fixture);
        let mut outcomes = Vec::new();
        // Each mode rescans its own candidate of the same tampered generation.
        for reuse in [Reuse::Verified, Reuse::PersistFirst, Reuse::MetadataOnly] {
            fixture.checkout = tampered
                .private_candidate(
                    &mut crate::kernel::KernelAccounting::native(
                        &fixture.setup,
                        fixture.setup.cancellation(),
                    ),
                    fixture.setup.cancellation(),
                )
                .unwrap();
            let (output, _, _, root) = rescan(&runtime, &mut fixture, reuse, false);
            let receipt = output.result.clone().unwrap();
            outcomes.push((output.bytes[index] == host, receipt.changed_paths, root));
        }
        let [verified, persisted, metadata_only] = outcomes.try_into().unwrap();
        // Host bytes win and exactly the tampered path changes.
        assert_eq!((verified.0, verified.1), (true, 1), "file {index}");
        assert_eq!(verified, persisted, "file {index}");
        // Negative control: a stamp-trusting skip keeps the stale record.
        assert_eq!(
            (metadata_only.0, metadata_only.1),
            (false, 0),
            "file {index}"
        );
        assert_ne!(metadata_only.2, verified.2, "file {index}");
    }
}

// Literal heap requests and work for one rescan of the unchanged fixture,
// independent of the restored staging control, which must fail them.
#[cfg(target_os = "linux")]
const RESCAN: (u64, [(&str, u64); 6]) = (
    440,
    [
        ("objectBytesWritten", 0),
        ("bytesHashed", 72_389),
        ("bytesEncoded", 384),
        ("bytesCopied", 17_862),
        ("objectProbes", 18),
        ("allocationOperations", 422),
    ],
);
#[cfg(target_os = "macos")]
const RESCAN: (u64, [(&str, u64); 6]) = (
    454,
    [
        ("objectBytesWritten", 0),
        ("bytesHashed", 72_389),
        ("bytesEncoded", 384),
        ("bytesCopied", 18_638),
        ("objectProbes", 18),
        ("allocationOperations", 422),
    ],
);
#[cfg(windows)]
const RESCAN: (u64, [(&str, u64); 6]) = (
    428,
    [
        ("objectBytesWritten", 0),
        ("bytesHashed", 72_245),
        ("bytesEncoded", 487),
        ("bytesCopied", 17_055),
        ("objectProbes", 18),
        ("allocationOperations", 410),
    ],
);

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn exact_rescan(heap: AllocationInfo, work: &serde_json::Value) -> Result<(), String> {
    let observed = (
        heap.count_total,
        RESCAN
            .1
            .map(|(name, _)| (name, work[name].as_u64().unwrap())),
    );
    (observed == RESCAN)
        .then_some(())
        .ok_or_else(|| format!("observed {observed:?}, expected {RESCAN:?}"))
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn exact_capture(
    heap: AllocationInfo,
    work: &serde_json::Value,
    trace: &[&str],
    baseline: &[&str],
) -> Result<(), String> {
    exact_rescan(heap, work)?;
    if !baseline.is_empty() && trace == baseline {
        Ok(())
    } else {
        Err(format!(
            "native trace {trace:?} differs from baseline {baseline:?}"
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[test]
fn unchanged_rescan_authenticates_bytes_without_writing_objects() {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = setup();
    for cancelled in [false, true] {
        // Separate host roots, stores, checkouts, cancellations and ledgers.
        let [
            ((before, before_work, before_heap, _), before_trace),
            ((after, after_work, after_heap, _), trace),
        ] = differential::pair::<_, _, &'static str>(
            || fixture(&runtime),
            |mut state, old| {
                let reuse = if old {
                    Reuse::PersistFirst
                } else {
                    Reuse::Verified
                };
                let probe = crate::native_host::stat_probe::Probe::rescan(false);
                let observed = rescan(&runtime, &mut state, reuse, cancelled);
                probe.publish();
                drop(state.source);
                observed
            },
        );
        eprintln!(
            "VERIFIED_REUSE cancelled={cancelled} heap={} old_heap={} work={after_work} old_work={before_work}",
            after_heap.count_total, before_heap.count_total
        );
        // Ledgers include independent fixture construction; compare custody per side.
        assert_eq!(
            (&before.result, &before.bytes, before.unchanged_root),
            (&after.result, &after.bytes, after.unchanged_root)
        );
        // The checkout retains its decoded-object cache; verification admits no more.
        let retained = |output: &Output| output.live_after.wrapping_sub(output.live_before);
        eprintln!(
            "VERIFIED_REUSE retained={} old_retained={}",
            retained(&after),
            retained(&before)
        );
        assert!(retained(&after) <= retained(&before));
        assert_eq!(after.result.is_ok(), !cancelled);
        assert_eq!(trace, before_trace);
        if cancelled {
            assert_eq!(before_work, after_work);
            assert_eq!(before_heap, after_heap);
            continue;
        }
        assert!(after.unchanged_root);
        assert!(!before_trace.is_empty());
        assert_eq!(
            after.bytes,
            FILES.map(|(_, length)| body(length, 7)).to_vec()
        );
        let receipt = after.result.as_ref().unwrap();
        assert_eq!((receipt.examined_paths, receipt.changed_paths), (5, 0));
        exact_capture(after_heap, &after_work, &trace, &before_trace).unwrap();
        // Negative control: the restored staging work fails the same oracle.
        assert!(exact_capture(before_heap, &before_work, &before_trace, &before_trace).is_err());
        assert!(before_work["objectBytesWritten"].as_u64().unwrap() > 0);
        // Every source byte is still read in both.
        assert_eq!(
            before_work["sourceBytesRead"],
            after_work["sourceBytesRead"]
        );
        for fault in [Reuse::RedundantLargeStat, Reuse::OmitLargeValidation] {
            let ((broken, work, heap, _), broken_trace) = differential::observe::<_, _, &'static str>(
                || fixture(&runtime),
                |mut state| {
                    let probe = crate::native_host::stat_probe::Probe::rescan(false);
                    let observed = rescan(&runtime, &mut state, fault, false);
                    probe.publish();
                    observed
                },
            );
            assert_eq!(
                (&broken.result, &broken.bytes),
                (&after.result, &after.bytes)
            );
            // The extra stat escapes the six-counter oracle. Both controls
            // fail the same combined heap/work/ordered-trace oracle.
            if fault == Reuse::RedundantLargeStat {
                exact_rescan(heap, &work).unwrap();
            }
            assert!(exact_capture(heap, &work, &broken_trace, &before_trace).is_err());
            assert_ne!(broken_trace, before_trace, "{fault:?}");
        }
    }
}

#[cfg(target_os = "linux")]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn sparse_rescan_reads_each_allocated_byte_once(
        offset_blocks in 1_u64..8,
        data_blocks in 1_usize..4,
        split in proptest::bool::ANY,
    ) {
        let _subscriber = tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let runtime = setup();
        let length = data_blocks * 4096;
        let mut observed = Vec::new();
        for mode in [Reuse::PersistFirst, Reuse::Verified, Reuse::VerifySparse] {
            // Each run builds its own sparse host file and authenticated store.
            let mut fixture = fixture_with_sparse(&runtime, Some((offset_blocks * 4096, length, split)));
            let pair = differential::observe::<_, _, &'static str>(|| (), |()| {
                let probe = crate::native_host::stat_probe::Probe::rescan(false);
                let result = rescan(&runtime, &mut fixture, mode, false);
                probe.publish();
                result
            });
            observed.push(pair);
        }
        let [((old, old_work, old_heap, _), old_trace), ((new, work, heap, _), trace), ((broken, broken_work, broken_heap, _), _)] =
            observed.try_into().unwrap();
        proptest::prop_assert_eq!((&old.result, &old.bytes), (&new.result, &new.bytes));
        proptest::prop_assert_eq!((&broken.result, &broken.bytes), (&new.result, &new.bytes));
        proptest::prop_assert_eq!(old_trace, trace);
        let ranges = 1 + u64::from(split);
        let data = length as u64 * ranges;
        // Path collection contributes 21 bytes; sparse mutations clone a
        // six-byte path for two resizes and each write. No hole is read.
        let expected = data + 21 + 6 * (2 + ranges);
        eprintln!("SPARSE_REUSE ranges={ranges} heap={} broken_heap={} work={work}", heap.count_total, broken_heap.count_total);
        proptest::prop_assert_eq!(work["sourceBytesRead"].as_u64(), Some(expected));
        proptest::prop_assert_eq!(old_work["sourceBytesRead"].as_u64(), Some(expected));
        // The frozen baseline eagerly boxed one error per range and one
        // mutation-count error; the candidate keeps those failures scalar.
        proptest::prop_assert_eq!(old_heap.count_total, heap.count_total + ranges + 1);
        // Restoring speculative verification reads the sparse data a second time.
        proptest::prop_assert_eq!(broken_work["sourceBytesRead"].as_u64(), Some(expected + data));
        proptest::prop_assert_ne!(
            (broken_work["sourceBytesRead"].as_u64(), broken_heap.count_total),
            (Some(expected), old_heap.count_total)
        );
        let mut expected_bytes = vec![0; FILES[2].1];
        expected_bytes[offset_blocks as usize * 4096..offset_blocks as usize * 4096 + length]
            .copy_from_slice(&body(length, 7));
        if split {
            let offset = offset_blocks as usize * 4096 + length + 4096;
            expected_bytes[offset..offset + length].copy_from_slice(&body(length, 9));
        }
        proptest::prop_assert_eq!(&new.bytes[2], &expected_bytes);
    }
}
