use super::*;
use crate::model::{CaseSensitivity, FilesystemProfile};
use crate::{MemoryAuthorityBackend, MemoryObjectBackend, WorkCounters};
use acyclic_test_support::{
    AllocationInfo,
    differential::{pair, record},
    measure,
};
use proptest::prelude::*;

#[derive(Debug, Eq, PartialEq)]
enum Event {
    Observations(crate::kernel::CheckoutDependencies),
    Operation(String),
}

type TestTransaction = Transaction<MemoryAuthorityBackend, MemoryObjectBackend>;

#[derive(Clone, Copy, Debug)]
struct Case {
    depth: usize,
    existing: usize,
    // 1 binds a file and 2 a link below the existing prefix. 3 replaces an
    // empty base directory there with a file, and 4 removes two base
    // directories there, inside the measured transaction.
    collision: u8,
    concurrent: bool,
    // Leave a mutation pending before the call, so candidate lookups use the
    // transaction root while observations stay on the base generation.
    pending: bool,
    maximum_paths: u32,
    profile: FilesystemProfile,
    folded: bool,
    // Distinct dependencies the measured transaction may still admit.
    extra_dependencies: Option<u32>,
}

const BASE: Case = Case {
    depth: 4,
    existing: 4,
    collision: 0,
    concurrent: false,
    pending: false,
    maximum_paths: 64,
    profile: FilesystemProfile::Posix,
    folded: false,
    extra_dependencies: None,
};

const fn sized(case: Case, depth: usize, existing: usize) -> Case {
    Case {
        depth,
        existing,
        ..case
    }
}

fn path(depth: usize) -> String {
    if depth == 0 {
        return "/".into();
    }
    (0..depth).map(|index| format!("/d{index}")).collect()
}

// Spells the first `upper` names in a different case than they were created.
fn folded_path(depth: usize, upper: usize) -> String {
    (0..depth)
        .map(|index| {
            if index < upper {
                format!("/D{index}")
            } else {
                format!("/d{index}")
            }
        })
        .collect()
}

async fn fixture(case: Case) -> TestTransaction {
    let fs = Fs::memory();
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = case.profile;
    if case.folded {
        config.case_sensitivity = CaseSensitivity::ProfileFolded;
    }
    config.limits.maximum_paths_per_batch = case.maximum_paths;
    fs.create_workspace_with_operation::<_, (), WorkspaceError>(
        "directory-work",
        config,
        OperationId::from_bytes([1; 16]),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        None,
    )
    .await
    .expect("create");
    let workspace = unbounded!(fs, open_workspace("directory-work")).expect("open");
    let mut seed = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([2; 16]))
    )
    .expect("seed");
    let directories = match case.collision {
        3 => case.existing + 1,
        4 => case.existing + 2,
        _ => case.existing,
    };
    unbounded!(seed, create_dir_all(&path(directories))).expect("seed directories");
    seed.write_text("/untouched", "unchanged bytes\n")
        .await
        .expect("seed bytes");
    let name = path(case.existing + 1);
    match case.collision {
        1 => seed
            .write_text(&name, "not a directory")
            .await
            .expect("seed file"),
        2 => unbounded!(seed, create_symbolic_link(&name, Bytes::from_static(b"/")))
            .expect("seed link"),
        _ => {}
    }
    commit(&mut seed).await.expect("seed commit");
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([3; 16]))
    )
    .expect("transaction");
    match case.collision {
        0 if case.pending => transaction
            .write_text("/pending", "pending bytes\n")
            .await
            .expect("pending write"),
        // The base still binds a non-directory where the transaction now has none.
        1 | 2 if case.pending => unbounded!(transaction, remove(&name)).expect("pending remove"),
        3 => {
            unbounded!(transaction, remove(&name)).expect("pending remove");
            transaction
                .write_text(&name, "pending file")
                .await
                .expect("pending file");
        }
        4 => {
            unbounded!(transaction, remove(&path(case.existing + 2))).expect("pending remove");
            unbounded!(transaction, remove(&name)).expect("pending remove");
        }
        _ => {}
    }
    if let Some(extra) = case.extra_dependencies {
        transaction.checkout.limit_dependencies_after(extra);
    }
    transaction
}

// The original per-prefix production loop, kept as the differential oracle.
async fn individually(
    transaction: &mut TestTransaction,
    path: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    for end in path
        .match_indices('/')
        .skip(1)
        .map(|(index, _)| index)
        .chain(std::iter::once(path.len()))
    {
        let prefix = path.get(..end).ok_or(crate::FsError::InvalidDiff)?;
        let path = transaction.mutation_path(prefix, accounting, cancellation)?;
        match transaction
            .checkout
            .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
            .await?
            .0
            .record
        {
            Some(record) if record.kind == FileKind::Directory => path.retire(accounting)?,
            Some(_) => {
                path.retire(accounting)?;
                return Err(WorkspaceError::NotDirectory);
            }
            None => {
                transaction
                    .create_directory_path(path, accounting, cancellation)
                    .await?;
            }
        }
    }
    Ok(())
}

// Both variants start on independent providers.
async fn original(
    transaction: &mut TestTransaction,
    requested: &str,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<()>, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
    let result = async {
        accounting.check()?;
        let depth = PortablePath::validate(requested, transaction.checkout.volume_config().limits)
            .map_err(|error| {
                WorkspaceError::from(crate::kernel::NamespacePathError::Path(error))
            })?;
        if depth == 0 {
            return Ok(());
        }
        individually(transaction, requested, &mut accounting, cancellation).await
    }
    .await;
    result
        .map(|value| crate::OperationReceipt {
            value,
            work: accounting.spent(),
        })
        .map_err(|error| crate::OperationFailure::new(error, accounting.spent()))
}

// The full-path lookup alone: atomic admission without prefix observations.
async fn terminal_only(
    transaction: &mut TestTransaction,
    requested: &str,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<()>, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
    let parsed = transaction
        .mutation_path(requested, &mut accounting, cancellation)
        .expect("valid path");
    let lookup = transaction
        .checkout
        .lookup_no_follow_with_accounting(&parsed.value, &mut accounting, cancellation, true)
        .await;
    parsed.retire(&mut accounting).expect("retire path");
    match lookup {
        Ok(_) => Ok(crate::OperationReceipt {
            value: (),
            work: accounting.spent(),
        }),
        Err(error) => Err(crate::OperationFailure::new(
            WorkspaceError::from(error),
            accounting.spent(),
        )),
    }
}

async fn trace(
    transaction: &mut TestTransaction,
    case: Case,
    result: Result<(), String>,
) -> Vec<String> {
    let mut trace = vec![format!("create: {result:?}")];
    if case.concurrent && case.existing != 0 {
        let mut other = unbounded!(
            transaction.workspace,
            begin_transaction(IdempotencyKey::from_bytes([4; 16]))
        )
        .expect("concurrent transaction");
        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o700),
            ..FileMetadata::default()
        };
        unbounded!(other, set_metadata("/d0", metadata)).expect("metadata");
        commit(&mut other).await.expect("concurrent commit");
    }
    // Every outcome is followed by a real mutation and publication. Observations
    // must survive, including observations made before an error.
    let later = transaction.write_text("/later", "updated bytes\n").await;
    trace.push(format!("later write: {later:?}"));
    trace.push(match commit(transaction).await {
        Ok(TransactionCommit::Committed(generation)) => {
            format!("committed: {:?}", generation.id())
        }
        Ok(TransactionCommit::AlreadyCommitted(generation)) => {
            format!("already: {:?}", generation.id())
        }
        Ok(TransactionCommit::Conflict { actual }) => format!("conflict: {:?}", actual.id()),
        Ok(_) => panic!("unexpected commit outcome"),
        Err(error) => format!("commit: {error:?}"),
    });
    let workspace = &transaction.workspace;
    for depth in 0..=case.depth.max(case.existing + 2) {
        let name = path(depth);
        trace.push(format!(
            "stat {name}: {:?}",
            unbounded!(workspace, stat(&name))
        ));
    }
    trace.push(format!(
        "bytes: {:?}",
        unbounded!(workspace, read("/untouched", 64))
    ));
    trace.push(format!(
        "later bytes: {:?}",
        unbounded!(workspace, read("/later", 64))
    ));
    trace
}

fn compare(case: Case) -> [(WorkCounters, AllocationInfo); 2] {
    compare_path(case, &path(case.depth))
}

fn compare_path(case: Case, requested: &str) -> [(WorkCounters, AllocationInfo); 2] {
    compare_variant(case, requested, false)
}

fn compare_variant(
    case: Case,
    requested: &str,
    terminal_lookup_only: bool,
) -> [(WorkCounters, AllocationInfo); 2] {
    let pair = pair::<_, _, Event>(
        || {
            let start = std::time::Instant::now();
            let transaction = futures::executor::block_on(fixture(case));
            (transaction, start.elapsed())
        },
        |(mut transaction, setup), old| {
            let mut result = None;
            let cancellation = CancellationToken::new();
            let start = std::time::Instant::now();
            let allocations = measure(|| {
                result = Some(futures::executor::block_on(async {
                    if old {
                        original(&mut transaction, requested, &cancellation).await
                    } else if terminal_lookup_only {
                        terminal_only(&mut transaction, requested, &cancellation).await
                    } else {
                        transaction
                            .create_dir_all(requested, WorkBudget::UNBOUNDED, &cancellation)
                            .await
                    }
                }));
            });
            let call = start.elapsed();
            let start = std::time::Instant::now();
            let result = result.expect("production call completed");
            record(Event::Observations(
                transaction.checkout.directory_observations(),
            ));
            let work = match &result {
                Ok(receipt) => receipt.work,
                Err(failure) => *failure.work,
            };
            for event in futures::executor::block_on(trace(
                &mut transaction,
                case,
                result
                    .map(|_| ())
                    .map_err(|failure| format!("{:?}", failure.error)),
            )) {
                record(Event::Operation(event));
            }
            (work, allocations, [setup, call, start.elapsed()])
        },
    );
    assert_eq!(pair[0].1, pair[1].1, "complete production trace");
    if std::env::var_os("GC_LOCAL_DIRECTORY_TIMINGS").is_some() {
        for (index, ((_, _, phases), _)) in pair.iter().enumerate() {
            eprintln!(
                "DIRECTORY_PHASE {}",
                serde_json::json!({
                    "depth": case.depth, "existing": case.existing, "old": index == 0,
                    "setup_ns": phases[0].as_nanos() as u64,
                    "call_ns": phases[1].as_nanos() as u64,
                    "audit_ns": phases[2].as_nanos() as u64,
                })
            );
        }
    }
    pair.map(|((work, allocations, _), _)| (work, allocations))
}

fn expect_panic(run: impl FnOnce() + std::panic::UnwindSafe, expected: &str) {
    let failure = std::panic::catch_unwind(run).expect_err("the control must fail");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains(expected), "wrong failure: {message}");
}

#[test]
fn shared_directory_frontier_preserves_outputs_errors_and_observations() {
    for (depth, existing, collision, concurrent, pending, maximum_paths) in [
        (0, 0, 0, false, false, 64),
        (1, 1, 0, false, false, 64),
        (1, 0, 0, false, false, 64),
        (4, 4, 0, false, false, 64),
        (8, 8, 0, false, false, 64),
        (4, 0, 0, false, false, 64),
        (4, 2, 0, false, false, 64),
        (4, 2, 1, false, false, 64),
        (4, 2, 2, false, false, 64),
        (4, 4, 0, true, false, 64),
        (4, 2, 1, true, false, 64),
        (4, 2, 0, false, false, 1),
        (4, 2, 0, false, true, 64),
        (4, 2, 1, false, true, 64),
        (4, 2, 2, false, true, 64),
        (4, 2, 1, true, true, 64),
        (2, 0, 3, false, false, 64),
        (4, 1, 3, true, false, 64),
        (4, 1, 4, false, false, 64),
        (6, 0, 4, true, false, 64),
    ] {
        let case = Case {
            depth,
            existing,
            collision,
            concurrent,
            pending,
            maximum_paths,
            ..BASE
        };
        let work = compare(case);
        eprintln!("DIRECTORY_WORK {case:?} {work:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn shared_directory_frontier_matches_original_for_every_bounded_prefix(
        depth in 1usize..7, existing in 0usize..7, collision in 0u8..5,
        concurrent in any::<bool>(), pending in any::<bool>(),
        maximum_paths in prop_oneof![Just(1u32), Just(64u32)],
        profile in prop_oneof![Just(FilesystemProfile::Posix), Just(FilesystemProfile::Windows), Just(FilesystemProfile::Portable)],
        folded in any::<bool>(), upper in 0usize..3,
        extra in prop_oneof![Just(None), (0u32..9).prop_map(Some)],
    ) {
        let existing = existing.min(depth);
        let collision = match collision {
            1..=3 if existing < depth => collision,
            4 if existing + 2 <= depth => collision,
            _ => 0,
        };
        let case = Case { depth, existing, collision, concurrent, pending, maximum_paths, profile, folded, extra_dependencies: extra };
        compare_path(case, &folded_path(depth, upper.min(existing)));
    }
}

// Separate lookups admit each prefix's observations before the next one, so
// a limit failure keeps every completed prefix. The limit is swept across
// every boundary of each shape.
#[test]
fn dependency_limit_failures_keep_completed_prefix_observations() {
    for (depth, existing, collision, pending) in [
        (2, 2, 0, false),
        (4, 4, 0, false),
        (4, 2, 0, false),
        (4, 0, 0, true),
        (4, 2, 1, false),
        (4, 2, 1, true),
        (2, 0, 3, false),
        (4, 1, 4, false),
    ] {
        for extra in 0..=10 {
            for folded in [false, true] {
                compare(Case {
                    depth,
                    existing,
                    collision,
                    pending,
                    folded,
                    extra_dependencies: Some(extra),
                    ..BASE
                });
            }
        }
    }
}

#[test]
fn completed_prefix_observations_survive_the_limit_failure() {
    let case = Case {
        depth: 2,
        existing: 2,
        extra_dependencies: Some(3),
        ..BASE
    };
    let mut transaction = futures::executor::block_on(fixture(case));
    let held = transaction.checkout.directory_observations().len();
    let failure = futures::executor::block_on(transaction.create_dir_all(
        &path(2),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .expect_err("four observations exceed three");
    assert!(
        format!("{:?}", failure.error).contains("TooManyDependencies"),
        "{:?}",
        failure.error
    );
    // The name and record of /d0.
    assert_eq!(
        transaction.checkout.directory_observations().len(),
        held + 2
    );
}

// A pending file stops the loop at its own prefix. The deeper base directory
// is never observed, so two dependencies suffice to report the collision.
#[test]
fn pending_file_bounds_the_observed_base_prefixes() {
    let case = Case {
        depth: 2,
        existing: 0,
        collision: 3,
        extra_dependencies: Some(2),
        ..BASE
    };
    let mut transaction = futures::executor::block_on(fixture(case));
    let failure = futures::executor::block_on(transaction.create_dir_all(
        &path(2),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .expect_err("a file binds /d0");
    assert!(matches!(failure.error, WorkspaceError::NotDirectory));
    compare(case);
}

#[test]
fn atomic_terminal_admission_fails_the_same_limit_comparison() {
    let case = Case {
        depth: 2,
        existing: 2,
        extra_dependencies: Some(3),
        ..BASE
    };
    expect_panic(
        || {
            compare_variant(case, &path(case.depth), true);
        },
        "complete production trace",
    );
}

#[test]
fn invalid_directory_paths_preserve_errors_and_later_mutations() {
    let case = Case {
        depth: 3,
        existing: 2,
        ..BASE
    };
    for requested in [
        "",
        "relative",
        "/d0//d1",
        "/d0/../d1",
        "/d0/./d1",
        "/d0/",
        "/d0/\0",
    ] {
        compare_path(case, requested);
    }
}

#[test]
fn native_directory_frontier_preserves_the_original_custody() {
    for existing in [0, 2, 4] {
        let case = Case { existing, ..BASE };
        let results = pair::<_, _, Event>(
            || {
                let transaction = futures::executor::block_on(fixture(case));
                let cancellation = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    selected_transaction_budget(),
                    &cancellation,
                )
                .map_err(|(error, _)| error)
                .expect("native work");
                let transaction =
                    futures::executor::block_on(transaction.workspace.begin_transaction(
                        IdempotencyKey::from_bytes([3; 16]),
                        &original,
                        &cancellation,
                    ))
                    .expect("native checkout");
                (transaction, original, cancellation)
            },
            |(mut transaction, original, cancellation), old| {
                let requested = path(case.depth);
                let result = futures::executor::block_on(async {
                    if old {
                        let mut accounting = KernelAccounting::native(&original, &cancellation);
                        individually(&mut transaction, &requested, &mut accounting, &cancellation)
                            .await
                    } else {
                        transaction
                            .create_dir_all(&requested, &original, &cancellation)
                            .await
                    }
                });
                record(Event::Observations(
                    transaction.checkout.directory_observations(),
                ));
                result.expect("native directory creation");
                futures::executor::block_on(async {
                    let mut accounting = KernelAccounting::native(&original, &cancellation);
                    for depth in 0..=case.depth {
                        let name = path(depth);
                        let parsed = transaction
                            .mutation_path(&name, &mut accounting, &cancellation)
                            .expect("native path");
                        let lookup = transaction
                            .checkout
                            .lookup_no_follow_with_accounting(
                                &parsed.value,
                                &mut accounting,
                                &cancellation,
                                true,
                            )
                            .await
                            .expect("native lookup")
                            .0;
                        record(Event::Operation(format!("record: {:?}", lookup.record)));
                        parsed.retire(&mut accounting).expect("retire path");
                    }
                });
                original
                    .receipt()
                    .verify(selected_transaction_budget())
                    .expect("bounded work");
            },
        );
        assert_eq!(results[0], results[1]);
    }
}

#[test]
fn cancelled_directory_call_dispatches_no_storage_work() {
    let mut transaction = futures::executor::block_on(fixture(BASE));
    let before = transaction.checkout.directory_observations();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = futures::executor::block_on(transaction.create_dir_all(
        &path(BASE.depth),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .expect_err("cancelled");
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    assert_eq!(before, transaction.checkout.directory_observations());
}

#[test]
fn missing_prefix_observation_fails_the_same_trace_comparison() {
    expect_panic(
        || {
            compare_variant(BASE, &path(BASE.depth), true);
        },
        "complete production trace",
    );
}

fn costs(measurement: (WorkCounters, AllocationInfo)) -> serde_json::Value {
    let (work, allocation) = measurement;
    serde_json::json!({
        "work": work,
        "allocation": [allocation.count_total as i64, allocation.count_current, allocation.count_max as i64,
            allocation.bytes_total as i64, allocation.bytes_current, allocation.bytes_max as i64],
    })
}

fn expected_costs() -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/workspace/directories/work.json");
    serde_json::from_slice(&std::fs::read(path).expect("checked-in cost fixture"))
        .expect("cost fixture JSON")
}

fn assert_work(actual: &serde_json::Value, expected: &serde_json::Value) {
    assert_eq!(actual["work"], expected["work"], "exact production work");
}

fn assert_allocations(actual: &serde_json::Value, expected: &serde_json::Value) {
    assert_eq!(
        actual["allocation"], expected["allocation"],
        "exact physical allocations"
    );
}

// Measures both implementations on the pinned workloads. Page reads and parsed
// components are also checked against counts derived independently of the
// fixture: separate lookups of an existing depth-N path read 1 + 2i pages and
// parse i names for each prefix i; the shared walk reads 1 + 2N and parses N.
fn measured_costs() -> serde_json::Map<String, serde_json::Value> {
    let windows = Case {
        profile: FilesystemProfile::Windows,
        ..BASE
    };
    let folded = Case {
        folded: true,
        ..windows
    };
    let mut measured = serde_json::Map::new();
    for (name, case, pages, components) in [
        ("existing4", BASE, [24, 9], [10, 4]),
        ("existing8", sized(BASE, 8, 8), [80, 17], [36, 8]),
        ("missing4", sized(BASE, 4, 0), [66, 42], [10, 10]),
        ("partial4", sized(BASE, 4, 2), [60, 38], [10, 7]),
        ("windows4", windows, [24, 9], [10, 4]),
        ("windows8", sized(windows, 8, 8), [80, 17], [36, 8]),
        ("folded4", folded, [24, 9], [10, 4]),
        ("folded8", sized(folded, 8, 8), [80, 17], [36, 8]),
    ] {
        let samples = compare(case);
        for index in [0, 1] {
            assert_eq!(
                samples[index].0.page_reads, pages[index],
                "authenticated frontier reads: {name}"
            );
            assert_eq!(
                samples[index].0.source_path_components, components[index],
                "parsed component copies: {name}"
            );
        }
        measured.insert(name.into(), serde_json::json!(samples.map(costs)));
    }
    measured
}

#[test]
fn directory_work_and_physical_allocations_are_exact() {
    let measured = measured_costs();
    eprintln!(
        "DIRECTORY_PROFILE {}",
        serde_json::Value::Object(measured.clone())
    );
    let expected = expected_costs();
    for (name, pair) in &measured {
        for index in [0, 1] {
            assert_work(&pair[index], &expected[name][index]);
            assert_allocations(&pair[index], &expected[name][index]);
        }
    }
}

#[test]
fn restored_prefix_walks_fail_the_same_work_and_allocation_assertions() {
    let measured = measured_costs();
    let expected = expected_costs();
    for (name, pair) in &measured {
        for (check, message) in [
            (assert_work as fn(&_, &_), "exact production work"),
            (
                assert_allocations as fn(&_, &_),
                "exact physical allocations",
            ),
        ] {
            // The original per-prefix traversal measured against the pinned candidate.
            let failure = std::panic::catch_unwind(|| check(&pair[0], &expected[name][1]))
                .expect_err("restoring the original production traversal must be detected");
            let text = failure
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| failure.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(text.contains(message), "wrong failure ({name}): {text}");
        }
    }
}

#[test]
fn directory_frontiers_preserve_each_filesystem_name_profile() {
    for profile in [
        FilesystemProfile::Portable,
        FilesystemProfile::Browser,
        FilesystemProfile::Posix,
        FilesystemProfile::Windows,
    ] {
        for folded in [false, true] {
            for (existing, collision, upper) in [
                (4, 0, 0),
                (4, 0, 2),
                (2, 0, 0),
                (2, 0, 1),
                (2, 1, 0),
                (2, 1, 2),
                (0, 0, 0),
            ] {
                let case = Case {
                    existing,
                    collision,
                    profile,
                    folded,
                    ..BASE
                };
                compare_path(case, &folded_path(case.depth, upper));
            }
        }
    }
}
