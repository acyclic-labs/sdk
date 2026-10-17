use super::*;
use proptest::prelude::*;
use std::cell::{Cell, RefCell};

thread_local! {
    static CLONE_LEAVES: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn clone_leaves() -> bool {
    CLONE_LEAVES.get()
}

struct CopyLeaves(bool);
impl Drop for CopyLeaves {
    fn drop(&mut self) {
        CLONE_LEAVES.set(self.0);
    }
}

fn path(text: &str) -> NamespacePath {
    let limits = crate::model::VolumeLimits::default();
    NamespacePath::from_portable(
        &crate::path::PortablePath::parse(text, limits).unwrap(),
        limits,
    )
    .unwrap()
}

fn work(budget: WorkBudget) -> NativeMaterializationWork {
    NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap()
}

// A leaf needs exactly one owned path in the output. Copying it first creates
// one component vector plus one byte vector per component, all immediately freed.
fn redundant_copy(paths: &[(NamespacePath, bool)]) -> (u64, u64, u64) {
    paths.iter().filter(|(_, directory)| !directory).fold(
        (0, 0, 0),
        |(allocations, components, bytes), (path, _)| {
            (
                allocations + path.depth() as u64 + 1,
                components + path.depth() as u64,
                bytes
                    + path
                        .components()
                        .iter()
                        .map(|n| n.as_bytes().len() as u64)
                        .sum::<u64>(),
            )
        },
    )
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn production_leaf_walks_have_no_redundant_heap_copy() -> Result<(), Box<dyn std::error::Error>> {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let source = tempfile::tempdir()?;
    for name in ["f", "d/long-λ", "d/sub/file"] {
        let host = source.path().join(name);
        std::fs::create_dir_all(host.parent().unwrap())?;
        std::fs::write(host, b"x")?;
    }
    std::fs::hard_link(source.path().join("f"), source.path().join("alias"))?;
    let setup = work(WorkBudget::UNBOUNDED);
    let mut checkout = runtime.block_on(super::capture_policy_tests::native_checkout(&setup))?;
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let root = HostRoot::open(source.path(), &mut &setup)
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    let options = CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: root.identity(),
        maximum_paths: 8,
        maximum_extent_spans: 8,
    };
    let policy = CapturePolicy::default();
    runtime.block_on(capture_subtrees_with_native_work(
        &mut checkout,
        &[path("/")],
        &options,
        &policy,
        setup.cancellation(),
        None,
        &setup,
    ))?;
    // Directory proofs stay cached on the checkout's original allocation ledger.
    let warm = CaptureWorkScope::new(&setup)?;
    let mut warmed = Vec::new();
    runtime.block_on(collect_checkout_subtree_paths(
        &mut checkout,
        limits,
        path("/"),
        &policy,
        8,
        &mut warmed,
        &warm.native,
    ))?;
    drop(warmed);
    drop(warm);
    for host in [true, false] {
        let mut run = |old| {
            let _copy = CopyLeaves(CLONE_LEAVES.replace(old));
            let live_before = setup.live_allocation_bytes();
            let scope = CaptureWorkScope::new(&setup).unwrap();
            let start = path("/");
            let mut paths = Vec::new();
            let mut result = None;
            let measured = acyclic_test_support::measure(|| {
                result = Some(if host {
                    collect_host_walk_measured(
                        &root,
                        profile,
                        limits,
                        start,
                        &policy,
                        &scope.native,
                        |p, _, meter| append_scanned_path(&mut paths, p, 8, meter),
                    )
                } else {
                    runtime.block_on(collect_checkout_subtree_paths(
                        &mut checkout,
                        limits,
                        start,
                        &policy,
                        8,
                        &mut paths,
                        &scope.native,
                    ))
                });
            });
            result.unwrap().unwrap();
            assert_eq!(paths.len(), 6);
            drop(paths);
            drop(scope);
            assert_eq!(setup.live_allocation_bytes(), live_before);
            measured
        };
        // Warm the host adapter, executor and authenticated checkout records.
        run(false);
        run(true);
        let moved = run(false);
        let copied = run(true);
        let repeated = run(false);
        eprintln!("LEAF_HEAP host={host} moved={moved:?} copied={copied:?} repeated={repeated:?}");
        // Fixed warm six-path heap counts, independent of receipts or controls.
        let expected = match (host, cfg!(windows)) {
            (true, true) => 43,
            (true, false) => 46,
            (false, _) => 199,
        };
        let no_copy = |allocations| {
            if allocations == expected {
                Ok(())
            } else {
                Err((expected, allocations))
            }
        };
        no_copy(moved.count_total).unwrap();
        no_copy(repeated.count_total).unwrap();
        assert!(no_copy(copied.count_total).is_err());
        assert!(copied.count_total > moved.count_total);
    }
    Ok(())
}

#[test]
fn moved_leaves_match_copying_walks_and_eliminate_only_redundant_work()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let source = tempfile::tempdir()?;
    let files = ["/f", "/g", "/d/long-λ", "/d/sub/file", "/blocked/file"];
    for file in files {
        let host = source.path().join(&file[1..]);
        std::fs::create_dir_all(host.parent().unwrap())?;
        std::fs::write(host, b"x")?;
    }
    std::fs::create_dir(source.path().join("d/empty"))?;
    std::fs::hard_link(source.path().join("f"), source.path().join("alias"))?;
    let setup = work(WorkBudget::UNBOUNDED);
    let mut checkout = runtime.block_on(super::capture_policy_tests::native_checkout(&setup))?;
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let root = HostRoot::open(source.path(), &mut &setup)
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    let options = CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: root.identity(),
        maximum_paths: 32,
        maximum_extent_spans: 8,
    };
    runtime.block_on(capture_subtrees_with_native_work(
        &mut checkout,
        &[path("/")],
        &options,
        &CapturePolicy::default(),
        setup.cancellation(),
        None,
        &setup,
    ))?;
    let mut all: Vec<_> = files
        .into_iter()
        .chain(["/alias"])
        .map(|p| (path(p), false))
        .chain(["/d", "/d/sub", "/d/empty", "/blocked"].map(|p| (path(p), true)))
        .collect();
    all.sort();
    // Warm every directory record once, outside differential work receipts.
    let warm = CaptureWorkScope::new(&setup)?;
    let mut warmed = Vec::new();
    runtime.block_on(collect_checkout_subtree_paths(
        &mut checkout,
        limits,
        path("/"),
        &CapturePolicy::default(),
        32,
        &mut warmed,
        &warm.native,
    ))?;
    drop(warmed);
    drop(warm);
    let checkout = RefCell::new(checkout);
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // Both real walks must return the scalar filtered path set, preserve host
    // identity/metadata, reject the same path bounds, and retire all custody.
    // The copying variant must fail the no-redundant-copy work oracle.
    let check = |(subtree, excluded, twice, maximum): (usize, u8, bool, usize)| {
        let mut checkout = checkout.borrow_mut();
        runtime.block_on(async {
            let subtree = path(["/", "/d", "/d/sub", "/blocked"][subtree]);
            let policy = CapturePolicy::excluding(
                ["/f", "/d/empty", "/d/sub", "/blocked"]
                    .into_iter()
                    .enumerate()
                    .filter(|(bit, _)| excluded & (1 << bit) != 0)
                    .map(|(_, p)| path(p))
                    .collect(),
            )
            .unwrap();
            let expected: Vec<_> = all
                .iter()
                .filter(|(p, _)| *p != subtree && p.is_within(&subtree) && !policy.excludes(p))
                .cloned()
                .collect();
            // A subtree itself is already admitted; exclusions apply to its descendants.
            let copies = redundant_copy(&expected);
            for host in [true, false] {
                let mut outcomes = Vec::new();
                for old in [false, true] {
                    let _copy = CopyLeaves(CLONE_LEAVES.replace(old));
                    let outcome = {
                        // The checkout's proof custody belongs to its original ledger.
                        let meter = setup.clone();
                        let before = meter.receipt();
                        let live_before = meter.live_allocation_bytes();
                        let scope = CaptureWorkScope::new(&meter).unwrap();
                        let mut paths = Vec::new();
                        let mut failure = None;
                        for _ in 0..=usize::from(twice) {
                            let start = clone_capture_path(&subtree, &scope.native).unwrap();
                            let result = if host {
                                collect_host_walk_measured(
                                    &root,
                                    profile,
                                    limits,
                                    start,
                                    &policy,
                                    &scope.native,
                                    |p, stat, meter| {
                                        let expected =
                                            test_host_stat(&root, &namespace_to_host_path(&p)?)
                                                .unwrap();
                                        ensure_same_host_node(&expected, &stat)?;
                                        append_scanned_path(&mut paths, p, maximum, meter)
                                    },
                                )
                            } else {
                                collect_checkout_subtree_paths(
                                    &mut *checkout,
                                    limits,
                                    start,
                                    &policy,
                                    maximum,
                                    &mut paths,
                                    &scope.native,
                                )
                                .await
                            };
                            if let Err(error) = result {
                                failure = Some(error.to_string());
                                break;
                            }
                        }
                        let mut receipt = meter.receipt();
                        receipt.allocation_operations -= before.allocation_operations;
                        receipt.source_path_components -= before.source_path_components;
                        receipt.source_bytes_read -= before.source_bytes_read;
                        receipt.bytes_copied -= before.bytes_copied;
                        let output = paths.iter().map(|(p, ())| p.clone()).collect::<Vec<_>>();
                        drop(paths);
                        drop(scope);
                        assert_eq!(meter.live_allocation_bytes(), live_before);
                        (failure, output, receipt)
                    };
                    prop_assert_eq!(
                        outcome.0.is_none(),
                        expected.len() <= maximum,
                        "host={} old={} failure={:?}",
                        host,
                        old,
                        outcome.0
                    );
                    if outcome.0.is_none() {
                        prop_assert_eq!(
                            &outcome.1,
                            &expected.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>()
                        );
                    }
                    outcomes.push(outcome);
                }
                prop_assert_eq!(&outcomes[0].0, &outcomes[1].0);
                prop_assert_eq!(&outcomes[0].1, &outcomes[1].1);
                if outcomes[0].0.is_none() {
                    let factor = 1 + u64::from(twice);
                    let new = outcomes[0].2;
                    let old = outcomes[1].2;
                    let bound = old.allocation_operations - copies.0 * factor;
                    prop_assert_eq!(new.allocation_operations, bound);
                    prop_assert_eq!(
                        old.source_path_components - new.source_path_components,
                        copies.1 * factor
                    );
                    prop_assert_eq!(
                        old.source_bytes_read - new.source_bytes_read,
                        copies.2 * factor
                    );
                    prop_assert_eq!(old.bytes_copied - new.bytes_copied, copies.2 * factor);
                    if copies.0 != 0 {
                        prop_assert!(old.allocation_operations > bound);
                    }
                }
            }
            Ok(())
        })
    };
    check((0, 0, true, 32)).unwrap();
    runner.run(&(0usize..4, 0u8..16, any::<bool>(), 0usize..12), check)?;
    Ok(())
}
