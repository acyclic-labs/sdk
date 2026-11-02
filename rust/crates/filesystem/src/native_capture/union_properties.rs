use super::*;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

fn path(key: u8) -> NamespacePath {
    let limits = crate::model::VolumeLimits::default();
    NamespacePath::from_portable(
        &crate::path::PortablePath::parse(&format!("/p{key:03}"), limits).unwrap(),
        limits,
    )
    .unwrap()
}

fn inputs(
    host: &BTreeSet<u8>,
    checkout: &BTreeSet<u8>,
    stat: &HostStat,
    meter: &NativeMaterializationWork,
) -> (
    Vec<(NamespacePath, Option<HostStat>)>,
    Vec<(NamespacePath, ())>,
) {
    let mut observed = capture_vec(host.len(), meter).unwrap();
    for key in host {
        observed.push((
            clone_capture_path(&path(*key), meter).unwrap(),
            Some(stat.clone()),
        ));
    }
    let mut paths = capture_vec(checkout.len(), meter).unwrap();
    for key in checkout {
        paths.push((clone_capture_path(&path(*key), meter).unwrap(), ()));
    }
    (observed, paths)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn original_host_checkout_union_preserves_facts_and_linear_funded_work(
        host in prop::collection::btree_set(any::<u8>(), 0..65),
        checkout in prop::collection::btree_set(any::<u8>(), 0..65),
    ) {
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(
            crate::performance::work_budget(100_000, 8 * 1024 * 1024),
            &cancellation,
        ).unwrap();
        // Metadata is captured from the real no-follow host producer. An equal
        // checkout name must retain these original facts and original path.
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("host"), b"original").unwrap();
        let root = HostRoot::open(source.path(), &mut &original).unwrap();
        #[cfg(unix)]
        let leaf = Path::new("host");
        #[cfg(windows)]
        let leaf: Vec<_> = "host".encode_utf16().collect();
        let stat = root.stat(&leaf, &mut &original).unwrap();
        let baseline = original.live_allocation_bytes();
        let scope = CaptureWorkScope::new(&original).unwrap();
        let (observed, paths) = inputs(&host, &checkout, &stat, &scope.native);
        let original_paths: BTreeMap<_, _> = observed.iter()
            .map(|(path, _)| (path.clone(), path.components().as_ptr()))
            .collect();
        let expected: BTreeSet<_> = host.union(&checkout).copied().collect();
        let before = original.receipt();
        let (observations, paths) = order_host_checkout_union(
            observed, paths, expected.len(), &scope.native,
        ).unwrap();
        let after = original.receipt();
        prop_assert_eq!(paths.len(), expected.len());
        prop_assert_eq!(observations.len(), expected.len());
        for ((actual, observation), key) in paths.iter().zip(&observations).zip(&expected) {
            prop_assert_eq!(actual, &path(*key));
            prop_assert_eq!(observation.is_some(), host.contains(key));
            if let Some(observation) = observation {
                prop_assert_eq!(observation.identity(), stat.identity());
                prop_assert_eq!(observation.len(), stat.len());
                prop_assert_eq!(actual.components().as_ptr(), original_paths[actual]);
            }
        }
        // Each pass consumes an input per step. Reinstating insertion shifts
        // or copying duplicate path payloads violates these output-only costs.
        prop_assert!(after.items_examined - before.items_examined <= 4 * (host.len() + checkout.len()) as u64);
        prop_assert_eq!(after.backend_read_operations, before.backend_read_operations);
        prop_assert_eq!(after.allocation_operations - before.allocation_operations, 2 * u64::from(!expected.is_empty()));
        prop_assert_eq!(after.bytes_copied - before.bytes_copied,
            (expected.len() * (std::mem::size_of::<NamespacePath>() + std::mem::size_of::<Option<HostStat>>())) as u64);
        drop((paths, observations, scope));
        prop_assert_eq!(original.live_allocation_bytes(), baseline);

        if !expected.is_empty() {
            let scope = CaptureWorkScope::new(&original).unwrap();
            let (observed, paths) = inputs(&host, &checkout, &stat, &scope.native);
            let before = original.receipt();
            prop_assert!(matches!(order_host_checkout_union(
                observed, paths, expected.len() - 1, &scope.native,
            ), Err(CaptureError::InvalidOptions)));
            // The exact cardinality refusal precedes either output allocation.
            prop_assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
            drop(scope);
            prop_assert_eq!(original.live_allocation_bytes(), baseline);
        }

        let denied = original.native_volume_work(WorkBudget {
            bytes_copied: 8,
            ..crate::performance::work_budget(100_000, 8 * 1024 * 1024)
        }).unwrap();
        let scope = CaptureWorkScope::new(&denied).unwrap();
        let duplicate = BTreeSet::from([1]);
        let (observed, paths) = inputs(&duplicate, &duplicate, &stat, &scope.native);
        // Both original names use the eight admitted input bytes. The former
        // uncharged unzip would incorrectly succeed with no output copy credit.
        let error = order_host_checkout_union(observed, paths, 1, &scope.native).unwrap_err();
        let refused_copy = matches!(error, CaptureError::Kernel(error) if matches!(
            *error, crate::FsError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. }),
        ));
        prop_assert!(refused_copy, "the original output copy allowance must refuse");
        drop((scope, denied));
        prop_assert_eq!(original.live_allocation_bytes(), baseline);

        cancellation.cancel();
        let error = order_host_checkout_union(Vec::new(), Vec::new(), 0, &original).unwrap_err();
        prop_assert!(matches!(error, CaptureError::Kernel(error) if matches!(*error, crate::FsError::Cancelled(_))));
        prop_assert_eq!(original.live_allocation_bytes(), baseline);
    }
}
