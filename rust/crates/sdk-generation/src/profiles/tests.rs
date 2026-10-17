use super::*;

proptest::proptest! {
    #[test]
    fn owner_receipts_are_disjoint_only_for_distinct_canonical_target_names(
        names in proptest::collection::vec("[a-zA-Z_-]{1,8}", 0..8),
    ) {
        let canonical = names.iter().map(|name| name.replace('-', "_").to_ascii_lowercase()).collect::<BTreeSet<_>>();
        proptest::prop_assert_eq!(distinct_receipt_names(names.iter().map(String::as_str)), canonical.len() == names.len());
        for aliases in [["demo", "DEMO"], ["demo-lib", "demo_lib"], ["demo", "demo"]] {
            proptest::prop_assert!(!distinct_receipt_names(aliases));
        }
    }
}
use proptest::prelude::*;

proptest! {
    #[test]
    fn equivalent_profile_keys_are_exactly_the_transitive_local_cargo_feature_set(
        graph in prop::array::uniform4(0u8..16),
        requested in 0u8..16,
        defaults in any::<bool>(),
    ) {
        let names = ["default", "base", "extra", "other"];
        let features = names.iter().enumerate().map(|(from, name)| {
            ((*name).to_owned(), names.iter().enumerate()
                .filter(|(to, _)| graph[from] & (1 << to) != 0)
                .map(|(_, name)| (*name).to_owned()).collect())
        }).collect::<BTreeMap<_, Vec<_>>>();
        let profile = ProfileSpec {
            package: "demo".into(), target: "native".into(), default_features: defaults,
            features: names.iter().enumerate().filter(|(bit, _)| requested & (1 << bit) != 0)
                .map(|(_, name)| (*name).to_owned()).collect(),
        };
        // Independent reference: boolean-matrix transitive closure, including cycles.
        let mut reach = std::array::from_fn::<_, 4, _>(|from| graph[from] | (1 << from));
        for via in 0..4 {
            for from in 0..4 {
                if reach[from] & (1 << via) != 0 { reach[from] |= reach[via]; }
            }
        }
        let seeds = requested | u8::from(defaults);
        let expected = (0..4).filter(|from| seeds & (1 << from) != 0)
            .fold(0, |all, from| all | reach[from]);
        let expected = names.iter().enumerate().filter(|(bit, _)| expected & (1 << bit) != 0)
            .map(|(_, name)| (*name).to_owned()).collect::<BTreeSet<_>>();
        let key = local_feature_configuration(&features, &profile).unwrap();
        prop_assert_eq!(&key.2, &expected);
        let normalized = ProfileSpec { default_features: false, features: expected, ..profile.clone() };
        prop_assert_eq!(Some(key), local_feature_configuration(&features, &normalized));
        let mut nonlocal = features;
        nonlocal.get_mut("other").unwrap().push("dependency/feature".into());
        prop_assert!(local_feature_configuration(&nonlocal, &profile).is_none());
    }
}

#[test]
fn absent_default_feature_and_unknown_requests_are_distinct_from_declared_defaults() {
    let mut features = BTreeMap::from([("base".into(), Vec::new())]);
    let profile = ProfileSpec {
        package: "demo".into(),
        target: "native".into(),
        default_features: true,
        features: BTreeSet::new(),
    };
    assert!(local_feature_configuration(&features, &profile)
        .unwrap()
        .2
        .is_empty());
    features.insert("default".into(), vec!["base".into()]);
    assert_eq!(
        local_feature_configuration(&features, &profile).unwrap().2,
        BTreeSet::from(["default".into(), "base".into()])
    );
    let invalid = ProfileSpec {
        features: BTreeSet::from(["unknown".into()]),
        ..profile
    };
    assert!(local_feature_configuration(&features, &invalid).is_none());
}

#[test]
fn paired_owner_plan_sharing_measurement() {
    use crate::parallel::Arc;
    use std::{hint::black_box, time::Instant};
    let plan = Arc::new(historical::Plan {
        released_packages: (0..16).map(|index| sdk_docs::historical::ReleasedPackage {
            package: format!("owner-{index}"), version: "1.0.0".into(), yanked: false,
            registry_checksum: "0".repeat(64), source_revision: "0".repeat(40), path_in_vcs: String::new(),
        }).collect(),
        source_files: (0..4096).map(|index| (format!("source/{index}.rs"), "0".repeat(64))).collect(),
        archives: Vec::new(), captured_source: None, owner_metadata: BTreeMap::new(),
    });
    let started = Instant::now();
    let old = plan.released_packages.iter().map(|release| {
        // The previous production transport cloned the complete plan per owner.
        let mut copy = historical::Plan {
            released_packages: plan.released_packages.clone(), source_files: plan.source_files.clone(),
            archives: plan.archives.clone(), captured_source: plan.captured_source.clone(),
            owner_metadata: plan.owner_metadata.clone(),
        };
        copy.released_packages = vec![release.clone()];
        copy
    }).collect::<Vec<_>>();
    black_box(&old);
    let copied_ns = started.elapsed().as_nanos();
    let started = Instant::now();
    let shared = Arc::clone(&plan);
    let jobs = shared.released_packages.clone();
    black_box((&shared, &jobs));
    let shared_ns = started.elapsed().as_nanos();
    assert!(Arc::ptr_eq(&plan, &shared));
    assert_eq!(jobs, plan.released_packages);
    assert_eq!(old.len(), jobs.len());
    eprintln!("PAIRED owner_plan owners={} source_entries={} copied_entries={} shared_entries=0 copied_ns={} shared_ns={}",
        jobs.len(), plan.source_files.len(), old.len()*plan.source_files.len(), copied_ns, shared_ns);
}
