use super::{load_workspace_metadata, profiles_for_package};
use std::collections::BTreeSet;

#[test]
fn public_feature_profiles_are_bounded_and_target_exact() {
    let root = acyclic_test_support::source_path("");
    let metadata = load_workspace_metadata(
        &root,
        &acyclic_test_support::source_path("workspace-metadata.json"),
    )
    .unwrap();
    let targets = [
        "x86_64-unknown-linux-gnu",
        "x86_64-pc-windows-msvc",
        "aarch64-apple-darwin",
        "wasm32-unknown-unknown",
    ]
    .map(str::to_owned)
    .into_iter()
    .collect::<BTreeSet<_>>();
    let public_roots = metadata
        .workspace_packages()
        .into_iter()
        .filter(|package| {
            package
                .publish
                .as_ref()
                .is_none_or(|registries| !registries.is_empty())
        })
        .collect::<Vec<_>>();
    assert!(!public_roots.is_empty());

    for package in public_roots {
        let package_name = package.name.as_ref();
        let declared = package
            .features
            .keys()
            .filter(|feature| feature.as_str() != "default")
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut ids = BTreeSet::new();
        let mut count = None;
        for target in &targets {
            let profiles = profiles_for_package(&metadata, package_name, target, &targets).unwrap();
            if package.features.is_empty() {
                assert_eq!(profiles.len(), 1, "{package_name}");
                assert!(profiles[0].default_features);
                assert!(profiles[0].features.is_empty());
            }
            assert!(profiles.len() <= declared.len() + 3, "{package_name}");
            assert_eq!(*count.get_or_insert(profiles.len()), profiles.len());
            let mut covered = BTreeSet::new();
            let mut has_all_features = false;
            for profile in &profiles {
                assert_eq!(&profile.target, target);
                assert!(ids.insert(profile.id()));
                assert!(profile.features.is_subset(&declared));
                if profile.default_features {
                    has_all_features |= profile.features == declared;
                } else {
                    covered.extend(&profile.features);
                }
            }
            for feature in &declared {
                assert!(covered.contains(feature), "{package_name}: {feature}");
            }
            assert!(has_all_features, "{package_name}: {target}");
        }
    }
}
