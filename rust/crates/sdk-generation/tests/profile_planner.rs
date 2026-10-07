use sdk_docs::rustdoc_profiles::{load_metadata, profiles_for_package};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
}

fn host_target() -> String {
    let output = Command::new("rustc").args(["-vV"]).output().unwrap();
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap()
        .to_owned()
}

#[test]
fn public_feature_profiles_are_bounded_and_target_exact() {
    let root = repository_root();
    let metadata = load_metadata(root.join("Cargo.toml")).unwrap();
    let host = host_target();
    let targets = BTreeSet::from([host.clone(), "wasm32-unknown-unknown".to_owned()]);
    let public_feature_roots = [
        "acyclic-fs",
        "acyclic-objects",
        "acyclic-stream",
        "acyclic-harness",
        "acyclic-machines",
        "acyclic-inference",
    ];

    for package_name in public_feature_roots {
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == package_name)
            .unwrap_or_else(|| panic!("missing package {package_name}"));
        let declared = package
            .features
            .keys()
            .filter(|feature| feature.as_str() != "default")
            .cloned()
            .collect::<BTreeSet<_>>();
        assert!(
            !declared.is_empty(),
            "{package_name} should expose features"
        );

        let host_profiles = profiles_for_package(&metadata, package_name, &host, &targets).unwrap();
        let wasm_profiles =
            profiles_for_package(&metadata, package_name, "wasm32-unknown-unknown", &targets)
                .unwrap();

        // The planner samples default, no-default, each declared feature
        // closure, and one all-features profile; it must not enumerate 2^N.
        assert!(host_profiles.len() <= declared.len() + 3, "{package_name}");
        assert_eq!(host_profiles.len(), wasm_profiles.len());
        assert!(host_profiles.iter().all(|profile| profile.target == host));
        assert!(wasm_profiles
            .iter()
            .all(|profile| profile.target == "wasm32-unknown-unknown"));

        let host_ids = host_profiles
            .iter()
            .map(|profile| profile.id())
            .collect::<BTreeSet<_>>();
        let wasm_ids = wasm_profiles
            .iter()
            .map(|profile| profile.id())
            .collect::<BTreeSet<_>>();
        assert!(host_ids.is_disjoint(&wasm_ids));

        for profile in &host_profiles {
            assert!(!profile.features.contains("default"));
            assert!(profile
                .features
                .iter()
                .all(|feature| declared.contains(feature)));
        }
        for feature in &declared {
            assert!(host_profiles.iter().any(|profile| {
                !profile.default_features && profile.features.contains(feature)
            }));
        }
        assert!(host_profiles
            .iter()
            .any(|profile| { profile.default_features && profile.features == declared }));
    }
}

#[test]
fn public_roots_without_optional_features_stay_single_default_profiles() {
    let root = repository_root();
    let metadata = load_metadata(root.join("Cargo.toml")).unwrap();
    let host = host_target();
    let targets = BTreeSet::from([host.clone()]);
    for package_name in [
        "acyclic-actors",
        "acyclic-workers",
        "acyclic-native-runtime",
        "acyclic-plugin",
    ] {
        let package = metadata
            .packages
            .iter()
            .find(|package| package.name.as_ref() == package_name)
            .unwrap_or_else(|| panic!("missing package {package_name}"));
        assert!(
            package.features.is_empty(),
            "{package_name} unexpectedly has feature metadata"
        );
        let profiles = profiles_for_package(&metadata, package_name, &host, &targets).unwrap();
        assert_eq!(profiles.len(), 1, "{package_name}");
        assert!(profiles[0].default_features);
        assert!(profiles[0].features.is_empty());
    }
}
