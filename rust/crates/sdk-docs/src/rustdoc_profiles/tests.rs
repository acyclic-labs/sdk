use super::*;
use rustdoc_types::FORMAT_VERSION;
use std::{fs, path::PathBuf, process::Command};

mod workspace;

fn local_item_names(path: impl AsRef<std::path::Path>) -> Result<BTreeSet<String>, ProfileError> {
    let (_, receipt) = super::receipts::read_public_receipt(path.as_ref())?;
    Ok(receipt
        .index
        .values()
        .filter(|item| item.crate_id == 0)
        .filter_map(|item| item.name.clone())
        .collect())
}

fn metadata() -> Metadata {
    let json = serde_json::json!({
        "packages": [{
            "name": "feature-rich", "version": "0.1.0", "id": "path+file:///fixture#feature-rich@0.1.0",
            "source": null, "dependencies": [], "targets": [],
            "features": {"default": ["json"], "json": ["codec", "dep:serde_json"], "codec": [], "wasm": ["codec", "other/feature"]},
            "manifest_path": "C:/fixture/Cargo.toml", "categories": [], "keywords": [], "readme": null,
            "repository": null, "homepage": null, "documentation": null, "edition": "2021",
            "metadata": null, "links": null, "publish": null, "default_run": null, "rust_version": null
        }],
        "workspace_members": ["path+file:///fixture#feature-rich@0.1.0"],
        "workspace_default_members": ["path+file:///fixture#feature-rich@0.1.0"],
        "resolve": null, "workspace_root": "C:/fixture", "target_directory": "C:/fixture/target",
        "build_directory": null, "metadata": null, "version": 1
    });
    serde_json::from_value(json).expect("valid Cargo metadata fixture")
}

#[test]
fn browser_profiles_follow_features_and_fail_if_target_is_missing() {
    let mut metadata = metadata();
    let package = &metadata.packages[0];
    let targets = BTreeSet::from(["wasm32-unknown-unknown".into()]);
    let profile = browser_profile_for_package(package, false, &targets)
        .unwrap()
        .unwrap();
    assert!(!profile.default_features);
    assert_eq!(
        profile.features,
        BTreeSet::from(["wasm".into(), "codec".into()])
    );
    assert!(matches!(
        browser_profile_for_package(package, false, &BTreeSet::new()),
        Err(ProfileError::UnsupportedTarget { .. })
    ));
    metadata.packages[0].features.clear();
    assert!(
        browser_profile_for_package(&metadata.packages[0], false, &targets)
            .unwrap()
            .is_none()
    );
    assert!(
        browser_profile_for_package(&metadata.packages[0], true, &targets)
            .unwrap()
            .is_some()
    );
}

#[test]
fn browser_binding_policy_excludes_native_features_and_rejects_invalid_policy() {
    let mut metadata = metadata();
    let targets = BTreeSet::from(["wasm32-unknown-unknown".into()]);
    metadata.packages[0].metadata = serde_json::json!({"napi":{
        "wasm-package":"feature-rich", "wasm-features":["wasm"], "wasm-no-default-features":true}});
    let profile = browser_profile_for_package(&metadata.packages[0], true, &targets)
        .unwrap()
        .unwrap();
    assert!(!profile.default_features);
    assert!(!profile.features.contains("json"));
    metadata.packages[0].metadata["napi"]["wasm-features"] = serde_json::json!(["absent"]);
    assert!(matches!(
        browser_profile_for_package(&metadata.packages[0], true, &targets),
        Err(ProfileError::InvalidFeature { .. })
    ));
    metadata.packages[0].metadata["napi"]["wasm-features"] = serde_json::json!("wasm");
    assert!(browser_profile_for_package(&metadata.packages[0], true, &targets).is_err());
}

#[test]
fn unsupported_target_is_hard_error() {
    let result = profiles_for_package(
        &metadata(),
        "feature-rich",
        "wasm32-wasip1",
        &BTreeSet::new(),
    );
    assert_eq!(
        result,
        Err(ProfileError::UnsupportedTarget {
            target: "wasm32-wasip1".into()
        })
    );
}

#[test]
fn historical_defaults_cover_features_without_changing_current_profiles() {
    let metadata = metadata();
    let targets = BTreeSet::from(["host".into()]);
    let historical =
        historical_profiles_for_package(&metadata, "feature-rich", "host", &targets).unwrap();
    assert!(historical.iter().all(|profile| profile.default_features));
    assert!(historical.iter().any(|profile| profile.features.is_empty()));
    assert!(historical
        .iter()
        .any(|profile| profile.features == BTreeSet::from(["codec".into(), "json".into()])));
    assert!(historical.iter().any(|profile| profile.features
        == BTreeSet::from(["codec".into(), "json".into(), "wasm".into()])));
    assert!(
        profiles_for_package(&metadata, "feature-rich", "host", &targets)
            .unwrap()
            .iter()
            .any(|profile| !profile.default_features && profile.features.is_empty())
    );
}

#[test]
fn historical_docs_rs_policy_is_required_and_validated() {
    let mut metadata = metadata();
    let targets = BTreeSet::from(["host".into()]);
    metadata.packages[0].metadata =
        serde_json::json!({"docs":{"rs":{"all-features":true,"no-default-features":true}}});
    let profiles =
        historical_profiles_for_package(&metadata, "feature-rich", "host", &targets).unwrap();
    assert!(profiles
        .iter()
        .any(|profile| profile.default_features && profile.features.is_empty()));
    assert!(profiles.iter().any(|profile| !profile.default_features
        && profile.features == BTreeSet::from(["codec".into(), "json".into(), "wasm".into()])));
    metadata.packages[0].metadata = serde_json::json!({"docs":{"rs":{"features":["absent"]}}});
    assert!(matches!(
        historical_profiles_for_package(&metadata, "feature-rich", "host", &targets),
        Err(ProfileError::InvalidFeature { .. })
    ));
    metadata.packages[0].metadata = serde_json::json!({"docs":{"rs":{"all-features":"true"}}});
    assert!(historical_profiles_for_package(&metadata, "feature-rich", "host", &targets).is_err());
    metadata.packages[0].metadata =
        serde_json::json!({"docs":{"rs":{"rustdoc-args":["--cfg","pretend"]}}});
    assert!(historical_profiles_for_package(&metadata, "feature-rich", "host", &targets).is_err());
}

#[test]
fn feature_profiles_include_local_closure_and_all_features() {
    let profiles = profiles_for_package(
        &metadata(),
        "feature-rich",
        "host",
        &BTreeSet::from(["host".into()]),
    )
    .unwrap();
    assert!(profiles
        .iter()
        .any(|profile| profile.default_features && profile.features.is_empty()));
    assert!(profiles
        .iter()
        .any(|profile| profile.features == BTreeSet::from(["json".into(), "codec".into()])));
    assert!(profiles.iter().any(|profile| profile.default_features
        && profile.features == BTreeSet::from(["codec".into(), "json".into(), "wasm".into()])));
}

#[test]
fn profile_ids_distinguish_defaults_for_the_same_features() {
    let mut profile = ProfileSpec {
        package: "p".into(),
        target: "t".into(),
        default_features: false,
        features: BTreeSet::from(["a".into(), "b".into()]),
    };
    let without_defaults = profile.id();
    profile.default_features = true;
    assert_ne!(without_defaults, profile.id());
}

#[test]
fn rustdoc_version_must_match_cargo_package_even_without_items() {
    let valid = RustdocObservation {
        format_version: FORMAT_VERSION,
        crate_name: "feature_rich".into(),
        crate_version: Some("0.1.0".into()),
        target: "host".into(),
        includes_private: false,
        item_count: 1,
    };
    validate_rustdoc_version(&metadata(), "feature-rich", &valid).unwrap();
    let mut invalid = valid.clone();
    invalid.crate_version = Some("0.9.0".into());
    assert!(validate_rustdoc_version(&metadata(), "feature-rich", &invalid).is_err());
    invalid.crate_version = None;
    assert!(validate_rustdoc_version(&metadata(), "feature-rich", &invalid).is_err());
}

#[test]
fn invalid_feature_fails_before_cargo_runs() {
    let profile = ProfileSpec {
        package: "feature-rich".into(),
        target: "host".into(),
        default_features: false,
        features: BTreeSet::from(["missing".into()]),
    };
    let error = execute_profile(
        "missing-Cargo.toml",
        &metadata(),
        &profile,
        &BTreeSet::from(["host".into()]),
        "unused-target",
        "unused-output.json",
        CargoExecutionContext::default(),
        &RustdocTarget::Library,
    )
    .expect_err("invalid feature must be rejected before spawning Cargo");
    assert_eq!(
        error,
        ProfileError::InvalidFeature {
            package: "feature-rich".into(),
            feature: "missing".into(),
        }
    );
}

#[test]
fn private_binding_keeps_binding_receipt_and_resolves_published_owner() {
    let value = serde_json::json!({
        "packages": [
            {
                "name": "published-root", "version": "0.1.0", "id": "path+file:///fixture#published-root@0.1.0",
                "source": null, "dependencies": [], "targets": [], "features": {},
                "manifest_path": "C:/fixture/Cargo.toml", "categories": [], "keywords": [], "readme": null,
                "repository": null, "homepage": null, "documentation": null, "edition": "2021",
                "metadata": null, "links": null, "publish": null, "default_run": null, "rust_version": null
            },
            {
                "name": "published-root-napi", "version": "0.1.0", "id": "path+file:///fixture#published-root-napi@0.1.0",
                "source": null,
                "dependencies": [{"name": "published-root", "source": null, "req": "*", "kind": null, "optional": false, "uses_default_features": true, "features": [], "target": null, "rename": null, "registry": null, "path": "C:/fixture"}],
                "targets": [], "features": {}, "manifest_path": "C:/fixture/Cargo.toml", "categories": [], "keywords": [], "readme": null,
                "repository": null, "homepage": null, "documentation": null, "edition": "2021",
                "metadata": null, "links": null, "publish": [], "default_run": null, "rust_version": null
            }
        ],
        "workspace_members": [], "workspace_default_members": [], "resolve": null,
        "workspace_root": "C:/fixture", "target_directory": "C:/fixture/target", "build_directory": null,
        "metadata": null, "version": 1
    });
    let metadata: Metadata = serde_json::from_value(value).expect("valid binding metadata fixture");
    let owner = api_owner_for_package(&metadata, "published-root-napi").unwrap();
    assert_eq!(owner.published_package, "published-root");
    assert_eq!(owner.rustdoc_package, "published-root-napi");
    assert_eq!(owner.kind, ApiOwnerKind::OtherBinding);
}

#[test]
fn real_format_60_fixture_is_accepted_when_requested() {
    let Some(path) = std::env::var_os("RUSTDOC_PROFILE_FIXTURE") else {
        return;
    };
    let observation = observe_rustdoc(path).unwrap();
    assert_eq!(observation.format_version, FORMAT_VERSION);
    assert_eq!(observation.crate_name, "acyclic_actors");
    assert_eq!(observation.crate_version.as_deref(), Some("0.2.0"));
    assert!(!observation.includes_private);
    assert_eq!(observation.target, "x86_64-pc-windows-msvc");
    assert!(observation.item_count > 100);
}

#[test]
fn real_binding_rustdoc_retains_binding_ownership_when_requested() {
    let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
        return;
    };
    let owner = ApiOwner {
        published_package: "acyclic-fs".into(),
        rustdoc_package: "acyclic-fs-napi".into(),
        kind: ApiOwnerKind::NapiBinding,
    };
    let items = extract_owned_api(
        path,
        &owner,
        ProfileId("binding-host-default".into()),
        &owner.rustdoc_package.replace('-', "_"),
    )
    .unwrap();
    assert!(!items.is_empty());
    assert!(items.iter().all(|item| {
        item.rustdoc_package == "acyclic-fs-napi" && item.published_owner == "acyclic-fs"
    }));
    assert!(items.iter().any(|item| item.docs_present));
}

#[test]
fn real_binding_top_level_exports_are_documented_when_requested() {
    let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
        return;
    };
    let owner = ApiOwner {
        published_package: "acyclic-fs".into(),
        rustdoc_package: "acyclic-fs-napi".into(),
        kind: ApiOwnerKind::NapiBinding,
    };
    let items = extract_owned_api(
        path,
        &owner,
        ProfileId("binding-host-default".into()),
        &owner.rustdoc_package.replace('-', "_"),
    )
    .unwrap();
    let top_level = items
        .iter()
        .filter(|item| item.key.path.matches("::").count() == 1)
        .collect::<Vec<_>>();
    assert!(!top_level.is_empty());
    assert!(top_level.iter().all(|item| item.docs_present));
}

#[test]
fn real_wasm_binding_rustdoc_retains_browser_owned_api_when_requested() {
    let Some(path) = std::env::var_os("RUSTDOC_WASM_BINDING_FIXTURE") else {
        return;
    };
    let owner = ApiOwner {
        published_package: "acyclic-fs".into(),
        rustdoc_package: "acyclic-fs-wasm".into(),
        kind: ApiOwnerKind::WasmBinding,
    };
    let observation = observe_rustdoc(&path).unwrap();
    assert_eq!(observation.format_version, FORMAT_VERSION);
    assert_eq!(observation.crate_name, "acyclic_fs_wasm");
    assert_eq!(observation.crate_version.as_deref(), Some("0.2.0"));
    assert_eq!(observation.target, "wasm32-unknown-unknown");
    let items = extract_owned_api(
        path,
        &owner,
        ProfileId("wasm-binding-no-defaults".into()),
        &owner.rustdoc_package.replace('-', "_"),
    )
    .unwrap();
    assert!(!items.is_empty());
    assert!(items.iter().all(|item| {
        item.rustdoc_package == "acyclic-fs-wasm" && item.published_owner == "acyclic-fs"
    }));
    assert!(items
        .iter()
        .any(|item| { item.key.path.contains("Browser") || item.key.path.contains("browser") }));
}

#[test]
fn real_binding_projection_keeps_binding_only_types_when_requested() {
    let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
        return;
    };
    let owner = ApiOwner {
        published_package: "acyclic-fs".into(),
        rustdoc_package: "acyclic-fs-napi".into(),
        kind: ApiOwnerKind::NapiBinding,
    };
    let items = extract_owned_api(
        path,
        &owner,
        ProfileId("binding-host-default".into()),
        &owner.rustdoc_package.replace('-', "_"),
    )
    .unwrap();

    // These types belong to the binding crate. A projection through the
    // published core owner would lose the binding namespace entirely.
    assert!(items.iter().any(|item| {
        item.key.kind == "struct"
            && item.key.path.starts_with("acyclic_fs_napi::Native")
            && item.rustdoc_package == "acyclic-fs-napi"
            && item.published_owner == "acyclic-fs"
    }));
}

#[test]
fn projection_sidecar_preserves_binding_identity_versions_and_capabilities() {
    let Some(path) = std::env::var_os("RUSTDOC_BINDING_FIXTURE") else {
        return;
    };
    let Some(metadata_path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
        return;
    };
    let metadata: Metadata =
        serde_json::from_slice(&fs::read(metadata_path).unwrap()).expect("Cargo metadata");
    let owner = ApiOwner {
        published_package: "acyclic-fs".into(),
        rustdoc_package: "acyclic-fs-napi".into(),
        kind: ApiOwnerKind::NapiBinding,
    };
    let profile = ProfileSpec {
        package: "acyclic-fs-napi".into(),
        target: "x86_64-pc-windows-msvc".into(),
        default_features: true,
        features: BTreeSet::new(),
    };
    let profile_id = profile.id();
    let items = extract_owned_api(
        path,
        &owner,
        profile_id.clone(),
        &owner.rustdoc_package.replace('-', "_"),
    )
    .unwrap();
    let api_items = items
        .iter()
        .map(|item| crate::ApiItem {
            id: format!("binding:{}", item.key.path),
            parent_id: None,
            name: item.key.path.rsplit("::").next().unwrap().into(),
            kind: item.key.kind.clone(),
            path: item.key.path.clone(),
            signature: item.key.signature.clone(),
            docs: None,
            links: BTreeMap::new(),
            source: None,
            reexport: None,
            reexport_target: None,
        })
        .collect();
    let data = crate::DocsData {
        schema: crate::DATA_SCHEMA_VERSION.into(),
        schema_version: crate::DATA_SCHEMA_VERSION.into(),
        version: "0.2.0".into(),
        channel: crate::Channel::Preview,
        source: crate::SourceInfo {
            captured_source: None,
            publication_status: crate::PublicationStatus::Candidate,
            released_packages: Vec::new(),
            revision: "a".repeat(40),
            source_state: "working-tree".into(),
            source_sha256: None,
            input_sha256: "a".repeat(64),
            rustdoc_format_versions: vec![FORMAT_VERSION],
            generator: "sdk-docs/test".into(),
        },
        navigation: crate::Navigation {
            entries: Vec::new(),
        },
        packages: crate::PackageCatalog {
            entries: Vec::new(),
        },
        search: crate::SearchIndex {
            entries: Vec::new(),
        },
        families: vec![crate::Family {
            slug: "acyclic-fs-napi".into(),
            title: "acyclic-fs-napi".into(),
            crate_name: "acyclic_fs_napi".into(),
            items: api_items,
            guides: Vec::new(),
        }],
    };
    let profiles = BTreeMap::from([(profile_id, profile)]);
    let sidecar = project_into_docs(&data, &metadata, items, &profiles).unwrap();
    assert_eq!(sidecar.schema, PROFILE_AVAILABILITY_SCHEMA);
    let native_binding = sidecar
        .entries
        .iter()
        .find(|entry| entry.path.starts_with("acyclic_fs_napi::Native") && entry.kind == "struct");
    let native_binding = native_binding.expect("binding-only type should be projected");
    assert!(native_binding
        .profiles
        .iter()
        .any(|profile| profile.rustdoc_package == "acyclic-fs-napi"
            && profile.published_owner == "acyclic-fs"
            && profile.rustdoc_version == "0.2.0"
            && profile.published_version == "0.2.0"
            && profile.capabilities == vec!["napi", "native"]));
}

#[test]
fn real_cargo_metadata_drives_binding_owner_profiles_when_requested() {
    let Some(path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
        return;
    };
    let metadata: Metadata =
        serde_json::from_slice(&fs::read(path).unwrap()).expect("Cargo metadata fixture");
    let napi = api_owner_for_package(&metadata, "acyclic-fs-napi").unwrap();
    assert_eq!(napi.published_package, "acyclic-fs");
    assert_eq!(napi.rustdoc_package, "acyclic-fs-napi");
    assert_eq!(napi.kind, ApiOwnerKind::NapiBinding);
    let wasm = api_owner_for_package(&metadata, "acyclic-fs-wasm").unwrap();
    assert_eq!(wasm.published_package, "acyclic-fs");
    assert_eq!(wasm.kind, ApiOwnerKind::WasmBinding);
    let plugin = api_owner_for_package(&metadata, "acyclic-plugin").unwrap();
    assert_eq!(plugin.published_package, "acyclic-plugin");
    assert_eq!(plugin.rustdoc_package, "acyclic-plugin");
    assert_eq!(plugin.kind, ApiOwnerKind::PublishedRoot);
}

#[test]
fn real_cargo_profiles_have_distinct_public_items_when_requested() {
    let Some(manifest) = std::env::var_os("RUSTDOC_PROFILE_MANIFEST") else {
        return;
    };
    let Some(metadata_path) = std::env::var_os("CARGO_METADATA_PROFILE_FIXTURE") else {
        return;
    };
    let Some(target_dir) = std::env::var_os("RUSTDOC_PROFILE_TARGET_DIR") else {
        return;
    };
    let metadata: Metadata =
        serde_json::from_slice(&fs::read(metadata_path).unwrap()).expect("Cargo metadata fixture");
    let target = "x86_64-pc-windows-msvc";
    let available_targets = BTreeSet::from([target.to_owned()]);
    let profiles =
        profiles_for_package(&metadata, "acyclic-objects", target, &available_targets).unwrap();
    let json = profiles
        .iter()
        .find(|profile| profile.features == BTreeSet::from(["json".into()]))
        .unwrap();
    let all_features = profiles
        .iter()
        .find(|profile| {
            profile.default_features
                && profile.features.contains("http")
                && profile.features.contains("local")
        })
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let out_dir = directory.path();
    let first = out_dir.join("objects-json.json");
    let second = out_dir.join("objects-all-features.json");
    let first_observation = execute_profile(
        &manifest,
        &metadata,
        json,
        &available_targets,
        &target_dir,
        &first,
        CargoExecutionContext::default(),
        &RustdocTarget::Library,
    )
    .unwrap();
    let second_observation = execute_profile(
        &manifest,
        &metadata,
        all_features,
        &available_targets,
        &target_dir,
        &second,
        CargoExecutionContext::default(),
        &RustdocTarget::Library,
    )
    .unwrap();
    assert_eq!(first_observation.crate_name, "acyclic_objects");
    assert_eq!(second_observation.crate_name, "acyclic_objects");
    assert_ne!(
        local_item_names(&first).unwrap(),
        local_item_names(&second).unwrap()
    );
}
