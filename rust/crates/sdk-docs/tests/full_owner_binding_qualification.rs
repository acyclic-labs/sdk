use sdk_docs::{build_data, BuildInput, Channel};
use sdk_docs::rustdoc_profiles::{api_owner_for_package, extract_owned_api_for_crate, load_metadata_with_cargo, observe_rustdoc, project_into_docs, validate_rustdoc_version, ProfileSpec};
use std::{collections::BTreeMap, path::{Path, PathBuf}};

#[test]
fn qualify_all_published_and_binding_receipts() {
    let root = PathBuf::from(r"C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk");
    let metadata = load_metadata_with_cargo(root.join("Cargo.toml"), Some(Path::new(r"Q:\rustup\toolchains\1.98.1-x86_64-pc-windows-msvc\bin\cargo.exe"))).unwrap();
    let mut rows = vec![
        ("acyclic-actors", "acyclic_actors.json", "x86_64-pc-windows-msvc"),
        ("acyclic-fs", "acyclic_fs.json", "x86_64-pc-windows-msvc"),
        ("acyclic-harness", "acyclic_harness.json", "x86_64-pc-windows-msvc"),
        ("acyclic-inference", "acyclic_inference.json", "x86_64-pc-windows-msvc"),
        ("acyclic-inference-contract", "acyclic_inference_contract.json", "x86_64-pc-windows-msvc"),
        ("acyclic-machines", "acyclic_machines.json", "x86_64-pc-windows-msvc"),
        ("acyclic-native-runtime", "acyclic_native_runtime.json", "x86_64-pc-windows-msvc"),
        ("acyclic-objects", "acyclic_objects.json", "x86_64-pc-windows-msvc"),
        ("acyclic-plugin", "acyclic_plugin.json", "x86_64-pc-windows-msvc"),
        ("acyclic-stream", "acyclic_stream.json", "x86_64-pc-windows-msvc"),
        ("acyclic-workers", "acyclic_workers.json", "x86_64-pc-windows-msvc"),
        ("acyclic-actors-napi", "acyclic_actors_napi.json", "x86_64-pc-windows-msvc"),
        ("acyclic-actors-wasm", "acyclic_actors_wasm.json", "wasm32-unknown-unknown"),
        ("acyclic-fs-napi", "acyclic_fs_napi.json", "x86_64-pc-windows-msvc"),
        ("acyclic-fs-wasm", "acyclic_fs_wasm.json", "wasm32-unknown-unknown"),
        ("acyclic-inference-wasm", "acyclic_inference_wasm.json", "wasm32-unknown-unknown"),
        ("acyclic-machines-wasm", "acyclic_machines_wasm.json", "wasm32-unknown-unknown"),
        ("acyclic-objects-wasm", "acyclic_objects_wasm.json", "wasm32-unknown-unknown"),
        ("acyclic-stream-napi", "acyclic_stream_napi.json", "x86_64-pc-windows-msvc"),
        ("acyclic-stream-wasm", "acyclic_stream_wasm.json", "wasm32-unknown-unknown"),
    ];
    let mut files = Vec::new();
    let mut items = Vec::new();
    let mut profiles = BTreeMap::new();
    for (package, filename, target) in rows.drain(..) {
        let receipt_dir = if package.ends_with("-napi") || package.ends_with("-wasm") { "receipts" } else { "owner-receipts" };
        let path = PathBuf::from(r"Q:\scratch\rust-profile-qualification").join(receipt_dir).join(filename);
        let observation = observe_rustdoc(&path).unwrap();
        assert_eq!(observation.format_version, 60, "{package}");
        assert_eq!(observation.crate_version.as_deref(), Some("0.2.0"), "{package}");
        assert_eq!(observation.target, target, "{package}");
        validate_rustdoc_version(&metadata, package, &observation).unwrap();
        let owner = api_owner_for_package(&metadata, package).unwrap();
        let spec = ProfileSpec { package: package.into(), target: target.into(), default_features: true, features: Default::default() };
        let id = spec.id();
        let extracted = extract_owned_api_for_crate(&path, &owner, id.clone(), &observation.crate_name).unwrap();
        assert!(!extracted.is_empty(), "no public API for {package}");
        items.extend(extracted);
        profiles.insert(id, spec);
        files.push(path);
    }
    let data = build_data(&BuildInput { version: "0.2.0".into(), channel: Channel::Preview, revision: "0123456789012345678901234567890123456789".into(), source_state: "working-tree".into(), source_sha256: None, repository_root: root, rustdoc_files: files, generated_sources: Vec::new(), mark_latest: false }).unwrap();
    let sidecar = project_into_docs(&data, &metadata, items, &profiles).unwrap();
    assert_eq!(data.families.len(), 20);
    assert!(!sidecar.entries.is_empty());
    assert!(sidecar.entries.iter().flat_map(|entry| &entry.profiles).any(|profile| profile.capabilities.iter().any(|capability| capability == "native")));
    assert!(sidecar.entries.iter().flat_map(|entry| &entry.profiles).any(|profile| profile.capabilities.iter().any(|capability| capability == "browser")));
    eprintln!("families={} entries={} profiles={}", data.families.len(), sidecar.entries.len(), profiles.len());
}
