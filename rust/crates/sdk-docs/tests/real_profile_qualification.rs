use sdk_docs::{build_data, BuildInput, Channel};
use sdk_docs::rustdoc_profiles::{api_owner_for_package, extract_owned_api_for_crate, load_metadata_with_cargo, observe_rustdoc, project_into_docs, validate_rustdoc_version, ProfileSpec};
use std::{collections::{BTreeMap, BTreeSet}, path::{Path, PathBuf}};
#[test]
fn qualify_real_binding_matrix() {
    let root = PathBuf::from(r"C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk");
    let metadata = load_metadata_with_cargo(root.join("Cargo.toml"), Some(Path::new(r"Q:\rustup\toolchains\1.98.1-x86_64-pc-windows-msvc\bin\cargo.exe"))).unwrap();
    let entries = [("acyclic-actors-napi", "acyclic_actors_napi.json", "x86_64-pc-windows-msvc"), ("acyclic-actors-wasm", "acyclic_actors_wasm.json", "wasm32-unknown-unknown"), ("acyclic-fs-napi", "acyclic_fs_napi.json", "x86_64-pc-windows-msvc"), ("acyclic-fs-wasm", "acyclic_fs_wasm.json", "wasm32-unknown-unknown"), ("acyclic-inference-wasm", "acyclic_inference_wasm.json", "wasm32-unknown-unknown"), ("acyclic-machines-wasm", "acyclic_machines_wasm.json", "wasm32-unknown-unknown"), ("acyclic-objects-wasm", "acyclic_objects_wasm.json", "wasm32-unknown-unknown"), ("acyclic-stream-napi", "acyclic_stream_napi.json", "x86_64-pc-windows-msvc"), ("acyclic-stream-wasm", "acyclic_stream_wasm.json", "wasm32-unknown-unknown")];
    let mut files = Vec::new(); let mut items = Vec::new(); let mut profiles = BTreeMap::new(); let mut native = false; let mut browser = false;
    for (package, filename, target) in entries {
        let path = PathBuf::from(r"Q:\scratch\rust-profile-qualification\receipts").join(filename); let obs = observe_rustdoc(&path).unwrap();
        assert_eq!(obs.format_version, 60, "{package}"); assert_eq!(obs.crate_version.as_deref(), Some("0.2.0"), "{package}"); assert_eq!(obs.target, target, "{package}"); assert!(!obs.includes_private, "{package}"); validate_rustdoc_version(&metadata, package, &obs).unwrap();
        let owner = api_owner_for_package(&metadata, package).unwrap(); assert_ne!(owner.published_package, package);
        let spec = ProfileSpec { package: package.into(), target: target.into(), default_features: true, features: BTreeSet::new() }; let id = spec.id(); let crate_name = package.replace('-', "_");
        let owned = extract_owned_api_for_crate(&path, &owner, id.clone(), &crate_name).unwrap(); assert!(!owned.is_empty(), "no public API extracted for {package}"); assert!(owned.iter().all(|item| item.rustdoc_version.as_deref() == Some("0.2.0")));
        items.extend(owned); profiles.insert(id, spec); files.push(path); if target.starts_with("wasm32") { browser = true; } else { native = true; }
    }
    assert!(native && browser);
    let data = build_data(&BuildInput { version: "0.2.0".into(), channel: Channel::Preview, revision: "0123456789012345678901234567890123456789".into(), source_state: "working-tree".into(), source_sha256: None, repository_root: root, rustdoc_files: files, generated_sources: Vec::new(), mark_latest: false }).unwrap();
    let sidecar = project_into_docs(&data, &metadata, items, &profiles).unwrap(); assert_eq!(sidecar.schema, "sdk-docs-profile-availability.v1"); assert!(!sidecar.entries.is_empty()); assert!(sidecar.entries.iter().flat_map(|e| &e.profiles).any(|p| p.capabilities.iter().any(|x| x == "browser"))); assert!(sidecar.entries.iter().flat_map(|e| &e.profiles).any(|p| p.capabilities.iter().any(|x| x == "native"))); assert!(sidecar.entries.iter().all(|e| e.profiles.iter().all(|p| p.rustdoc_version == "0.2.0" && p.published_version == "0.2.0")));
    eprintln!("families={} sidecar_entries={} profiles={}", data.families.len(), sidecar.entries.len(), profiles.len());
}
