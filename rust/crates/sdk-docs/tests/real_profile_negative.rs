use sdk_docs::rustdoc_profiles::{load_metadata_with_cargo, observe_rustdoc, validate_rustdoc_version};
use serde_json::Value;
use std::{fs, path::{Path, PathBuf}};

#[test]
fn real_stale_release_receipt_is_rejected() {
    let source = PathBuf::from(r"Q:\scratch\rust-profile-qualification\receipts\acyclic_fs_wasm.json");
    let stale = PathBuf::from(r"Q:\scratch\rust-profile-qualification\stale-version.json");
    let mut value: Value = serde_json::from_slice(&fs::read(&source).unwrap()).unwrap();
    value["crate_version"] = Value::String("0.1.0".into());
    fs::write(&stale, serde_json::to_vec(&value).unwrap()).unwrap();
    let observation = observe_rustdoc(&stale).unwrap();
    let metadata = load_metadata_with_cargo(
        Path::new(r"C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk\Cargo.toml"),
        Some(Path::new(r"Q:\rustup\toolchains\1.98.1-x86_64-pc-windows-msvc\bin\cargo.exe")),
    ).unwrap();
    assert!(validate_rustdoc_version(&metadata, "acyclic-fs-wasm", &observation).is_err());
    fs::remove_file(stale).unwrap();
}
