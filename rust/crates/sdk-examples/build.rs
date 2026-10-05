#[path = "src/source_closure.rs"]
mod source_closure;

use acyclic_sdk_contract_wire::{BindingTransport, transport_control};
use std::{env, path::PathBuf, process::Command};

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let workspace_root = manifest_dir.join("../../..");
    let files = source_closure::closure_files(&workspace_root)
        .unwrap_or_else(|error| panic!("resolve source-owned example closure: {error}"));
    let target = env::var("TARGET").ok();
    let digest = source_closure::digest_files(&workspace_root, &files, target.as_deref())
        .unwrap_or_else(|error| panic!("hash source-owned example closure: {error}"));
    for relative in &files {
        println!(
            "cargo:rerun-if-changed={}",
            workspace_root.join(relative).display()
        );
    }
    println!("cargo:rustc-env=SDK_EXAMPLES_SOURCE_SHA256={digest}");
    // Capture the revision while compiling the producer so a cached binary
    // cannot report a later checkout's HEAD when it is executed.
    println!("cargo:rerun-if-env-changed=SDK_EXAMPLES_SOURCE_GIT_REVISION");
    let source_git_revision = env::var("SDK_EXAMPLES_SOURCE_GIT_REVISION")
        .ok()
        .map(|revision| {
            assert!(
                revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "SDK_EXAMPLES_SOURCE_GIT_REVISION must be a full 40-character hexadecimal revision"
            );
            revision
        })
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&workspace_root)
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        })
        .filter(|revision| !revision.is_empty());
    if let Some(revision) = source_git_revision {
        println!("cargo:rustc-env=SDK_EXAMPLES_SOURCE_GIT_REVISION={revision}");
    }
    if let Some(target) = target {
        println!("cargo:rustc-env=SDK_EXAMPLES_BUILD_TARGET={target}");
    }
    let control_output = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR")).join("control");
    transport_control::generate_control_bindings(
        &control_output,
        BindingTransport::Tonic {
            client: true,
            server: true,
        },
    )
    .unwrap_or_else(|error| panic!("generate Rust transport control bindings: {error}"));
}
