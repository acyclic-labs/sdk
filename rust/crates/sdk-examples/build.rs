#[path = "src/source_closure.rs"]
mod source_closure;

use std::{env, path::PathBuf};

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
    if let Some(target) = target {
        println!("cargo:rustc-env=SDK_EXAMPLES_BUILD_TARGET={target}");
    }
}
