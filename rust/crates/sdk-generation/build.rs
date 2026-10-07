use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

const TYPESCRIPT_BINDING_SOURCE: &[&str] = &[
    "rust/crates/actors/src/codegen.rs",
    "rust/crates/actors/src/contract.rs",
    "rust/crates/actors/src/domain.rs",
    "rust/crates/actors/src/wire.rs",
    "rust/crates/actors/Cargo.toml",
    "rust/crates/sdk-generation/Cargo.toml",
    "rust/crates/sdk-generation/Cargo.lock",
    "rust/crates/sdk-generation/build.rs",
    "rust/crates/sdk-generation/src/main.rs",
];

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest_dir.join("../../..");
    let mut hasher = Sha256::new();
    for relative in TYPESCRIPT_BINDING_SOURCE {
        let path = root.join(relative);
        println!("cargo:rerun-if-changed={}", path.display());
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        hasher.update(fs::read(&path).unwrap_or_else(|error| {
            panic!("cannot read TypeScript binding source {}: {error}", path.display())
        }));
        hasher.update([0]);
    }
    println!(
        "cargo:rustc-env=SDK_GENERATION_TYPESCRIPT_BINDING_SHA256=sha256:{:x}",
        hasher.finalize()
    );
}
