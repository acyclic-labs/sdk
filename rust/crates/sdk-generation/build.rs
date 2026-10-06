use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

const ACTORS_SOURCE: &[&str] = &[
    "src/codegen.rs",
    "src/contract.rs",
    "src/domain.rs",
    "src/wire.rs",
];

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let actors = manifest_dir.join("../actors");
    let mut hasher = Sha256::new();
    for relative in ACTORS_SOURCE {
        let path = actors.join(relative);
        println!("cargo:rerun-if-changed={}", path.display());
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        hasher.update(fs::read(&path).unwrap_or_else(|error| {
            panic!("cannot read Actors source {}: {error}", path.display())
        }));
        hasher.update([0]);
    }
    println!(
        "cargo:rustc-env=SDK_GENERATION_ACTORS_SOURCE_SHA256=sha256:{:x}",
        hasher.finalize()
    );
}
