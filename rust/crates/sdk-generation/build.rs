use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

#[path = "src/compiled_generator_inputs.rs"]
mod compiled_generator_inputs;

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest_dir.join("../../..");
    let mut hasher = Sha256::new();
    for relative in compiled_generator_inputs::PATHS {
        let path = root.join(relative);
        println!("cargo:rerun-if-changed={}", path.display());
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        hasher.update(fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "cannot read compiled generator source {}: {error}",
                path.display()
            )
        }));
        hasher.update([0]);
    }
    println!(
        "cargo:rustc-env=SDK_GENERATION_COMPILED_SOURCE_SHA256=sha256:{:x}",
        hasher.finalize()
    );
}
