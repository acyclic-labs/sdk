//! N-API build metadata generation.

fn main() {
    // Declaration checks select a fresh output directory on every invocation.
    // Make Cargo rerun the macros even when the Rust source is unchanged.
    println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_FS_NAPI");
    napi_build::setup();
}
