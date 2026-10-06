//! N-API build metadata generation.

fn main() {
    println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_STREAM_NATIVE");
    napi_build::setup();
}
