//! N-API build metadata generation.

fn main() {
    println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_INFERENCE_NATIVE");
    napi_build::setup();
}
