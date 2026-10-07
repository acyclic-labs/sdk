//! Build script for the Actors N-API bridge.

fn main() {
    println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_ACTORS_NAPI");
    napi_build::setup();
}
