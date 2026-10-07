//! Build script for the Stream N-API bridge.

fn main() {
    println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_STREAM_NAPI");
    napi_build::setup();
}
