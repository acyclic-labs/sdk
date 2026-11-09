//! Register the crate-local model-checking configuration.
fn main() {
    // Without a rerun-if line Cargo reruns this script on any package change.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(loom)");
}
