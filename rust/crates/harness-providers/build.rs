//! Register the crate-local model-checking configuration.
fn main() {
    // Without a rerun-if line Cargo reruns this script on any package change.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(loom)");
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
}
