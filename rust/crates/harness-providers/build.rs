//! Opt Loom models in when ACYCLIC_LOOM=1 and the model-checking feature is on.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(loom)");
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
}
