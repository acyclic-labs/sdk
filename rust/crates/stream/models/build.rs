//! Emits `cfg(loom)` only under ACYCLIC_LOOM=1 with the model-checking feature (CONTRIBUTING.md).

fn main() {
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
}
