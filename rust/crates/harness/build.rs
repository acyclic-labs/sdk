//! Select the Loom model-checking configuration when the environment asks for it.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
    Ok(())
}
