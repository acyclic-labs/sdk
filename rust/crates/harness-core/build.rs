//! Qualifies the harness's opt-in model checking and fuzz cfgs.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    let version = std::env::var("CARGO_PKG_VERSION")?;
    let initialization = format!(
        "{{\"capabilities\":{{}},\"clientInfo\":{{\"name\":\"acyclic-harness\",\"version\":\"{version}\"}},\"protocolVersion\":\"2025-11-25\"}}"
    );
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR missing")?);
    std::fs::write(
        output.join("build_metadata.rs"),
        format!(
            "#[cfg(not(target_arch = \"wasm32\"))]\npub(crate) const PACKAGE_VERSION: &str = {version:?};\npub(crate) const MCP_INITIALIZATION: &str = {initialization:?};\n"
        ),
    )?;
    Ok(())
}
