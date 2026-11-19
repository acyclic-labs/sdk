//! Generates case folding tables.

#[path = "build/case_fold.rs"]
mod case_fold;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
    println!("cargo:rustc-check-cfg=cfg(kani)");
    println!("cargo:rustc-check-cfg=cfg(loom)");
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    println!("cargo:rustc-check-cfg=cfg(unit_test_split)");
    for area in [
        "mount",
        "mount_driver",
        "mount_lazy",
        "mount_adapter",
        "mount_adapter_namespace",
        "mount_views",
        "mount_watch",
        "host",
        "workspace",
        "workspace_accounting",
        "workspace_merge",
        "workspace_recovery",
        "materializer",
        "facade",
        "facade_checkout",
        "facade_recovery",
        "publication",
        "kernel",
        "foundation",
    ] {
        println!("cargo:rustc-check-cfg=cfg(unit_test_{area})");
    }
    case_fold::generate()?;
    if std::env::var_os("CARGO_FEATURE_NAPI_TYPES").is_some() {
        // napi-derive emits the shared enums into this external directory.
        // A native build without declaration output must not leave Cargo's
        // cached dependency usable when a declaration build requests it.
        println!("cargo:rerun-if-env-changed=NAPI_TYPE_DEF_TMP_FOLDER");
        println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_FS");
    }
    Ok(())
}
