//! Builds the reviewed high-level FUSE-T bridge on macOS only.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_NATIVE_MOUNT");

    if std::env::var_os("CARGO_FEATURE_NATIVE_MOUNT").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=src/native_mount/fuse_t_bridge.c");

    #[cfg(target_os = "macos")]
    if let Err(error) = build_fuse_t_bridge() {
        eprintln!("FUSE-T libfuse3 development package is required on macOS: {error}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "macos")]
fn build_fuse_t_bridge() -> Result<(), String> {
    use std::path::PathBuf;

    // FUSE-T versions its pkg-config package after the bridge release (for
    // example 1.2.7), not after the compatible libfuse ABI (3.x).
    println!("cargo:rerun-if-env-changed=FUSE_T_PREFIX");
    let (include_paths, library_paths) = match pkg_config::Config::new()
        .cargo_metadata(false)
        .probe("fuse3")
    {
        Ok(library) => (library.include_paths, library.link_paths),
        Err(probe_error) => {
            let prefix = std::env::var_os("FUSE_T_PREFIX")
                .map_or_else(|| PathBuf::from("/usr/local"), PathBuf::from);
            let include = prefix.join("include/fuse3");
            let library = prefix.join("lib");
            if !include.join("fuse.h").is_file() || !library.join("libfuse3.a").is_file() {
                return Err(format!(
                    "pkg-config failed ({probe_error}); verified FUSE-T headers and static library are absent below {}",
                    prefix.display()
                ));
            }
            (vec![include], vec![library])
        }
    };
    let static_library = library_paths
        .iter()
        .find(|path| path.join("libfuse3.a").is_file())
        .ok_or_else(|| "FUSE-T did not provide the required static libfuse3 archive".to_owned())?;
    emit_static_fuse_t_link(static_library)?;
    let mut build = cc::Build::new();
    build.file("src/native_mount/fuse_t_bridge.c");
    for include in include_paths {
        build.include(include);
    }
    build.flag_if_supported("-std=c11");
    build.warnings(true);
    build.compile("acyclic_fs_fuse_t_bridge");
    // The static archive makes every final consumer binary independent of
    // Cargo's DYLD_LIBRARY_PATH and mutable machine-global loader paths. Its
    // constructor uses DiskArbitration, so declare the required frameworks.
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
    println!("cargo:rustc-link-lib=framework=DiskArbitration");
    Ok(())
}

#[cfg(target_os = "macos")]
fn emit_static_fuse_t_link(library: &std::path::Path) -> Result<(), String> {
    let encoded = library
        .to_str()
        .ok_or_else(|| "FUSE-T library path is not UTF-8".to_owned())?;
    println!("cargo:rustc-link-search=native={encoded}");
    println!("cargo:rustc-link-lib=static=fuse3");
    Ok(())
}
