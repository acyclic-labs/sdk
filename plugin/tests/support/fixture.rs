use std::{fs, path::PathBuf};

pub fn test_tempdir(prefix: &str) -> tempfile::TempDir {
    let root = std::env::var_os("ACYCLIC_E2E_SCRATCH_ROOT")
        .map_or_else(default_e2e_scratch_root, PathBuf::from)
        .join("acyclic-e2e-scratch");
    fs::create_dir_all(&root).expect("test scratch root");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(root)
        .expect("workspace-local temporary directory")
}

#[cfg(target_os = "linux")]
fn default_e2e_scratch_root() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .expect("Linux qualification requires XDG_CACHE_HOME or HOME")
}

#[cfg(target_os = "windows")]
fn default_e2e_scratch_root() -> PathBuf {
    // MSVC's linker cannot open its generated object file when a native
    // checkout has this test repository's deeply nested parent path either.
    // Keep the qualification root short enough to test the projection rather
    // than that unrelated linker MAX_PATH limit.
    std::env::temp_dir()
}

#[cfg(all(not(target_os = "linux"), not(target_os = "windows")))]
fn default_e2e_scratch_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("plugin workspace root")
        .parent()
        .expect("plugin workspace parent")
        .to_path_buf()
}

