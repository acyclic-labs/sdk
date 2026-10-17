pub fn workspace() -> acyclic_harness::Result<tempfile::TempDir> {
    let root = env!("CARGO_TARGET_TMPDIR");
    std::fs::create_dir_all(root)
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    tempfile::tempdir_in(root).map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
}
