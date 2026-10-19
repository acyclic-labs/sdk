pub fn workspace() -> acyclic_harness::Result<tempfile::TempDir> {
    tempfile::tempdir().map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
}
