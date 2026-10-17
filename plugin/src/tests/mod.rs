use super::*;
mod installation;
mod manifests;
mod runtime;
struct Fixture {
    data: PathBuf,
    _directory: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        Self {
            data: directory.path().to_path_buf(),
            _directory: directory,
        }
    }
}
