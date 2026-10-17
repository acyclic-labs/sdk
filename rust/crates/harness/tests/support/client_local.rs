//! Isolated local providers on RAM-backed storage where available. These tests
//! exercise command behavior and process reopen, not persistence across power loss.
use acyclic_harness::{
    LocalHarnessConfig, Result,
    model::{Model, ModelProvider},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Directory {
    _root: tempfile::TempDir,
    workspace: PathBuf,
}

impl Directory {
    pub fn new() -> std::io::Result<Self> {
        let root = if cfg!(target_os = "linux") {
            temporary_directory(Path::new("/dev/shm"))
        } else {
            tempfile::tempdir()
        }?;
        let workspace = root.path().join("project");
        std::fs::create_dir(&workspace)?;
        Ok(Self {
            _root: root,
            workspace,
        })
    }

    pub fn path(&self) -> &Path {
        &self.workspace
    }
}

fn temporary_directory(preferred: &Path) -> std::io::Result<tempfile::TempDir> {
    tempfile::tempdir_in(preferred).or_else(|_| tempfile::tempdir())
}

pub fn config(
    workspace: impl Into<PathBuf>,
    model: (Model, Arc<dyn ModelProvider>),
) -> Result<LocalHarnessConfig> {
    let workspace = workspace.into();
    // Keep all three journals beside the checkout, inside its owning fixture.
    // Reopening the same checkout selects the same state, without consulting HOME.
    let state = workspace.with_extension("state");
    Ok(
        acyclic_harness::test_support::local_harness_config(workspace, model)?
            .state_directory(state),
    )
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn local_directories_are_independent_and_removed(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
        let first = Directory::new().unwrap();
        let second = Directory::new().unwrap();
        let first_root = first.path().parent().unwrap().to_owned();
        let second_root = second.path().parent().unwrap();
        assert_ne!(&first_root, second_root);
        assert!(!first_root.starts_with(second_root));
        std::fs::write(first.path().join("marker"), &bytes).unwrap();
        assert!(!second.path().join("marker").exists());
        assert_eq!(std::fs::read(first.path().join("marker")).unwrap(), bytes);
        drop(first);
        assert!(!first_root.exists());
        assert!(second.path().exists());
        // Missing shared memory must retain the same isolated-fixture contract.
        let fallback = temporary_directory(&second.path().join("missing")).unwrap();
        assert!(!fallback.path().starts_with(second.path()));
    }
}
