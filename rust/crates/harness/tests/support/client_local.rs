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

async fn check_directories<const EXPLICIT_STATE: bool>(bytes: &[u8]) -> Result<()> {
    let first = Directory::new().unwrap();
    let second = Directory::new().unwrap();
    let first_root = std::fs::canonicalize(first.path().parent().unwrap()).unwrap();
    let second_root = std::fs::canonicalize(second.path().parent().unwrap()).unwrap();
    assert_ne!(first_root, second_root);
    assert!(!first_root.starts_with(&second_root));
    std::fs::write(first.path().join("marker"), bytes).unwrap();
    assert!(!second.path().join("marker").exists());
    assert_eq!(std::fs::read(first.path().join("marker")).unwrap(), bytes);
    #[cfg(feature = "native-execution")]
    let states = check_owners::<EXPLICIT_STATE>(&first, &second).await?;
    drop(first);
    assert!(!first_root.exists());
    #[cfg(feature = "native-execution")]
    assert!(
        !states.0.exists(),
        "first owner state survived fixture cleanup"
    );
    assert!(second.path().exists());
    #[cfg(feature = "native-execution")]
    assert!(states.1.join("owner.json").is_file());
    // Missing shared memory must retain the same isolated-fixture contract.
    let fallback = temporary_directory(&second.path().join("missing")).unwrap();
    assert!(!fallback.path().starts_with(second.path()));
    drop(second);
    assert!(!second_root.exists());
    #[cfg(feature = "native-execution")]
    assert!(
        !states.1.exists(),
        "second owner state survived fixture cleanup"
    );
    Ok(())
}

#[cfg(feature = "native-execution")]
async fn check_owners<const EXPLICIT_STATE: bool>(
    first: &Directory,
    second: &Directory,
) -> Result<(PathBuf, PathBuf)> {
    use acyclic_harness::{LocalHarness, test_support::ScriptedModelProvider};
    let configure = |directory: &Directory| {
        let model = Model::new(
            "scripted".into(),
            "fixture".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let binding = (
            model,
            Arc::new(ScriptedModelProvider::new([])) as Arc<dyn ModelProvider>,
        );
        let configured = if EXPLICIT_STATE {
            config(directory.path(), binding)?
        } else {
            acyclic_harness::test_support::local_harness_config(directory.path(), binding)?
        };
        Ok::<_, acyclic_harness::Error>(configured.without_default_tools())
    };
    let first_harness = LocalHarness::open(configure(first)?).await?;
    let second_harness = LocalHarness::open(configure(second)?).await?;
    let first_state = first.path().with_extension("state");
    let second_state = second.path().with_extension("state");
    for (directory, state, harness) in [
        (first, &first_state, &first_harness),
        (second, &second_state, &second_harness),
    ] {
        assert!(
            state.join("owner.json").is_file(),
            "owner state escaped fixture cleanup root"
        );
        let state = std::fs::canonicalize(state).unwrap();
        let root = std::fs::canonicalize(directory.path().parent().unwrap()).unwrap();
        assert!(state.starts_with(root));
        assert!(!state.starts_with(std::fs::canonicalize(directory.path()).unwrap()));
        let owner: serde_json::Value =
            serde_json::from_slice(&std::fs::read(state.join("owner.json")).unwrap()).unwrap();
        assert_eq!(
            owner["project"],
            serde_json::to_value(harness.project()).unwrap()
        );
        assert_eq!(
            owner["workspace"],
            serde_json::to_value(std::fs::canonicalize(directory.path()).unwrap()).unwrap()
        );
        for journal in [
            "stream/stream.journal",
            "filesystem/stream/stream.journal",
            "filesystem/objects/mutations.log",
        ] {
            assert!(state.join(journal).is_file(), "missing {journal}");
        }
    }
    assert_ne!(first_state, second_state);
    assert_ne!(first_harness.project(), second_harness.project());
    first_harness.close().await?;
    second_harness.close().await?;
    Ok((first_state, second_state))
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn local_directories_are_independent_and_removed(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_directories::<true>(&bytes)).unwrap();
    }
}

#[cfg(all(target_os = "linux", feature = "native-execution"))]
#[test]
fn omitting_state_directory_is_detected() {
    const CHILD: &str = "ACYCLIC_COMMAND_FIXTURE_OMIT_STATE";
    if acyclic_harness::test_support::environment(CHILD).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(check_directories::<false>(b"negative control"))
            .unwrap();
        return;
    }
    // Only the child changes XDG_DATA_HOME; parallel tests retain their own state.
    let home = Directory::new().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "client_commands::client_local::omitting_state_directory_is_detected",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("XDG_DATA_HOME", home.path())
        .output()
        .unwrap();
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "missing state_directory escaped the shared assertions"
    );
    assert!(
        message.contains("owner state escaped fixture cleanup root"),
        "{message}"
    );
}
