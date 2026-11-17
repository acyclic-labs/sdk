use super::*;
use crate::local::{config::LocalProject, owner, storage};
use acyclic_native_runtime::signing_key::{MemorySigningStore, SigningCustody};

#[tokio::test]
async fn temporary_state_retires_signing_custody_after_its_resources() -> Result<()> {
    // Child-local environment proves both ambient refusals without mutating
    // the test runner's environment or selecting a real credential backend.
    if std::env::var_os("ACYCLIC_TEMPORARY_SIGNING_CHILD").is_none() {
        let directory = tempfile::tempdir().map_err(storage)?;
        for policy in ["invalid", "native"] {
            let output = std::process::Command::new(std::env::current_exe().map_err(storage)?)
                .args([
                    "--exact",
                    "local::tests::signing::temporary_state_retires_signing_custody_after_its_resources",
                    "--nocapture",
                ])
                .env("ACYCLIC_TEMPORARY_SIGNING_CHILD", "1")
                .env("ACYCLIC_SIGNING_CUSTODY", policy)
                .env("ACYCLIC_SIGNING_CUSTODY_DIRECTORY", directory.path().join("unused"))
                .output()
                .map_err(storage)?;
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert_eq!(
            std::fs::read_dir(directory.path())
                .map_err(storage)?
                .count(),
            0
        );
        return Ok(());
    }
    let workspace = tempfile::tempdir().map_err(storage)?;
    let configured = || -> Result<LocalHarnessConfig> {
        Ok(
            crate::test_support::local_harness_config(workspace.path(), (model()?, text()))?
                .without_default_tools(),
        )
    };
    let explicit = configured()?
        .state_directory(workspace.path().with_extension("explicit"))
        .state_refusals();
    assert!(
        matches!(explicit.as_slice(), [Error::Storage(_)]),
        "explicit ambient custody must still be checked: {explicit:?}"
    );
    let allocations = crate::local::config::TEMPORARY_STATES.get();
    let refusals = configured()?.preflight();
    assert!(refusals.is_empty(), "{refusals:?}");
    let refusals = LocalHarness::doctor(configured()?).await;
    assert!(refusals.is_empty(), "{refusals:?}");
    assert_eq!(crate::local::config::TEMPORARY_STATES.get(), allocations);
    let config = configured()?.prepare_state()?;
    let store = config
        .signing_store
        .clone()
        .expect("temporary memory custody");
    let released = config
        .test_state
        .as_ref()
        .expect("temporary owner")
        .release_barrier();
    let state = config
        .state_directory
        .clone()
        .expect("prepared temporary state");
    let harness = LocalHarness::open(config).await?;
    let custody = SigningCustody::Store(store.as_ref());
    let owner = owner::Owner::open(
        &state,
        &LocalProject::Workspace(workspace.path().to_path_buf()),
        Some(&owner::Checkout::capture(workspace.path())?),
        custody,
        None,
    )?;
    assert!(owner.key_ref.with_seed(custody, |_| ()).is_ok());
    let ambient = owner.key_ref.validate_custody(SigningCustody::Configured);
    assert!(ambient.is_err());
    harness.disconnect().await?;
    released.released().await;
    assert!(!state.exists());
    assert!(owner.key_ref.with_seed(custody, |_| ()).is_err());
    Ok(())
}

#[tokio::test]
async fn injected_signing_custody_follows_store_reopen_and_deletion() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let first_project = volume(&root.path().join("first-data")).await?;
    let second_project = volume(&root.path().join("second-data")).await?;
    let store = Arc::new(MemorySigningStore::new().map_err(storage)?);
    let config = |name: &str| -> Result<LocalHarnessConfig> {
        let project = if name == "first" {
            first_project.clone()
        } else {
            second_project.clone()
        };
        Ok(
            volume_config(&root.path().join(format!("{name}-data")), project, text())?
                .state_directory(root.path().join(name))
                .signing_store(store.clone()),
        )
    };
    let first = LocalHarness::open(config("first")?).await?;
    first.disconnect().await?;
    let reopened = LocalHarness::open(config("first")?).await?;
    reopened.disconnect().await?;
    assert_eq!(store.len(), 1, "reopening created another signing seed");
    let second = LocalHarness::open(config("second")?).await?;
    second.disconnect().await?;
    assert_eq!(store.len(), 2);
    config("first")?.delete_state()?;
    assert!(!root.path().join("first").exists());
    assert_eq!(store.len(), 1, "deleting a store did not retire its seed");
    let second = LocalHarness::open(config("second")?).await?;
    second.disconnect().await?;
    config("second")?.delete_state()?;
    assert!(store.is_empty());
    Ok(())
}
