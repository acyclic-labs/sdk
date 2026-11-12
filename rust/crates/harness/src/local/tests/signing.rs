use super::*;
use crate::local::{config::LocalProject, owner, storage};
use acyclic_native_runtime::signing_key::{MemorySigningStore, SigningCustody};

#[tokio::test]
async fn temporary_state_retires_signing_custody_after_its_resources() -> Result<()> {
    let workspace = tempfile::tempdir().map_err(storage)?;
    let config = crate::test_support::local_harness_config(workspace.path(), (model()?, text()))?
        .prepare_state()?;
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
