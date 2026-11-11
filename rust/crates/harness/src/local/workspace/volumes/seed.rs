//! Admit a checkout's first home overlay before settings or roles are read.
use super::*;
use acyclic_fs::{
    CaptureOptions, CapturePolicy,
    model::{CheckoutMode, GenerationSelector},
};

// The original source is retained, never deleted or merged into a later home.
// This namespace is excluded from every authored project view.
const SOURCE: &str = ".volumes/agents/.home-seed-v1";

pub(in crate::local::workspace) async fn adopt_home(
    filesystem: &acyclic_fs::LocalFs,
    home: &VolumeRef,
    root: &acyclic_fs::HeldRoot,
) -> Result<()> {
    let directory = match std::fs::symlink_metadata(root.path().join(HOME_PATH)) {
        Ok(metadata) => metadata.is_dir() && !metadata.file_type().is_symlink(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(storage(error)),
    };
    let retained = match std::fs::symlink_metadata(root.path().join(SOURCE)) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(storage(error)),
    };
    if !directory && !retained {
        return Ok(());
    }
    let cancellation = CancellationToken::new();
    let work = WorkBudget::UNBOUNDED;
    let name = home.storage_name(&mut None)?;
    let workspace = filesystem
        .open_workspace(&name, work, &cancellation)
        .await
        .map_err(storage)?
        .value;
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.home-seed/v1\0");
    hash.update(name.as_bytes());
    let mut key = [0; 16];
    key.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    if workspace
        .operation_generation(
            acyclic_fs::IdempotencyKey::from_bytes(key),
            work,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value
        .is_some()
    {
        if directory {
            return Err(Error::Conflict(
                "home overlay was already initialized; refusing a replacement directory".into(),
            ));
        }
        return Ok(());
    }
    if directory && retained {
        return Err(Error::Conflict(
            "home seed source already exists; refusing another directory".into(),
        ));
    }
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            work,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    let path = NamespacePath::new(Vec::new(), checkout.volume_config().limits).map_err(storage)?;
    if !checkout
        .list_directory(&path, None, 1, work, &cancellation)
        .await
        .map_err(storage)?
        .value
        .entries
        .is_empty()
    {
        return Err(Error::Conflict(
            "home seeding requires an empty home volume".into(),
        ));
    }
    if directory {
        let parent = root
            .open_directory(Path::new(".volumes"))
            .map_err(storage)?;
        let destination = held_directories(root, Path::new(HOST_VIEW_ROOT))?;
        parent
            .rename_leaf(Path::new("home"), &destination, Path::new(".home-seed-v1"))
            .map_err(storage)?;
    }
    let source = root.open_directory(Path::new(SOURCE)).map_err(storage)?;
    acyclic_fs::capture_baseline(
        &mut checkout,
        &CaptureOptions {
            source_root: source.path().to_path_buf(),
            expected_root_identity: source.identity(),
            maximum_paths: None,
            maximum_extent_spans: None,
        },
        &CapturePolicy::default(),
        work,
        &cancellation,
    )
    .await
    .map_err(storage)?;
    let committed = checkout
        .commit_even_if_unchanged(
            acyclic_fs::OperationId::from_bytes(key),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            work,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map_err(storage)?
        .value;
    if !matches!(
        committed,
        acyclic_fs::LivePublicationObservation::Committed { .. }
            | acyclic_fs::LivePublicationObservation::AlreadyCommitted { .. }
    ) {
        return Err(Error::Conflict(
            "home seed publication raced with another writer".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_precreated_home_records_completion_and_does_not_reimport() -> Result<()> {
        let project = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::create_dir_all(project.path().join(HOME_PATH)).map_err(storage)?;
        let config = || -> Result<LocalHarnessConfig> {
            Ok(crate::test_support::local_harness_config(
                project.path(),
                (
                    crate::model::Model::new("scripted".into(), "test".into(), "1".into())?,
                    crate::local::tests::text(),
                ),
            )?
            .state_directory(state.path())
            .workspace_mounting(crate::local::WorkspaceMounting::Physical)
            .compaction(crate::context::CompactionPolicy::Disabled))
        };
        let harness = LocalHarness::open(config()?).await?;
        assert_eq!(harness.sampling, None);
        drop(harness);
        std::fs::write(
            project.path().join(SOURCE).join("agent.ts"),
            "export default (base) => ({ ...base, sampling: { temperature: 0.9 } });",
        )
        .map_err(storage)?;
        let harness = LocalHarness::open(config()?).await?;
        assert_eq!(
            harness.sampling, None,
            "completed empty seed must not be replayed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn precreated_home_is_admitted_before_settings_and_is_not_reimported() -> Result<()> {
        let project = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let home = project.path().join(HOME_PATH);
        std::fs::create_dir_all(home.join("templates/worker")).map_err(storage)?;
        let settings =
            "export default { model: 'scripted/test', sampling: { temperature: 0.25 } };";
        let role =
            "---\nversion: 1\ncontext: fork\nmay_spawn: []\nmodel: workers/luna\n---\nWork.\n";
        std::fs::write(home.join("agent.ts"), settings).map_err(storage)?;
        std::fs::write(home.join("templates/worker/AGENT.md"), role).map_err(storage)?;
        let config = || -> Result<LocalHarnessConfig> {
            Ok(crate::test_support::local_harness_config(
                project.path(),
                (
                    crate::model::Model::new("scripted".into(), "test".into(), "1".into())?,
                    crate::local::tests::text(),
                ),
            )?
            .state_directory(state.path())
            .workspace_mounting(crate::local::WorkspaceMounting::Physical)
            .compaction(crate::context::CompactionPolicy::Disabled))
        };
        let harness = LocalHarness::open(config()?).await?;
        assert_eq!(
            harness
                .sampling
                .as_ref()
                .and_then(|sampling| sampling.temperature),
            Some(0.25)
        );
        let filesystem = &harness.workspace().unwrap().filesystem;
        let cancellation = CancellationToken::new();
        let workspace = filesystem
            .open_workspace(
                &harness.scratch.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        let head = workspace
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        assert_eq!(
            head.read(
                "/templates/worker/AGENT.md",
                4096,
                WorkBudget::UNBOUNDED,
                &cancellation
            )
            .await
            .map_err(storage)?
            .value
            .as_ref(),
            role.as_bytes()
        );
        crate::local::tests::settings::write(
            &harness,
            "agent.ts",
            "export default (base) => ({ ...base, sampling: { temperature: 0.75 } });",
        )
        .await?;
        drop(harness);
        // Changing the retained original cannot overwrite admitted home edits.
        std::fs::write(
            project.path().join(SOURCE).join("agent.ts"),
            "export default (base) => ({ ...base, sampling: { temperature: 0.9 } });",
        )
        .map_err(storage)?;
        let harness = LocalHarness::open(config()?).await?;
        assert_eq!(
            harness
                .sampling
                .as_ref()
                .and_then(|sampling| sampling.temperature),
            Some(0.75)
        );
        drop(harness);
        // A new real directory after initialization is refused without damage.
        std::fs::create_dir_all(&home).map_err(storage)?;
        std::fs::write(home.join("replacement"), b"preserve").map_err(storage)?;
        assert!(LocalHarness::open(config()?).await.is_err());
        assert_eq!(
            std::fs::read(home.join("replacement")).map_err(storage)?,
            b"preserve"
        );
        Ok(())
    }
}
