use super::*;

const RENAME_REPETITIONS: usize = 16;

pub(super) async fn rename_hydration_case() -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let mount_path = root.path().join("mount");
    fs::create_dir(&mount_path)?;
    let workspace = prepare_rename_fixture(root.path()).await?;
    let mount = workspace
        .mount(&mount_path, MountOptions::read_write())
        .await
        .map_err(stop_failed_mount)?;
    let child_result = capture_child(
        Command::new(std::env::current_exe()?)
            .arg("--rename-hydration-child")
            .arg(&mount_path),
        Duration::from_secs(10),
    );
    let synced = mount.sync().await.map_err(Failure::from);
    let unmounted = mount.unmount().await.map_err(Failure::from);
    finish_mounted_child(
        "rename hydration child",
        child_result,
        Ok(()),
        synced,
        unmounted,
        &mount_path,
    )?;
    for index in 0..RENAME_REPETITIONS {
        let renamed = format!("/after-{index}.txt");
        let replacement = format!("/replacement-{index}.txt");
        if unbounded!(workspace, read(&renamed, 64))?
            .as_ref()
            != format!("renamed-before-hydration-{index}").as_bytes()
        {
            return Err(format!("renamed content {index} was not published").into());
        }
        if unbounded!(workspace, read(&replacement, 64))?
            .as_ref()
            != format!("replacement-content-{index}").as_bytes()
        {
            return Err(format!("replacement content {index} was not published").into());
        }
        #[cfg(windows)]
        if !matches!(
            unbounded!(workspace, stat(&renamed))?.metadata.windows_attributes,
            Some(attributes) if attributes & 1 != 0
        ) {
            return Err(format!("renamed metadata {index} was not published").into());
        }
    }
    Ok(())
}

pub(super) fn rename_hydration_child(mount: &Path) -> Result<(), Failure> {
    for index in 0..RENAME_REPETITIONS {
        let source = mount.join(format!("before-{index}.txt"));
        let renamed = mount.join(format!("after-{index}.txt"));
        let replacement = mount.join(format!("replacement-{index}.txt"));
        fs::rename(&source, &renamed)?;
        #[cfg(windows)]
        mutate_default_metadata(&renamed)?;
        fs::write(&source, format!("replacement-content-{index}"))?;
        fs::rename(&source, &replacement)?;
        if fs::read(&renamed)? != format!("renamed-before-hydration-{index}").as_bytes() {
            return Err(
                format!("renamed placeholder {index} returned stale or missing content").into(),
            );
        }
        if fs::read(&replacement)? != format!("replacement-content-{index}").as_bytes() {
            return Err(
                format!("replacement path {index} returned stale or missing content").into(),
            );
        }
    }
    Ok(())
}

async fn prepare_rename_fixture(root: &Path) -> Result<LocalWorkspace, Failure> {
    let engine = Fs::local(LocalOptions::new(root.join("store"))).await?;
    let workspace = unbounded!(
        engine,
        create_workspace_with_config(
            "rename-hydration",
            VolumeConfig::native(Lifecycle::Durable),
        )
    )?;
    let mut fixture = unbounded!(workspace, begin_transaction(acyclic_fs::IdempotencyKey::new()))?;
    for index in 0..RENAME_REPETITIONS {
        unbounded!(
            fixture,
            create_file(
                &format!("/before-{index}.txt"),
                Bytes::from(format!("renamed-before-hydration-{index}")),
                FileMetadata::default(),
            )
        )?;
    }
    match fixture
        .commit(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::default(),
                    },
                )
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
    {
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {}
        _ => return Err("rename hydration fixture transaction was not committed".into()),
    }
    Ok(workspace)
}
