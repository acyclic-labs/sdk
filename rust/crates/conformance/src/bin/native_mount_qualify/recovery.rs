use super::*;

pub(super) fn crash_recovery() -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let crash_root = root.path().join("child");
    let mount = root.path().join("mount");
    let ready = root.path().join("ready");
    fs::create_dir(&mount)?;
    let qualification = crash_recovery_inner(&crash_root, &mount, &ready);
    #[cfg(windows)]
    // The recovered path becomes an ordinary directory after the resumed
    // provider stops. The preserved residue lives under the same temporary
    // parent and is removed only when this isolated fixture is dropped.
    let cleanup = fs::remove_dir(&mount).map_err(Failure::from);
    #[cfg(not(windows))]
    let cleanup = recover_with_retry(&mount);
    match (qualification, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Err(cleanup_error)) => {
            Err(format!("{error}; final mount cleanup also failed: {cleanup_error}").into())
        }
    }
}

fn crash_recovery_inner(crash_root: &Path, mount: &Path, ready: &Path) -> Result<(), Failure> {
    let mut child = spawn_captured_child(
        Command::new(std::env::current_exe()?)
            .arg("--crash-child")
            .arg(crash_root)
            .arg(mount)
            .arg(ready),
    )?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() && Instant::now() < deadline {
        if captured_child_output_exceeds_limit(&child)? {
            child.process.terminate(None)?;
            return Err(
                format!("crash child output exceeded {MAX_CHILD_OUTPUT_BYTES} bytes").into(),
            );
        }
        if let Some(status) = child.process.try_wait(None)? {
            let output = captured_child_output(status, &mut child.stdout, &mut child.stderr)?;
            return Err(format!(
                "crash child exited before readiness ({}): {}{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !ready.exists() {
        child.process.terminate(None)?;
        return Err("crash child did not mount within 30 seconds".into());
    }
    eprintln!("qualification phase: crash child ready; termination starting");
    child.process.terminate(None)?;
    eprintln!("qualification phase: crash child terminated; recovery starting");
    #[cfg(windows)]
    {
        let engine = Fs::local(LocalOptions::new(crash_root.join("store")));
        let runtime = tokio::runtime::Runtime::new()?;
        let volume = runtime.block_on(async {
            let engine = engine.await?;
            let workspace = engine
                .open_workspace(
                    "crash-owner",
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
            Ok::<_, Failure>(workspace.value.id().volume_id())
        })?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let parent_path = mount.parent().ok_or("crash mount has no parent")?;
        let (parent, _) = work.open_root_directory(parent_path)?;
        let preserved = parent_path.join("preserved");
        // Generic disposable recovery must still reject a stale writable
        // ProjFS cache. Retain that same actual owner through preservation,
        // rather than racing its asynchronous retirement with reacquisition.
        let mut owner = NativeMountRecoveryOwner::acquire_in_directory(mount, &parent, &work)?;
        if owner.recover().is_ok() {
            return Err("generic recovery discarded a stale ProjFS root".into());
        }
        if !owner.preserve_crashed_in_directory(&parent, Path::new("preserved"), volume, &work)? {
            return Err("crash recovery failed to preserve the stale ProjFS root".into());
        }
        if !preserved.is_dir() || mount.exists() {
            return Err("crash recovery did not move the stale root aside".into());
        }
        if fs::read(preserved.join("unpublished.txt"))? != b"unpublished" {
            return Err("crash recovery lost unpublished authored residue".into());
        }
        let mut retry = NativeMountRecoveryOwner::acquire_in_directory(mount, &parent, &work)?;
        if retry.preserve_crashed_in_directory(&parent, Path::new("preserved"), volume, &work)? {
            return Err("crash recovery was not idempotent".into());
        }
        fs::create_dir(mount)?;
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(verify_remounted_workspace(crash_root, mount))?;
        if !preserved.is_dir() {
            return Err("recovered mount discarded the preserved residue".into());
        }
        eprintln!("qualification phase: preserved stale root and resumed at original path");
        Ok(())
    }
    #[cfg(not(windows))]
    {
        recover_with_retry(mount)?;
        eprintln!("qualification phase: crash recovery finished");
        if mount.exists() && (!mount.is_dir() || fs::read_dir(mount)?.next().is_some()) {
            return Err("crash recovery did not restore an empty ordinary directory".into());
        }
        fs::create_dir_all(mount)?;
        fs::write(mount.join("restored.txt"), b"ordinary")?;
        fs::remove_file(mount.join("restored.txt"))?;
        Ok(())
    }
}

#[cfg(not(windows))]
fn recover_with_retry(mount: &Path) -> Result<(), Failure> {
    let mut recovery = NativeMountRecoveryOwner::acquire(mount)?;
    let mut recovery_error = None;
    for attempt in 0..2 {
        match recovery.recover() {
            Ok(_) => return Ok(()),
            Err(error) => {
                recovery_error = Some(error);
                if attempt == 0 {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
    Err(acyclic_fs::NativeMountError::RecoveryFailed {
        error: Box::new(recovery_error.unwrap_or_else(|| {
            acyclic_fs::NativeMountError::Lifecycle("native mount recovery failed")
        })),
        owner: recovery,
    }
    .into())
}

pub(super) fn crash_child(root: PathBuf, mount: PathBuf, ready: PathBuf) -> Result<(), Failure> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        fs::create_dir_all(&root)?;
        let engine = Fs::local(LocalOptions::new(root.join("store"))).await?;
        let workspace = engine
            .create_workspace_with_config(
                "crash-owner",
                VolumeConfig::native(Lifecycle::Durable),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        workspace.write_text("/alive.txt", "alive").await?;
        let _mount = workspace
            .mount(&mount, MountOptions::read_write())
            .await
            .map_err(stop_failed_mount)?;
        #[cfg(windows)]
        fs::write(mount.join("unpublished.txt"), b"unpublished")?;
        #[cfg(not(windows))]
        {
            if fs::read(mount.join("alive.txt"))? != b"alive" {
                return Err("crash fixture did not hydrate projected content".into());
            }
            let _open_hydrated_file = File::open(mount.join("alive.txt"))?;
        }
        fs::write(ready, b"ready")?;
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    })
}

#[cfg(windows)]
async fn verify_remounted_workspace(crash_root: &Path, mount: &Path) -> Result<(), Failure> {
    eprintln!("qualification phase: reopening durable crash workspace");
    let engine = Fs::local(LocalOptions::new(crash_root.join("store"))).await?;
    let workspace = engine
        .open_workspace(
            "crash-owner",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    if workspace
        .read(
            "/alive.txt",
            16,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .as_ref()
        != b"alive"
    {
        return Err::<(), Failure>("durable workspace lost crash fixture".into());
    }
    eprintln!("qualification phase: remounting the original ProjFS destination");
    let resumed = workspace
        .mount(mount, MountOptions::read_write())
        .await
        .map_err(stop_failed_mount)?;
    eprintln!("qualification phase: reading durable ProjFS content");
    let contents = fs::read(mount.join("alive.txt"))?;
    resumed.unmount().await?;
    if contents != b"alive" {
        return Err::<(), Failure>("recovered mount lost the durable workspace".into());
    }
    Ok::<(), Failure>(())
}
