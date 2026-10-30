use super::*;

pub(super) fn crash_recovery() -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let crash_root = root.path().join("child");
    let mount = root.path().join("mount");
    fs::create_dir(&mount)?;
    let qualification = crash_recovery_inner(&crash_root, &mount);
    #[cfg(windows)]
    let qualification = qualification.and_then(|()| fs::remove_dir(&mount).map_err(Failure::from));
    if qualification.is_err() {
        // Preserve the original recovery owner/error and avoid deleting through
        // a mount whose detach was not proved. Cleanup never replays recovery.
        eprintln!(
            "crash qualification fixture retained at {}",
            root.keep().display()
        );
    }
    qualification
}

const CRASH_READY: &[u8] = b"acyclic-crash-mount-ready\n";

fn crash_recovery_inner(crash_root: &Path, mount: &Path) -> Result<(), Failure> {
    tokio::runtime::Runtime::new()?.block_on(async {
        let mut child = CapturedChild::spawn(
            Command::new(std::env::current_exe()?)
                .arg("--crash-child")
                .arg(crash_root)
                .arg(mount),
        )?;
        let ready = child.receipt(CRASH_READY).await;
        eprintln!("qualification phase: crash child receipt settled; termination starting");
        let stopped = child.stop().await;
        match (ready, stopped) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(cleanup)) => {
                Err(format!("{error}; crash child cleanup also failed: {cleanup}").into())
            }
        }
    })?;
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
        recover_mount(mount)?;
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
fn recover_mount(mount: &Path) -> Result<(), Failure> {
    let mut recovery = NativeMountRecoveryOwner::acquire(mount)?;
    recovery.recover().map(|_| ()).map_err(|error| {
        acyclic_fs::NativeMountError::RecoveryFailed {
            error: Box::new(error),
            owner: recovery,
        }
        .into()
    })
}

pub(super) fn crash_child(root: PathBuf, mount: PathBuf) -> Result<(), Failure> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        fs::create_dir_all(&root)?;
        let engine = Fs::local(LocalOptions::new(root.join("store"))).await?;
        let workspace = unbounded!(
            engine,
            create_workspace_with_config("crash-owner", VolumeConfig::native(Lifecycle::Durable),)
        )?;
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
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(CRASH_READY)?;
        stdout.flush()?;
        drop(stdout);
        let mut unexpected_release = [0];
        std::io::stdin()
            .lock()
            .read_exact(&mut unexpected_release)?;
        Err::<(), Failure>(
            "crash fixture received an orderly release instead of termination".into(),
        )
    })
}

#[cfg(windows)]
async fn verify_remounted_workspace(crash_root: &Path, mount: &Path) -> Result<(), Failure> {
    eprintln!("qualification phase: reopening durable crash workspace");
    let engine = Fs::local(LocalOptions::new(crash_root.join("store"))).await?;
    let workspace = unbounded!(engine, open_workspace("crash-owner"))?;
    if unbounded!(workspace, read("/alive.txt", 16))?.as_ref() != b"alive" {
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
