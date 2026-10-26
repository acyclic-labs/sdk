use super::*;

enum QualificationWatcher {
    Native(acyclic_fs::watch::NativeEventWatcher),
    Poll(notify::PollWatcher),
}

impl QualificationWatcher {
    fn watch(&mut self, path: &Path) -> notify::Result<()> {
        match self {
            Self::Native(watcher) => watcher.watch(path, RecursiveMode::Recursive),
            Self::Poll(watcher) => watcher.watch(path, RecursiveMode::Recursive),
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
pub(super) async fn mutation_matrix(kind: &'static str) -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("store");
    let mount_path = root.path().join("mount");
    fs::create_dir(&mount_path)?;
    fs::write(root.path().join("outside-guard"), b"outside-safe")?;
    let engine = Fs::local(LocalOptions::new(&store)).await?;
    let workspace = unbounded!(
        engine,
        create_workspace_with_config("native-matrix", VolumeConfig::native(Lifecycle::Durable),)
    )?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(acyclic_fs::IdempotencyKey::new())
    )?;
    unbounded!(transaction, create_dir_all("/seed/nested"))?;
    unbounded!(
        transaction,
        create_file(
            "/seed/nested/original.txt",
            Bytes::from_static(b"seed"),
            FileMetadata::default(),
        )
    )?;
    unbounded!(
        transaction,
        create_file(
            "/seed/nested/default-metadata.txt",
            Bytes::new(),
            FileMetadata::default(),
        )
    )?;
    unbounded!(
        transaction,
        create_file(
            "/seed/nested/default-read.txt",
            Bytes::from_static(b"read-only-observation"),
            FileMetadata::default(),
        )
    )?;
    unbounded!(
        transaction,
        create_file(
            "/seed/nested/rename-before-read.txt",
            Bytes::from_static(b"renamed-before-hydration"),
            FileMetadata::default(),
        )
    )?;
    unbounded!(
        transaction,
        hard_link(
            "/seed/nested/original.txt",
            "/seed/nested/original-hard.txt",
        )
    )?;
    unbounded!(
        transaction,
        create_symbolic_link(
            "/seed/nested/original-link.txt",
            native_link_bytes("original.txt"),
        )
    )?;
    add_large_windows_fixture(kind, &mut transaction).await?;
    if kind == "macos-nfs" {
        unbounded!(transaction, create_dir_all("/large-tree"))?;
        for index in 0..MACOS_NFS_LARGE_TREE_ENTRIES {
            unbounded!(
                transaction,
                create_file(
                    &format!("/large-tree/entry-{index:04}.txt"),
                    Bytes::from_static(b"large-tree"),
                    FileMetadata::default(),
                )
            )?;
        }
    }
    match transaction
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
        _ => return Err("fixture transaction was not committed".into()),
    }
    #[cfg(windows)]
    let default_read_metadata =
        unbounded!(workspace, stat("/seed/nested/default-read.txt"))?.metadata;

    let (watch_tx, watch_rx) = mpsc::channel();
    let mut watcher = if kind == "macos-nfs" {
        QualificationWatcher::Poll(notify::PollWatcher::new(
            move |event| {
                let _ = watch_tx.send(event);
            },
            notify::Config::default().with_poll_interval(Duration::from_millis(100)),
        )?)
    } else {
        QualificationWatcher::Native(acyclic_fs::watch::NativeEventWatcher::new(
            move |event| {
                let _ = watch_tx.send(event);
            },
            notify::Config::default(),
        )?)
    };
    let mount = workspace
        .mount(&mount_path, MountOptions::read_write())
        .await
        .map_err(stop_failed_mount)?;
    watcher.watch(&mount_path)?;
    let child_result = capture_child(
        Command::new(std::env::current_exe()?)
            .arg("--io-child")
            .arg(&mount_path)
            .env("ACYCLIC_EXPECTED_NATIVE_KIND", kind),
        Duration::from_secs(10),
    );
    let watched = wait_for_watch_after_child(
        &watch_rx,
        &mount_path.join("nested/watch-sentinel.txt"),
        &child_result,
    );
    eprintln!("qualification phase: I/O child finished; parallel clients starting");
    let parallel = if kind == "macos-nfs" {
        run_parallel_io_children(&mount_path, MACOS_NFS_PARALLEL_CLIENTS)
    } else {
        Ok(())
    };
    eprintln!("qualification phase: parallel clients finished; mount sync starting");
    let synced = mount.sync().await.map_err(Failure::from);
    eprintln!("qualification phase: mount sync finished; unmount starting");
    let detached = mount.unmount().await.map_err(Failure::from);
    eprintln!("qualification phase: unmount finished");
    finish_mounted_child(
        "I/O child",
        child_result,
        watched,
        synced,
        detached,
        &mount_path,
    )?;
    parallel?;
    verify_published_mutations(&workspace, root.path()).await?;
    let metadata = unbounded!(workspace, stat("/nested/metadata.txt"))?.metadata;
    #[cfg(windows)]
    {
        if metadata
            .windows_attributes
            .is_none_or(|attributes| attributes & 1 == 0)
        {
            return Err("Windows read-only metadata was not published".into());
        }
        let timestamps = unbounded!(workspace, stat("/nested/timestamps.txt"))?.metadata;
        if timestamps.modified_ns != Some(QUALIFICATION_MODIFIED_NS) {
            return Err(format!(
                "modified timestamp was not published: {:?}",
                timestamps.modified_ns
            )
            .into());
        }
        let defaulted = unbounded!(workspace, stat("/seed/nested/default-metadata.txt"))?.metadata;
        if defaulted
            .windows_attributes
            .is_none_or(|attributes| attributes & 1 == 0)
            || defaulted.modified_ns != Some(QUALIFICATION_MODIFIED_NS)
        {
            return Err(format!(
                "metadata on a default-metadata projection was not published: {defaulted:?}"
            )
            .into());
        }
        let observed = unbounded!(workspace, stat("/seed/nested/default-read.txt"))?.metadata;
        if observed != default_read_metadata {
            return Err(format!(
                "plain read published metadata for a default-metadata projection: before {default_read_metadata:?}, after {observed:?}"
            )
            .into());
        }
    }
    #[cfg(unix)]
    if metadata.posix_mode.is_none_or(|mode| mode & 0o777 != 0o640) {
        return Err("POSIX mode metadata was not published".into());
    }
    if kind == "macos-nfs" {
        macos_nfs_handoff_and_mount_loss(&workspace, root.path()).await?;
    }
    Ok(())
}

async fn verify_published_mutations(
    workspace: &LocalWorkspace,
    root: &Path,
) -> Result<(), Failure> {
    verify_mount_isolation(workspace, root).await?;
    for (path, expected, failure) in [
        (
            "/nested/created.txt",
            b"created-v2".as_slice(),
            "created content was not published",
        ),
        (
            "/nested/atomic.txt",
            b"atomic-new".as_slice(),
            "atomic save was not published",
        ),
        (
            "/nested/renamed.txt",
            b"rename-me".as_slice(),
            "rename was not published",
        ),
    ] {
        if unbounded!(workspace, read(path, 64))?.as_ref() != expected {
            return Err(failure.into());
        }
    }
    if unbounded!(workspace, stat("/nested/delete-me.txt")).is_ok() {
        return Err("delete was not published".into());
    }
    let original = unbounded!(workspace, stat("/nested/hard-source.txt"))?;
    let linked = unbounded!(workspace, stat("/nested/hard-link.txt"))?;
    if original.file_id != linked.file_id || original.link_count < 2 {
        return Err("hard-link identity was not preserved".into());
    }
    if workspace
        .read_symbolic_link(
            "/nested/symbolic.txt",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.bytes)
        .map_err(|failure| failure.error)?
        != native_link_bytes("created.txt").into()
    {
        return Err("symbolic-link target was not published".into());
    }
    Ok(())
}

async fn add_large_windows_fixture(
    kind: &str,
    transaction: &mut LocalTransaction,
) -> Result<(), Failure> {
    if kind != "windows-projfs" {
        return Ok(());
    }
    unbounded!(transaction, create_dir_all("/large"))?;
    for ordinal in 0..300_u16 {
        let extension = if ordinal % 10 == 0 { "rs" } else { "txt" };
        unbounded!(
            transaction,
            create_file(
                &format!("/large/entry-{ordinal:04}.{extension}"),
                Bytes::from(format!("entry-{ordinal}")),
                FileMetadata::default(),
            )
        )?;
    }
    Ok(())
}

fn wait_for_watch(
    receiver: &mpsc::Receiver<notify::Result<notify::Event>>,
    sentinel: &Path,
) -> Result<(), Failure> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_error = None;
    while Instant::now() < deadline {
        match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(Ok(event)) if event.paths.iter().any(|path| path == sentinel) => {
                return Ok(());
            }
            Ok(Ok(_)) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Ok(Err(error)) => last_error = Some(error),
            Err(error) => return Err(format!("watcher channel failed: {error}").into()),
        }
    }
    Err(last_error
        .map_or_else(
            || "no sentinel watcher event observed within 15 seconds".to_owned(),
            |error| format!("no sentinel watcher event observed; last watcher error: {error}"),
        )
        .into())
}

fn wait_for_watch_after_child(
    receiver: &mpsc::Receiver<notify::Result<notify::Event>>,
    sentinel: &Path,
    child: &Result<std::process::Output, Failure>,
) -> Result<(), Failure> {
    match child {
        Ok(output) if output.status.success() => wait_for_watch(receiver, sentinel),
        _ => Ok(()),
    }
}

pub(super) fn verify_large_windows_directory(mount: &Path) -> Result<(), Failure> {
    let large = mount.join("large");
    if fs::read_dir(&large)?.count() != 300
        || fs::read(large.join("entry-0000.rs"))? != b"entry-0"
        || fs::read(large.join("entry-0299.txt"))? != b"entry-299"
    {
        return Err("large projected directory was incomplete or corrupt".into());
    }
    let wildcard = tool_output(
        Command::new("cmd")
            .current_dir(&large)
            .args(["/D", "/Q", "/C", "dir", "/B", "/A:-D", "/ON", "*.rs"]),
    )?;
    let wildcard_output = String::from_utf8_lossy(&wildcard.stdout);
    let wildcard_count = wildcard_output.lines().count();
    if !wildcard.status.success() || wildcard_count != 30 {
        return Err(format!(
            "wildcard enumeration failed (status {}, count {wildcard_count}): stdout={wildcard_output:?}, stderr={:?}",
            wildcard.status,
            String::from_utf8_lossy(&wildcard.stderr)
        )
        .into());
    }
    Ok(())
}

async fn verify_mount_isolation(workspace: &LocalWorkspace, root: &Path) -> Result<(), Failure> {
    if fs::read(root.join("outside-guard"))? != b"outside-safe" {
        return Err("escape probe changed content outside the mount".into());
    }
    let escaped = root.join("escape-attempt.txt");
    if fs::read(&escaped)? != b"host-only" {
        return Err("traversal mutation did not remain in the temporary host parent".into());
    }
    if unbounded!(workspace, stat("/escape-attempt.txt")).is_ok() {
        return Err("traversal mutation escaped into workspace state".into());
    }
    fs::remove_file(escaped)?;
    Ok(())
}
