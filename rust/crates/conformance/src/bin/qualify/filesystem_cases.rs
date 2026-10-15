use super::*;

pub(super) async fn filesystem_history(root: std::path::PathBuf, seed: u64) -> Result<(), String> {
    let fs = Box::pin(Fs::local(LocalOptions::new(&root)))
        .await
        .map_err(|error| error.to_string())?;
    let workspace = Box::pin(fs.create_workspace(
        "consumer-history",
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    ))
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
    .map_err(|error| error.to_string())?;
    let mut rng = seed | 1;
    let mut expected = std::collections::BTreeMap::<String, Bytes>::new();
    for step in 0..64_u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let path = format!("/history-{}.txt", rng % 8);
        if step % 5 == 0 && expected.remove(&path).is_some() {
            Box::pin(workspace.remove(&path))
                .await
                .map_err(|error| error.to_string())?;
            if Box::pin(async {
                workspace
                    .read(
                        &path,
                        64,
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            })
            .await
            .is_ok()
            {
                return Err(format!("step {step}: removed path remains readable"));
            }
        } else {
            let value = Bytes::from(format!("{step}:{rng}"));
            Box::pin(workspace.write(&path, value.clone()))
                .await
                .map_err(|error| error.to_string())?;
            expected.insert(path.clone(), value.clone());
            if Box::pin(async {
                workspace
                    .read(
                        &path,
                        64,
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            })
            .await
            .map_err(|error| error.to_string())?
            .as_ref()
                != value.as_ref()
            {
                return Err(format!("step {step}: written value differs from model"));
            }
        }
    }
    Box::pin(workspace.write("/unicode-雪.txt", Bytes::from_static(b"utf8 path")))
        .await
        .map_err(|error| error.to_string())?;
    expected.insert("/unicode-雪.txt".into(), Bytes::from_static(b"utf8 path"));
    drop(workspace);
    drop(fs);
    verify_reopened_history(&root, expected).await
}

pub(super) async fn verify_reopened_history(
    root: &Path,
    expected: std::collections::BTreeMap<String, Bytes>,
) -> Result<(), String> {
    let reopened = Box::pin(Fs::local(LocalOptions::new(root)))
        .await
        .map_err(|error| error.to_string())?;
    let workspace = Box::pin(reopened.open_workspace(
        "consumer-history",
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    ))
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
    .map_err(|error| error.to_string())?;
    for (path, value) in expected {
        let observed = Box::pin(async {
            workspace
                .read(
                    &path,
                    64,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
        })
        .await
        .map_err(|error| error.to_string())?;
        if observed.as_ref() != value.as_ref() {
            return Err(format!("reopened path {path} differs from model"));
        }
    }
    Ok(())
}

pub(super) async fn filesystem_demand(root: std::path::PathBuf) -> Result<(), String> {
    std::fs::create_dir_all(root.join("unrelated")).map_err(|error| error.to_string())?;
    for index in 0..512 {
        std::fs::write(
            root.join("unrelated").join(format!("{index:04}")),
            b"ignored",
        )
        .map_err(|error| error.to_string())?;
    }
    std::fs::write(root.join("wanted"), b"0123456789").map_err(|error| error.to_string())?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(&root).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
    .map_err(|error| error.to_string())?;
    let wanted = PortablePath::parse("/wanted", VolumeLimits::default())
        .map_err(|error| error.to_string())?;
    let wanted = acyclic_fs::kernel::NamespacePath::from_portable(&wanted, VolumeLimits::default())
        .map_err(|error| error.to_string())?;
    let cancellation = CancellationToken::new();
    let provider = &source;
    let looked_up = provider
        .lookup(
            provider.reference(),
            &wanted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?;
    if looked_up.work.source_path_components != 1
        || looked_up.work.source_entries_visited != 0
        || looked_up.work.source_bytes_read != 0
    {
        return Err("exact lookup traversed unrelated source content".into());
    }
    let version = looked_up.value.ok_or("wanted path is absent")?.version;
    let range = provider
        .read_range(
            provider.reference(),
            &wanted,
            version,
            3,
            4,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?;
    if range.value.bytes.as_ref() != b"3456"
        || range.work.source_entries_visited != 0
        || range.work.source_bytes_read != 4
    {
        return Err("range read was incorrect or traversed unrelated source content".into());
    }
    Ok(())
}

pub(super) fn sqlite_wal_child(path: &Path) -> Result<(), String> {
    use rusqlite::Connection;

    let writer = Connection::open(path).map_err(|error| error.to_string())?;
    writer
        .busy_timeout(Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    let mode: String = writer
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if mode != "wal" {
        return Err(format!("SQLite declined WAL mode: {mode}"));
    }
    writer
        .execute_batch(
            "PRAGMA synchronous=FULL; CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL); \
             BEGIN IMMEDIATE; INSERT INTO entries VALUES (1, 'first'); COMMIT;",
        )
        .map_err(|error| error.to_string())?;
    let reader = Connection::open(path).map_err(|error| error.to_string())?;
    reader
        .execute_batch("BEGIN")
        .map_err(|error| error.to_string())?;
    let read_count = |connection: &Connection| -> Result<i64, String> {
        connection
            .query_row("SELECT count(*) FROM entries", [], |row| row.get(0))
            .map_err(|error| error.to_string())
    };
    if read_count(&reader)? != 1 {
        return Err("SQLite reader missed the first commit".into());
    }
    writer
        .execute("INSERT INTO entries VALUES (2, 'second')", [])
        .map_err(|error| error.to_string())?;
    if read_count(&reader)? != 1 {
        return Err("SQLite WAL reader lost snapshot isolation".into());
    }
    reader
        .execute_batch("COMMIT")
        .map_err(|error| error.to_string())?;
    if read_count(&reader)? != 2 {
        return Err("SQLite reader missed the second commit".into());
    }
    let mut wal_name = path.as_os_str().to_os_string();
    wal_name.push("-wal");
    if std::fs::metadata(std::path::PathBuf::from(wal_name))
        .map_err(|error| error.to_string())?
        .len()
        == 0
    {
        return Err("SQLite WAL contains no durable records".into());
    }
    // Exit without SQLite destructors: the mount must publish and recover a
    // committed WAL without a clean-close checkpoint.
    std::process::exit(0)
}

pub(super) fn sqlite_verify_child(path: &Path) -> Result<(), String> {
    let connection = rusqlite::Connection::open(path).map_err(|error| error.to_string())?;
    let count: i64 = connection
        .query_row("SELECT count(*) FROM entries", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if count != 2 || integrity != "ok" {
        return Err(format!(
            "SQLite remount differs from committed state: rows={count}, integrity={integrity}"
        ));
    }
    let mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if mode != "wal" {
        return Err(format!("SQLite remount lost WAL mode: {mode}"));
    }
    let (busy, _, _): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| error.to_string())?;
    if busy != 0 {
        return Err("SQLite recovery checkpoint remained busy".into());
    }
    Ok(())
}

pub(super) async fn run_sqlite_child(mode: &str, path: &Path) -> Result<(), String> {
    let mut command =
        std::process::Command::new(std::env::current_exe().map_err(|error| error.to_string())?);
    command.arg(mode).arg(path);
    let output = acyclic_native_runtime::run_blocking_io(
        move || {
            acyclic_native_runtime::process_output(
                &mut command,
                std::time::Duration::from_secs(120),
                8 * 1024 * 1024,
            )
        },
        None,
    )
    .map_err(|error| error.to_string())?
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "SQLite child {mode} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

pub(super) async fn filesystem_sqlite_wal(root: std::path::PathBuf) -> Result<(), String> {
    let store = root.join("store");
    let destination = root.join("mount");
    std::fs::create_dir_all(&destination).map_err(|error| error.to_string())?;
    let fs = Box::pin(Fs::local(LocalOptions::new(&store)))
        .await
        .map_err(|error| error.to_string())?;
    let workspace = Box::pin(fs.create_workspace(
        "sqlite-wal",
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    ))
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
    .map_err(|error| error.to_string())?;
    let mount = workspace
        .mount(&destination, MountOptions::read_write())
        .await
        .map_err(|error| error.to_string())?;
    let child = run_sqlite_child("--sqlite-wal-child", &destination.join("wal.db")).await;
    let detached = mount.unmount().await.map_err(|error| error.to_string());
    child?;
    detached?;
    let body = Box::pin(async {
        workspace
            .read(
                "/wal.db",
                1024 * 1024,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    })
    .await
    .map_err(|error| error.to_string())?;
    if body.is_empty() {
        return Err("mounted SQLite database was not published".into());
    }
    let wal = Box::pin(async {
        workspace
            .read(
                "/wal.db-wal",
                1024 * 1024,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    })
    .await
    .map_err(|error| error.to_string())?;
    if wal.is_empty() {
        return Err("committed SQLite WAL was not published".into());
    }
    let remount_path = root.join("remount");
    std::fs::create_dir_all(&remount_path).map_err(|error| error.to_string())?;
    let remount = workspace
        .mount(&remount_path, MountOptions::read_write())
        .await
        .map_err(|error| error.to_string())?;
    let child = run_sqlite_child("--sqlite-verify-child", &remount_path.join("wal.db")).await;
    let detached = remount.unmount().await.map_err(|error| error.to_string());
    child?;
    detached?;
    Ok(())
}
