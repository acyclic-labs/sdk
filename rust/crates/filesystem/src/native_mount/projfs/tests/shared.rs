use super::*;

/// A shared instance whose start fails returns the failure: the failed
/// instance is dropped under the registry lock that serializes starts, so
/// its teardown must not take that lock; if it did this would deadlock,
/// which the bounded wait reports instead of hanging.
#[test]
fn a_failed_shared_instance_start_returns_its_error() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("not-a-directory");
    std::fs::write(&root, b"file")?;
    let (sent, received) = std::sync::mpsc::channel();
    let probe = root.clone();
    std::thread::spawn(move || {
        let _ = sent.send(super::super::shared::acquire_instance(&probe).map(drop));
    });
    let result = received
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| "a failed shared start deadlocked")?;
    assert!(result.is_err(), "a file cannot be a virtualization root");
    // The registry is free again: a second failed start also returns.
    assert!(super::super::shared::acquire_instance(&root).is_err());
    Ok(())
}

fn mount_shared(
    source: &Arc<MemorySource>,
) -> Result<(std::path::PathBuf, crate::NativeMountSession), Box<dyn std::error::Error>> {
    let issued = crate::native_mount::shared_view_destination(None)?;
    let destination = issued.path().to_path_buf();
    let session = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.clone(),
            writable: true,
            shared: true,
        },
        Arc::clone(source) as Arc<dyn MountFilesystem>,
    )?;
    Ok((destination, session))
}

/// Two views of one shared instance each serve their own source, capture
/// their own writes and refuse renames into each other; retiring one leaves
/// the other intact and no trace of itself, and retiring the last detaches
/// the instance's root.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn shared_views_serve_their_own_sources_and_retire_without_trace()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let first = windows_checkout_source_with_files(2).await?;
    let second = windows_checkout_source_with_files(3).await?;
    let (one, mut one_session) = mount_shared(&first)?;
    let (two, mut two_session) = mount_shared(&second)?;
    assert_eq!(one.parent(), two.parent(), "both views are of one instance");
    assert_eq!(sorted_names(&one)?, ["f0.txt", "f1.txt"]);
    assert_eq!(sorted_names(&two)?, ["f0.txt", "f1.txt", "f2.txt"]);
    assert_eq!(std::fs::read(one.join("f1.txt"))?.len(), 4096);
    // ProjFS notifies its provider of other processes' I/O only, so a
    // client process makes every change.
    let logs = tempfile::tempdir()?;
    let log = logs.path().join("client.log");
    let script = format!(
        r"$ErrorActionPreference = 'Stop'
        trap {{ ($_ | Out-String) | Set-Content -LiteralPath '{log}'; exit 1 }}
        Set-Content -LiteralPath '{one}\new.txt' -Value one
        Remove-Item -LiteralPath '{one}\f0.txt'
        try {{ Move-Item -LiteralPath '{two}\f2.txt' -Destination '{one}\moved.txt'; exit 3 }} catch {{ }}
        try {{ Move-Item -LiteralPath '{two}' -Destination '{one}\nested'; exit 4 }} catch {{ }}
        exit 0",
        one = one.display(),
        two = two.display(),
        log = log.display(),
    );
    succeeded(powershell(&script)?).map_err(|error| {
        format!(
            "a client change or a refused rename failed: {error}: {}",
            std::fs::read_to_string(&log).unwrap_or_default()
        )
    })?;
    one_session.flush_callbacks(&work)?;
    assert!(first.lookup(&windows_path("new.txt"))?.is_some());
    assert!(first.lookup(&windows_path("f0.txt"))?.is_none());
    assert!(second.lookup(&windows_path("new.txt"))?.is_none());
    assert!(second.lookup(&windows_path("f2.txt"))?.is_some());
    one_session.stop()?;
    assert_eq!(
        std::fs::symlink_metadata(&one)
            .map_err(|error| error.kind())
            .err(),
        Some(std::io::ErrorKind::NotFound),
        "a retired view leaves no entry"
    );
    assert_eq!(sorted_names(&two)?, ["f0.txt", "f1.txt", "f2.txt"]);
    let root = two.parent().ok_or("a view has a root")?.to_path_buf();
    two_session.stop()?;
    for view in [&one, &two] {
        assert_eq!(
            std::fs::symlink_metadata(view)
                .map_err(|error| error.kind())
                .err(),
            Some(std::io::ErrorKind::NotFound),
            "a stopped instance leaves no entry of a retired view"
        );
    }
    use std::os::windows::fs::MetadataExt as _;
    assert_eq!(
        std::fs::symlink_metadata(&root)?.file_attributes() & 0x400,
        0,
        "the last view's retirement detaches the root"
    );
    assert_eq!(std::fs::read_dir(&root)?.count(), 0);
    Ok(())
}

/// A shared view must be mounted at a fresh issued name: a forged name, or
/// one already listed, is refused before anything is created.
#[test]
fn a_shared_view_name_is_listed_once() -> Result<(), Box<dyn std::error::Error>> {
    let root = crate::shared_view_root()?;
    for forged in ["task", "v0000000000000000"] {
        assert!(matches!(
            super::super::shared::SharedProjFsView::create(&root.join(forged)),
            Err(NativeMountError::InvalidDestination)
        ));
        assert!(!root.join(forged).exists());
    }
    Ok(())
}

#[test]
fn dead_shared_root_cleanup_never_reopens_a_replaced_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = tempfile::tempdir()?;
    let path = fixture.path().join("admitted");
    let moved = fixture.path().join("held");
    std::fs::create_dir(&path)?;
    let admitted = crate::hold_root(&path)?;
    let name = std::path::Path::new("dead");
    let staging = super::super::staging_path(name);
    for leaf in [name, staging.as_path()] {
        std::fs::create_dir(path.join(leaf))?;
        std::fs::write(path.join(leaf).join("retained"), b"old owner")?;
    }
    std::fs::rename(&path, &moved)?;
    std::fs::create_dir(&path)?;
    for leaf in [name, staging.as_path()] {
        std::fs::create_dir(path.join(leaf))?;
        std::fs::write(path.join(leaf).join("retained"), b"replacement")?;
    }
    super::super::cleanup::detach_dead_shared_root(&admitted, name)?;
    for leaf in [name, staging.as_path()] {
        assert!(!moved.join(leaf).exists());
        assert_eq!(
            std::fs::read(path.join(leaf).join("retained"))?,
            b"replacement"
        );
    }
    // The previous path-based cleanup follows the replacement admission.
    // Its deletion is observable under exactly the same directory rename.
    std::fs::remove_dir_all(path.join(name))?;
    assert!(!path.join(name).exists());
    Ok(())
}
