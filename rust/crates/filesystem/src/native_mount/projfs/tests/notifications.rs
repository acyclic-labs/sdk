use super::*;
#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn created_and_renamed_files_stay_observed_after_capture()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let external = |command: &str| -> Result<(), Box<dyn std::error::Error>> {
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new("cmd.exe")
                .args(["/D", "/C", command])
                .current_dir(&destination),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(output.status.success(), "{command} failed: {output:?}");
        Ok(())
    };
    let content = |name: &str| -> Result<Vec<u8>, MountSourceError> {
        let path = windows_path(name);
        let length = source.lookup(&path)?.ok_or(MountSourceError::NotFound)?;
        let length = u32::try_from(length.node.logical_bytes)
            .map_err(|_| MountSourceError::Invalid("test file too large".into()))?;
        Ok(source.read_range(&path, 0, length)?.to_vec())
    };

    external("echo first> created.txt")?;
    session.flush_callbacks(&work)?;
    assert_eq!(content("created.txt")?, b"first\r\n");
    external("echo second> created.txt")?;
    session.flush_callbacks(&work)?;
    assert_eq!(content("created.txt")?, b"second\r\n");
    external("ren created.txt renamed.txt")?;
    session.flush_callbacks(&work)?;
    external("echo third> renamed.txt")?;
    session.flush_callbacks(&work)?;
    assert_eq!(content("renamed.txt")?, b"third\r\n");
    external("mklink /H linked.txt renamed.txt > nul && del renamed.txt")?;
    session.flush_callbacks(&work)?;
    assert!(source.lookup(&windows_path("created.txt"))?.is_none());
    assert!(source.lookup(&windows_path("renamed.txt"))?.is_none());
    assert_eq!(content("linked.txt")?, b"third\r\n");
    external("del linked.txt")?;
    session.flush_callbacks(&work)?;
    assert!(source.lookup(&windows_path("linked.txt"))?.is_none());
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn native_watcher_observes_both_projected_rename_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    use crate::watch::{NativeWatch, NativeWatchOptions, WatchBatch, WatchChange};
    use std::time::{Duration, Instant};

    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "watch-projfs", &work).await?;
    workspace
        .write("/original.txt", Bytes::from_static(b"original"))
        .await?;

    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    let mount = workspace
        .mount(&destination, manual_mount_options())
        .await?;
    let original = destination.join("original.txt");
    assert_eq!(std::fs::read(&original)?, b"original");
    let mut watcher = NativeWatch::open(
        &destination,
        NativeWatchOptions::new(crate::model::VolumeLimits::default()),
    )?;
    watcher.begin_rescan()?;
    assert!(matches!(
        watcher.finish_rescan()?,
        WatchBatch::Changes { .. }
    ));

    let external = acyclic_native_runtime::process_output(
        std::process::Command::new("cmd.exe")
            .arg("/D")
            .arg("/C")
            .arg("ren original.txt renamed.txt && echo replacement> original.txt")
            .current_dir(&destination),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(
        external.status.success(),
        "external projected rename failed: {}",
        String::from_utf8_lossy(&external.stderr)
    );
    let limits = crate::model::VolumeLimits::default();
    let expected_path = |path| -> Result<crate::kernel::NamespacePath, Box<dyn std::error::Error>> {
        let portable = crate::path::PortablePath::parse(path, limits)?;
        crate::kernel::NamespacePath::from_portable_in_profile(
            &portable,
            crate::model::FilesystemProfile::Windows,
            limits,
        )
        .map_err(Into::into)
    };
    let original_path = expected_path("/original.txt")?;
    let renamed_path = expected_path("/renamed.txt")?;
    let mut seen = std::collections::BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match watcher
            .poll(128, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value
        {
            WatchBatch::Changes { changes, .. } => {
                for change in changes {
                    match change {
                        WatchChange::Created(path)
                        | WatchChange::Modified(path)
                        | WatchChange::Arrived(path)
                        | WatchChange::MetadataChanged(path)
                        | WatchChange::Removed(path) => {
                            seen.insert(path);
                        }
                        WatchChange::Renamed { from, to } => {
                            seen.insert(from);
                            seen.insert(to);
                        }
                    }
                }
            }
            WatchBatch::RescanRequired { reason, .. } => {
                return Err(format!("watcher invalidated: {reason}").into());
            }
        }
        if seen.contains(&original_path) && seen.contains(&renamed_path) {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!("watcher missed a projected rename path: {seen:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    mount.unmount().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn nested_new_directory_hardlink_and_rename_publish() -> Result<(), Box<dyn std::error::Error>>
{
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "nested-projfs", &work).await?;
    let path = root.path().join("mount");
    std::fs::create_dir(&path)?;
    let mount = workspace.mount(&path, manual_mount_options()).await?;
    let nested = path.join("target").join("debug").join("deps");
    std::fs::create_dir_all(&nested)?;
    let source = nested.join("unit.o");
    std::fs::write(&source, b"object")?;
    std::fs::hard_link(&source, nested.join("linked.o"))?;
    let still_open = nested.join("open.o");
    let mut handle = std::fs::File::create(&still_open)?;
    std::io::Write::write_all(&mut handle, b"open object")?;
    std::fs::hard_link(&still_open, nested.join("linked-open.o"))?;
    drop(handle);
    std::fs::rename(&nested, path.join("renamed-deps"))?;
    mount.sync().await?;
    std::fs::remove_dir_all(path.join("renamed-deps"))?;
    mount.sync().await?;
    mount.unmount().await?;
    Ok(())
}

include!("enumeration.rs");
