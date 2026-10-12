use super::*;
use crate::kernel::KernelAccounting;
#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn stopped_projection_cannot_publish_a_late_open_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    use std::io::{Seek as _, Write as _};
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    let source = windows_checkout_source().await?;
    let seed = windows_path("seed.txt");
    source.create_file(&seed, FileMetadata::default(), None, &work)?;
    source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let projected = destination.join("seed.txt");
    assert_eq!(std::fs::read(&projected)?, b"seed");

    let mut late = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0)
        .open(&projected)?;
    session
        .stop()
        .expect_err("an open non-delete-sharing handle must fence cleanup");

    late.rewind()?;
    late.write_all(b"late")?;
    late.flush()?;
    drop(late);

    assert_eq!(source.read_range(&seed, 0, 4)?.as_ref(), b"seed"[..]);
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn write_through_a_handle_opened_before_a_rename_lands_at_the_new_name()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let seed = windows_path("seed.txt");
    source.create_file(&seed, FileMetadata::default(), None, &work)?;
    source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let script = "$seed = Join-Path $env:ACYCLIC_FS_TEST_ROOT 'seed.txt';
        $moved = Join-Path $env:ACYCLIC_FS_TEST_ROOT 'moved.txt';
        $h = [IO.File]::Open($seed, 'Open', 'ReadWrite', 'ReadWrite,Delete');
        [IO.File]::Move($seed, $moved);
        $h.Seek(0, 'End') | Out-Null; $h.WriteByte(33); $h.Close()";
    let output = acyclic_native_runtime::process_output(
        std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .env("ACYCLIC_FS_TEST_ROOT", &destination),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(output.status.success(), "external edit failed: {output:?}");
    session.flush_callbacks(&work)?;
    assert!(source.lookup(&seed)?.is_none());
    let moved = windows_path("moved.txt");
    assert_eq!(source.read_range(&moved, 0, 5)?.as_ref(), b"seed!");
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn an_opened_file_follows_its_source() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let tracked = windows_path("tracked.txt");
    source.create_file(&tracked, FileMetadata::default(), None, &work)?;
    source.write_range(&tracked, 0, Bytes::from_static(b"before"))?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let projected = destination.join("tracked.txt");
    succeeded(powershell(&format!(
        "[IO.File]::Open('{}', 'Open', 'Read', 'ReadWrite').Close()",
        projected.display()
    ))?)?;
    assert_eq!(super::reparse_tag(&projected)?.0, None, "a placeholder");
    session.flush_callbacks(&work)?;
    source.write_range(&tracked, 0, Bytes::from_static(b"after!"))?;
    session.revalidate(&work)?;
    assert_eq!(std::fs::read(&projected)?, b"after!");
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn an_opened_file_the_user_wrote_stays() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let kept = windows_path("kept.txt");
    source.create_file(&kept, FileMetadata::default(), None, &work)?;
    source.write_range(&kept, 0, Bytes::from_static(b"source"))?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let projected = destination.join("kept.txt");
    succeeded(powershell(&format!(
        "[IO.File]::AppendAllText('{}', ' and user')",
        projected.display()
    ))?)?;
    session.flush_callbacks(&work)?;
    session.revalidate(&work)?;
    assert_eq!(std::fs::read(&projected)?, b"source and user");
    assert_eq!(
        source.read_range(&kept, 0, 15)?.as_ref(),
        b"source and user"
    );
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_link_the_user_replaced_stays() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let link = windows_path("link");
    source.create_symbolic_link(
        &link,
        setup_chunk(&Bytes::from(windows_name("target.txt")), &work)?,
        FileMetadata::default(),
        &work,
    )?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let projected = destination.join("link");
    assert_eq!(
        std::fs::read_link(&projected)?,
        std::path::Path::new("target.txt")
    );
    std::fs::remove_file(&projected)?;
    std::fs::create_dir(&projected)?;
    source.remove(&link, None, &work)?;
    session.revalidate(&work)?;
    assert!(std::fs::symlink_metadata(&projected)?.is_dir());
    session.stop()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_held_placeholder_is_replaced_before_revalidation_returns()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;

    let root = tempfile::tempdir()?;
    let source_root = root.path().join("source");
    std::fs::create_dir(&source_root)?;
    std::fs::write(source_root.join("held.txt"), b"old")?;
    let demand = windows_demand_source(&source_root, work.cancellation()).await?;
    let (lazy, _) = crate::LazyWorkspace::attach(
        &Fs::memory(),
        "held-placeholder",
        Arc::new(demand),
        crate::MemoryLazyWorkspaceStore::default(),
        VolumeConfig::native(Lifecycle::Ephemeral),
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = Arc::new(lazy.mount(&destination, manual_mount_options()).await?);
    let held_path = destination.join("held.txt");
    assert_eq!(std::fs::metadata(&held_path)?.len(), 3);
    let held = std::fs::File::open(&held_path)?;
    std::fs::write(source_root.join("held.txt"), b"much newer")?;
    let revalidating = {
        let mount = Arc::clone(&mount);
        std::thread::spawn(move || mount.revalidate().map_err(|error| error.to_string()))
    };
    std::thread::sleep(std::time::Duration::from_millis(300));
    if !revalidating.is_finished() {
        drop(held);
    }
    revalidating
        .join()
        .map_err(|_| "revalidation panicked")?
        .map_err(|error| error.to_string())?;
    assert_eq!(std::fs::metadata(&held_path)?.len(), 10);
    assert_eq!(std::fs::read(&held_path)?, b"much newer");
    mount.unmount().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_read_only_placeholder_is_replaced_when_its_source_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;

    let root = tempfile::tempdir()?;
    let source_root = root.path().join("source");
    std::fs::create_dir(&source_root)?;
    let source_file = source_root.join("locked.txt");
    let set_read_only = |read_only: bool| -> std::io::Result<()> {
        let mut permissions = std::fs::metadata(&source_file)?.permissions();
        permissions.set_readonly(read_only);
        std::fs::set_permissions(&source_file, permissions)
    };
    std::fs::write(&source_file, b"old")?;
    set_read_only(true)?;
    let demand = windows_demand_source(&source_root, work.cancellation()).await?;
    let (lazy, _) = crate::LazyWorkspace::attach(
        &Fs::memory(),
        "read-only-placeholder",
        Arc::new(demand),
        crate::MemoryLazyWorkspaceStore::default(),
        VolumeConfig::native(Lifecycle::Ephemeral),
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = lazy.mount(&destination, manual_mount_options()).await?;
    let projected = destination.join("locked.txt");
    assert_eq!(std::fs::metadata(&projected)?.len(), 3);
    assert!(std::fs::metadata(&projected)?.permissions().readonly());

    set_read_only(false)?;
    std::fs::write(&source_file, b"much newer")?;
    set_read_only(true)?;
    mount.revalidate()?;
    assert_eq!(std::fs::metadata(&projected)?.len(), 10);
    assert_eq!(std::fs::read(&projected)?, b"much newer");
    set_read_only(false)?;
    mount.unmount().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn projected_directories_rename_and_what_moved_with_them_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;

    let root = tempfile::tempdir()?;
    let source_root = root.path().join("source");
    std::fs::create_dir_all(source_root.join("pkg").join("sub"))?;
    std::fs::write(source_root.join("pkg").join("shared.rs"), b"shared")?;
    std::fs::write(source_root.join("pkg").join("sub").join("deep.rs"), b"deep")?;
    let demand = windows_demand_source(&source_root, work.cancellation()).await?;
    let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
    let (lazy, _) = crate::LazyWorkspace::attach(
        &engine,
        "renamed-projfs",
        Arc::new(demand),
        crate::LocalCoreStateStore::open_owned(root.path().join("core"))?,
        VolumeConfig::native(Lifecycle::Ephemeral),
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = lazy.mount(&destination, manual_mount_options()).await?;
    assert_eq!(
        sorted_names(&destination.join("pkg"))?,
        ["shared.rs", "sub"]
    );
    let moved = acyclic_native_runtime::process_output(
        std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "move pkg lib"])
            .current_dir(&destination),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(moved.status.success(), "move failed: {moved:?}");
    assert_eq!(sorted_names(&destination)?, ["lib"]);
    assert_eq!(
        std::fs::read(&destination.join("lib").join("shared.rs"))?,
        b"shared"
    );
    assert_eq!(
        std::fs::read(&destination.join("lib").join("sub").join("deep.rs"))?,
        b"deep"
    );
    mount.sync().await?;
    assert!(
        lazy.lookup(
            "/lib/sub/deep.rs",
            &mut KernelAccounting::native(&work, work.cancellation())
        )
        .await
        .is_ok()
    );
    assert!(
        lazy.lookup(
            "/pkg",
            &mut KernelAccounting::native(&work, work.cancellation())
        )
        .await
        .is_err()
    );
    mount.unmount().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_file_renamed_before_it_is_read_stays_as_renamed()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;

    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "rename-unread-projfs", &work).await?;
    workspace
        .write("/before.txt", Bytes::from_static(b"unread"))
        .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = workspace
        .mount(&destination, manual_mount_options())
        .await?;
    let external = acyclic_native_runtime::process_output(
        std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "ren before.txt after.txt"])
            .current_dir(&destination),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(external.status.success(), "rename failed: {external:?}");
    mount.sync().await?;
    let renamed = destination.join("after.txt");
    assert_eq!(std::fs::read(&renamed)?, b"unread");
    mount.unmount().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn projected_file_rename_and_replacement_survive_remount()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "replace-projfs", &work).await?;
    workspace
        .write("/original.txt", Bytes::from_static(b"original"))
        .await?;

    let first = root.path().join("first-mount");
    std::fs::create_dir(&first)?;
    let mount = workspace.mount(&first, manual_mount_options()).await?;
    let original = first.join("original.txt");
    for _ in 0..3 {
        assert_eq!(std::fs::read(&original)?, b"original");
    }
    let external = acyclic_native_runtime::process_output(
        std::process::Command::new("cmd.exe")
            .args([
                "/D",
                "/C",
                "ren original.txt renamed.txt && echo replacement> original.txt",
            ])
            .current_dir(&first),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(
        external.status.success(),
        "external edit failed: {external:?}"
    );
    mount.sync().await?;
    mount.unmount().await?;

    let second = root.path().join("second-mount");
    std::fs::create_dir(&second)?;
    let remount = workspace.mount(&second, manual_mount_options()).await?;
    assert_eq!(
        std::fs::read(second.join("renamed.txt")).expect("renamed path after remount"),
        b"original"
    );
    assert_eq!(
        std::fs::read(second.join("original.txt")).expect("replacement after remount"),
        b"replacement\r\n"
    );
    remount.unmount().await?;
    Ok(())
}

#[test]
fn rename_matches_windows_case_spelling() {
    let source = windows_path("Old");
    let callback_source = windows_path("old");
    let destination = windows_path("New");
    let mut failures = PostOperationFailures::default();
    failures.queue_capture(source.clone(), "capture failed".to_owned());
    failures.rename_pending_captures(&callback_source, &destination);
    assert!(!failures.pending_captures.contains_key(&source));
    assert!(failures.pending_captures.contains_key(&destination));
}
