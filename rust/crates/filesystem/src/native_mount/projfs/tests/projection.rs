use super::*;
use crate::kernel::KernelAccounting;
#[test]
fn projfs_name_order_differs_from_sdk_cursor_order() {
    let name = windows_name;
    assert_eq!(
        super::compare_projfs_name(&name("Alpha"), &name("alpha")),
        Some(0)
    );
    assert!(name("Alpha") < name("alpha"));
    assert!(super::compare_projfs_name(&name("before"), &name("later")) < Some(0));
    assert!(super::compare_projfs_name(&name("later"), &name("before")) > Some(0));
    let mut projected = ["éclair", "Zebra", "entry000", "Alpha"];
    projected.sort_unstable_by(|left, right| {
        super::compare_projfs_name(&name(left), &name(right))
            .expect("valid Windows name")
            .cmp(&0)
    });
    assert_eq!(projected, ["Alpha", "entry000", "Zebra", "éclair"]);
    assert_ne!(
        ["Alpha", "Zebra", "entry000", "éclair"],
        projected,
        "SDK cursor order differs from the ProjFS merge order"
    );
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn listing_follows_source_and_mount_changes() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    source.create_file(&windows_path("a.txt"), FileMetadata::default(), None, &work)?;
    let (_root, destination, mut session) = mount_source(&source)?;
    assert_eq!(sorted_names(&destination)?, ["a.txt"]);
    assert_eq!(sorted_names(&destination)?, ["a.txt"]);
    source.create_file(&windows_path("b.txt"), FileMetadata::default(), None, &work)?;
    session.revalidate(&work)?;
    assert_eq!(sorted_names(&destination)?, ["a.txt", "b.txt"]);

    let external = acyclic_native_runtime::process_output(
        std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "echo c> c.txt && del b.txt"])
            .current_dir(&destination),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(
        external.status.success(),
        "external edit failed: {external:?}"
    );
    assert_eq!(sorted_names(&destination)?, ["a.txt", "c.txt"]);
    session.flush_callbacks(&work)?;
    assert_eq!(sorted_names(&destination)?, ["a.txt", "c.txt"]);
    assert!(source.lookup(&windows_path("b.txt"))?.is_none());
    assert!(source.lookup(&windows_path("c.txt"))?.is_some());
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_projected_link_follows_its_source_target() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let target = |name: &str| Bytes::from(windows_name(name));
    let link = windows_path("link");
    source.create_symbolic_link(
        &link,
        setup_chunk(&target("first.txt"), &work)?,
        FileMetadata::default(),
        &work,
    )?;
    let (_root, destination, mut session) = mount_source(&source)?;
    assert_eq!(
        std::fs::read_link(destination.join("link"))?,
        std::path::Path::new("first.txt")
    );
    source.remove(&link, None, &work)?;
    source.create_symbolic_link(
        &link,
        setup_chunk(&target("second.txt"), &work)?,
        FileMetadata::default(),
        &work,
    )?;
    session.revalidate(&work)?;
    assert_eq!(sorted_names(&destination)?, ["link"]);
    assert_eq!(
        std::fs::read_link(destination.join("link"))?,
        std::path::Path::new("second.txt")
    );
    source.remove(&link, None, &work)?;
    session.revalidate(&work)?;
    assert!(sorted_names(&destination)?.is_empty());
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_projected_directory_link_leads_into_its_directory()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    use std::os::windows::fs::FileTypeExt as _;

    let source = windows_checkout_source().await?;
    let folder = windows_path("folder");
    source.create_directory(&folder, FileMetadata::default(), &work)?;
    let inner = folder.child(windows_name("inner.txt"));
    source.create_file(&inner, FileMetadata::default(), None, &work)?;
    source.write_range(&inner, 0, Bytes::from_static(b"inner"))?;
    source.create_symbolic_link(
        &windows_path("folder-link"),
        setup_chunk(&Bytes::from(windows_name("folder")), &work)?,
        FileMetadata::default(),
        &work,
    )?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let link = destination.join("folder-link");
    assert!(
        std::fs::symlink_metadata(&link)?
            .file_type()
            .is_symlink_dir()
    );
    assert_eq!(std::fs::read(link.join("inner.txt"))?, b"inner");
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_held_link_is_removed_or_replaced_after_close() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
    };
    for replacement in [false, true] {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        let name = windows_path("name");
        source.create_symbolic_link(
            &name,
            setup_chunk(&Bytes::from(windows_name("target.txt")), &work)?,
            FileMetadata::default(),
            &work,
        )?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("name");
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0)
            .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
            .open(&projected)?;
        source.remove(&name, None, &work)?;
        if replacement {
            source.create_directory(&name, FileMetadata::default(), &work)?;
            let inner = name.child(windows_name("inner.txt"));
            source.create_file(&inner, FileMetadata::default(), None, &work)?;
            source.write_range(&inner, 0, Bytes::from_static(b"inner"))?;
        }
        let closer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            drop(held);
        });
        session.revalidate(&work)?;
        closer.join().map_err(|_| "closer panicked")?;
        if replacement {
            assert!(std::fs::symlink_metadata(&projected)?.is_dir());
            assert_eq!(std::fs::read(projected.join("inner.txt"))?, b"inner");
        } else {
            assert!(sorted_names(&destination)?.is_empty());
        }
        session.stop()?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn paths_the_source_gains_appear_once_revalidated() -> Result<(), Box<dyn std::error::Error>>
{
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let absent = |name: &str| {
        std::fs::metadata(destination.join(name))
            .err()
            .map(|error| error.kind())
    };

    assert_eq!(absent("listed.txt"), Some(std::io::ErrorKind::NotFound));
    source.create_file(
        &windows_path("listed.txt"),
        FileMetadata::default(),
        None,
        &work,
    )?;
    source.write_range(
        &windows_path("listed.txt"),
        0,
        Bytes::from_static(b"listed"),
    )?;
    session.revalidate(&work)?;
    assert_eq!(sorted_names(&destination)?, ["listed.txt"]);
    assert_eq!(std::fs::read(destination.join("listed.txt"))?, b"listed");

    assert_eq!(
        absent("invalidated.txt"),
        Some(std::io::ErrorKind::NotFound)
    );
    source.create_file(
        &windows_path("invalidated.txt"),
        FileMetadata::default(),
        None,
        &work,
    )?;
    session.invalidate(b"/invalidated.txt")?;
    session.revalidate(&work)?;
    assert_eq!(
        std::fs::metadata(destination.join("invalidated.txt"))?.len(),
        0
    );
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn advanced_view_reveals_a_path_absent_before() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
    let workspace = engine
        .create_workspace("advance-projfs", WorkBudget::UNBOUNDED, work.cancellation())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let path = root.path().join("mount");
    std::fs::create_dir(&path)?;
    let mount = workspace
        .mount(
            &path,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?;
    let added = path.join("added.txt");
    assert_eq!(
        std::fs::metadata(&added).err().map(|error| error.kind()),
        Some(std::io::ErrorKind::NotFound)
    );
    workspace
        .write("/added.txt", Bytes::from_static(b"added"))
        .await?;
    mount.advance_to_head().await?;
    assert_eq!(std::fs::read(&added)?, b"added");
    mount.unmount().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_file_holds_only_the_content_its_listing_named() -> Result<(), Box<dyn std::error::Error>>
{
    for listed_first in [false, true] {
        writes_only_promised_content(listed_first).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_lazy_workspace_lists_through_the_projection() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;

    let root = tempfile::tempdir()?;
    let source_root = root.path().join("source");
    std::fs::create_dir(&source_root)?;
    std::fs::write(source_root.join("a.txt"), b"a")?;
    std::fs::write(source_root.join("b.rs"), b"b")?;
    let demand = windows_demand_source(&source_root, work.cancellation()).await?;
    let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
    let (lazy, _) = crate::LazyWorkspace::attach(
        &engine,
        "listed-projfs",
        Arc::new(demand),
        crate::LocalCoreStateStore::open_owned(root.path().join("core"))?,
        VolumeConfig::native(Lifecycle::Ephemeral),
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = lazy
        .mount(
            &destination,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?;
    assert_eq!(sorted_names(&destination)?, ["a.txt", "b.rs"]);
    mount.unmount().await?;
    Ok(())
}

#[test]
fn source_failures_keep_distinct_statuses() {
    let statuses = [
        MountSourceError::NotFound,
        MountSourceError::AlreadyExists,
        MountSourceError::Invalid("".into()),
        MountSourceError::Unsupported("".into()),
        MountSourceError::Engine("".into()),
        MountSourceError::Stale,
    ]
    .iter()
    .map(source_hresult)
    .collect::<HashSet<_>>();
    assert_eq!(statuses.len(), 6);
    assert!(!statuses.contains(&super::HR_UNEXPECTED));
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn rustc_links_object_files_created_inside_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
    let workspace = engine
        .create_workspace("rustc-projfs", WorkBudget::UNBOUNDED, work.cancellation())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace
        .write(
            "/acyclic-workflow.rs",
            Bytes::from_static(b"fn main() { println!(\"ok\"); }\n"),
        )
        .await?;
    let path = root.path().join("mount");
    std::fs::create_dir(&path)?;
    let mount = workspace
        .mount(
            &path,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?;
    let compiler_path = path.clone();
    let output = tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::process_output(
            std::process::Command::new("rustc")
                .current_dir(compiler_path)
                .args([
                    "--edition",
                    "2021",
                    "acyclic-workflow.rs",
                    "-o",
                    "acyclic-workflow-bin.exe",
                ]),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )
    })
    .await??;
    assert!(
        output.status.success(),
        "rustc failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    mount.sync().await?;
    let executable = path.join("acyclic-workflow-bin.exe");
    let execution = tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::process_output(
            &mut std::process::Command::new(executable),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )
    })
    .await??;
    assert!(execution.status.success());
    assert_eq!(execution.stdout, b"ok\n");
    mount.unmount().await?;
    Ok(())
}

#[test]
fn newer_capture_is_not_cleared_by_an_older_flush() {
    let executor = CallbackGate::start().expect("callback gate");
    let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
    let path = MountPath::root().child(b"changed".to_vec());
    failure
        .lock()
        .expect("failure state")
        .queue_capture(path.clone(), "first write".to_owned());
    let during_capture = Arc::clone(&failure);
    let error = flush_callback_gate(&executor, Arc::clone(&failure), move |paths| {
        during_capture
            .lock()
            .expect("failure state")
            .queue_capture(paths[0].0.clone(), "second write".to_owned());
        Ok(())
    })
    .expect_err("the newer write remains pending");
    assert!(error.to_string().contains("second write"));
    flush_callback_gate(&executor, Arc::clone(&failure), |_| Ok(()))
        .expect("a later flush captures the newer write");
}

#[test]
fn closing_callback_admission_preserves_an_already_admitted_operation() {
    let gate = CallbackGate::start().expect("callback gate");
    let admitted = gate.enter().expect("open callback gate");
    gate.close_admission();
    assert!(gate.enter().is_none());
    assert_eq!(
        gate.call_observed(0, || 7, |result| assert_eq!(*result, 7)),
        7
    );
    drop(admitted);
    gate.drain();
}
