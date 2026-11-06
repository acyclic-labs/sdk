use super::MountFilesystem;
use super::{
    Arc, Bytes, CallbackGate, FILE_ATTRIBUTE_NORMAL, FileMetadata, Fs, HSTRING, HashSet, Lifecycle,
    LocalOptions, MaterializeQueue, MetadataField, MountPath, MountSourceError, Mutex,
    PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, PostOperationFailures, Projection, VolumeConfig,
    callback_runtime, decode_utf16_name, flush_callback_gate, local_workspace, lock_recover,
    manual_mount_options, mount_source, owned_component, projected_entry, setup_chunk, setup_work,
    sorted_names, source_hresult, windows_checkout_source, windows_demand_source, windows_name,
    windows_path, writes_only_promised_content,
};

#[test]
fn materialization_queue_retains_original_errors_and_retires_panicking_items() {
    for terminal in 0..5 {
        let queue = MaterializeQueue::new(vec![MountPath::root()]);
        let mut completed = 0;
        let returned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.run(|path| {
                if path.components().is_empty() {
                    return Ok(vec![windows_path("child"), windows_path("sibling")]);
                }
                completed += 1;
                match terminal {
                    0 => Ok(Vec::new()),
                    1 => Err("original source refusal".to_owned()),
                    2 => panic!("original source panic"),
                    3 => std::panic::panic_any("owned source panic".to_owned()),
                    _ => std::panic::panic_any(17_u8),
                }
            });
        }));
        assert!(returned.is_ok(), "an item cannot abandon its queue custody");
        assert_eq!(lock_recover(&queue.state).writing, 0);
        assert!(
            queue.next().is_none(),
            "a retired queue cannot strand a reader"
        );
        let result = queue.finish();
        if terminal == 0 {
            assert_eq!(completed, 2);
            assert_eq!(result, Ok(()));
        } else {
            assert_eq!(completed, 1, "a failed source cannot dispatch another item");
            let cause = result.expect_err("original failure was lost");
            match terminal {
                1 => assert_eq!(cause, "original source refusal"),
                2 => assert!(cause.contains("original source panic")),
                3 => assert!(cause.contains("owned source panic")),
                _ => assert!(!cause.is_empty()),
            }
        }
    }
}

#[tokio::test]
async fn directory_link_projection_has_no_target_sized_conversion()
-> Result<(), Box<dyn std::error::Error>> {
    let mut costs = [[acyclic_test_support::AllocationInfo::default(); 2]; 2];
    let mut reads = costs;
    for (index, units) in [1, 1024].into_iter().enumerate() {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        let path = windows_path("link");
        let target = [b'x', 0].repeat(units);
        let lookup = source.create_symbolic_link(
            &path,
            setup_chunk(&target, &work)?,
            FileMetadata {
                windows_attributes: MetadataField::Value(FILE_ATTRIBUTE_NORMAL.0),
                ..FileMetadata::default()
            },
            &work,
        )?;
        let root = tempfile::tempdir()?;
        let runtime = callback_runtime(source, root.path(), &work)?;
        let projection = Projection {
            runtime: &runtime,
            work: &work,
            context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT::default(),
            starting: false,
        };
        let directory = MountPath::root();
        drop(runtime.source.read_link(&path, &work)?);
        for (control, restore_conversion) in [false, true].into_iter().enumerate() {
            let mut read = None;
            reads[index][control] = acyclic_test_support::measure(|| {
                read = Some(runtime.source.read_link(&path, &work));
            });
            let read = read.ok_or("missing source read")??;
            assert_eq!(read.bytes.as_ref(), target);
            drop(read);
            let component = owned_component(projection, &windows_name("link"))?;
            let mut result = Ok(None);
            costs[index][control] = acyclic_test_support::measure(|| {
                result = projected_entry(projection, &directory, component, lookup, None);
                if restore_conversion && let Ok(Some(entry)) = &result {
                    // Reintroduce the discarded conversion as the negative control.
                    let target = entry.symlink_target.as_ref().expect("symbolic link");
                    std::hint::black_box(HSTRING::from_wide(
                        &decode_utf16_name(&target.bytes).expect("aligned target"),
                    ));
                }
            });
            let entry = result?.ok_or("missing projected link")?;
            assert_eq!(
                entry.symlink_target.as_ref().unwrap().bytes.as_ref(),
                target
            );
            assert_eq!(entry.facts.lookup, lookup);
            assert!(!entry.directory_link);
        }
    }
    // Subtract the same source's required read storage before comparing conversion.
    let overhead_count = |index: usize| {
        i128::from(costs[index][0].count_total) - i128::from(reads[index][0].count_total)
    };
    let overhead_bytes = |index: usize, control: usize| {
        i128::from(costs[index][control].bytes_total)
            - i128::from(reads[index][control].bytes_total)
    };
    assert_eq!(overhead_count(0), overhead_count(1));
    assert_eq!(overhead_bytes(0, 0), overhead_bytes(1, 0));
    let conversion_bytes = |index| overhead_bytes(index, 1) - overhead_bytes(index, 0);
    assert!(conversion_bytes(0) > 0);
    assert!(conversion_bytes(1) > conversion_bytes(0));
    Ok(())
}
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
        acyclic_native_runtime::ProcessCommand::new("cmd.exe")
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
async fn sharing_denied_entries_are_retired_only_after_the_original_handle_closes()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
    };
    #[derive(Clone, Copy, Debug)]
    enum Replacement {
        Removed,
        File,
        Directory,
    }
    for link in [false, true] {
        for replacement in [
            Replacement::Removed,
            Replacement::File,
            Replacement::Directory,
        ] {
            let work = setup_work()?;
            let source = windows_checkout_source().await?;
            let name = windows_path("name");
            if link {
                source.create_symbolic_link(
                    &name,
                    setup_chunk(&Bytes::from(windows_name("target.txt")), &work)?,
                    FileMetadata::default(),
                    &work,
                )?;
            } else {
                source.create_file(&name, FileMetadata::default(), None, &work)?;
                source.write_range(&name, 0, Bytes::from_static(b"old"))?;
            }
            let (_root, destination, mut session) = mount_source(&source)?;
            let projected = destination.join("name");
            let held = std::fs::OpenOptions::new()
                .read(true)
                .share_mode(FILE_SHARE_READ.0)
                .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
                .open(&projected)
                .map_err(|error| {
                    format!("open held entry, link={link}, {replacement:?}: {error}")
                })?;
            let driver = session.driver.as_ref().ok_or("missing ProjFS driver")?;
            let runtime = Arc::clone(driver.runtime.as_ref().ok_or("missing ProjFS runtime")?);
            let context = driver.context.ok_or("missing ProjFS context")?;
            let tree = lock_recover(&runtime.projector.tree);
            source.remove(&name, None, &work)?;
            match replacement {
                Replacement::Removed => {}
                Replacement::File => {
                    source.create_file(&name, FileMetadata::default(), None, &work)?;
                    source.write_range(&name, 0, Bytes::from_static(b"newer"))?;
                }
                Replacement::Directory => {
                    source.create_directory(&name, FileMetadata::default(), &work)?;
                    let inner = name.child(windows_name("inner.txt"));
                    source.create_file(&inner, FileMetadata::default(), None, &work)?;
                    source.write_range(&inner, 0, Bytes::from_static(b"inner"))?;
                }
            }
            runtime.projector.materialize_locked(
                Projection {
                    runtime: &runtime,
                    work: &work,
                    context,
                    starting: false,
                },
                vec![MountPath::root()],
            )?;
            let target = {
                let state = lock_recover(&runtime.projector.state);
                assert!(
                    state.pending.contains_key(&name),
                    "the original handle must block retirement"
                );
                assert!(
                    state.processed < state.notified,
                    "the changed view must await its original worker"
                );
                state.notified
            };
            let projector = Arc::clone(&runtime.projector);
            let pending = name.clone();
            let closer = std::thread::spawn(move || {
                let mut state = lock_recover(&projector.state);
                while state.processed < target && state.failure.is_none() && !state.stopping {
                    state = projector
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                assert!(
                    state.failure.is_none() && !state.stopping,
                    "the original worker must complete"
                );
                assert!(state.processed >= target);
                assert!(
                    state.pending.contains_key(&pending),
                    "the worker must retain the held incarnation"
                );
                drop(state);
                drop(held);
            });
            drop(tree);
            session.revalidate(&work).map_err(|error| {
                format!("revalidate held entry, link={link}, {replacement:?}: {error}")
            })?;
            closer.join().map_err(|_| "closer panicked")?;
            match replacement {
                Replacement::Removed => assert!(sorted_names(&destination)?.is_empty()),
                Replacement::File => assert_eq!(std::fs::read(&projected)?, b"newer"),
                Replacement::Directory => {
                    assert!(std::fs::symlink_metadata(&projected)?.is_dir());
                    assert_eq!(std::fs::read(projected.join("inner.txt"))?, b"inner");
                }
            }
            session.stop()?;
        }
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
    let workspace = local_workspace(root.path(), "advance-projfs", &work).await?;
    let path = root.path().join("mount");
    std::fs::create_dir(&path)?;
    let mount = workspace
        .mount(&path, manual_mount_options(&workspace).await?)
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
        crate::VolumeInitialization {
            config: VolumeConfig::native(Lifecycle::Ephemeral),
            access_policy: None,
        },
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = lazy
        .mount(&destination, manual_mount_options(lazy.workspace()).await?)
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
        MountSourceError::NotDirectory,
        MountSourceError::IsDirectory,
        MountSourceError::DirectoryNotEmpty,
        MountSourceError::Invalid("".into()),
        MountSourceError::Unsupported("".into()),
        MountSourceError::Engine("".into()),
        MountSourceError::Stale,
        MountSourceError::Cancelled,
    ]
    .iter()
    .map(source_hresult)
    .collect::<HashSet<_>>();
    assert_eq!(statuses.len(), 10);
    assert!(!statuses.contains(&super::HR_UNEXPECTED));
    for (error, status) in [
        (MountSourceError::NotDirectory, super::HR_DIRECTORY),
        (MountSourceError::IsDirectory, super::HR_ACCESS_DENIED),
        (
            MountSourceError::DirectoryNotEmpty,
            super::HR_DIRECTORY_NOT_EMPTY,
        ),
    ] {
        assert_eq!(source_hresult(&error), status, "{error:?}");
    }
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn rustc_links_object_files_created_inside_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "rustc-projfs", &work).await?;
    workspace
        .write(
            "/acyclic-workflow.rs",
            Bytes::from_static(b"fn main() { println!(\"ok\"); }\n"),
        )
        .await?;
    let path = root.path().join("mount");
    std::fs::create_dir(&path)?;
    let mount = workspace
        .mount(&path, manual_mount_options(&workspace).await?)
        .await?;
    let compiler_path = path.clone();
    let output = tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::process_output(
            acyclic_native_runtime::ProcessCommand::new("rustc")
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
            &mut acyclic_native_runtime::ProcessCommand::new(executable),
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
    let error = flush_callback_gate(&executor, &failure, move |paths| {
        during_capture
            .lock()
            .expect("failure state")
            .queue_capture(paths[0].0.clone(), "second write".to_owned());
        Ok(())
    })
    .expect_err("the newer write remains pending");
    assert!(error.to_string().contains("second write"));
    flush_callback_gate(&executor, &failure, |_| Ok(()))
        .expect("a later flush captures the newer write");
}

#[test]
fn closing_callback_admission_preserves_an_already_admitted_operation() {
    let gate = CallbackGate::start().expect("callback gate");
    let admitted = gate.enter().expect("open callback gate");
    gate.close_admission();
    assert!(gate.enter().is_none());
    let mut operated = false;
    gate.call_observed(
        0,
        || {
            operated = true;
            Ok(())
        },
        |result| assert!(result.is_ok()),
    )
    .expect("the admitted callback completes");
    assert!(operated);
    drop(admitted);
    gate.drain();
}
