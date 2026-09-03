use super::*;

#[test]
fn native_root_identity_encoding_is_exact_and_round_trips() {
    let identity = NativeRootIdentity {
        device: 0x0102_0304_0506_0708,
        object: 0x1112_1314_1516_1718,
    };
    let bytes = identity.to_bytes();
    assert_eq!(&bytes[..8], &0x0102_0304_0506_0708_u64.to_le_bytes());
    assert_eq!(&bytes[8..], &0x1112_1314_1516_1718_u64.to_le_bytes());
    assert_eq!(NativeRootIdentity::from_bytes(bytes), identity);
}
use notify::event::EventAttributes;
use notify::event::{CreateKind, DataChange, ModifyKind};
use std::path::PathBuf;

fn event(kind: EventKind, paths: Vec<PathBuf>) -> Event {
    Event {
        kind,
        paths,
        attrs: EventAttributes::new(),
    }
}

#[test]
fn paired_rename_is_exact_and_ambiguous_rename_invalidates()
-> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    let from = root.join("from");
    let to = root.join("to");
    let paired = map_event(
        &event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            vec![from, to],
        ),
        &root,
        VolumeLimits::default(),
    )?;
    assert!(matches!(paired.as_slice(), [WatchChange::Renamed { .. }]));
    assert_eq!(
        map_event(
            &event(
                EventKind::Modify(ModifyKind::Name(RenameMode::From)),
                vec![root.join("old")],
            ),
            &root,
            VolumeLimits::default(),
        ),
        Err(WatchInvalidationReason::AmbiguousRename)
    );
    Ok(())
}

#[test]
fn access_is_ignored_but_native_rescan_and_root_removal_invalidate()
-> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    assert!(
        map_event(
            &event(EventKind::Access(notify::event::AccessKind::Any), vec![]),
            &root,
            VolumeLimits::default(),
        )?
        .is_empty()
    );
    let mut rescan = event(EventKind::Any, vec![root.join("a")]);
    rescan.attrs.set_flag(notify::event::Flag::Rescan);
    assert_eq!(
        map_event(&rescan, &root, VolumeLimits::default()),
        Err(WatchInvalidationReason::NativeRescanRequired)
    );
    assert_eq!(
        map_event(
            &event(
                EventKind::Remove(notify::event::RemoveKind::Folder),
                vec![root.clone()]
            ),
            &root,
            VolumeLimits::default(),
        ),
        Err(WatchInvalidationReason::RootChanged)
    );
    Ok(())
}

#[test]
fn bounded_callback_overflow_invalidates_instead_of_dropping_silently()
-> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    let (sender, _receiver) = sync_channel(1);
    let queued = Arc::new(AtomicU32::new(0));
    let shared = Arc::new(Mutex::new(SharedState { invalidation: None }));
    for name in ["a", "b"] {
        accept_native_event(
            Ok(event(
                EventKind::Create(CreateKind::File),
                vec![root.join(name)],
            )),
            &root,
            VolumeLimits::default(),
            &sender,
            &shared,
            &queued,
        );
    }
    assert_eq!(
        shared.lock().map_err(|_| "poisoned")?.invalidation,
        Some(WatchInvalidationReason::QueueOverflow)
    );
    Ok(())
}

#[test]
fn content_and_metadata_changes_remain_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    let data = map_event(
        &event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            vec![root.join("file")],
        ),
        &root,
        VolumeLimits::default(),
    )?;
    let metadata = map_event(
        &event(
            EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any)),
            vec![root.join("file")],
        ),
        &root,
        VolumeLimits::default(),
    )?;
    assert!(matches!(data.as_slice(), [WatchChange::Modified(_)]));
    // Darwin coalesces FSEvents flags, so a metadata hint may stand for a
    // coalesced data write and must degrade to Modified; exact backends keep
    // the distinction.
    #[cfg(target_os = "macos")]
    assert!(matches!(metadata.as_slice(), [WatchChange::Modified(_)]));
    #[cfg(not(target_os = "macos"))]
    assert!(matches!(
        metadata.as_slice(),
        [WatchChange::MetadataChanged(_)]
    ));
    Ok(())
}

#[test]
fn rename_immediately_followed_by_a_coalesced_metadata_hint_on_the_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    let from = root.join("from");
    let to = root.join("to");
    let renamed = map_event(
        &event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            vec![from, to.clone()],
        ),
        &root,
        VolumeLimits::default(),
    )?;
    let renamed_to = match renamed.as_slice() {
        [WatchChange::Renamed { to, .. }] => to.clone(),
        other => return Err(format!("expected exactly one Renamed change, got {other:?}").into()),
    };
    // A coalesced metadata hint arriving right after, on the destination
    // path, must resolve on its own terms (per platform) rather than being
    // collapsed into or attributed to the preceding rename.
    let after = map_event(
        &event(
            EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any)),
            vec![to],
        ),
        &root,
        VolumeLimits::default(),
    )?;
    #[cfg(target_os = "macos")]
    assert!(matches!(after.as_slice(), [WatchChange::Modified(path)] if *path == renamed_to));
    #[cfg(not(target_os = "macos"))]
    assert!(
        matches!(after.as_slice(), [WatchChange::MetadataChanged(path)] if *path == renamed_to)
    );
    Ok(())
}

#[test]
fn capabilities_never_misrepresent_process_local_sequences_as_restart_cursors() {
    let capabilities = native_watch_capabilities();
    assert!(!capabilities.persistent_restart);
    assert_eq!(
        capabilities.recursive,
        capabilities.backend != NativeWatchBackend::Unsupported
    );
    assert_eq!(capabilities.root_identity_fencing, cfg!(any(unix, windows)));
    let expected = if cfg!(target_os = "windows") {
        "windows-read-directory-changes"
    } else if cfg!(target_os = "macos") {
        "macos-fsevents"
    } else if cfg!(target_os = "linux") {
        "linux-inotify"
    } else {
        "unsupported"
    };
    assert_eq!(capabilities.backend.as_str(), expected);
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn live_native_backend_delivers_a_bounded_relative_change() -> Result<(), Box<dyn std::error::Error>>
{
    use std::time::{Duration, Instant};

    let directory = tempfile::tempdir()?;
    let mut watch = NativeWatch::open(
        directory.path(),
        NativeWatchOptions::new(VolumeLimits::default()),
    )?;
    let initial = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert!(matches!(
        initial.value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::InitialSnapshotRequired,
            ..
        }
    ));
    let failed_epoch = watch.begin_rescan()?;
    assert_eq!(failed_epoch.get(), 1);
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    assert!(matches!(
        watch
            .poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::NativeRescanRequired,
            ..
        }
    ));
    let epoch = watch.begin_rescan()?;
    assert_eq!(epoch.get(), 2);
    assert!(matches!(watch.finish_rescan()?, WatchBatch::Changes { .. }));
    let empty = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert_eq!(empty.work.backend_read_operations, 1);

    std::fs::write(directory.path().join("observed"), b"content")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let observed = loop {
        let receipt = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        match receipt.value {
            WatchBatch::Changes { changes, .. } if !changes.is_empty() => break changes,
            WatchBatch::Changes { .. } if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            WatchBatch::Changes { .. } => return Err("native watcher timed out".into()),
            WatchBatch::RescanRequired { reason, .. } => {
                return Err(format!("native watcher invalidated: {reason}").into());
            }
        }
    };
    assert!(observed.iter().any(|change| match change {
        WatchChange::Created(path)
        | WatchChange::Modified(path)
        | WatchChange::MetadataChanged(path)
        | WatchChange::Removed(path) => path.depth() <= 1,
        WatchChange::Renamed { from, to } => from.depth() <= 1 && to.depth() <= 1,
    }));
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn live_native_backend_fences_a_replaced_root_without_backend_assistance()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("watched");
    let displaced = parent.path().join("displaced");
    std::fs::create_dir(&root)?;
    let mut watch = NativeWatch::open(&root, NativeWatchOptions::new(VolumeLimits::default()))?;
    watch.begin_rescan()?;
    assert!(matches!(watch.finish_rescan()?, WatchBatch::Changes { .. }));

    std::fs::rename(&root, &displaced)?;
    std::fs::create_dir(&root)?;
    let observed = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert!(matches!(
        observed.value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::RootChanged,
            ..
        }
    ));
    let root_probe_work = WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::default()
    };
    assert!(
        observed.work == WorkCounters::default() || observed.work == root_probe_work,
        "root replacement must use only native invalidation or one identity probe"
    );

    drop(watch);
    std::fs::remove_dir(&root)?;
    std::fs::rename(displaced, root)?;
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn live_native_backend_saturation_fails_closed_and_recovers()
-> Result<(), Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};

    let directory = tempfile::tempdir()?;
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.maximum_queued_changes = 1;
    let mut watch = NativeWatch::open(directory.path(), options)?;
    watch.begin_rescan()?;
    assert!(matches!(watch.finish_rescan()?, WatchBatch::Changes { .. }));

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut attempt = 0_u32;
    let reason = loop {
        for index in 0..64_u32 {
            std::fs::write(
                directory
                    .path()
                    .join(format!("saturation-{attempt:04}-{index:04}")),
                index.to_le_bytes(),
            )?;
        }
        std::thread::sleep(Duration::from_millis(100));
        match watch
            .poll(1, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value
        {
            WatchBatch::RescanRequired { reason, .. } => break reason,
            WatchBatch::Changes { .. } if Instant::now() < deadline => {
                attempt = attempt.checked_add(1).ok_or("attempt overflow")?;
            }
            WatchBatch::Changes { .. } => return Err("watcher saturation timed out".into()),
        }
    };
    assert!(matches!(
        reason,
        WatchInvalidationReason::QueueOverflow
            | WatchInvalidationReason::NativeRescanRequired
            | WatchInvalidationReason::UnrepresentablePath
    ));

    let epoch = watch.begin_rescan()?;
    assert!(epoch.get() > 1);
    assert!(matches!(watch.finish_rescan()?, WatchBatch::Changes { .. }));
    Ok(())
}
