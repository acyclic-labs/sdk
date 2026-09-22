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
use crate::kernel::NameEncoding;
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
fn watcher_names_follow_the_volume_profile() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    let event = event(
        EventKind::Modify(ModifyKind::Data(DataChange::Content)),
        vec![root.join("name")],
    );
    let encoding = |profile| -> Result<NameEncoding, Box<dyn std::error::Error>> {
        let changes = map_event(&event, &root, profile, VolumeLimits::default())?;
        let path = match changes.as_slice() {
            [WatchChange::Modified(path)] => path,
            other => return Err(format!("expected one modified path, got {other:?}").into()),
        };
        Ok(path.components()[0].encoding())
    };

    assert_eq!(encoding(FilesystemProfile::Portable)?, NameEncoding::Utf8);
    assert_eq!(encoding(FilesystemProfile::Browser)?, NameEncoding::Utf8);
    assert_eq!(
        encoding(FilesystemProfile::Posix)?,
        NameEncoding::PosixBytes
    );
    assert_eq!(
        encoding(FilesystemProfile::Windows)?,
        NameEncoding::WindowsUtf16Le
    );
    Ok(())
}

#[cfg(target_os = "macos")]
#[test]
fn replayed_creation_of_the_admitted_root_is_not_a_namespace_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_path_buf();
    let root_file = open_native_root(&root)?;
    let identity = NativeRootIdentity::from_file(&root_file)?;
    validate_replayed_root_creation(
        &event(EventKind::Create(CreateKind::Folder), vec![root.clone()]),
        &root,
        identity,
    )?;
    assert_eq!(
        validate_replayed_root_creation(
            &event(EventKind::Create(CreateKind::Folder), vec![root.clone()]),
            &root,
            NativeRootIdentity {
                device: identity.device,
                object: identity.object ^ 1,
            },
        ),
        Err(WatchInvalidationReason::RootChanged)
    );
    let changes = map_event(
        &event(EventKind::Create(CreateKind::Folder), vec![root.clone()]),
        &root,
        FilesystemProfile::Portable,
        VolumeLimits::default(),
    )?;
    assert!(changes.is_empty());
    Ok(())
}

#[cfg(target_os = "macos")]
#[test]
fn coalesced_root_modification_requires_a_rescan() {
    let root = PathBuf::from("/root");
    assert_eq!(
        map_event(
            &event(
                EventKind::Modify(ModifyKind::Data(DataChange::Any)),
                vec![root.clone()],
            ),
            &root,
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        ),
        Err(WatchInvalidationReason::NativeRescanRequired)
    );
}

#[test]
fn root_replacement_during_a_baseline_invalidates_the_rescan()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("watched");
    let displaced = parent.path().join("displaced");
    std::fs::create_dir(&root)?;
    let mut watch = NativeWatch::open(&root, NativeWatchOptions::new(VolumeLimits::default()))?;
    watch.begin_rescan()?;
    std::fs::rename(&root, &displaced)?;
    std::fs::create_dir(&root)?;
    assert!(matches!(
        watch.finish_rescan()?,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::RootChanged,
            ..
        }
    ));
    Ok(())
}

#[test]
fn paired_rename_is_exact_and_an_unpaired_half_is_platform_defined()
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
        FilesystemProfile::Portable,
        VolumeLimits::default(),
    )?;
    assert!(matches!(paired.as_slice(), [WatchChange::Renamed { .. }]));
    for mode in [RenameMode::From, RenameMode::To] {
        let unpaired = map_event(
            &event(
                EventKind::Modify(ModifyKind::Name(mode)),
                vec![root.join("one-sided")],
            ),
            &root,
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        );
        // inotify pairs halves by cookie before `map_event`, so a lone half
        // there is a lost half; FSEvents and `ReadDirectoryChangesW` never
        // pair, so a lone half is an ordinary "this path changed" hint.
        if cfg!(target_os = "linux") {
            assert_eq!(unpaired, Err(WatchInvalidationReason::AmbiguousRename));
        } else {
            let one_sided = relative_namespace_path(
                &root,
                &root.join("one-sided"),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )?;
            assert_eq!(unpaired, Ok(vec![WatchChange::Modified(one_sided)]));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[test]
fn rename_any_yields_one_modified_hint_per_path() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(if cfg!(windows) { r"C:\root" } else { "/root" });
    let changes = map_event(
        &event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
            vec![root.join("staged"), root.join("final")],
        ),
        &root,
        FilesystemProfile::Portable,
        VolumeLimits::default(),
    )?;
    let relative = |name: &str| {
        relative_namespace_path(
            &root,
            &root.join(name),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
    };
    assert_eq!(
        changes,
        vec![
            WatchChange::Modified(relative("staged")?),
            WatchChange::Modified(relative("final")?),
        ]
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn tracked_rename(kind: RenameMode, paths: Vec<PathBuf>, tracker: usize) -> Event {
    let mut event = event(EventKind::Modify(ModifyKind::Name(kind)), paths);
    event.attrs.set_tracker(tracker);
    event
}

#[cfg(target_os = "linux")]
#[test]
fn linux_tracked_rename_uses_one_queue_slot_and_keeps_exact_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from("/root");
    let from = root.join("from");
    let to = root.join("to");
    let context = NativeEventContext {
        root,
        root_identity: NativeRootIdentity {
            device: 0,
            object: 0,
        },
        profile: FilesystemProfile::Portable,
        limits: VolumeLimits::default(),
        maximum_queued_changes: 1,
    };
    let (sender, receiver) = sync_channel(1);
    let queued = Arc::new(AtomicU32::new(0));
    let shared = Arc::new(Mutex::new(SharedState {
        invalidation: None,
        pending_rename: None,
    }));
    for event in [
        tracked_rename(RenameMode::From, vec![from.clone()], 7),
        tracked_rename(RenameMode::To, vec![to.clone()], 7),
        tracked_rename(RenameMode::Both, vec![from, to], 7),
    ] {
        accept_native_event(Ok(event), &context, &sender, &shared, &queued);
    }
    assert_eq!(
        shared.lock().map_err(|_| "poisoned")?.seal_invalidation(),
        None
    );
    assert_eq!(queued.load(Ordering::Acquire), 1);
    assert!(matches!(receiver.try_recv()?, WatchChange::Renamed { .. }));
    assert!(receiver.try_recv().is_err());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn linux_unpaired_or_mismatched_rename_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from("/root");
    let from = root.join("from");
    let to = root.join("to");
    let context = NativeEventContext {
        root: root.clone(),
        root_identity: NativeRootIdentity {
            device: 0,
            object: 0,
        },
        profile: FilesystemProfile::Portable,
        limits: VolumeLimits::default(),
        maximum_queued_changes: 1,
    };
    let scenarios = [
        vec![tracked_rename(RenameMode::To, vec![to.clone()], 7)],
        vec![tracked_rename(RenameMode::From, vec![from.clone()], 7)],
        vec![event(
            EventKind::Modify(ModifyKind::Name(RenameMode::From)),
            vec![from.clone()],
        )],
        vec![event(
            EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            vec![to.clone()],
        )],
        vec![tracked_rename(
            RenameMode::Both,
            vec![from.clone(), to.clone()],
            7,
        )],
        vec![
            tracked_rename(RenameMode::From, vec![from.clone()], 7),
            tracked_rename(RenameMode::From, vec![from.clone()], 8),
        ],
        vec![
            tracked_rename(RenameMode::From, vec![from.clone()], 7),
            tracked_rename(RenameMode::To, vec![to.clone()], 8),
        ],
        vec![
            tracked_rename(RenameMode::From, vec![from.clone()], 7),
            tracked_rename(RenameMode::To, vec![to.clone()], 7),
            tracked_rename(RenameMode::To, vec![to.clone()], 7),
        ],
        vec![
            tracked_rename(RenameMode::From, vec![from.clone()], 7),
            tracked_rename(RenameMode::To, vec![to.clone()], 7),
            tracked_rename(RenameMode::Both, vec![from.clone(), to.clone()], 8),
        ],
        vec![
            tracked_rename(RenameMode::From, vec![from.clone()], 7),
            tracked_rename(RenameMode::To, vec![to.clone()], 7),
            tracked_rename(RenameMode::Both, vec![from, root.join("wrong")], 7),
        ],
    ];
    for events in scenarios {
        let (sender, receiver) = sync_channel(1);
        let queued = Arc::new(AtomicU32::new(0));
        let shared = Arc::new(Mutex::new(SharedState {
            invalidation: None,
            pending_rename: None,
        }));
        for event in events {
            accept_native_event(Ok(event), &context, &sender, &shared, &queued);
        }
        assert_eq!(
            shared.lock().map_err(|_| "poisoned")?.seal_invalidation(),
            Some(WatchInvalidationReason::AmbiguousRename)
        );
        assert!(receiver.try_recv().is_err());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn linux_pending_rename_invalidates_rescan_completion() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut watch = NativeWatch::open(
        directory.path(),
        NativeWatchOptions {
            maximum_queued_changes: 1,
            ..NativeWatchOptions::new(VolumeLimits::default())
        },
    )?;
    watch.begin_rescan()?;
    watch.shared.lock().map_err(|_| "poisoned")?.pending_rename = Some(PendingRename {
        tracker: 7,
        from: directory.path().join("moved-out"),
        to: None,
    });
    assert!(matches!(
        watch.finish_rescan()?,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::AmbiguousRename,
            ..
        }
    ));
    watch.begin_rescan()?;
    assert!(
        watch
            .shared
            .lock()
            .map_err(|_| "poisoned")?
            .pending_rename
            .is_none()
    );
    assert!(matches!(watch.finish_rescan()?, WatchBatch::Changes { .. }));
    watch.shared.lock().map_err(|_| "poisoned")?.pending_rename = Some(PendingRename {
        tracker: 8,
        from: directory.path().join("moved-out-again"),
        to: None,
    });
    assert!(matches!(
        watch
            .poll(1, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::AmbiguousRename,
            ..
        }
    ));
    watch.shared.lock().map_err(|_| "poisoned")?.pending_rename = None;
    assert!(matches!(
        watch
            .poll(1, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::AmbiguousRename,
            ..
        }
    ));
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
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )?
        .is_empty()
    );
    let mut rescan = event(EventKind::Any, vec![root.join("a")]);
    rescan.attrs.set_flag(notify::event::Flag::Rescan);
    assert_eq!(
        map_event(
            &rescan,
            &root,
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        ),
        Err(WatchInvalidationReason::NativeRescanRequired)
    );
    assert_eq!(
        map_event(
            &event(
                EventKind::Remove(notify::event::RemoveKind::Folder),
                vec![root.clone()]
            ),
            &root,
            FilesystemProfile::Portable,
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
    let shared = Arc::new(Mutex::new(SharedState {
        invalidation: None,
        #[cfg(target_os = "linux")]
        pending_rename: None,
    }));
    let context = NativeEventContext {
        root: root.clone(),
        root_identity: NativeRootIdentity {
            device: 0,
            object: 0,
        },
        profile: FilesystemProfile::Portable,
        limits: VolumeLimits::default(),
        #[cfg(target_os = "linux")]
        maximum_queued_changes: 1,
    };
    for name in ["a", "b"] {
        accept_native_event(
            Ok(event(
                EventKind::Create(CreateKind::File),
                vec![root.join(name)],
            )),
            &context,
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
        FilesystemProfile::Portable,
        VolumeLimits::default(),
    )?;
    let metadata = map_event(
        &event(
            EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any)),
            vec![root.join("file")],
        ),
        &root,
        FilesystemProfile::Portable,
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
        FilesystemProfile::Portable,
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
        FilesystemProfile::Portable,
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

#[cfg(target_os = "linux")]
#[test]
fn linux_demand_watch_ignores_unobserved_subtrees_and_tracks_observed_directories()
-> Result<(), Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};

    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("observed"))?;
    let file = root.path().join("observed/file");
    std::fs::write(&file, b"before")?;
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.recursive = false;
    let mut watch = NativeWatch::open(root.path(), options)?;
    watch.accept_lazy_baseline()?;

    std::fs::write(&file, b"unobserved")?;
    std::thread::sleep(Duration::from_millis(100));
    assert!(matches!(
        watch
            .poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::Changes { changes, .. } if changes.is_empty()
    ));

    let portable = crate::path::PortablePath::parse("/observed", VolumeLimits::default())?;
    let observed = NamespacePath::from_portable(&portable, VolumeLimits::default())?;
    watch.watch_directory(&observed)?;
    std::fs::write(&file, b"after")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match watch
            .poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value
        {
            WatchBatch::Changes { changes, .. }
                if changes.iter().any(|change| match change {
                    WatchChange::Created(path)
                    | WatchChange::Modified(path)
                    | WatchChange::MetadataChanged(path)
                    | WatchChange::Removed(path) => path.depth() == 2,
                    WatchChange::Renamed { from, to } => from.depth() == 2 || to.depth() == 2,
                }) =>
            {
                break;
            }
            WatchBatch::Changes { .. } if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            WatchBatch::Changes { .. } => return Err("demand watcher timed out".into()),
            WatchBatch::RescanRequired { reason, .. } => {
                return Err(format!("demand watcher invalidated: {reason}").into());
            }
        }
    }
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
    // The native rename callback can invalidate first; either reason fences
    // the replaced root before any stale change is published.
    assert!(matches!(
        observed.value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::RootChanged | WatchInvalidationReason::AmbiguousRename,
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
