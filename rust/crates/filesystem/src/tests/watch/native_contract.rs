//! One fence contract for precise notifications and conservative root hints.
use super::*;

struct Fixture {
    // Retire the producer before removing its watched directory.
    watch: NativeWatch,
    directory: tempfile::TempDir,
    excluded: Vec<NamespacePath>,
}

fn fixture(held: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    #[cfg(windows)]
    let directory = crate::windows_test_filesystems::ntfs_tempdir()?;
    #[cfg(not(windows))]
    let directory = tempfile::tempdir()?;
    std::fs::create_dir(directory.path().join(".git"))?;
    let limits = VolumeLimits::default();
    let options = NativeWatchOptions::new(limits);
    let mut watch = if held {
        let work = native_watch_work()?;
        NativeWatch::open_with_profile_with_native_work(
            directory.path(),
            native_filesystem_profile(),
            NativeWatchOptions {
                recursive: false,
                ..options
            },
            &work,
        )?
    } else {
        NativeWatch::open(directory.path(), options)?
    };
    let excluded = [".acyclic-sdk", ".git"]
        .into_iter()
        .map(|name| {
            relative_namespace_path(
                directory.path(),
                &directory.path().join(name),
                native_filesystem_profile(),
                limits,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    watch.exclude_subtrees(&excluded)?;
    let initial = poll(&mut watch)?;
    assert!(matches!(
        initial,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::InitialSnapshotRequired,
            ..
        }
    ));
    let rescan = watch.begin_rescan()?;
    let failed_epoch = rescan.epoch();
    assert_eq!(failed_epoch, 1);
    rescan.abort(WatchInvalidationReason::NativeRescanRequired);
    assert!(matches!(
        poll(&mut watch)?,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::NativeRescanRequired,
            ..
        }
    ));
    // Drain the startup interval before admitting the baseline for our write.
    // This is one fence, not a retry of the operation under test.
    baseline(&mut watch)?;
    watch.fence()?;
    baseline(&mut watch)?;
    Ok(Fixture {
        directory,
        watch,
        excluded,
    })
}

fn baseline(watch: &mut NativeWatch) -> Result<(), Box<dyn std::error::Error>> {
    let rescan = watch.begin_rescan()?;
    assert!(matches!(
        watch.finish_rescan(rescan)?,
        WatchBatch::Changes { .. }
    ));
    Ok(())
}

fn poll(watch: &mut NativeWatch) -> Result<WatchBatch, Box<dyn std::error::Error>> {
    Ok(watch
        .poll(64, WorkBudget::UNBOUNDED, &CancellationToken::new())?
        .value)
}

fn assert_notice(batch: &WatchBatch, expected: &NamespacePath, excluded: &[NamespacePath]) {
    match batch {
        WatchBatch::Changes { changes, .. } => {
            assert!(
                changes.iter().any(|change| match change {
                    WatchChange::Created(path)
                    | WatchChange::Modified(path)
                    | WatchChange::Arrived(path)
                    | WatchChange::MetadataChanged(path) => path == expected,
                    WatchChange::Removed(_) | WatchChange::Renamed { .. } => false,
                }),
                "a confirmed fence must queue the completed write: {batch:?}"
            );
            assert!(changes.iter().all(|change| match change {
                WatchChange::Created(path)
                | WatchChange::Modified(path)
                | WatchChange::Arrived(path)
                | WatchChange::MetadataChanged(path)
                | WatchChange::Removed(path) =>
                    path.depth() <= 1 && !excluded.iter().any(|root| path.is_within(root)),
                WatchChange::Renamed { .. } => false,
            }));
        }
        WatchBatch::RescanRequired { reason, .. } => {
            assert_eq!(*reason, WatchInvalidationReason::NativeRescanRequired);
        }
    }
}

fn written_fixture(held: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let mut fixture = fixture(held)?;
    let file = fixture.directory.path().join("observed");
    let expected = relative_namespace_path(
        fixture.directory.path(),
        &file,
        native_filesystem_profile(),
        VolumeLimits::default(),
    )?;
    std::fs::write(&file, b"content")?;
    fixture.watch.fence()?;
    let batch = poll(&mut fixture.watch)?;
    assert_notice(&batch, &expected, &fixture.excluded);
    if let WatchBatch::RescanRequired { epoch, reason } = batch {
        // A conservative notice is sticky until a new baseline; it must never
        // silently become an empty, supposedly contiguous change interval.
        assert!(
            matches!(poll(&mut fixture.watch)?, WatchBatch::RescanRequired {
            epoch: repeated, reason: repeated_reason,
        } if epoch == repeated && reason == repeated_reason)
        );
        assert_eq!(std::fs::read(&file)?, b"content");
        baseline(&mut fixture.watch)?;
    }
    assert!(!fixture.directory.path().join(".acyclic-sdk").exists());
    assert_eq!(
        std::fs::read_dir(fixture.directory.path().join(".git"))?.count(),
        0
    );
    Ok(fixture)
}

pub(super) fn completed_write(held: bool) -> Result<(), Box<dyn std::error::Error>> {
    written_fixture(held)?;
    Ok(())
}

pub(super) fn cookie_is_hidden(held: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = written_fixture(held)?;
    for _ in 0..20 {
        fixture.watch.fence()?;
        assert!(matches!(poll(&mut fixture.watch)?,
            WatchBatch::Changes { changes, .. } if changes.is_empty()));
    }
    assert!(!fixture.directory.path().join(".acyclic-sdk").exists());
    assert_eq!(
        std::fs::read_dir(fixture.directory.path().join(".git"))?.count(),
        0
    );
    Ok(())
}

#[test]
fn native_arrival_retains_early_batches_and_ends_on_cancel_or_producer_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(true)?;
    let original = native_watch_work()?;
    let file = fixture.directory.path().join("arrival");
    let expected = relative_namespace_path(
        fixture.directory.path(),
        &file,
        native_filesystem_profile(),
        VolumeLimits::default(),
    )?;
    // The actual producer may deliver before registration or while the OS wait
    // is entered. Both orderings retain the same batch, without a fence write.
    std::fs::write(&file, b"first")?;
    let before = original.receipt();
    fixture.watch.wait_for_changes_with_native_work(&original)?;
    let after = original.receipt();
    assert_eq!(
        before.backend_read_operations,
        after.backend_read_operations
    );
    assert_eq!(
        before.backend_write_operations,
        after.backend_write_operations
    );
    assert_notice(&poll(&mut fixture.watch)?, &expected, &fixture.excluded);
    baseline(&mut fixture.watch)?;

    // The original fence settles delivery before the arrival observer exists.
    std::fs::write(&file, b"second")?;
    fixture.watch.fence()?;
    fixture.watch.wait_for_changes_with_native_work(&original)?;
    assert_notice(&poll(&mut fixture.watch)?, &expected, &fixture.excluded);
    baseline(&mut fixture.watch)?;

    let renamed = fixture.directory.path().join("renamed");
    std::fs::rename(&file, &renamed)?;
    fixture.watch.wait_for_changes_with_native_work(&original)?;
    let batch = poll(&mut fixture.watch)?;
    match batch {
        WatchBatch::Changes { changes, .. } => assert!(changes.iter().any(
            |change| matches!(change, WatchChange::Renamed { from, .. } if from == &expected)
        )),
        WatchBatch::RescanRequired { reason, .. } => {
            assert_eq!(reason, WatchInvalidationReason::NativeRescanRequired);
        }
    }
    // The real producer's fence establishes the quiet cancellation point.
    fixture.watch.fence()?;
    baseline(&mut fixture.watch)?;

    let cancellation = CancellationToken::new();
    let waiting = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    std::thread::scope(|scope| {
        let (entered, entering) = std::sync::mpsc::channel();
        let watch = &fixture.watch;
        let waiter = scope.spawn(move || {
            entered.send(()).expect("observer entry receiver");
            watch.wait_for_changes_with_native_work(&waiting)
        });
        entering.recv().expect("observer entry producer");
        cancellation.cancel();
        assert!(matches!(
            waiter.join().expect("arrival observer"),
            Err(NativeWatchError::Cancelled(_))
        ));
    });

    #[cfg(target_os = "linux")]
    std::thread::scope(|scope| {
        let (entered, entering) = std::sync::mpsc::channel();
        let watch = &fixture.watch;
        let original = &original;
        let waiter = scope.spawn(move || {
            entered.send(()).expect("observer entry receiver");
            watch.wait_for_changes_with_native_work(original)
        });
        entering.recv().expect("observer entry producer");
        let NativeWatchDriver::Source(source) = &fixture.watch.watcher else {
            panic!("the held fixture must retain its native producer");
        };
        // Retire the actual original producer; no replacement error or timer.
        source
            .stop_with_work(&mut crate::NativeMaterializationWork::clone(original))
            .expect("native producer retirement");
        assert!(matches!(
            waiter.join().expect("arrival observer"),
            Err(NativeWatchError::SourceUnavailable)
        ));
        // The terminal state remains observable after its first notification.
        assert!(matches!(
            watch.wait_for_changes_with_native_work(original),
            Err(NativeWatchError::SourceUnavailable)
        ));
    });
    Ok(())
}
