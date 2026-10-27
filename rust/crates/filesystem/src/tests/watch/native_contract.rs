//! One fence contract for precise notifications and conservative root hints.
use super::*;
use std::time::Duration;

const FENCE: Duration = Duration::from_secs(5);

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
    watch.fence(FENCE)?;
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
    fixture.watch.fence(FENCE)?;
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
        fixture.watch.fence(FENCE)?;
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
