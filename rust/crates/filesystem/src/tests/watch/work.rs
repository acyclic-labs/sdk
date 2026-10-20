use super::*;

pub(super) fn measured_watch_options() -> NativeWatchOptions {
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.recursive = false;
    options
}

pub(super) fn measured_watch_startup_work() -> WorkCounters {
    WorkCounters {
        backend_read_operations: if cfg!(windows) { 6 } else { 5 },
        backend_write_operations: 2,
        ..WorkCounters::default()
    }
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn watch_open_rejects_denied_cancelled_and_missing_roots() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let missing = root.join("missing");
    let live = CancellationToken::new();
    let stopped = CancellationToken::new();
    stopped.cancel();
    for requested in [&root, &missing] {
        for (budget, cancel) in [
            (WorkBudget::default(), &live),
            (measured_watch_startup_work(), &stopped),
        ] {
            let failure = NativeWatch::open_with_profile_measured(
                requested,
                FilesystemProfile::Portable,
                measured_watch_options(),
                budget,
                cancel,
            )
            .err()
            .expect("root admission must fail");
            assert!(matches!(
                failure.error,
                NativeWatchError::Work(_) | NativeWatchError::Cancelled(_)
            ));
            assert_eq!(*failure.work, WorkCounters::default());
        }
    }
    let failure = NativeWatch::open_with_profile_measured(
        &missing,
        FilesystemProfile::Portable,
        measured_watch_options(),
        measured_watch_startup_work(),
        &live,
    )
    .err()
    .expect("missing root");
    assert!(matches!(failure.error, NativeWatchError::Io(_)));
    let mut short = measured_watch_startup_work();
    short.backend_write_operations = 1;
    let failure = NativeWatch::open_with_profile_measured(
        &root,
        FilesystemProfile::Portable,
        measured_watch_options(),
        short,
        &live,
    )
    .err()
    .expect("subscription budget denied");
    assert!(matches!(failure.error, NativeWatchError::Work(_)));
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn baseline_failures_leave_the_rescan_recoverable() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut watch = NativeWatch::open_with_profile_measured(
        directory.path().canonicalize()?,
        FilesystemProfile::Portable,
        measured_watch_options(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )?
    .value;
    let cancelled = CancellationToken::new();
    let mut cancelled_work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
            .map_err(|(error, _)| error)?;
    cancelled.cancel();
    let mut denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            backend_read_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    for work in [&cancelled_work, &denied] {
        assert!(matches!(
            watch.accept_lazy_baseline_with_native_work(work),
            Err(NativeWatchError::Cancelled(_) | NativeWatchError::Work(_))
        ));
    }
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    let mut work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    watch.begin_rescan_with_work(&mut work)?;
    for failed in [&mut cancelled_work, &mut denied] {
        assert!(matches!(
            watch.finish_rescan_with_work(failed),
            Err(NativeWatchError::Cancelled(_) | NativeWatchError::Work(_))
        ));
    }
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    assert!(
        matches!(watch.accept_lazy_baseline_with_native_work(&work)?,
        WatchBatch::Changes { changes, .. } if changes.is_empty())
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn partial_watch_cleanup_can_be_retried() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    for name in ["a", "b"] {
        std::fs::create_dir(root.join(name))?;
    }
    let cancellation = CancellationToken::new();
    let mut watch = NativeWatch::open_with_profile_measured(
        &root,
        FilesystemProfile::Portable,
        measured_watch_options(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )?
    .value;
    for name in ["a", "b"] {
        let portable =
            crate::path::PortablePath::parse(&format!("/{name}"), VolumeLimits::default())?;
        watch.watch_directory(&NamespacePath::from_portable(
            &portable,
            VolumeLimits::default(),
        )?)?;
    }
    let NativeWatchDriver::Source(source) = &watch.watcher else {
        panic!("Linux demand watch owns the native registrations");
    };
    for name in ["", "a", "b"] {
        assert!(source.is_admitted(Path::new(name)), "{name} is registered");
    }
    let mut denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            backend_write_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    assert!(matches!(
        watch.begin_rescan_with_work(&mut denied),
        Err(NativeWatchError::Work(_))
    ));
    let NativeWatchDriver::Source(source) = &watch.watcher else {
        panic!("Linux demand watch owns the native registrations");
    };
    assert!(!source.is_admitted(Path::new("a")));
    assert!(source.is_admitted(Path::new("b")));
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    let cancelled = CancellationToken::new();
    let cancelled_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
        .map_err(|(error, _)| error)?;
    cancelled.cancel();
    assert!(matches!(
        watch.accept_lazy_baseline_with_native_work(&cancelled_work),
        Err(NativeWatchError::Cancelled(_))
    ));
    let NativeWatchDriver::Source(source) = &watch.watcher else {
        panic!("Linux demand watch owns the native registrations");
    };
    assert!(source.is_admitted(Path::new("b")));
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(
        matches!(watch.accept_lazy_baseline_with_native_work(&work)?,
        WatchBatch::Changes { changes, .. } if changes.is_empty())
    );
    let NativeWatchDriver::Source(source) = &watch.watcher else {
        panic!("Linux demand watch owns the native registrations");
    };
    assert!(source.is_admitted(Path::new("")));
    assert!(!source.is_admitted(Path::new("a")));
    assert!(!source.is_admitted(Path::new("b")));
    Ok(())
}
