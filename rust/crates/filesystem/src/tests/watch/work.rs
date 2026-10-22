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

#[test]
fn native_shutdown_retires_birth_resources_with_a_fresh_invocation()
-> Result<(), Box<dyn std::error::Error>> {
    for cancel_birth in [false, true] {
        let directory = tempfile::tempdir()?;
        std::fs::create_dir(directory.path().join("child"))?;
        let birth_cancel = CancellationToken::new();
        let birth = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &birth_cancel)
            .map_err(|(error, _)| error)?;
        let mut watch = NativeWatch::open_with_profile_with_native_work(
            &directory.path().canonicalize()?,
            FilesystemProfile::Portable,
            measured_watch_options(),
            &birth,
        )?;
        let child = crate::path::PortablePath::parse("/child", VolumeLimits::default())?;
        watch.watch_directory_with_work(
            &NamespacePath::from_portable(&child, VolumeLimits::default())?,
            &mut crate::NativeMaterializationWork::clone(&birth),
        )?;
        let current_cancel = CancellationToken::new();
        let cancelled =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &current_cancel)
                .map_err(|(error, _)| error)?;
        current_cancel.cancel();
        assert!(watch.shutdown_with_native_work(&cancelled).is_err());
        let NativeWatchDriver::Source(source) = &watch.watcher else {
            panic!("native producer must remain owned after rejected cleanup");
        };
        assert!(!source.is_stopped());
        let before = birth.receipt();
        if cancel_birth {
            birth_cancel.cancel();
        }
        let current =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        assert!(watch.shutdown_with_native_work(&current)?);
        let NativeWatchDriver::Source(source) = &watch.watcher else {
            panic!("cleanup preserves the producer identity");
        };
        assert!(source.is_stopped());
        #[cfg(windows)]
        assert!(watch.windows_directories.is_empty());
        assert!(birth.receipt().backend_write_operations > before.backend_write_operations);
        // Only the held root close belongs to the fresh invocation. Platform
        // watch retirement consumes credits accepted before those watches began.
        assert_eq!(current.receipt().backend_write_operations, 1);
    }
    Ok(())
}

#[test]
fn overlapping_native_watches_charge_only_occupied_queue_storage()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::watch::initialize_native_watch_services()?;
    let first = tempfile::tempdir()?;
    let second = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            peak_allocation_bytes: 4 * 1024 * 1024,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .unwrap();
    let baseline = work.live_allocation_bytes();
    let options = crate::watch::NativeWatchOptions {
        maximum_queued_changes: 65_536,
        recursive: false,
        ..crate::watch::NativeWatchOptions::new(crate::VolumeLimits::default())
    };
    let mut first = crate::watch::NativeWatch::open_with_profile_with_native_work(
        first.path(),
        crate::FilesystemProfile::Portable,
        options,
        &work,
    )?;
    let mut second = crate::watch::NativeWatch::open_with_profile_with_native_work(
        second.path(),
        crate::FilesystemProfile::Portable,
        options,
        &work,
    )?;
    let occupied = work.live_allocation_bytes();
    assert!(occupied > baseline, "live producer resources remain charged");
    // Settle both producers before checking their released payload. A platform
    // delivery task may still own its small control block after terminal stop.
    assert!(first.shutdown_with_native_work(&work)?);
    assert!(second.shutdown_with_native_work(&work)?);
    drop(first);
    drop(second);
    assert!(work.live_allocation_bytes() < occupied);
    Ok(())
}
