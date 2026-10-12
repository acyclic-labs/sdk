use super::*;

pub(super) fn measured_watch_options() -> NativeWatchOptions {
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.recursive = false;
    options
}

pub(super) fn measured_watch_startup_work() -> WorkCounters {
    // Admission metadata, canonicalization, root open, metadata, identity;
    // Windows additionally queries whether the filesystem reports every change.
    WorkCounters {
        backend_read_operations: if cfg!(windows) { 6 } else { 5 },
        // Native watcher construction and root subscription.
        backend_write_operations: 2,
        ..WorkCounters::default()
    }
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn measured_native_watch_open_admits_only_started_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let missing = root.join("missing");
    let cancellation = CancellationToken::new();
    let cancelled = CancellationToken::new();
    cancelled.cancel();

    for requested in [&root, &missing] {
        let failure = NativeWatch::open_with_profile_measured(
            requested,
            FilesystemProfile::Portable,
            measured_watch_options(),
            WorkBudget::default(),
            &cancellation,
        )
        .err()
        .expect("zero budget must reject before admission metadata");
        assert!(matches!(
            failure.error,
            NativeWatchError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                observed: 1,
                maximum: 0,
            })
        ));
        assert_eq!(*failure.work, WorkCounters::default());

        let failure = NativeWatch::open_with_profile_measured(
            requested,
            FilesystemProfile::Portable,
            measured_watch_options(),
            measured_watch_startup_work(),
            &cancelled,
        )
        .err()
        .expect("pre-cancellation must reject before root I/O");
        assert!(matches!(failure.error, NativeWatchError::Cancelled(_)));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let failure = NativeWatch::open_with_profile_measured(
        &missing,
        FilesystemProfile::Portable,
        measured_watch_options(),
        measured_watch_startup_work(),
        &cancellation,
    )
    .err()
    .expect("the root does not exist");
    assert!(matches!(failure.error, NativeWatchError::Io(_)));
    assert_eq!(
        *failure.work,
        WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        }
    );

    let mut short = measured_watch_startup_work();
    short.backend_write_operations = 1;
    let failure = NativeWatch::open_with_profile_measured(
        &root,
        FilesystemProfile::Portable,
        measured_watch_options(),
        short,
        &cancellation,
    )
    .err()
    .expect("root subscription requires a second admitted write");
    assert!(matches!(
        failure.error,
        NativeWatchError::Work(WorkError::BudgetExceeded {
            counter: "backend_write_operations",
            observed: 2,
            maximum: 1,
        })
    ));
    assert_eq!(*failure.work, short);

    let opened = NativeWatch::open_with_profile_measured(
        &root,
        FilesystemProfile::Portable,
        measured_watch_options(),
        measured_watch_startup_work(),
        &cancellation,
    )?;
    assert_eq!(opened.work, measured_watch_startup_work());
    let mut watch = opened.value;
    let root_file = open_native_root(&root)?;
    assert_eq!(
        watch.root_identity(),
        NativeRootIdentity::from_file(&root_file)?
    );
    let baseline_work = WorkCounters {
        backend_read_operations: 2,
        ..WorkCounters::default()
    };
    let baseline = watch.accept_lazy_baseline_measured(baseline_work, &cancellation)?;
    assert_eq!(baseline.work, baseline_work);
    assert!(matches!(
        baseline.value,
        WatchBatch::Changes { epoch, changes, .. } if epoch == 1 && changes.is_empty()
    ));
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn measured_native_watch_baseline_failures_leave_the_rescan_recoverable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut watch = NativeWatch::open_with_profile_measured(
        directory.path().canonicalize()?,
        FilesystemProfile::Portable,
        measured_watch_options(),
        measured_watch_startup_work(),
        &cancellation,
    )?
    .value;
    let cancelled = CancellationToken::new();
    let cancelled_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
        .map_err(|(error, _)| error)?;
    cancelled.cancel();
    assert!(matches!(
        watch.accept_lazy_baseline_with_native_work(&cancelled_work),
        Err(NativeWatchError::Cancelled(_))
    ));
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            backend_read_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    assert!(matches!(
        watch.accept_lazy_baseline_with_native_work(&denied),
        Err(NativeWatchError::Work(_))
    ));
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    watch.begin_rescan_with_work(&mut NativeSourceWork::from_native_materialization_work(
        &work,
    ))?;
    assert!(matches!(
        watch.finish_rescan_with_work(&mut NativeSourceWork::from_native_materialization_work(
            &cancelled_work
        )),
        Err(NativeWatchError::Cancelled(_))
    ));
    assert!(matches!(
        watch.finish_rescan_with_work(&mut NativeSourceWork::from_native_materialization_work(
            &denied
        )),
        Err(NativeWatchError::Work(_))
    ));
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    assert!(
        matches!(watch.accept_lazy_baseline_with_native_work(&work)?,
        WatchBatch::Changes { changes, .. } if changes.is_empty())
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn measured_native_watch_cleanup_retains_prior_work_and_can_be_retried()
-> Result<(), Box<dyn std::error::Error>> {
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
        measured_watch_startup_work(),
        &cancellation,
    )?
    .value;
    for name in ["/a", "/b"] {
        let portable = crate::path::PortablePath::parse(name, VolumeLimits::default())?;
        watch.watch_directory(&NamespacePath::from_portable(
            &portable,
            VolumeLimits::default(),
        )?)?;
    }
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            backend_write_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    assert!(matches!(
        watch.begin_rescan_with_work(&mut NativeSourceWork::from_native_materialization_work(
            &denied
        )),
        Err(NativeWatchError::Work(_))
    ));
    assert!(!watch.watched_directories.contains(&root.join("a")));
    assert!(watch.watched_directories.contains(&root.join("b")));
    watch.abort_rescan(WatchInvalidationReason::NativeRescanRequired)?;
    let cancelled = CancellationToken::new();
    let cancelled_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
        .map_err(|(error, _)| error)?;
    cancelled.cancel();
    assert!(matches!(
        watch.accept_lazy_baseline_with_native_work(&cancelled_work),
        Err(NativeWatchError::Cancelled(_))
    ));
    assert!(watch.watched_directories.contains(&root.join("b")));
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(
        matches!(watch.accept_lazy_baseline_with_native_work(&work)?,
        WatchBatch::Changes { changes, .. } if changes.is_empty())
    );
    assert_eq!(watch.watched_directories, vec![root]);
    Ok(())
}
