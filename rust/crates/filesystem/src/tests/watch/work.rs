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

fn cancelled_registration_fixture()
-> Result<(tempfile::TempDir, NativeWatch), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    std::fs::create_dir_all(directory.path().join("arrived/child"))?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let watch = NativeWatch::open_with_profile_with_native_work(
        directory.path(),
        FilesystemProfile::Portable,
        measured_watch_options(),
        &original,
    )?;
    cancellation.cancel();
    Ok((directory, watch))
}

#[test]
fn tree_registration_refuses_the_cancelled_original_before_enumeration()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut watch) = cancelled_registration_fixture()?;
    // No directory below the root is eligible for registration. The old
    // walker enumerated this tree and returned success without ever reaching
    // a watch-directory call, whose later admission could mask that defect.
    assert!(matches!(
        watch.watch_tree(&["arrived"]),
        Err(NativeWatchError::Cancelled(_))
    ));
    Ok(())
}

#[test]
fn arrival_registration_refuses_the_cancelled_original_before_enumeration()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, mut watch) = cancelled_registration_fixture()?;
    let arrived = relative_namespace_path(
        directory.path(),
        &directory.path().join("arrived"),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
    )?;
    let batch = WatchBatch::Changes {
        epoch: 0,
        first_sequence: 0,
        next_sequence: 1,
        changes: vec![WatchChange::Arrived(arrived)],
        original: Default::default(),
    };
    assert!(matches!(
        watch.watch_arrivals(&batch, &crate::CapturePolicy::default(), 16),
        Err(NativeWatchError::Cancelled(_))
    ));
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn arriving_subtrees_share_one_registration_path_bound() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut changes = Vec::new();
    for name in ["first", "second"] {
        let path = directory.path().join(name);
        std::fs::create_dir(&path)?;
        std::fs::write(path.join("leaf"), b"authored")?;
        changes.push(WatchChange::Arrived(relative_namespace_path(
            directory.path(),
            &path,
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )?));
    }
    let mut watch = NativeWatch::open(directory.path(), measured_watch_options())?;
    let batch = WatchBatch::Changes {
        epoch: 0,
        first_sequence: 0,
        next_sequence: 2,
        changes,
        original: Default::default(),
    };
    assert!(matches!(
        watch.watch_arrivals(&batch, &crate::CapturePolicy::default(), 3),
        Err(NativeWatchError::InvalidOptions)
    ));
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[test]
fn ordinary_watch_retains_preparation_custody_through_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let options = measured_watch_options();
    let opened = NativeWatch::open_with_profile_measured(
        directory.path(),
        FilesystemProfile::Portable,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )?;
    let mut watch = opened.value;
    let birth = watch
        .original_work
        .as_ref()
        .ok_or("ordinary constructor lost native birth custody")?
        .clone();
    assert_eq!(birth.receipt(), opened.work);
    assert!(opened.work.backend_read_operations >= 2);
    watch.accept_lazy_baseline_with_native_work(&birth)?;
    cancellation.cancel();
    let fresh =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    assert!(watch.shutdown_with_native_work(&fresh)?);
    assert!(watch.held_root.root.is_none());
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[test]
fn ordinary_poll_returns_storage_custody_after_producer_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let birth_cancel = CancellationToken::new();
    let birth = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &birth_cancel)
        .map_err(|(error, _)| error)?;
    let options = measured_watch_options();
    // An explicit native constructor keeps this oracle independent of ordinary
    // startup custody, so each public entry point has its own causal control.
    let mut watch = NativeWatch::open_with_profile_with_native_work(
        directory.path(),
        FilesystemProfile::Portable,
        options,
        &birth,
    )?;
    watch.accept_lazy_baseline_with_native_work(&birth)?;
    let polling = CancellationToken::new();
    let polled = watch.poll(1, WorkBudget::UNBOUNDED, &polling)?;
    let WatchBatch::Changes { original, .. } = &polled.value else {
        return Err("quiet admitted watch lost continuity".into());
    };
    assert!(original.has_native_changes_storage_custody());
    birth_cancel.cancel();
    polling.cancel();
    let fresh =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    assert!(watch.shutdown_with_native_work(&fresh)?);
    // The returned interval survives both its producer and cancelled invocations.
    assert!(matches!(&polled.value, WatchBatch::Changes { changes, .. } if changes.is_empty()));
    Ok(())
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
            .expect_err("root admission must fail");
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
    .expect_err("missing root");
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
    .expect_err("subscription budget denied");
    assert!(matches!(failure.error, NativeWatchError::Work(_)));
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn cancelling_a_baseline_future_releases_its_scan_without_certifying_hints()
-> Result<(), Box<dyn std::error::Error>> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

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
    let mut capture = Box::pin(async {
        let rescan = watch.begin_rescan()?;
        std::future::pending::<()>().await;
        watch.finish_rescan(rescan)
    });
    assert!(matches!(
        capture
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(capture);
    let polled = watch.poll(u32::MAX, WorkBudget::UNBOUNDED, &cancellation);
    assert!(
        matches!(
            polled,
            Ok(OperationReceipt {
                value: WatchBatch::RescanRequired {
                    reason: WatchInvalidationReason::BackendError,
                    ..
                },
                ..
            })
        ),
        "cancelled baseline retained scan custody or certified hints: {polled:?}"
    );
    assert!(matches!(watch.accept_lazy_baseline()?,
        WatchBatch::Changes { changes, .. } if changes.is_empty()));
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn a_baseline_owner_cannot_finish_a_different_watcher() -> Result<(), Box<dyn std::error::Error>> {
    let first_directory = tempfile::tempdir()?;
    let second_directory = tempfile::tempdir()?;
    let mut first = NativeWatch::open(first_directory.path(), measured_watch_options())?;
    let mut second = NativeWatch::open(second_directory.path(), measured_watch_options())?;
    let first_scan = first.begin_rescan()?;
    let second_scan = second.begin_rescan()?;
    assert_eq!(first_scan.epoch(), second_scan.epoch());
    assert!(matches!(
        second.begin_rescan(),
        Err(NativeWatchError::RescanAlreadyInProgress)
    ));
    assert!(matches!(
        second.finish_rescan(first_scan),
        Err(NativeWatchError::RescanOwnerMismatch)
    ));
    assert!(matches!(
        second.finish_rescan(second_scan)?,
        WatchBatch::Changes { .. }
    ));
    let cancellation = CancellationToken::new();
    assert!(matches!(
        first
            .poll(u32::MAX, WorkBudget::UNBOUNDED, &cancellation)?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::BackendError,
            ..
        }
    ));
    assert!(matches!(
        first.accept_lazy_baseline()?,
        WatchBatch::Changes { .. }
    ));
    Ok(())
}

#[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
#[test]
fn failed_compound_baselines_release_scan_custody_without_acknowledging_the_root()
-> Result<(), Box<dyn std::error::Error>> {
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
    let budget = WorkBudget {
        backend_read_operations: 0,
        ..WorkBudget::UNBOUNDED
    };
    for native in [false, true] {
        let result = if native {
            let work = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            watch.accept_lazy_baseline_with_native_work(&work)
        } else {
            watch
                .accept_lazy_baseline_measured(budget, &cancellation)
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
        };
        assert!(matches!(result, Err(NativeWatchError::Work(_))));
        assert!(
            watch
                .shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .rescan
                .is_none(),
            "failed compound baseline retained rescan custody"
        );
        assert!(
            matches!(
                watch
                    .poll(u32::MAX, WorkBudget::UNBOUNDED, &cancellation)?
                    .value,
                WatchBatch::RescanRequired {
                    reason: WatchInvalidationReason::BackendError,
                    ..
                }
            ),
            "failed baseline acknowledged an unverified root"
        );
        assert!(matches!(watch.accept_lazy_baseline()?,
            WatchBatch::Changes { changes, .. } if changes.is_empty()));
    }
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
    assert!(
        watch
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .rescan
            .is_none(),
        "failed lazy baseline retained rescan custody"
    );
    let mut work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    for failed in [&mut cancelled_work, &mut denied] {
        let rescan = watch.begin_rescan_with_work(&mut work)?;
        assert!(matches!(
            watch.finish_rescan_with_work(rescan, failed),
            Err(NativeWatchError::Cancelled(_) | NativeWatchError::Work(_))
        ));
    }
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
    assert!(
        watch
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .rescan
            .is_none(),
        "failed rescan preparation retained rescan custody"
    );
    assert!(matches!(
        watch
            .poll(u32::MAX, WorkBudget::UNBOUNDED, &cancellation)?
            .value,
        WatchBatch::RescanRequired { .. }
    ));
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
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::watch::initialize_native_watch_services()?;
    for cancel_birth in [false, true] {
        let directory = tempfile::tempdir()?;
        std::fs::create_dir(directory.path().join("child"))?;
        let birth_cancel = CancellationToken::new();
        let birth = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &birth_cancel)
            .map_err(|(error, _)| error)?;
        let mut watch = NativeWatch::open_with_profile_with_native_work(
            &directory.path().canonicalize()?,
            FilesystemProfile::Portable,
            // FSEvents observes only whole trees; non-recursive registration
            // is unavailable there by design.
            NativeWatchOptions {
                recursive: cfg!(target_os = "macos"),
                ..measured_watch_options()
            },
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
        assert_eq!(watch.windows_registered(), 0);
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
    .map_err(|(error, _)| error)?;
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
    assert!(
        occupied > baseline,
        "live producer resources remain charged"
    );
    // Settle both producers before checking their released payload. A platform
    // delivery task may still own its small control block after terminal stop.
    assert!(first.shutdown_with_native_work(&work)?);
    assert!(second.shutdown_with_native_work(&work)?);
    drop(first);
    drop(second);
    assert!(work.live_allocation_bytes() < occupied);
    Ok(())
}
