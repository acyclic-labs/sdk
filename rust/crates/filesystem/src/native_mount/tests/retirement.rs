use super::*;

#[test]
fn taking_a_terminal_cause_preserves_the_distinct_cleanup_cause()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let producer =
        std::fs::read(root.path().join("absent")).expect_err("producer input must be absent");
    let cleanup =
        std::fs::read(root.path()).expect_err("a directory cannot be read as cleanup bytes");
    let producer_code = producer.raw_os_error();
    let cleanup_code = cleanup.raw_os_error();
    assert_ne!(producer_code, cleanup_code);
    let mut failures = crate::native_mount::retirement::TerminalFailures {
        producer: Some(producer.into()),
        cleanup: Some(cleanup.into()),
    };
    assert!(
        matches!(failures.take(), Some(NativeMountError::Io(cause)) if cause.raw_os_error() == producer_code)
    );
    assert!(!failures.is_empty());
    let (last, extra) = failures.take_pair().ok_or("cleanup outcome was lost")?;
    assert!(matches!(last, NativeMountError::Io(cause) if cause.raw_os_error() == cleanup_code));
    assert!(extra.is_none());
    assert!(failures.is_empty());
    assert!(failures.take_pair().is_none());
    Ok(())
}

#[test]
fn stopped_session_reports_each_cause_after_releasing_its_fence()
-> Result<(), Box<dyn std::error::Error>> {
    for mode in 0..3 {
        let Fixture {
            root,
            mut session,
            work,
            fence,
        } = Fixture::new()?;
        let producer = std::fs::read(root.path().join("absent-worker"))
            .expect_err("worker input must be absent");
        let cleanup =
            std::fs::read(root.path()).expect_err("a directory cannot be read as cleanup bytes");
        let producer_code = producer.raw_os_error();
        let cleanup_code = cleanup.raw_os_error();
        session.terminal_failures = crate::native_mount::retirement::TerminalFailures {
            producer: Some(producer.into()),
            cleanup: Some(cleanup.into()),
        };
        let wait = CancellationToken::new();
        let stop = match mode {
            0 => NativeSessionStopWork::Meter(&work),
            1 => NativeSessionStopWork::AcceptedWait(&wait),
            _ => NativeSessionStopWork::AcceptedDrop,
        };
        let before = work.receipt();
        assert!(
            matches!(session.stop_with_context(&stop), Err(NativeMountError::Io(cause)) if cause.raw_os_error() == producer_code)
        );
        retired(root.path(), &fence, before, &work)?;
        assert!(session.cleanup_state().cleanup_complete());
        assert!(
            matches!(session.stop_with_context(&stop), Err(NativeMountError::Io(cause)) if cause.raw_os_error() == cleanup_code)
        );
        assert!(!session.stop_with_context(&stop)?);
    }
    Ok(())
}

pub(in crate::native_mount) struct Fixture {
    pub(in crate::native_mount) root: tempfile::TempDir,
    pub(in crate::native_mount) session: NativeMountSession,
    pub(in crate::native_mount) work: NativeMaterializationWork,
    pub(in crate::native_mount) fence: PathBuf,
}

impl Fixture {
    pub(in crate::native_mount) fn new() -> Result<Self, Box<dyn std::error::Error>> {
        registry::initialize()?;
        let root = tempfile::tempdir()?;
        let destination = root.path().join("retired-mount");
        std::fs::create_dir(&destination)?;
        std::fs::write(root.path().join("authored"), b"keep")?;
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
        let mut guard = None;
        MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
        let held = guard.as_mut().ok_or("destination fence absent")?;
        let name = std::str::from_utf8(
            held.fence_name
                .as_ref()
                .ok_or("destination fence name absent")?,
        )?;
        let fence = registry::with_directory(|directory| Ok(directory.join(name)))?;
        assert!(fence.is_file(), "fixture has no physical destination fence");
        held.prepare_destination_cleanup(&destination, &work)?;
        let mut session = NativeMountSession::new(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id: VolumeId::new(),
                destination,
                writable: false,
                shared: false,
            },
            None,
            None,
        );
        session.destination_guard = guard;
        session.namespace_detach_confirmed = true;
        Ok(Self {
            root,
            session,
            work,
            fence,
        })
    }
}

pub(in crate::native_mount) fn retired(
    root: &Path,
    fence: &Path,
    before: crate::WorkCounters,
    work: &NativeMaterializationWork,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(!root.join("retired-mount").exists());
    assert!(!fence.exists(), "retired namespace kept its physical fence");
    assert_eq!(std::fs::read(root.join("authored"))?, b"keep");
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations + 4,
        "directory removal, registry lock, fence unlink and unlock use original credit"
    );
    Ok(())
}

#[test]
fn detached_driver_retirement_completes_fence_after_waiter_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let Fixture {
        root,
        mut session,
        work,
        fence,
    } = Fixture::new()?;
    let mut packet = NativeMountRetirement {
        retained_source: None,
        startup_source_watch: None,
        terminal_source_failure: None,
        destination_guard: session.destination_guard.take(),
        #[cfg(windows)]
        warm_namespace: None,
    };
    let before = work.receipt();
    work.cancellation().cancel();
    let wait = CancellationToken::new();
    wait.cancel();
    let result = packet.retire(
        Some(&wait),
        crate::native_mount::retirement::SourceRetirementWait::Caller,
    );
    assert!(
        matches!(result, Ok(true)),
        "detached driver did not finish accepted fence release: {result:?}"
    );
    assert!(packet.is_retired());
    retired(root.path(), &fence, before, &work)?;
    let completed = work.receipt();
    assert!(!packet.retire(
        Some(&wait),
        crate::native_mount::retirement::SourceRetirementWait::Caller
    )?);
    assert_eq!(work.receipt(), completed);
    Ok(())
}

#[test]
fn detached_session_retirement_completes_fence_after_waiter_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    for mode in 0..3 {
        let Fixture {
            root,
            mut session,
            work,
            fence,
        } = Fixture::new()?;
        let before = work.receipt();
        work.cancellation().cancel();
        let wait = CancellationToken::new();
        wait.cancel();
        let stop = match mode {
            0 => NativeSessionStopWork::Meter(&work),
            1 => NativeSessionStopWork::AcceptedWait(&wait),
            _ => NativeSessionStopWork::AcceptedDrop,
        };
        assert!(session.stop_with_context(&stop)?);
        assert!(session.cleanup_state().cleanup_complete());
        retired(root.path(), &fence, before, &work)?;
        let completed = work.receipt();
        assert!(!session.stop_with_context(&stop)?);
        assert_eq!(work.receipt(), completed);
    }
    Ok(())
}

#[test]
fn terminal_source_outcome_is_reported_after_its_destination_retires()
-> Result<(), Box<dyn std::error::Error>> {
    for driver_owned in [false, true] {
        let Fixture {
            root,
            mut session,
            work,
            fence,
        } = Fixture::new()?;
        // This is terminal outcome data from a real failed host operation,
        // not a replacement watch or native completion fixture.
        let error = std::fs::read(root.path().join("absent-source"))
            .expect_err("fixture source is actually absent");
        let actual_code = error.raw_os_error();
        let before = work.receipt();
        let result = if driver_owned {
            let mut packet = NativeMountRetirement {
                retained_source: None,
                startup_source_watch: None,
                terminal_source_failure: Some(NativeMountError::Io(error)),
                destination_guard: session.destination_guard.take(),
                #[cfg(windows)]
                warm_namespace: None,
            };
            let result = packet.retire(
                None,
                crate::native_mount::retirement::SourceRetirementWait::Physical,
            );
            assert!(packet.is_retired());
            assert!(packet.terminal_source_failure.is_none());
            result
        } else {
            session.terminal_source_failure = Some(NativeMountError::Io(error));
            let result = session.stop_with_context(&NativeSessionStopWork::AcceptedDrop);
            assert!(session.cleanup_state().cleanup_complete());
            assert!(session.terminal_source_failure.is_none());
            result
        };
        let Err(NativeMountError::Io(error)) = result else {
            panic!("terminal source outcome disappeared: {result:?}");
        };
        assert_eq!(error.raw_os_error(), actual_code);
        retired(root.path(), &fence, before, &work)?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[test]
fn stopped_watch_cancellation_releases_the_driver_and_session_fences()
-> Result<(), Box<dyn std::error::Error>> {
    for driver_owned in [false, true] {
        let Fixture {
            root,
            mut session,
            mut work,
            fence,
        } = Fixture::new()?;
        let watched = root.path().join("source");
        std::fs::create_dir(&watched)?;
        let wait = CancellationToken::new();
        let watch = crate::source_watch::terminal_cancellation_watch(&watched, &mut work, &wait)?;
        session.startup_source_watch =
            Some(lazy::NativeMountSourceWatchOwner::from_native_watch_fixture(watch));
        let before = work.receipt();
        let result = if driver_owned {
            let mut packet = NativeMountRetirement {
                retained_source: session.retained_source.take(),
                startup_source_watch: session.startup_source_watch.take(),
                terminal_source_failure: None,
                destination_guard: session.destination_guard.take(),
            };
            let result = packet.retire(
                Some(&wait),
                crate::native_mount::retirement::SourceRetirementWait::Caller,
            );
            assert!(
                packet.is_retired(),
                "terminal watch outcome stranded its fence"
            );
            assert!(packet.terminal_source_failure.is_none());
            result
        } else {
            let result = session.stop_with_context(&NativeSessionStopWork::AcceptedWait(&wait));
            assert!(session.cleanup_state().cleanup_complete());
            assert!(session.terminal_source_failure.is_none());
            result
        };
        assert!(wait.is_cancelled(), "actual FSEvents terminal did not run");
        assert!(
            matches!(
                result,
                Err(NativeMountError::Source(MountSourceError::Cancelled))
            ),
            "original post-terminal cancellation was replaced: {result:?}"
        );
        assert!(!fence.exists(), "stopped producer kept its physical fence");
        assert!(!root.path().join("retired-mount").exists());
        assert_eq!(std::fs::read(root.path().join("authored"))?, b"keep");
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations
        );
        assert_eq!(
            work.receipt().peak_allocation_bytes,
            before.peak_allocation_bytes
        );
    }
    Ok(())
}

#[test]
fn failed_view_startup_retains_directory_until_original_physical_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    registry::initialize()?;
    for (handoff, driver_owned) in [(true, false), (true, true), (false, true)] {
        let root = tempfile::tempdir()?;
        let admitted = crate::hold_root(root.path())?;
        let issued = IssuedDestination::create(Some(&admitted))?;
        let destination = issued.path().to_path_buf();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
        let mut prepared =
            PreparedNativeMountStart::new(&work)?.with_view_destination(Some(issued));
        let mut guard = None;
        MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
        if handoff {
            prepared.prepare_view_destination(guard.as_mut().ok_or("missing fence")?, &work)?;
        }
        let mut owner = NativeMountSession::new(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id: VolumeId::new(),
                destination: destination.clone(),
                writable: false,
                shared: false,
            },
            None,
            None,
        );
        owner.destination_guard = guard;
        owner.namespace_detach_confirmed = true;
        let mut packet = NativeMountRetirement {
            retained_source: None,
            startup_source_watch: None,
            terminal_source_failure: None,
            destination_guard: driver_owned
                .then(|| owner.destination_guard.take())
                .flatten(),
            #[cfg(windows)]
            warm_namespace: None,
        };
        let cause = std::fs::read(root.path().join("absent-startup-input"))
            .expect_err("actual missing startup input");
        let code = cause.raw_os_error();
        let mut failed = prepared.failed(cause.into(), owner);
        assert!(
            matches!(failed.error(), NativeMountError::Io(cause) if cause.raw_os_error() == code)
        );
        // The negative restores post-start ownership: returning the failure
        // drops the issuance before the driver's physical retirement.
        assert_eq!(destination.is_dir(), handoff);
        let before = work.receipt();
        work.cancellation().cancel();
        let wait = CancellationToken::new();
        wait.cancel();
        if driver_owned {
            drop(failed);
            assert_eq!(destination.is_dir(), handoff);
            packet.retire(
                Some(&wait),
                crate::native_mount::retirement::SourceRetirementWait::Physical,
            )?;
            assert!(packet.is_retired());
        } else {
            failed
                .owner_mut()
                .stop_with_context(&NativeSessionStopWork::AcceptedWait(&wait))?;
        }
        assert!(!destination.exists());
        // One accepted directory removal plus the original three fence writes.
        assert_eq!(
            work.receipt().backend_write_operations - before.backend_write_operations,
            if handoff { 4 } else { 3 }
        );
    }
    #[cfg(unix)]
    replaced_view_parent_is_not_adopted()?;
    Ok(())
}

#[cfg(unix)]
fn replaced_view_parent_is_not_adopted() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let parent = root.path().join("admitted");
    std::fs::create_dir(&parent)?;
    let admitted = crate::hold_root(&parent)?;
    let issued = IssuedDestination::create(Some(&admitted))?;
    let destination = issued.path().to_path_buf();
    let leaf = destination.file_name().ok_or("missing issued leaf")?;
    let moved = root.path().join("moved");
    std::fs::rename(&parent, &moved)?;
    std::fs::create_dir(&parent)?;
    std::fs::create_dir(&destination)?;
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
    let mut prepared = PreparedNativeMountStart::new(&work)?.with_view_destination(Some(issued));
    let result = prepared.prepare_view_destination(guard.as_mut().ok_or("missing fence")?, &work);
    assert!(matches!(result, Err(NativeMountError::InvalidDestination)));
    drop(prepared);
    assert!(
        !moved.join(leaf).exists(),
        "original issuance lost cleanup custody"
    );
    assert!(destination.is_dir(), "replacement directory was adopted");
    let mut packet = NativeMountRetirement {
        retained_source: None,
        startup_source_watch: None,
        terminal_source_failure: None,
        destination_guard: guard,
        #[cfg(windows)]
        warm_namespace: None,
    };
    packet.retire(
        None,
        crate::native_mount::retirement::SourceRetirementWait::Physical,
    )?;
    assert!(
        destination.is_dir(),
        "foreign directory was removed during retirement"
    );
    Ok(())
}
