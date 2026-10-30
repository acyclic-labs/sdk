use super::*;

#[cfg(feature = "test-support")]
#[test]
fn retirement_keeps_callback_storage_until_terminal_completion() {
    unsafe extern "C" {
        fn acyclic_fs_darwin_test_retirement_custody() -> c_int;
    }
    // SAFETY: the model has no arguments or host operations; it exercises the
    // production C adapter over every bounded callback ordering and dissent.
    assert_eq!(unsafe { acyclic_fs_darwin_test_retirement_custody() }, 0);
}

#[test]
fn retirement_uses_one_fallback_only_after_permission_denial() -> TestResult {
    for direct in [0, libc::EPERM, libc::EIO, libc::ENOENT] {
        for fallback in [0, libc::EACCES, libc::EIO] {
            let mut calls = 0;
            let result = super::super::reclaim::retire_mount(c"/fixture", |_, _| {
                calls += 1;
                let error = if calls == 1 { direct } else { fallback };
                if error == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::from_raw_os_error(error))
                }
            });
            assert_eq!(calls, if direct == libc::EPERM { 2 } else { 1 });
            let expected = if direct == libc::EPERM {
                fallback
            } else {
                direct
            };
            assert_eq!(
                result.err().and_then(|error| error.raw_os_error()),
                (expected != 0).then_some(expected)
            );
        }
    }
    Ok(())
}

#[test]
fn accepted_retirement_wakes_its_owner_after_the_caller_leaves() -> TestResult {
    for notify in [false, true] {
        let temporary = tempfile::tempdir()?;
        let original = work(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?;
        let owner = sdk(temporary.path(), &original)?;
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let task = acyclic_native_runtime::run_blocking_io(
            move || {
                blocked.recv().map_err(std::io::Error::other)?;
                Ok(0)
            },
            Some((original.runtime_meter(), true)),
        )?;
        let credit = original.reserve_work(WorkCounters::UNCHARGED)?;
        owner.state().control.direct = Some(DirectDetach {
            task,
            started: false,
            credit: original
                .reserve_work(WorkCounters::UNCHARGED)?
                .into_runtime_credit()?,
            wake: if notify {
                Arc::downgrade(&owner)
            } else {
                std::sync::Weak::new()
            },
        });
        owner
            .state()
            .control
            .poll_direct(&ProbeWork::Cleanup(&credit, None))?;
        let (finished, completion) = std::sync::mpsc::channel();
        let retiring = Arc::clone(&owner);
        let thread = std::thread::spawn(move || {
            retiring.finish();
            let _ = finished.send(());
        });
        // Taking the state lock observes finish's pending result before its
        // condition-variable wait. No mount or caller-owned poll drives it.
        drop(
            owner
                .changed
                .wait_while(owner.state(), |state| state.failure.is_empty())
                .unwrap_or_else(PoisonError::into_inner),
        );
        release.send(())?;
        if !notify {
            // Negative control: the old noop waker completes the actual native
            // operation but leaves the provider and its resources asleep.
            let task = owner
                .state()
                .control
                .direct
                .take()
                .ok_or("retained detach completion missing")?;
            task.retire()?;
            assert!(completion.try_recv().is_err());
            assert_ne!(
                owner.pointer(),
                0,
                "old shape retains the SDK after completion"
            );
            owner.retirement_wake.notify();
            owner.changed.notify_all();
        }
        completion.recv()?;
        thread.join().map_err(|_| "retirement owner panicked")?;
        assert_eq!(owner.pointer(), 0);
        assert!(owner.state().resources.is_none());
    }
    Ok(())
}

/// Drives the real finalizer after it proves this fixture has no mounted
/// namespace, then hands it the original source's remaining physical join.
pub(in crate::native_mount) fn retire_source_in_physical_loop(
    source: Arc<dyn MountFilesystem>,
) -> Result<bool, NativeMountError> {
    let (_, context, _) = checkout_context(MountPublication::Manual)
        .map_err(|cause| NativeMountError::Driver(cause.to_string()))?;
    let original = work(
        crate::performance::BOUNDARY_BUDGET,
        &crate::CancellationToken::new(),
    )?;
    let destination = tempfile::tempdir()?;
    let resources =
        DriverSessionResources::new(Arc::new(context), destination.path(), &original, false)?;
    {
        let mut state = resources.state();
        resources.retire(
            &mut state,
            None,
            crate::native_mount::retirement::SourceRetirementWait::Physical,
        )?;
        assert!(state.resources.is_none());
        assert!(state.control.namespace_detach_confirmed());
        state.retirement = Some(crate::native_mount::NativeMountRetirement {
            retained_source: Some(source),
            destination_guard: None,
            startup_source_watch: None,
            terminal_source_failure: None,
        });
    }
    resources.finish();
    let mut state = resources.state();
    if let Some(error) = state.failure.take() {
        return Err(error);
    }
    Ok(state.resources.is_none()
        && state
            .retirement
            .as_ref()
            .is_some_and(crate::native_mount::NativeMountRetirement::is_retired))
}

#[test]
fn recovery_probe_rejects_every_changed_kernel_mount_identity() -> TestResult {
    let mount = super::super::reclaim::mount_table()?
        .first()
        .copied()
        .ok_or("empty kernel mount table")?;
    let original = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let probe = MountProbe::for_mount(&mount, &original)?;
    probe.verify_mount(&mount)?;
    let mut changed = mount;
    changed.f_owner = changed.f_owner.wrapping_add(1);
    assert!(probe.verify_mount(&changed).is_err());
    changed = mount;
    changed.f_mntonname[0] ^= 1;
    assert!(probe.verify_mount(&changed).is_err());
    changed = mount;
    changed.f_mntfromname[0] ^= 1;
    assert!(probe.verify_mount(&changed).is_err());
    changed = mount;
    // SAFETY: Darwin's fsid_t is two initialized int32_t values.
    let mut identity = unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(mount.f_fsid) };
    identity[0] ^= 1;
    // SAFETY: the changed values retain the same fsid_t representation.
    changed.f_fsid = unsafe { std::mem::transmute::<[i32; 2], libc::fsid_t>(identity) };
    assert!(probe.verify_mount(&changed).is_err());
    Ok(())
}

fn sdk(
    path: &Path,
    work: &Work,
) -> Result<Arc<DriverSessionResources>, Box<dyn std::error::Error>> {
    let (_, context, _) = checkout_context(MountPublication::Manual)?;
    let resources = DriverSessionResources::new(Arc::new(context), path, work, false)?;
    resources.birth(work)?;
    Ok(resources)
}

#[test]
fn recovery_control_publishes_only_actual_namespace_absence_evidence() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let mut control = DarwinRecoveryControl::default();
    assert!(!control.namespace_detach_confirmed());
    let denied = work(
        crate::WorkBudget {
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )?;
    assert!(matches!(
        control.recover(temporary.path(), &denied),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(!control.namespace_detach_confirmed());
    let exact = work(
        crate::WorkBudget {
            backend_read_operations: 5,
            backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )?;
    control.recover(temporary.path(), &exact)?;
    assert!(control.namespace_detach_confirmed());
    assert!(!control.controls_pending());
    Ok(())
}

#[test]
fn actual_sdk_prebirth_interrupt_returns_without_creating_a_namespace() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let work = work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let baseline = work.live_allocation_bytes();
    let resources = sdk(temporary.path(), &work)?;
    resources.interrupt(None)?;
    let (context, destination, pointers) = resources
        .state
        .lock()
        .map_err(|_| "poisoned SDK loop state")?
        .resources
        .as_ref()
        .ok_or("provider resources missing")?
        .clone();
    work.charge(WRITE_WORK)?;
    let mut interrupted = 0;
    let argc = c_int::try_from(
        pointers
            .iter()
            .position(|pointer| *pointer == 0)
            .unwrap_or(pointers.len()),
    )?;
    // SAFETY: all SDK inputs and the callback owner remain live through the loop.
    let status = unsafe {
        acyclic_fs_darwin_mount_run(
            resources.pointer() as *mut c_void,
            argc,
            pointers.as_ptr().cast(),
            destination.as_ptr(),
            Arc::as_ptr(&context) as usize,
            report_mounted,
            Arc::as_ptr(&resources).cast_mut().cast(),
            &mut interrupted,
        )
    };
    assert_eq!(status, 0);
    assert_eq!(interrupted, 1);
    assert!(
        !resources
            .state
            .lock()
            .map_err(|_| "poisoned SDK loop state")?
            .mounted
    );
    let probe = MountProbe::new(temporary.path(), &work)?;
    assert!(!probe.mounted(temporary.path(), &ProbeWork::Live(&work))?);
    drop(probe);
    cancellation.cancel();
    let stopped = crate::CancellationToken::new();
    stopped.cancel();
    assert!(resources.free(Some(&stopped)).is_err());
    assert_ne!(resources.pointer(), 0);
    resources.free(Some(&crate::CancellationToken::new()))?;
    assert_eq!(resources.pointer(), 0);
    drop(context);
    drop(destination);
    drop(resources);
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(cancellation.is_cancelled());
    Ok(())
}

#[test]
fn pending_recovery_control_retires_after_its_original_invocation_is_cancelled() -> TestResult {
    let cancellation = crate::CancellationToken::new();
    let original = work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let credit = original
        .reserve_work(WorkCounters::UNCHARGED)?
        .into_runtime_credit()?;
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = credit.clone();
    let task = acyclic_native_runtime::run_blocking_io(
        move || -> std::io::Result<c_int> {
            blocked.recv().map_err(std::io::Error::other)?;
            Ok(0)
        },
        Some((meter, true)),
    )?;
    let mut control = DarwinRecoveryControl {
        direct: Some(DirectDetach {
            task,
            credit: Arc::clone(&credit),
            started: false,
            wake: std::sync::Weak::new(),
        }),
        ..Default::default()
    };
    control.poll_direct(&ProbeWork::Cleanup(&credit, None))?;
    assert!(
        control.controls_pending(),
        "the submitted operation remains owned"
    );
    cancellation.cancel();
    release.send(())?;
    control.retire_controls(&original)?;
    assert!(!control.controls_pending());
    assert!(cancellation.is_cancelled());
    assert!(!control.namespace_detach_confirmed());
    Ok(())
}

#[test]
fn cancelled_detach_retains_completion_and_reports_a_dropped_producer() -> TestResult {
    for dropped in [false, true] {
        let cancellation = crate::CancellationToken::new();
        let original = work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
        let credit = original
            .reserve_work(WorkCounters::UNCHARGED)?
            .into_runtime_credit()?;
        let (entered, started) = std::sync::mpsc::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let task = acyclic_native_runtime::run_blocking_io(
            move || -> std::io::Result<c_int> {
                entered.send(()).map_err(std::io::Error::other)?;
                blocked.recv().map_err(std::io::Error::other)?;
                Ok(0)
            },
            Some((original.runtime_meter(), true)),
        )?;
        let mut control = DarwinRecoveryControl {
            direct: Some(DirectDetach {
                task,
                credit,
                started: false,
                wake: std::sync::Weak::new(),
            }),
            ..Default::default()
        };
        control.poll_direct(&ProbeWork::Live(&original))?;
        started.recv()?;
        let caller_work = original.clone();
        let caller = std::thread::spawn(move || {
            let result = control.wait_direct(&ProbeWork::Live(&caller_work));
            (control, result)
        });
        cancellation.cancel();
        let (mut control, result) = caller.join().map_err(|_| "detach caller panicked")?;
        assert_eq!(
            callback_errno(&result.expect_err("caller was cancelled")),
            libc::ECANCELED
        );
        assert!(control.controls_pending());
        if !dropped {
            release.send(())?;
        }
        drop(release);
        let result = control.retire_controls(&original);
        assert_eq!(result.is_err(), dropped);
        assert!(!control.controls_pending());
    }
    Ok(())
}

#[test]
fn pending_detach_can_finish_with_its_actual_panic_without_a_wait_token() -> TestResult {
    let original = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let task = acyclic_native_runtime::run_blocking_io(
        move || -> std::io::Result<c_int> {
            let _ = blocked.recv();
            panic!("actual detach panic")
        },
        Some((original.runtime_meter(), true)),
    )?;
    let mut detach = DirectDetach {
        task,
        started: false,
        wake: std::sync::Weak::new(),
        credit: original
            .reserve_work(WorkCounters::UNCHARGED)?
            .into_runtime_credit()?,
    };
    let credit = original.reserve_work(WorkCounters {
        backend_read_operations: 1,
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })?;
    assert!(matches!(
        detach.poll(&ProbeWork::Cleanup(&credit, None))?,
        std::task::Poll::Pending
    ));
    drop(release);
    let error = detach.retire().expect_err("original panic");
    let NativeMountError::DriverPanicked { panic, .. } = error else {
        panic!("lost actual detach panic")
    };
    assert_eq!(
        panic
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .downcast_ref::<&str>(),
        Some(&"actual detach panic")
    );
    Ok(())
}

#[test]
fn stop_before_provider_birth_releases_the_session() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let work = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let resources = sdk(temporary.path(), &work)?;
    let mut session = DarwinMountSession {
        handler_failure: Arc::clone(&resources.context()?.handler_failure),
        unmount: resources,
        thread: None,
        loop_finished_before_teardown: None,
        loop_result: None,
        native_root_identity: None,
    };
    assert!(
        session
            .stop(&crate::native_mount::NativeSessionStopWork::Meter(&work))?
            .is_empty()
    );
    assert!(
        session
            .stop(&crate::native_mount::NativeSessionStopWork::Meter(&work))?
            .is_empty()
    );
    assert!(session.unmount.context().is_err());
    assert!(session.namespace_detach_confirmed());
    let provider = Arc::clone(&session.unmount);
    session.thread = Some(std::thread::spawn(move || {
        let _exit = DriverExit(&provider);
        7
    }));
    session
        .unmount
        .provider_exited
        .store(true, Ordering::Release);
    session.loop_finished_before_teardown = None;
    let mut outcome = session.stop(&crate::native_mount::NativeSessionStopWork::Meter(&work))?;
    assert!(matches!(
        outcome.take(),
        Some(NativeMountError::DarwinTeardown(DarwinTeardownFailure(7)))
    ));
    assert!(outcome.is_empty());
    assert!(
        session
            .stop(&crate::native_mount::NativeSessionStopWork::Meter(&work))?
            .is_empty()
    );
    assert!(session.thread.is_none());
    Ok(())
}

#[test]
fn provider_unwind_publishes_shutdown_before_joining_its_original_panic() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let original = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let resources = sdk(temporary.path(), &original)?;
    let provider = Arc::clone(&resources);
    let (entered, started) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let mut session = DarwinMountSession {
        handler_failure: Arc::clone(&resources.context()?.handler_failure),
        unmount: resources,
        thread: Some(std::thread::spawn(move || {
            let _exit = DriverExit(&provider);
            let _ = entered.send(());
            let _ = blocked.recv();
            std::panic::resume_unwind(Box::new("provider disappeared"));
        })),
        loop_finished_before_teardown: None,
        loop_result: None,
        native_root_identity: None,
    };
    started.recv()?;
    drop(release);
    let mut failures = session.stop(&crate::native_mount::NativeSessionStopWork::Meter(
        &original,
    ))?;
    let Some(NativeMountError::DriverPanicked { panic, .. }) = failures.take() else {
        return Err("provider panic was not retained".into());
    };
    assert_eq!(
        panic
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .downcast_ref::<&str>(),
        Some(&"provider disappeared")
    );
    assert!(session.thread.is_none());
    Ok(())
}

#[test]
fn physical_retirement_finishes_with_its_original_terminal_source_outcome() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let original = work(
        crate::performance::BOUNDARY_BUDGET,
        &crate::CancellationToken::new(),
    )?;
    let resources = sdk(temporary.path(), &original)?;
    {
        let mut state = resources.state();
        resources.retire(
            &mut state,
            None,
            crate::native_mount::retirement::SourceRetirementWait::Physical,
        )?;
        assert!(state.resources.is_none());
        let error = std::fs::read(temporary.path().join("absent-source"))
            .expect_err("fixture source is actually absent");
        state.retirement = Some(crate::native_mount::NativeMountRetirement {
            retained_source: None,
            destination_guard: None,
            startup_source_watch: None,
            terminal_source_failure: Some(NativeMountError::Io(error)),
        });
    }
    resources.finish();
    let mut state = resources.state();
    assert!(state.resources.is_none());
    assert!(
        state
            .retirement
            .as_ref()
            .is_some_and(crate::native_mount::NativeMountRetirement::is_retired)
    );
    assert!(matches!(
        state.failure.take(),
        Some(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    Ok(())
}
