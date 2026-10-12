use super::*;

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
    acyclic_native_runtime::initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let work = work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let (_, context, _setup) = checkout_context(MountPublication::Manual)?;
    let startup = StartupBuffers::new(temporary.path(), true, false, &work)?;
    let allocation = work.claim(
        std::mem::size_of::<DriverSessionResources>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let credit = work.reserve_work(crate::WorkCounters {
        backend_read_operations: 1,
        backend_write_operations: 2,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let (pointer, sdk_allocation) = DriverSessionResources::new_sdk_session(&work)?;
    let resources = Arc::new(DriverSessionResources {
        context: Arc::new(context),
        native: Mutex::new((pointer, Some(sdk_allocation), credit)),
        startup,
        _allocation: allocation,
    });
    resources.interrupt(None)?;
    let events = DarwinStopController::new(&resources, temporary.path(), &work)?;
    work.charge(WRITE_WORK)?;
    let mut interrupted = 0;
    // SAFETY: all SDK inputs, context, events and output stay owned through the loop.
    let status = unsafe {
        acyclic_fs_darwin_mount_run(
            pointer as *mut c_void,
            resources.startup.argument_count(),
            resources.startup.pointers.as_ptr().cast::<*const c_char>(),
            resources.startup.destination.as_ptr(),
            Arc::as_ptr(&resources.context) as usize,
            report_mounted,
            Arc::as_ptr(&events).cast_mut().cast(),
            &mut interrupted,
        )
    };
    assert_eq!(status, 0);
    assert_eq!(interrupted, 1);
    assert!(!events.mounted.load(Ordering::Acquire));
    let mut probe = MountProbe::new(temporary.path(), &work)?;
    assert!(!probe.mounted(temporary.path(), &ProbeWork::Live(&work))?);
    drop(probe);
    cancellation.cancel();
    drop(resources);
    drop(events);
    assert_eq!(work.live_allocation_bytes(), 0);
    assert!(cancellation.is_cancelled());
    Ok(())
}

#[test]
fn sdk_controls_keep_birth_credit_and_require_a_live_wait() -> TestResult {
    let original_cancel = crate::CancellationToken::new();
    let work = work(
        crate::WorkBudget {
            backend_read_operations: 1,
            backend_write_operations: 3,
            ..crate::WorkBudget::UNBOUNDED
        },
        &original_cancel,
    )?;
    let credit = work.reserve_work(crate::WorkCounters {
        backend_read_operations: 1,
        backend_write_operations: 2,
        ..crate::WorkCounters::UNCHARGED
    })?;
    assert_eq!(work.receipt().backend_write_operations, 0);
    let (_, context, _setup) = checkout_context(MountPublication::Manual)?;
    let temporary = tempfile::tempdir()?;
    let startup = StartupBuffers::new(temporary.path(), true, false, &work)?;
    let allocation = work.claim(
        std::mem::size_of::<DriverSessionResources>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let (pointer, sdk_allocation) = DriverSessionResources::new_sdk_session(&work)?;
    let resources = Arc::new(DriverSessionResources {
        context: Arc::new(context),
        native: Mutex::new((pointer, Some(sdk_allocation), credit)),
        startup,
        _allocation: allocation,
    });
    original_cancel.cancel();
    let wait = crate::CancellationToken::new();
    resources.interrupt(Some(&wait))?;
    resources.interrupt(Some(&wait))?;
    resources.start_join(&wait)?;
    let stopped = crate::CancellationToken::new();
    stopped.cancel();
    assert!(matches!(
        resources.free(Some(&stopped)),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Cancelled
        ))
    ));
    assert_eq!(resources.pointer(), pointer);
    resources.free(Some(&wait))?;
    assert_eq!(resources.pointer(), 0);
    assert!(work.cancellation().is_cancelled());
    Ok(())
}

#[test]
fn pending_detach_remains_pollable_without_a_wait_token() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let original = work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let task = acyclic_native_runtime::run_blocking_io(
        move || {
            let _ = blocked.recv_timeout(Duration::from_secs(1));
            Ok(0)
        },
        Some((original.runtime_meter(), true)),
    )?;
    let mut detach = DirectDetach {
        task,
        failure: None,
        original: original.allocation_work(),
    };
    let mut credit = original.reserve_work(crate::WorkCounters {
        backend_read_operations: 1,
        items_examined: 1,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let loan = credit.loan(None);
    assert!(matches!(
        detach.poll(&ProbeWork::Cleanup(&loan))?,
        std::task::Poll::Pending
    ));
    drop(release);
    Ok(())
}
