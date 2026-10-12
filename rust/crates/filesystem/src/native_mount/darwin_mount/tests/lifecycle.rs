use super::*;

#[test]
fn recovery_control_publishes_only_actual_namespace_absence_evidence() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let mut control = DarwinRecoveryControl::default();
    assert!(!control.namespace_detach_confirmed());
    let denied = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(matches!(
        control.recover(temporary.path(), &denied),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(!control.namespace_detach_confirmed());
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);
    let exact = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 5,
            backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
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
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
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
    let events = Arc::new(LoopEvents {
        _allocation: Some(work.claim(
            std::mem::size_of::<LoopEvents>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?),
        ..LoopEvents::default()
    });
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
    assert!(!events.current().mounted);
    let mut probe = MountProbe::new(temporary.path(), &work)?;
    assert!(!probe.mounted(
        temporary.path(),
        &ProbeWork {
            original: &work,
            cleanup: None
        }
    )?);
    drop(probe);
    cancellation.cancel();
    drop(resources);
    drop(events);
    assert_eq!(work.live_allocation_bytes(), 0);
    assert!(cancellation.is_cancelled());
    Ok(())
}

#[test]
fn actual_sdk_controls_use_birth_credit_only_after_owner_quiescence_and_wait_admission()
-> TestResult {
    let original_cancel = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 1,
            backend_write_operations: 3,
            ..crate::WorkBudget::UNBOUNDED
        },
        &original_cancel,
    )
    .map_err(|(cause, _)| cause)?;
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
    let mut resources = Arc::new(DriverSessionResources {
        context: Arc::new(context),
        native: Mutex::new((pointer, Some(sdk_allocation), credit)),
        startup,
        _allocation: allocation,
    });
    let owner = Arc::clone(&resources);
    let (release, waiting) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::spawn(move || {
        let _ = waiting.recv();
        drop(owner);
    });
    original_cancel.cancel();
    assert!(Arc::get_mut(&mut resources).is_none());
    let wait = crate::CancellationToken::new();
    resources.interrupt(Some(&wait))?;
    resources.interrupt(Some(&wait))?;
    release.send(())?;
    resources.start_join(&wait)?;
    thread
        .join()
        .map_err(|_| "resource-owner fixture panicked")?;
    let stopped = crate::CancellationToken::new();
    stopped.cancel();
    let retained = Arc::get_mut(&mut resources).ok_or("owner did not quiesce")?;
    assert!(matches!(
        retained.free(Some(&stopped)),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Cancelled
        ))
    ));
    assert_eq!(retained.pointer(), pointer);
    retained.free(Some(&wait))?;
    assert_eq!(retained.pointer(), 0);
    assert!(work.cancellation().is_cancelled());
    Ok(())
}
