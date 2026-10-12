use super::*;
use crate::demand::{DemandWork, NativeDemandReadMeter};

#[test]
fn interrupted_callback_barrier_retains_slot_and_retry_covers_new_callbacks() -> TestResult {
    let gate = CallbackGate::new();
    let before = gate.enter();
    let cancellation = crate::CancellationToken::new();
    let denied = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            items_examined: 1,
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(matches!(
        gate.drain(&denied),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    let after = gate.enter();
    drop(before);
    let retry = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            items_examined: 1,
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(matches!(
        gate.drain(&retry),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    drop(after);
    let exact = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            items_examined: 2,
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    gate.drain(&exact)?;
    assert!(
        gate.draining
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    );
    Ok(())
}

#[test]
fn denied_actual_expiry_clock_never_marks_external_changes_settled() -> TestResult {
    let (_, context, _setup) = checkout_context(MountPublication::Manual)?;
    context.around.made.store(1, Ordering::Release);
    let cancellation = crate::CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let result = context.revalidate(&original);
    assert!(matches!(
        result,
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert_eq!(
        original.receipt().backend_read_operations,
        0,
        "denied clock was not queried"
    );
    assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
    assert_eq!(context.around.made.load(Ordering::Acquire), 1);
    assert!(
        context
            .callbacks
            .draining
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    );
    let cancelled =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let cancelled_prefix = cancelled.receipt();
    cancellation.cancel();
    assert!(matches!(
        context.revalidate(&cancelled),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Cancelled
        ))
    ));
    assert_eq!(cancelled.receipt(), cancelled_prefix);
    assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
    Ok(())
}

const CALLBACK_POLICY: crate::WorkBudget = crate::performance::work_budget(1_000_000, 16_000_000);

#[test]
fn actual_callback_panic_and_receipt_survive_physical_context_retirement() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let cancellation = crate::CancellationToken::new();
    let context = callback_context(&source, CALLBACK_POLICY, &cancellation)?;
    assert_eq!(
        context.run::<()>(false, |work| {
            work.charge(scan_work(1))?;
            panic!("actual native callback panic");
        }),
        Err(libc::EIO)
    );
    let (failure, completed) = context
        .handler_failure
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take_failure()
        .ok_or("missing actual panic")?;
    assert_eq!(
        context.run(false, |_| -> Result<(), NativeMountError> {
            panic!("a failed callback cannot run again");
        }),
        Err(libc::EIO)
    );
    assert!(
        context
            .handler_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_failure()
            .is_none()
    );
    drop(context);
    assert!(completed.items_examined > 0);
    match failure {
        NativeMountError::DriverPanicked { panic, .. } => {
            let payload = panic.into_inner().unwrap_or_else(PoisonError::into_inner);
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"actual native callback panic")
            );
        }
        other => panic!("actual panic payload was replaced: {other:?}"),
    }
    Ok(())
}

fn callback_context(
    source: &Arc<MemorySource>,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<Arc<DarwinMountContext>, Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root_id = source
        .lookup(&MountPath::root())?
        .ok_or("root absent")?
        .node
        .file_id;
    let metadata = std::fs::metadata(".")?;
    let setup = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut context = DarwinMountContext::new(
        Arc::clone(source) as Arc<dyn MountFilesystem>,
        true,
        metadata.uid(),
        metadata.gid(),
        root_id,
        &setup,
    )?;
    context.handler_budget = budget;
    context.handler_cancellation = cancellation.clone();
    Ok(Arc::new(context))
}

struct DarwinAuthoredGuard {
    original: crate::NativeMaterializationWork,
    writable: AtomicBool,
}

impl crate::native_mount::NativeMountCallbackGuard for DarwinAuthoredGuard {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        if !self.original.shares_ledger(&work) {
            return Err(NativeMountError::ControlPending);
        }
        crate::native_mount::NativeMountCallbackFuture::new(work, || async move {
            work.admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        })
    }

    fn check_write<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        if !self.original.shares_ledger(&work) {
            return Err(NativeMountError::ControlPending);
        }
        crate::native_mount::NativeMountCallbackFuture::new_guarded(work, move || async move {
            if self.writable.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(std::io::Error::from_raw_os_error(libc::EACCES))
            }
        })
    }
}

fn authored_guard_context(
    writable: bool,
) -> Result<
    (
        Arc<MemorySource>,
        Arc<DarwinMountContext>,
        Arc<DarwinAuthoredGuard>,
        crate::NativeMaterializationWork,
    ),
    Box<dyn std::error::Error>,
> {
    let (shared, config) = checkout(MountPublication::Manual)?;
    let ordinary = Arc::new(MemorySource::new(Arc::clone(&shared), config)?);
    let root_id = ordinary
        .lookup(&MountPath::root())?
        .ok_or("root absent")?
        .node
        .file_id;
    let original =
        crate::NativeMaterializationWork::new(CALLBACK_POLICY, &crate::CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
    let guard = Arc::new(DarwinAuthoredGuard {
        original: original.clone(),
        writable: AtomicBool::new(writable),
    });
    let source = MemorySource::new_at_meter(
        shared,
        config,
        crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
        original.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
        &original,
        Some(&original),
    )?
    .with_native_callback_guard(guard.clone());
    let source = source.into_arc_meter(&original)?;
    let context = Arc::new(DarwinMountContext::new(
        source, true, 0, 0, root_id, &original,
    )?);
    Ok((ordinary, context, guard, original))
}

#[test]
fn raw_authored_write_denies_read_guard_before_payload_or_source_effect() -> TestResult {
    let (source, context, _guard, original) = authored_guard_context(false)?;
    let root = c"/";
    let mut attributes = std::mem::MaybeUninit::<NativeStat>::uninit();
    let address = Arc::as_ptr(&context) as usize;
    // SAFETY: real context and native output/path storage remain alive.
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_getattr(address, root.as_ptr(), 0, attributes.as_mut_ptr())
        },
        0
    );
    let path = c"/denied-by-role";
    let mut handle = 0;
    let before = original.receipt();
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_create(
                address,
                path.as_ptr(),
                0o644,
                0,
                0,
                libc::O_RDWR,
                &mut handle,
            )
        },
        -libc::EACCES
    );
    assert_eq!(handle, 0);
    let after = original.receipt();
    assert_eq!(
        after.items_examined, before.items_examined,
        "no authored path/provider frame is examined"
    );
    assert_eq!(
        after.bytes_copied, before.bytes_copied,
        "no authored payload is copied"
    );
    assert_eq!(
        after.backend_write_operations,
        before.backend_write_operations
    );
    assert!(
        source
            .lookup(&MountPath::root().child(b"denied-by-role".to_vec()))?
            .is_none()
    );
    assert_eq!(context.ledger.latest(), 0);
    let failure = context
        .handler_failure
        .lock()
        .map_err(|_| "poisoned original failure")?
        .take_failure()
        .ok_or("missing original typed guard denial")?;
    assert!(matches!(failure.0, NativeMountError::CallbackDenied { .. }));
    assert_eq!(
        failure.1, after,
        "failure remains on the installed original work"
    );
    Ok(())
}

#[test]
fn authored_post_effect_revocation_retains_actual_mutation_and_typed_cause() -> TestResult {
    let (source, context, guard, original) = authored_guard_context(true)?;
    let path = MountPath::root().child(b"already-created".to_vec());
    let result = context.run(true, |work| {
        context.mutate(
            || {
                context.source.create_file(
                    &path,
                    create_metadata(0o644, mode::IFREG, 0, 0),
                    None,
                    work,
                )
            },
            work,
        )?;
        guard.writable.store(false, Ordering::Release);
        Ok(())
    });
    assert_eq!(
        result,
        Err(libc::EACCES),
        "post-effect denial prevents successful SDK publication"
    );
    assert!(
        source.lookup(&path)?.is_some(),
        "real partial effect is not rolled back or hidden"
    );
    assert!(
        context.ledger.latest() > 0,
        "actual mutation remains covered by the original publication ledger"
    );
    let failure = context
        .handler_failure
        .lock()
        .map_err(|_| "poisoned original failure")?
        .take_failure()
        .ok_or("missing post-effect typed cause")?;
    assert!(matches!(failure.0, NativeMountError::CallbackDenied { .. }));
    assert_eq!(failure.1, original.receipt());
    assert_eq!(
        context
            .handler_failure
            .lock()
            .map_err(|_| "poisoned completed work")?
            .completed_future_request_work,
        crate::WorkCounters::UNCHARGED
    );
    Ok(())
}

#[test]
fn raw_create_denies_actual_path_allocation_before_source_mutation() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let cancellation = crate::CancellationToken::new();
    let context = callback_context(
        &source,
        crate::WorkBudget {
            allocation_operations: 0,
            ..CALLBACK_POLICY
        },
        &cancellation,
    )?;
    let path = CString::new("/not-created")?;
    let mut handle = 0;
    // SAFETY: all native argument storage remains valid through the call.
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_create(
                Arc::as_ptr(&context) as usize,
                path.as_ptr(),
                0o644,
                context.mount_uid,
                context.mount_gid,
                libc::O_RDWR,
                &mut handle,
            )
        },
        -libc::EIO
    );
    let failure = context
        .handler_failure
        .lock()
        .map_err(|_| "poisoned callback failure")?
        .take_failure()
        .ok_or("missing allocation denial")?;
    assert!(matches!(failure.0, NativeMountError::Work(_)));
    assert_eq!(failure.1.allocation_operations, 0);
    assert_eq!(failure.1.backend_write_operations, 0);
    assert_eq!(handle, 0);
    assert!(
        source
            .lookup(&MountPath::root().child(b"not-created".to_vec()))?
            .is_none()
    );
    Ok(())
}

#[test]
fn raw_getattr_uses_original_cancellation_and_does_not_poison_absence_probes() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let cancellation = crate::CancellationToken::new();
    let context = callback_context(&source, CALLBACK_POLICY, &cancellation)?;
    let path = CString::new("/later")?;
    let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
    let address = Arc::as_ptr(&context) as usize;
    // SAFETY: native storage and context are valid throughout each call.
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_getattr(address, path.as_ptr(), 0, output.as_mut_ptr()) },
        -libc::ENOENT
    );
    let miss = context
        .handler_failure
        .lock()
        .map_err(|_| "poisoned callback failure")?
        .completed_future_request_work;
    assert!(miss.items_examined > 0);
    assert!(
        context
            .handler_failure
            .lock()
            .map_err(|_| "poisoned callback failure")?
            .first_fatal
            .is_none()
    );
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(
            &MountPath::root().child(b"later".to_vec()),
            FileMetadata::default(),
            None,
            work,
        )
    })?;
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_getattr(address, path.as_ptr(), 0, output.as_mut_ptr()) },
        0
    );
    cancellation.cancel();
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_getattr(address, path.as_ptr(), 0, output.as_mut_ptr()) },
        -libc::ECANCELED
    );
    let failure = context
        .handler_failure
        .lock()
        .map_err(|_| "poisoned callback failure")?
        .take_failure()
        .ok_or("missing original cancellation")?;
    assert!(matches!(
        failure.0,
        NativeMountError::Demand(crate::DemandError::Cancelled)
            | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled)
            | NativeMountError::Source(MountSourceError::Cancelled)
    ));
    assert_eq!(failure.1.backend_read_operations, 0);
    assert!(
        context
            .handler_failure
            .lock()
            .map_err(|_| "poisoned callback failure")?
            .completed_future_request_work
            .items_examined
            > miss.items_examined
    );
    Ok(())
}

#[test]
fn owned_original_scope_rejects_other_meter_and_retains_real_sdk_callbacks_until_io_fork_drops()
-> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let cancellation = crate::CancellationToken::new();
    let context = callback_context(
        &source,
        crate::WorkBudget {
            items_examined: 0,
            ..CALLBACK_POLICY
        },
        &cancellation,
    )?;
    let original = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(
        CALLBACK_POLICY,
        &cancellation,
    ))));
    let other = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(
        CALLBACK_POLICY,
        &cancellation,
    ))));
    let original = crate::NativeMaterializationWork::from_meter(original);
    let other = crate::NativeMaterializationWork::from_meter(other);
    let active = DarwinStartupMeterScope::new(Arc::clone(&context), &original)?;
    assert!(matches!(
        DarwinStartupMeterScope::new(Arc::clone(&context), &other),
        Err(NativeMountError::ControlPending)
    ));
    assert_eq!(
        other.native_read_meter().lock().work,
        crate::WorkCounters::UNCHARGED
    );
    let accepted_io_scope = active.try_clone_for_io()?;
    drop(active);
    assert!(
        context
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade()
            .is_some_and(|meter| Arc::ptr_eq(
                meter.native_read_meter(),
                original.native_read_meter()
            ))
    );
    let root = CString::new("/")?;
    let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
    // SAFETY: the actual retained context and output remain alive for this SDK callback.
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_getattr(
                Arc::as_ptr(&context) as usize,
                root.as_ptr(),
                0,
                output.as_mut_ptr(),
            )
        },
        0
    );
    assert!(original.native_read_meter().lock().work.items_examined > 0);
    assert_eq!(
        context
            .handler_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work,
        crate::WorkCounters::UNCHARGED
    );
    drop(accepted_io_scope);
    assert!(
        context
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade()
            .is_none()
    );
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_getattr(
                Arc::as_ptr(&context) as usize,
                root.as_ptr(),
                0,
                output.as_mut_ptr(),
            )
        },
        -libc::EIO
    );
    Ok(())
}
#[test]
fn raw_write_denies_the_actual_incoming_copy_without_touching_existing_contents() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"original".to_vec());
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&path, FileMetadata::default(), None, work)
    })?;
    source.write_range(&path, 0, Bytes::from_static(b"before"))?;
    let cancellation = crate::CancellationToken::new();
    let context = callback_context(
        &source,
        crate::WorkBudget {
            bytes_copied: 0,
            ..CALLBACK_POLICY
        },
        &cancellation,
    )?;
    let native_path = CString::new("/original")?;
    let incoming = b"after";
    // SAFETY: context, terminated path and all readable input bytes remain alive.
    let status = unsafe {
        acyclic_fs_darwin_mount_write(
            Arc::as_ptr(&context) as usize,
            native_path.as_ptr(),
            0,
            incoming.as_ptr().cast(),
            incoming.len(),
            0,
        )
    };
    assert_eq!(status, -libc::EIO);
    let failure = context
        .handler_failure
        .lock()
        .map_err(|_| "poisoned callback failure")?
        .take_failure()
        .ok_or("missing actual copy denial")?;
    assert!(matches!(
        failure.0,
        NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        })
    ));
    assert_eq!(failure.1.bytes_copied, 0);
    assert_eq!(failure.1.backend_write_operations, 0);
    assert_eq!(
        source.open_file(&path)?.read_up_to(0, 16)?.as_ref(),
        b"before"
    );
    Ok(())
}
