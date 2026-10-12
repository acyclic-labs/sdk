use super::*;

const CALLBACK_POLICY: crate::WorkBudget = crate::performance::work_budget(1_000_000, 16_000_000);

#[test]
fn interrupted_barrier_can_be_retried() -> TestResult {
    let gate = CallbackGate::new();
    let before = gate.enter();
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        items_examined: 1,
        backend_read_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    assert!(gate.drain(&work(budget, &cancellation)?).is_err());
    let after = gate.enter();
    drop(before);
    assert!(gate.drain(&work(budget, &cancellation)?).is_err());
    drop(after);
    gate.drain(&work(
        crate::WorkBudget {
            items_examined: 2,
            ..budget
        },
        &cancellation,
    )?)?;
    Ok(())
}

#[test]
fn denied_clock_does_not_settle_external_changes() -> TestResult {
    let (_, context, _setup) = checkout_context(MountPublication::Manual)?;
    context.around.made.store(1, Ordering::Release);
    let cancellation = crate::CancellationToken::new();
    let denied = work(
        crate::WorkBudget {
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )?;
    assert!(context.revalidate(&denied).is_err());
    assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
    let cancelled = work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    cancellation.cancel();
    assert!(context.revalidate(&cancelled).is_err());
    assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn callback_panic_survives_context_retirement_and_stays_fatal() -> TestResult {
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
    let (failure, completed) = callback_failure(&context);
    assert_eq!(
        context.run::<()>(false, |_| panic!("fatal callback ran again")),
        Err(libc::EIO)
    );
    drop(context);
    assert!(completed.items_examined > 0);
    let NativeMountError::DriverPanicked { panic, .. } = failure else {
        panic!("lost callback panic")
    };
    assert_eq!(
        panic
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .downcast_ref::<&str>(),
        Some(&"actual native callback panic")
    );
    Ok(())
}

#[test]
fn authored_guard_denies_before_mutation_and_reports_revocation_after_it() -> TestResult {
    for writable in [false, true] {
        let (source, context, guard, original) = authored_guard_context(writable)?;
        let path = MountPath::root().child(b"guarded".to_vec());
        if writable {
            assert_eq!(
                context.run(true, |work| {
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
                }),
                Err(libc::EACCES)
            );
            assert!(source.lookup(&path)?.is_some());
            assert!(context.ledger.latest() > 0);
        } else {
            assert_eq!(getattr(&context, c"/"), 0);
            let before = original.receipt();
            assert_eq!(create(&context, c"/guarded"), (-libc::EACCES, 0));
            assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
            assert_eq!(
                original.receipt().backend_write_operations,
                before.backend_write_operations
            );
            assert!(source.lookup(&path)?.is_none());
        }
        let failure = callback_failure(&context);
        assert!(matches!(failure.0, NativeMountError::CallbackDenied { .. }));
        assert_eq!(failure.1, original.receipt());
    }
    Ok(())
}

#[test]
fn rejected_allocations_and_copies_leave_source_contents_unchanged() -> TestResult {
    for copying in [false, true] {
        let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"limited".to_vec());
        if copying {
            crate::native_mount::adapter::boundary(source.cancellation(), |work| {
                source.create_file(&path, FileMetadata::default(), None, work)
            })?;
            source.write_range(&path, 0, Bytes::from_static(b"before"))?;
        }
        let budget = if copying {
            crate::WorkBudget {
                bytes_copied: 0,
                ..CALLBACK_POLICY
            }
        } else {
            crate::WorkBudget {
                allocation_operations: 0,
                ..CALLBACK_POLICY
            }
        };
        let context = callback_context(&source, budget, &crate::CancellationToken::new())?;
        if copying {
            let incoming = b"after";
            // SAFETY: context, terminated path and input remain alive through the callback.
            assert_eq!(
                unsafe {
                    acyclic_fs_darwin_mount_write(
                        Arc::as_ptr(&context) as usize,
                        c"/limited".as_ptr(),
                        0,
                        incoming.as_ptr().cast(),
                        incoming.len(),
                        0,
                    )
                },
                -libc::EIO
            );
            assert_eq!(
                source.open_file(&path)?.read_up_to(0, 16)?.as_ref(),
                b"before"
            );
        } else {
            assert_eq!(create(&context, c"/limited"), (-libc::EIO, 0));
            assert!(source.lookup(&path)?.is_none());
        }
        let failure = callback_failure(&context);
        assert!(matches!(failure.0, NativeMountError::Work(_)));
        assert_eq!(failure.1.backend_write_operations, 0);
        if copying {
            assert_eq!(failure.1.bytes_copied, 0);
        } else {
            assert_eq!(failure.1.allocation_operations, 0);
        }
    }
    Ok(())
}

#[test]
fn absence_probes_remain_usable_until_original_cancellation() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let cancellation = crate::CancellationToken::new();
    let context = callback_context(&source, CALLBACK_POLICY, &cancellation)?;
    assert_eq!(getattr(&context, c"/later"), -libc::ENOENT);
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(
            &MountPath::root().child(b"later".to_vec()),
            FileMetadata::default(),
            None,
            work,
        )
    })?;
    assert_eq!(getattr(&context, c"/later"), 0);
    cancellation.cancel();
    assert_eq!(getattr(&context, c"/later"), -libc::ECANCELED);
    let failure = callback_failure(&context);
    assert!(matches!(
        failure.0,
        NativeMountError::Demand(crate::DemandError::Cancelled)
            | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled)
            | NativeMountError::Source(MountSourceError::Cancelled)
    ));
    assert_eq!(failure.1.backend_read_operations, 0);
    Ok(())
}

#[test]
fn accepted_io_scope_retains_original_work_until_its_last_clone_drops() -> TestResult {
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
    let original = work(CALLBACK_POLICY, &cancellation)?;
    let other = work(CALLBACK_POLICY, &cancellation)?;
    let active = NativeMountWorkScope::new(Arc::clone(&context), &original)?;
    assert!(matches!(
        NativeMountWorkScope::new(Arc::clone(&context), &other),
        Err(NativeMountError::ControlPending)
    ));
    let accepted = active.try_clone_for_io()?;
    drop(active);
    assert_eq!(getattr(&context, c"/"), 0);
    assert!(original.receipt().items_examined > 0);
    drop(accepted);
    assert_eq!(getattr(&context, c"/"), -libc::EIO);
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
    let setup = work(crate::WorkBudget::UNBOUNDED, cancellation)?;
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
    let original = work(CALLBACK_POLICY, &crate::CancellationToken::new())?;
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

fn callback_failure(context: &DarwinMountContext) -> (NativeMountError, crate::WorkCounters) {
    context
        .handler_failure
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take_failure()
        .expect("callback failed")
}

fn getattr(context: &Arc<DarwinMountContext>, path: &CStr) -> c_int {
    let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
    // SAFETY: the context and native input/output remain live through the call.
    unsafe {
        acyclic_fs_darwin_mount_getattr(
            Arc::as_ptr(context) as usize,
            path.as_ptr(),
            0,
            output.as_mut_ptr(),
        )
    }
}

fn create(context: &Arc<DarwinMountContext>, path: &CStr) -> (c_int, u64) {
    let mut handle = 0;
    // SAFETY: the context and native path/handle remain live through the call.
    let status = unsafe {
        acyclic_fs_darwin_mount_create(
            Arc::as_ptr(context) as usize,
            path.as_ptr(),
            0o644,
            context.mount_uid,
            context.mount_gid,
            libc::O_RDWR,
            &mut handle,
        )
    };
    (status, handle)
}
