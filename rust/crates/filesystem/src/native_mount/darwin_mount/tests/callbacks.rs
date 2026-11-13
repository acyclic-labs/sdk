use super::*;

const CALLBACK_POLICY: crate::WorkBudget = crate::performance::work_budget(1_000_000, 16_000_000);

#[test]
fn namespace_failure_keeps_its_darwin_failure_and_work_prefix() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let context = callback_context(&source, CALLBACK_POLICY, &crate::CancellationToken::new())?;
    assert_eq!(
        context.run::<()>(false, |work| {
            work.charge(scan_work(1))?;
            Err(NativeMountError::Source(MountSourceError::NamespacePath(
                crate::kernel::NamespacePathError::Cancelled,
            )))
        }),
        Err(libc::ECANCELED)
    );
    assert_eq!(
        context.run::<()>(false, |_| panic!("terminal callback ran again")),
        Err(libc::ECANCELED)
    );
    let (cause, prefix) = callback_failure(&context);
    assert!(matches!(
        cause,
        NativeMountError::Source(MountSourceError::NamespacePath(
            crate::kernel::NamespacePathError::Cancelled
        ))
    ));
    assert!(prefix.items_examined > 0);
    Ok(())
}

#[test]
fn failed_completion_keeps_its_darwin_failure_and_work_prefix() -> TestResult {
    let (source, _, _setup) = checkout_context(MountPublication::Manual)?;
    let context = callback_context(&source, CALLBACK_POLICY, &crate::CancellationToken::new())?;
    assert_eq!(
        context.run::<()>(false, |work| {
            work.charge(scan_work(1))?;
            Err(NativeMountError::Source(
                MountSourceError::MutationFinalization {
                    primary: Box::new(Some(MountSourceError::NotFound)),
                    cleanup: Box::new(Some(crate::DemandError::Cancelled)),
                    custody: None,
                },
            ))
        }),
        Err(libc::ENOENT)
    );
    assert_eq!(
        context.run::<()>(false, |_| panic!("terminal callback ran again")),
        Err(libc::ENOENT)
    );
    let (cause, prefix) = callback_failure(&context);
    assert!(matches!(
        cause,
        NativeMountError::Source(MountSourceError::MutationFinalization { .. })
    ));
    assert!(prefix.items_examined > 0);
    Ok(())
}

#[test]
fn interrupted_barrier_can_be_retried() -> TestResult {
    let gate = CallbackGate::new();
    let before = gate.enter();
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        backend_read_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    assert!(gate.drain(&work(budget, &cancellation)?).is_err());
    let after = gate.enter();
    drop(before);
    assert!(gate.drain(&work(budget, &cancellation)?).is_err());
    drop(after);
    gate.drain(&work(budget, &cancellation)?)?;
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
        let (context, guard, original) = authored_guard_context(writable)?;
        let path = MountPath::root().child(b"guarded".to_vec());
        if writable {
            assert_eq!(
                context.run(true, |work| {
                    context.mutate(
                        || {
                            context.source.create_file(
                                &path,
                                create_metadata(
                                    &crate::native_mount::clock::Clock::capture(),
                                    0o644,
                                    u32::from(libc::S_IFREG),
                                    0,
                                    0,
                                ),
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
        } else {
            assert_eq!(getattr(&context, c"/"), 0);
            assert_eq!(create(&context, c"/guarded"), (-libc::EACCES, 0));
        }
        let failure = callback_failure(&context);
        assert!(matches!(failure.0, NativeMountError::CallbackDenied { .. }));
        assert_eq!(failure.1, original.receipt());
        assert_eq!(
            context
                .source
                .lookup_with_meter(&path, &original)?
                .is_some(),
            writable
        );
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
        let target = super::super::requests::CallbackTarget::View(Arc::clone(&context));
        if copying {
            let incoming = b"after";
            assert_eq!(
                // SAFETY: context, terminated path and input remain alive through the callback.
                unsafe {
                    acyclic_fs_darwin_mount_write(
                        &target as *const super::super::requests::CallbackTarget as usize,
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
        assert!(matches!(
            failure.0,
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(
                crate::WorkError::BudgetExceeded { counter, .. }
            )) if counter == if copying { "bytes_copied" } else { "allocation_operations" }
        ));
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
    let active = NativeMountWorkScope::new(&context, &original)?;
    assert!(matches!(
        NativeMountWorkScope::new(&context, &other),
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
    let setup = work(crate::WorkBudget::UNBOUNDED, cancellation)?;
    let mut context = test_context(source, &setup)?;
    context.handler_budget = budget;
    context.handler_cancellation = cancellation.clone();
    Ok(Arc::new(context))
}

struct DarwinAuthoredGuard {
    original: Work,
    writable: AtomicBool,
}

impl crate::native_mount::NativeMountCallbackGuard for DarwinAuthoredGuard {
    fn check<'a>(
        &'a self,
        work: &'a Work,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        if !self.original.shares_ledger(work) {
            return Err(NativeMountError::ControlPending);
        }
        crate::native_mount::NativeMountCallbackFuture::new(work, || async move {
            work.admit(WorkCounters::UNCHARGED)?;
            Ok(())
        })
    }

    fn check_write<'a>(
        &'a self,
        work: &'a Work,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        if !self.original.shares_ledger(work) {
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

#[allow(
    clippy::type_complexity,
    reason = "The fixture returns its context, authorization guard and shared work directly."
)]
fn authored_guard_context(
    writable: bool,
) -> Result<(Arc<DarwinMountContext>, Arc<DarwinAuthoredGuard>, Work), Box<dyn std::error::Error>> {
    crate::native_mount::initialize_native_mount_services()?;
    let (shared, config) = checkout(MountPublication::Manual)?;
    let original = work(CALLBACK_POLICY, &crate::CancellationToken::new())?;
    let guard = Arc::new(DarwinAuthoredGuard {
        original: original.clone(),
        writable: AtomicBool::new(writable),
    });
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new_at_meter(
            shared,
            config,
            crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            original.claim(0, 0, WorkCounters::UNCHARGED)?,
            &original,
            Some(&original),
        )?,
        vec![],
        None,
    )?
    .with_native_callback_guard(guard.clone());
    let source = source.into_arc_meter(&original)?;
    let context = Arc::new(DarwinMountContext::new(
        source,
        true,
        Path::new("."),
        &original,
    )?);
    Ok((context, guard, original))
}

fn callback_failure(context: &DarwinMountContext) -> (NativeMountError, WorkCounters) {
    context
        .handler_failure
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take_failure()
        .expect("callback failed")
}

fn getattr(context: &Arc<DarwinMountContext>, path: &CStr) -> c_int {
    let target = super::super::requests::CallbackTarget::View(Arc::clone(context));
    let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
    // SAFETY: the context and native input/output remain live through the call.
    unsafe {
        acyclic_fs_darwin_mount_getattr(
            &target as *const super::super::requests::CallbackTarget as usize,
            path.as_ptr(),
            0,
            output.as_mut_ptr(),
        )
    }
}

fn create(context: &Arc<DarwinMountContext>, path: &CStr) -> (c_int, u64) {
    let target = super::super::requests::CallbackTarget::View(Arc::clone(context));
    let mut handle = 0;
    // SAFETY: the context and native path/handle remain live through the call.
    let status = unsafe {
        acyclic_fs_darwin_mount_create(
            &target as *const super::super::requests::CallbackTarget as usize,
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

fn fsync_keeps_its_open_handle(bypass_handle: bool) -> TestResult {
    use super::super::file_ops::{acyclic_fs_darwin_mount_fsync, acyclic_fs_darwin_mount_release};

    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"sync-owner".to_vec());
    context.mutate(
        || source.create_file(&path, FileMetadata::default(), None, &work),
        &work,
    )?;
    let handle = context.open(
        &path,
        crate::native_mount::MountOpenAccess::ReadWrite,
        0,
        &work,
    )?;
    let target = super::super::requests::CallbackTarget::View(Arc::new(context));
    let address = &target as *const super::super::requests::CallbackTarget as usize;
    let name = CString::new("/sync-owner")?;
    // SAFETY: the context and terminated path remain live through this call.
    assert_eq!(
        unsafe {
            super::super::namespace_ops::acyclic_fs_darwin_mount_remove(address, name.as_ptr(), 0)
        },
        0
    );
    // SAFETY: this live context owns the handle. A retained open handle needs
    // no path, including when the kernel has already unlinked its name.
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_fsync(address, std::ptr::null(), handle) },
        0
    );
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_release(address, std::ptr::null(), handle) },
        0
    );
    let result = if bypass_handle {
        // The old bridge erased the handle and synced the context directly.
        super::super::ffi(address, true, |context, work| {
            context.sync(work)?;
            Ok(0)
        })
    } else {
        unsafe { acyclic_fs_darwin_mount_fsync(address, std::ptr::null(), handle) }
    };
    assert_eq!(result, -libc::ESTALE, "fsync accepted a released handle");
    Ok(())
}

#[test]
fn fsync_rejects_a_released_handle_without_requiring_a_path() -> TestResult {
    fsync_keeps_its_open_handle(false)
}

#[test]
#[should_panic(expected = "fsync accepted a released handle")]
fn fsync_negative_control_detects_erased_handle() {
    fsync_keeps_its_open_handle(true).expect("real callback fixture");
}

fn view_ids_do_not_alias(substitute_receivers_handle: bool) -> TestResult {
    use super::super::file_ops::acyclic_fs_darwin_mount_fsync;

    let (source, first, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"view-identity".to_vec());
    first.mutate(
        || source.create_file(&path, FileMetadata::default(), None, &work),
        &work,
    )?;
    // The same source deliberately supplies identical FileIds to both views,
    // as two forks do. The provider must still issue distinct native ids.
    let second = Arc::new(test_context(&source, &work)?);
    let file_id = first.lookup(&path, &work)?.node.file_id;
    let first_inode = first.inode(file_id, &work)?;
    let second_inode = second.inode(file_id, &work)?;
    assert_ne!(first_inode, second_inode);
    assert_eq!(first.inode(file_id, &work)?, first_inode);
    assert_ne!(
        first.attributes(&MountPath::root(), 0, &work)?.inode,
        second.attributes(&MountPath::root(), 0, &work)?.inode
    );
    let open = |context: &DarwinMountContext| {
        context.open(
            &path,
            crate::native_mount::MountOpenAccess::ReadWrite,
            0,
            &work,
        )
    };
    let first_handle = open(&first)?;
    let second_handle = open(&second)?;
    let target = super::super::requests::CallbackTarget::View(Arc::clone(&second));
    assert_ne!(first_handle, second_handle);
    let foreign = if substitute_receivers_handle {
        second_handle
    } else {
        first_handle
    };
    // SAFETY: the receiving context remains live. No path is required for a
    // handle lookup, and this deliberately foreign id must never find a file.
    let status = unsafe {
        acyclic_fs_darwin_mount_fsync(
            &target as *const super::super::requests::CallbackTarget as usize,
            std::ptr::null(),
            foreign,
        )
    };
    assert_eq!(
        status,
        -libc::ESTALE,
        "foreign handle reached the receiving view"
    );
    // A foreign-handle refusal does not poison the receiving view.
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_fsync(
                &target as *const super::super::requests::CallbackTarget as usize,
                std::ptr::null(),
                second_handle,
            )
        },
        0
    );
    drop(first);
    let replacement = test_context(&source, &work)?;
    assert_ne!(replacement.inode(file_id, &work)?, first_inode);
    assert_ne!(open(&replacement)?, first_handle);
    Ok(())
}

#[test]
fn native_view_identities_do_not_alias_or_reuse_retired_ids() -> TestResult {
    view_ids_do_not_alias(false)
}

#[test]
#[should_panic(expected = "foreign handle reached the receiving view")]
fn native_view_identity_negative_control_detects_handle_substitution() {
    view_ids_do_not_alias(true).expect("real native view identity fixture");
}
