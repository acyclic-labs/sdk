use super::super::RENAME_NOREPLACE;
use super::*;

fn preserved(source: &MemorySource, path: &MountPath) -> Result<bool, MountSourceError> {
    Ok(source
        .lookup(path)?
        .is_some_and(|entry| entry.node.logical_bytes == 8)
        && source.read_range(path, 0, 8)?.as_ref() == b"original")
}

fn operation(
    original: &MemoryCheckout,
    rename: bool,
    allowance: Option<u64>,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let setup = fuse_fixture_work();
    let cancellation = crate::CancellationToken::new();
    let checkout = original.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&setup, &cancellation),
        &cancellation,
    )?;
    let source = Arc::new(
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                posix_config(),
            )?,
            vec![],
            None,
        )?,
    );
    let path = name("retained");
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &setup)?
        .attr
        .ino
        .0;
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &setup)?;
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            allocation_operations: allowance.unwrap_or(u64::MAX),
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    );
    let work = match work {
        Ok(work) => work,
        Err((error, _)) => {
            assert!(matches!(error,
                crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                    counter, maximum, ..
                }) if Some(maximum) == allowance && counter == "allocation_operations"
            ));
            assert!(
                preserved(source.as_ref(), &path)?,
                "ledger admission changed the namespace"
            );
            return Ok(None);
        }
    };
    let result = if rename {
        projection.rename_name(
            ROOT_INODE,
            std::ffi::OsStr::new("replacement"),
            ROOT_INODE,
            std::ffi::OsStr::new("retained"),
            0,
            &work,
        )
    } else {
        projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("retained"), &work)
    };
    if let Err(error) = result {
        assert!(
            preserved(source.as_ref(), &path)?,
            "namespace changed before a refused completion: allowance {allowance:?}: {error:?}"
        );
        let refusal = crate::workspace::workspace_bounded_error(&error);
        // A completion reservation reduces the ceiling available to nested work.
        // Its effective ceiling cannot exceed the caller's original ceiling.
        assert!(
            matches!(refusal, Some(crate::WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter, maximum, observed,
            })) if allowance.is_some_and(|allowance| maximum <= allowance) && observed > maximum && counter == "allocation_operations"),
            "allowance {allowance:?}: {error:?}; source {:?}",
            std::error::Error::source(&error)
        );
        Ok(None)
    } else {
        let file = projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .file_handle(inode, handle)
            .map_err(NativeMountError::from)?
            .open_file
            .clone();
        assert_eq!(file.file().read_range(0, 8)?.as_ref(), b"original");
        Ok(Some(work.receipt().allocation_operations))
    }
}

// Measure the actual finite operation first, then exercise every allocation
// refusal prefix against replicas of that same tree. A fresh volume has fresh
// file identities and may require a different number of trie allocations.
#[test]
fn allocation_refusal_precedes_unlink_and_replacement() -> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
    let checkout = memory_checkout(
        posix_config(),
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        },
        1,
    )?
    .pop()
    .ok_or("one checkout")?;
    let original = Arc::new(crate::native_mount::SharedCheckout::new(checkout));
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new(Arc::clone(&original), posix_config())?,
        vec![],
        None,
    )?;
    let setup = fuse_fixture_work();
    let path = name("retained");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    source.write_range(&path, 0, Bytes::from_static(b"original"))?;
    source.create_file(&name("replacement"), FileMetadata::default(), None, &setup)?;
    let cancellation = crate::CancellationToken::new();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let original = runtime.block_on(async {
        original.lock().await.private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(&setup, &cancellation),
            &cancellation,
        )
    })?;
    for rename in [false, true] {
        let required =
            operation(&original, rename, None)?.ok_or("unbounded fixture was refused")?;
        for allowance in 0..required {
            operation(&original, rename, Some(allowance))?;
        }
        assert!(operation(&original, rename, Some(required))?.is_some());
    }
    Ok(())
}

fn completion_retains_credit_after_source_acceptance(
    rename: bool,
    copy_credit: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    #[derive(Default)]
    struct CompletionPressure {
        accepted: AtomicBool,
        held: parking_lot::Mutex<Option<crate::NativeWorkReservation>>,
        copy_credit: bool,
    }
    impl ViewObserver for CompletionPressure {
        fn view_changed(&self, _: ViewStamp, _: crate::native_mount::ViewOrigin) {
            self.accepted.store(true, Ordering::Release);
        }
    }
    impl crate::native_mount::NativeMountCallbackGuard for CompletionPressure {
        fn check<'a>(
            &'a self,
            work: &'a crate::NativeMaterializationWork,
        ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
            crate::native_mount::NativeMountCallbackFuture::new(work, || async { Ok(()) })
        }

        fn check_write<'a>(
            &'a self,
            work: &'a crate::NativeMaterializationWork,
        ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
            crate::native_mount::NativeMountCallbackFuture::new(work, || async {
                if self.accepted.load(Ordering::Acquire) {
                    let mut held = self.held.lock();
                    if held.is_none() {
                        // The real source has adopted and returned to its policy
                        // fence. Another accepted operation takes all free credit.
                        let available = {
                            let state = work.state.lock();
                            if self.copy_credit {
                                super::super::Copy
                                    .delta(state.budget.bytes_copied - state.work.bytes_copied)
                            } else {
                                super::super::Items
                                    .delta(state.budget.items_examined - state.work.items_examined)
                            }
                        };
                        *held = Some(work.reserve_work(available)?);
                    }
                }
                Ok(())
            })
        }
    }
    let config = posix_config();
    let checkout = memory_checkout(
        config,
        crate::model::CheckoutMode::tracking_transaction(),
        1,
    )?
    .pop()
    .ok_or("one checkout")?;
    let original = crate::NativeMaterializationWork::new(
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let pressure = Arc::new(CompletionPressure {
        copy_credit,
        ..CompletionPressure::default()
    });
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new_at_meter(
            Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
            config,
            crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            original.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            &original,
            Some(&original),
        )?,
        vec![],
        None,
    )?
    .with_native_callback_guard(pressure.clone())
    .into_arc_meter(&original)?;
    source.create_file(&name("before"), FileMetadata::default(), None, &original)?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            FINITE_HANDLER_POLICY,
            original.cancellation(),
        )?,
    };
    let retained = if rename {
        None
    } else {
        source.write_range(&name("before"), 0, Bytes::from_static(b"original"))?;
        let inode = projection
            .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("before"), &original)?
            .attr
            .ino
            .0;
        let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &original)?;
        if copy_credit {
            projection.link_name(
                inode,
                ROOT_INODE,
                std::ffi::OsStr::new("survivor"),
                &original,
            )?;
        }
        Some((inode, handle))
    };
    let observer: Arc<dyn ViewObserver> = pressure.clone();
    source.observe_view(Arc::downgrade(&observer))?;
    let result = if rename {
        projection.rename_name(
            ROOT_INODE,
            std::ffi::OsStr::new("before"),
            ROOT_INODE,
            std::ffi::OsStr::new("after"),
            RENAME_NOREPLACE,
            &original,
        )
    } else {
        projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("before"), &original)
    };
    assert!(pressure.accepted.load(Ordering::Acquire));
    assert!(pressure.held.lock().is_some());
    let (credit, expected) = if copy_credit {
        (super::super::Copy, "bytes_copied")
    } else {
        (super::super::Items, "items_examined")
    };
    assert!(matches!(original.reserve_work(credit.delta(1)),
        Err(crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
            counter, ..
        })) if counter == expected
    ));
    drop(pressure.held.lock().take());
    assert!(source.lookup(&name("before"))?.is_none());
    if let Some((inode, handle)) = retained {
        let file = projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .file_handle(inode, handle)
            .map_err(NativeMountError::from)?
            .open_file
            .clone();
        assert_eq!(file.file().read_range(0, 8)?.as_ref(), b"original");
        if copy_credit {
            assert_eq!(
                source.read_range(&name("survivor"), 0, 8)?.as_ref(),
                b"original"
            );
        }
    } else {
        assert!(source.lookup(&name("after"))?.is_some());
    }
    assert!(
        result.is_ok(),
        "{} refused after its source was accepted: {result:?}",
        if rename { "rename" } else { "unlink" }
    );
    if copy_credit && let Some((inode, _)) = retained {
        let state = projection.core.state().map_err(NativeMountError::from)?;
        let entry = state.by_inode.get(&inode).ok_or("original inode lost")?;
        assert_eq!(entry.bindings.len(), 1);
        assert_eq!(*entry.bindings[0].path, name("survivor"));
    }
    original.admit(crate::WorkCounters::UNCHARGED)?;
    Ok(())
}

#[test]
fn rename_completion_retains_credit_after_source_acceptance()
-> Result<(), Box<dyn std::error::Error>> {
    completion_retains_credit_after_source_acceptance(true, false)
}

#[test]
fn unlink_completion_retains_credit_after_source_acceptance()
-> Result<(), Box<dyn std::error::Error>> {
    completion_retains_credit_after_source_acceptance(false, false)
}

#[test]
fn unlink_completion_retains_copy_credit_after_source_acceptance()
-> Result<(), Box<dyn std::error::Error>> {
    completion_retains_credit_after_source_acceptance(false, true)
}

#[test]
fn post_unlink_allocation_negative_control() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let setup = fuse_fixture_work();
    let path = name("retained");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    source.write_range(&path, 0, Bytes::from_static(b"original"))?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &setup)?
        .attr
        .ino
        .0;
    projection.open_node(inode, libc::O_RDONLY, &setup)?;
    let detached = super::super::FuseOpenFile(super::super::fuse_open_file(
        source.detach_file_with_meter(&path, &setup)?,
        &setup,
    )?);
    source.remove(&path, None, &setup)?;
    let refused = fuse_fixture_work();
    // The ledger itself is already funded. Admit no further allocations.
    {
        let mut state = refused.state.lock();
        state.budget.allocation_operations = state.work.allocation_operations;
    }
    assert!(
        projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .prepare_detached_handles(inode, &detached, &refused)
            .is_err()
    );
    assert!(
        !preserved(source.as_ref(), &path)?,
        "the old ordering must violate namespace preservation on refusal"
    );
    Ok(())
}

fn rename_and_forget(resurrect: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    source.create_file(&name("before"), FileMetadata::default(), None, &work)?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("before"), &work)?
        .attr
        .ino
        .0;
    let mut state = projection.core.state().map_err(NativeMountError::from)?;
    let mut install = state.prepare_rename_prefix(
        &name("before"),
        &name("after"),
        &name("after"),
        ROOT_INODE,
        &work,
    )?;
    state.release_lookup_reference(inode, 1);
    drop(state);
    let stamp = projection.core.source.rename(
        &name("before"),
        &name("after"),
        false,
        &work,
        Some(&mut install),
    )?;
    let mut state = projection.core.state().map_err(NativeMountError::from)?;
    install.install(&mut state, stamp);
    if resurrect {
        state
            .inode_by_path
            .insert(super::super::fuse_clone_path(&name("after"), &work)?, inode);
    }
    assert!(
        state
            .inode_by_path
            .values()
            .all(|inode| state.by_inode.contains_key(inode)),
        "retired inode was resurrected"
    );
    Ok(())
}

#[test]
fn prepared_rename_cannot_resurrect_a_concurrent_forget() -> Result<(), Box<dyn std::error::Error>>
{
    rename_and_forget(false)
}

#[test]
#[should_panic(expected = "retired inode was resurrected")]
fn forgotten_inode_negative_control() {
    rename_and_forget(true).unwrap();
}
