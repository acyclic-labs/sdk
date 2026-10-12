#[test]
fn native_source_intrinsic_guards_follow_self_and_cancellation_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    let borrowed = source(FilesystemProfile::Portable)?;
    assert!(borrowed.native_original_work()?.is_none());
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting =
        crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let (root, allocation) = NamespacePath::parse_in_profile_with_accounting(
        "/branch",
        config.profile,
        config.limits,
        &mut accounting,
        &cancellation,
    )?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation
    else {
        panic!("native namespace must retain its original guard");
    };
    assert_eq!(allocation.bytes(), root.retained_bytes());
    let native = original.clone();
    let source = CheckoutMountSource::new_at_meter(
        Arc::clone(&borrowed.checkout),
        config,
        root,
        allocation,
        &native,
        Some(&original),
    )?;
    let observed_cancellation = source.cancellation.clone();
    let cancellation_bytes = observed_cancellation.logical_owned_bytes();
    let source = source.into_arc_meter(&native)?;
    let before_binding = original.receipt();
    let bound = source
        .native_original_work()?
        .expect("native source retains its actual header");
    let bound = bound.clone();
    assert!(bound.shares_ledger(&original));
    assert!(bound.shares_resource_allowance(&original));
    assert_eq!(original.receipt(), before_binding);
    drop(bound);
    observed_cancellation.cancel();
    let before_revoked_binding = original.receipt();
    assert!(matches!(
        source.native_original_work(),
        Err(MountSourceError::Stale)
    ));
    assert_eq!(original.receipt(), before_revoked_binding);
    let weak = Arc::downgrade(&source);
    assert!(original.live_allocation_bytes() > baseline + cancellation_bytes);
    drop(source);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + cancellation_bytes
    );
    drop(observed_cancellation);
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(weak);
    Ok(())
}

#[test]
fn source_cleanup_cases() -> Result<(), Box<dyn std::error::Error>> {
    let borrowed = source(FilesystemProfile::Portable)?;
    for stopped_wait in [false, true] {
        let root_stop = CancellationToken::new();
        let root =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &root_stop)
                .map_err(|(cause, _)| cause)?;
        let original = root.native_volume_work(WorkBudget {
            items_examined: 2,
            ..crate::performance::BOUNDARY_BUDGET
        })?;
        let source = native_source_io_control(&borrowed, &original)?;
        let wait = CancellationToken::new();
        if stopped_wait {
            wait.cancel();
            let before = root.receipt();
            assert!(matches!(
                source.shutdown_with_cleanup_wait(&wait),
                Err(MountSourceError::Cancelled)
            ));
            assert_eq!(root.receipt(), before);
            assert!(source.native_original_work()?.is_some());
        }
        root_stop.cancel();
        assert!(
            original
                .reserve_work(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })
                .is_err()
        );
        let healthy_wait = CancellationToken::new();
        assert!(source.shutdown_with_cleanup_wait(&healthy_wait)?);
        assert!(source.capture_retired());
        let completed = root.receipt();
        healthy_wait.cancel();
        assert!(!source.shutdown_with_cleanup_wait(&healthy_wait)?);
        assert_eq!(root.receipt(), completed);
    }
    Ok(())
}

fn native_source_io_control(
    borrowed: &MemorySource,
    original: &crate::NativeMaterializationWork,
) -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
    Ok(CheckoutMountSource::new_at_meter(
        Arc::clone(&borrowed.checkout),
        VolumeConfig::portable(Lifecycle::Ephemeral),
        NamespacePath::new(Vec::new(), borrowed.limits)?,
        original.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
        original,
        Some(original),
    )?
    .into_arc_meter(original)?)
}

#[test]
fn source_io_preserves_volume_limits_and_invocation_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::{NativeFile, NativeFileMeter};
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let fixture = tempfile::tempdir()?;
    let path = fixture.path().join("borrowed-payload");
    std::fs::write(&path, b"original")?;
    for cancelled in [false, true] {
        let root_stop = CancellationToken::new();
        let root =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &root_stop)
                .map_err(|(cause, _)| cause)?;
        let volume = root.native_volume_work(WorkBudget {
            backend_read_operations: 1,
            ..crate::performance::BOUNDARY_BUDGET
        })?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let (incoming, local_stop) = root.invocation_view()?;
        let job = incoming.with_source_io_lease(source.native_source_io_lease()?)?;
        assert!(job.shares_ledger(&root));
        assert!(job.shares_resource_allowance(&volume));
        let meter = job.runtime_meter();
        let file = NativeFile::from_file(
            std::fs::File::open(&path)?,
            Some(NativeFileMeter::Owned(meter.clone())),
        )
        .map_err(|(error, _)| error)?;
        if cancelled {
            local_stop.cancel();
            let prefix = root.receipt();
            let error = file
                .read_bounded_async(0, 8, meter, false)
                .err()
                .ok_or("cancelled read admitted")?;
            assert!(matches!(
                job.runtime_error(error),
                crate::NativeTreeMaterializationError::Cancelled
            ));
            assert_eq!(root.receipt(), prefix);
            root.cancellation().check()?;
        } else {
            let before = root.receipt();
            let first = borrowed.runtime.block_on(
                || {
                    file.read_bounded_async(0, 8, meter.clone(), false)
                        .expect("read submission")
                },
                Some(meter.as_ref()),
            )??;
            assert_eq!(first.bytes.as_ref(), b"original");
            assert_eq!(volume.remaining()?.backend_read_operations, 0);
            assert!(root.remaining()?.backend_read_operations > 0);
            let prefix = root.receipt();
            assert_eq!(prefix.source_bytes_read, before.source_bytes_read + 8);
            let second = file
                .read_bounded_async(0, 8, meter.clone(), false)
                .and_then(|completion| {
                    borrowed
                        .runtime
                        .block_on(|| completion, Some(meter.as_ref()))?
                });
            let error = second.err().ok_or("volume ceiling ignored")?;
            assert!(matches!(
                job.runtime_error(error),
                crate::NativeTreeMaterializationError::Work(_)
            ));
            assert_eq!(
                (
                    root.receipt().backend_read_operations,
                    root.receipt().source_bytes_read
                ),
                (prefix.backend_read_operations, prefix.source_bytes_read),
            );
            assert!(!local_stop.is_cancelled());
        }
        assert!(!root_stop.is_cancelled());
        assert!(!source.cancellation.is_cancelled());
        drop(file);
        drop(job);
        drop(incoming);
        source.shutdown_accepted_cleanup()?;
    }
    Ok(())
}

#[test]
fn native_source_io_detached_data_and_cleanup_do_not_pin_accepted_job()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::{NativeFile as NativeRuntimeFile, ReadAllocation};
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let fixture = tempfile::tempdir()?;
    let path = fixture.path().join("borrowed-payload");
    std::fs::write(&path, b"original")?;
    let file = std::fs::File::open(&path)?;
    let root_stop = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &root_stop)
            .map_err(|(cause, _)| cause)?;
    let volume = root.native_volume_work(WorkBudget {
        backend_read_operations: 1,
        ..crate::performance::BOUNDARY_BUDGET
    })?;
    let source = native_source_io_control(&borrowed, &volume)?;
    let weak_source = Arc::downgrade(&source);
    let lease = source.native_source_io_lease()?;
    let last_lease = lease.clone();
    let (incoming, local_stop) = root.invocation_view()?;
    let job = incoming.with_source_io_lease(lease)?;
    let meter = job.runtime_meter();
    let data_guard = ReadAllocation::new(&meter, 8)?;
    let data = Box::new(*b"retained");
    let demand = crate::demand::NativeSourceWork::from_native_materialization_work(&job)
        .original_demand_work()
        .allocation_work();
    assert!(demand.source_execution_view().is_none());
    let credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(job.reserve_work(
        crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        },
    )?)?;
    let original_work = credit.original_work().clone();
    assert!(original_work.source_execution_view().is_none());
    assert!(original_work.shares_ledger(&root));
    assert!(original_work.shares_resource_allowance(&volume));
    let native_file = NativeRuntimeFile::from_file(
        file,
        Some(acyclic_native_runtime::NativeFileMeter::Owned(
            meter.clone(),
        )),
    )
    .map_err(|(error, file)| {
        drop(file);
        error
    })?;
    let result = borrowed.runtime.block_on(|| {
        native_file
            .read_bounded_async(0, 8, meter.clone(), false)
            .expect("actual read submission")
    })?;
    assert_eq!(result.bytes.as_ref(), b"original");
    drop(native_file);
    drop(meter);
    drop(job);
    drop(incoming);
    drop(source);
    assert!(
        weak_source.upgrade().is_some(),
        "the last real lease still owns the Source"
    );
    drop(last_lease);
    assert!(
        weak_source.upgrade().is_none(),
        "DATA, ReadAllocation, cleanup credit, original_work and demand keeper are not operative jobs"
    );
    assert_eq!(*data, *b"retained");
    assert_eq!(result.bytes.as_ref(), b"original");
    assert_eq!(
        demand
            .with_original_leaf(|work| work.work)?
            .backend_read_operations,
        1
    );
    assert!(credit.remaining_work().items_examined > 0);
    assert!(!local_stop.is_cancelled());
    assert_eq!(root.receipt().source_bytes_read, 8);
    // Retire physical payloads before their custody guards.
    drop(data);
    drop(data_guard);
    drop(result);
    drop(credit);
    drop(original_work);
    drop(demand);
    Ok(())
}

#[test]
fn native_source_io_projection_rejects_foreign_root_and_resource_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let cancellation = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let volume = root.native_volume_work(crate::performance::BOUNDARY_BUDGET)?;
    let source = native_source_io_control(&borrowed, &volume)?;
    let lease = source.native_source_io_lease()?;
    let foreign_root =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let unrelated_volume = root.native_volume_work(crate::performance::BOUNDARY_BUDGET)?;
    for incoming in [&foreign_root, &unrelated_volume] {
        let prefix = root.receipt();
        let foreign_prefix = foreign_root.receipt();
        assert!(matches!(
            incoming.with_source_io_lease(lease.clone()),
            Err(crate::NativeTreeMaterializationError::InvalidLayout)
        ));
        assert_eq!(
            root.receipt(),
            prefix,
            "Root/resource rejection precedes view allocation"
        );
        assert_eq!(foreign_root.receipt(), foreign_prefix);
    }
    drop(lease);
    source.shutdown_accepted_cleanup()?;
    Ok(())
}

#[test]
fn native_source_io_last_physical_lease_retires_after_poll_bank_exhaustion()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let root_stop = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &root_stop)
            .map_err(|(cause, _)| cause)?;
    let original = root.native_volume_work(WorkBudget {
        items_examined: 3,
        ..crate::performance::BOUNDARY_BUDGET
    })?;
    let source = native_source_io_control(&borrowed, &original)?;
    let weak_source = Arc::downgrade(&source);
    let lease = source.native_source_io_lease()?;
    let last_lease = lease.clone();
    assert_eq!(
        original.remaining()?.items_examined,
        0,
        "constructor two and accepted producer birth poll one are the entire original credit"
    );
    root_stop.cancel();
    assert!(matches!(
        source.shutdown_accepted_cleanup(),
        Err(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(!source.capture_retired());
    let closing_prefix = root.receipt();
    assert!(matches!(
        source.native_source_io_lease(),
        Err(MountSourceError::Stale)
    ));
    assert_eq!(
        root.receipt(),
        closing_prefix,
        "closing rejects new lease before allocation"
    );
    drop(lease);
    drop(source);
    let retained_source = weak_source
        .upgrade()
        .expect("last inner Arc physically retains Source");
    assert!(!retained_source.capture_retired());
    drop(last_lease);
    assert!(
        retained_source.shutdown_accepted_cleanup()?,
        "the originally dedicated final predicate completes without fresh poll credit"
    );
    assert!(retained_source.capture_retired());
    assert!(!retained_source.shutdown_accepted_cleanup()?);
    drop(retained_source);
    assert!(weak_source.upgrade().is_none());
    Ok(())
}

#[test]
fn native_source_qualified_guard_keeps_primary_and_actual_accounting_failure()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug)]
    enum GuardFailure {
        RootPublicationRejected(u64),
        Source(MountSourceError),
    }
    impl From<MountSourceError> for GuardFailure {
        fn from(cause: MountSourceError) -> Self {
            Self::Source(cause)
        }
    }

    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let workspace_id = crate::WorkspaceId::from_volume_id(borrowed.volume_id()?);
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let context = crate::lazy_workspace::NativeSnapshotContext::Authored {
        workspace_id,
        config,
    };
    let path = native_test_path("qualified-guard-original");
    let created = boundary(borrowed.cancellation(), |work| {
        borrowed.create_file(&path, metadata(), None, work)
    })?;
    let file = borrowed.open_created(&path, &created)?;
    let reached = Arc::new(AtomicBool::new(false));
    let observed_prefix = Arc::new(StdMutex::new(None));
    let root_stop = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &root_stop)
            .map_err(|(cause, _)| cause)?;
    let volume = root.native_volume_work(crate::performance::BOUNDARY_BUDGET)?;
    let source = native_source_io_control(&borrowed, &volume)?;
    let (incoming, local_stop) = root.invocation_view()?;
    let job = incoming.with_source_io_lease(source.native_source_io_lease()?)?;
    let native = job.clone();
    file.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"native publication prefix"),
            0,
        ),
        &native,
    )?;
    let before = root.receipt();
    let guard_stop = local_stop.clone();
    let guard_reached = Arc::clone(&reached);
    let guard_prefix = Arc::clone(&observed_prefix);
    let expected_meter = Arc::clone(root.native_read_meter());
    let mut before_publication = move |original: &crate::NativeMaterializationWork| {
        assert!(Arc::ptr_eq(original.native_read_meter(), &expected_meter));
        assert!(original.cancellation().same_state(&guard_stop));
        let stop = guard_stop.clone();
        let reached = Arc::clone(&guard_reached);
        let prefix = Arc::clone(&guard_prefix);
        let original = original.allocation_work();
        async move {
            reached.store(true, Ordering::Release);
            *prefix.lock().unwrap_or_else(PoisonError::into_inner) = Some(original.receipt());
            stop.cancel();
            Err::<(), GuardFailure>(GuardFailure::RootPublicationRejected(0x51_0c))
        }
    };
    let mut acknowledged = None;
    let failure = source
        .runtime
        .block_on(|| async {
            source
                .sync_async_with_permit_carrier_guarded_context_with_ack(
                    crate::PublicationPermit::Unrestricted,
                    true,
                    &mut NativeDemandCarrier::Native(&native),
                    &local_stop,
                    &mut before_publication,
                    Some(context),
                    &mut acknowledged,
                )
                .await
        })
        .expect_err("the actual Root guard must reject the prepared SDK DATA CAS");
    match failure.0 {
        GuardFailure::RootPublicationRejected(code) => assert_eq!(code, 0x51_0c),
        GuardFailure::Source(cause) => panic!("SDK failure replaced the Root primary: {cause:?}"),
    }
    assert!(
        matches!(failure.1, Some(MountSourceError::Cancelled)),
        "the original post-restore accounting record must preserve its real local-stop failure"
    );
    assert!(
        reached.load(Ordering::Acquire),
        "the genuine kernel publication guard must run"
    );
    assert!(
        acknowledged.is_none(),
        "guard rejection is before the real DATA CAS"
    );
    let at_guard = observed_prefix
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .expect("guard observed the cumulative original SDK prefix");
    let completed = root.receipt();
    assert!(at_guard.object_bytes_written > before.object_bytes_written);
    assert!(at_guard.items_examined > before.items_examined);
    assert_eq!(
        completed.backend_read_operations,
        at_guard.backend_read_operations
    );
    assert_eq!(
        completed.backend_write_operations,
        at_guard.backend_write_operations
    );
    assert_eq!(
        completed.object_bytes_written,
        at_guard.object_bytes_written
    );
    assert_eq!(completed.bytes_encoded, at_guard.bytes_encoded);
    assert_eq!(
        completed.authority_records_appended,
        before.authority_records_appended
    );
    assert!(local_stop.is_cancelled());
    assert!(!root_stop.is_cancelled());
    assert!(
        !source.cancellation.is_cancelled(),
        "SDK Source authority has its own original stop"
    );
    let retained_operation = source.runtime.block_on(|| async {
        let checkout = source.checkout.lock().await;
        assert!(checkout.has_pending_mutations());
        checkout
            .publication_operation
            .expect("failed publication retains the actual SDK operation")
    });
    drop(native);
    drop(job);
    drop(incoming);

    let (retry, retry_stop) = root.invocation_view()?;
    let retry = retry.with_source_io_lease(source.native_source_io_lease()?)?;
    let retry_native = retry.clone();
    let published = source
        .runtime
        .block_on(|| async {
            source
                .sync_async_with_permit_carrier_guarded_context_with_ack(
                    crate::PublicationPermit::Unrestricted,
                    false,
                    &mut NativeDemandCarrier::Native(&retry_native),
                    &retry_stop,
                    &mut |_original: &crate::NativeMaterializationWork| {
                        std::future::ready(Ok::<(), GuardFailure>(()))
                    },
                    Some(context),
                    &mut acknowledged,
                )
                .await
        })
        .expect("healthy current invocation retries the original retained SDK operation");
    assert_eq!(published.operation(), Some(retained_operation));
    assert_eq!(published.snapshot().workspace_id(), workspace_id);
    assert_eq!(published.snapshot().volume_config(), config);
    assert_eq!(acknowledged, Some(published));
    assert!(!retry_stop.is_cancelled());
    assert!(!root_stop.is_cancelled());
    assert!(!source.cancellation.is_cancelled());
    assert_eq!(
        root.receipt().authority_records_appended,
        before.authority_records_appended + 1
    );
    drop(retry_native);
    drop(retry);
    drop(file);
    source.shutdown_accepted_cleanup()?;
    Ok(())
}

#[test]
fn native_source_rejects_foreign_namespace_custody_before_new_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let borrowed = source(FilesystemProfile::Portable)?;
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let cancellation = CancellationToken::new();
    let first =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let second =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let first_baseline = first.live_allocation_bytes();
    let second_prefix = second.receipt();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&first, &cancellation);
    let (root, allocation) = NamespacePath::parse_in_profile_with_accounting(
        "/foreign",
        config.profile,
        config.limits,
        &mut accounting,
        &cancellation,
    )?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation
    else {
        panic!("native namespace must retain its original guard");
    };
    let result = CheckoutMountSource::new_at_meter(
        Arc::clone(&borrowed.checkout),
        config,
        root,
        allocation,
        &second,
        Some(&second),
    );
    assert!(matches!(
        result,
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::InvalidLayout
        ))
    ));
    assert_eq!(second.receipt(), second_prefix);
    assert_eq!(first.live_allocation_bytes(), first_baseline);
    Ok(())
}
