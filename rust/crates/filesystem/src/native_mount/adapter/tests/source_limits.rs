#[test]
fn native_namespace_guard_cases() -> Result<(), Box<dyn std::error::Error>> {
    let borrowed = source(FilesystemProfile::Portable)?;
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let cancellation = CancellationToken::new();
    let original = capture_meter(boundary_budget(), &cancellation)?;
    let foreign = capture_meter(boundary_budget(), &cancellation)?;
    let baseline = original.live_allocation_bytes();
    for invalid in [true, false] {
        let mut accounting = crate::kernel::KernelAccounting::native(&original, &cancellation);
        let (root, allocation) = NamespacePath::parse_in_profile_with_accounting(
            "/branch",
            config.profile,
            config.limits,
            &mut accounting,
            &cancellation,
        )?;
        let crate::kernel::KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
            panic!("namespace lacks native allocation");
        };
        let work = if invalid { &foreign } else { &original };
        let prefix = work.receipt();
        let result = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout),
            config,
            root,
            allocation,
            work,
            Some(work),
        );
        if invalid {
            assert!(matches!(
                result,
                Err(NativeMountError::Materialization(
                    crate::NativeTreeMaterializationError::InvalidLayout
                ))
            ));
            assert_eq!(foreign.receipt(), prefix);
            assert_eq!(original.live_allocation_bytes(), baseline);
            continue;
        }
        let source = result?.into_arc_meter(work)?;
        let bound = source
            .native_original_work()?
            .ok_or("missing source work")?;
        assert!(bound.shares_ledger(&original));
        assert!(bound.shares_resource_allowance(&original));
        drop(bound);
        let stop = source.cancellation().clone();
        stop.cancel();
        assert!(matches!(
            source.native_original_work(),
            Err(MountSourceError::Stale)
        ));
        let weak = Arc::downgrade(&source);
        drop(source);
        assert!(weak.upgrade().is_none());
        drop(stop);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
fn source_cleanup_cases() -> Result<(), Box<dyn std::error::Error>> {
    let borrowed = source(FilesystemProfile::Portable)?;
    for stopped_wait in [false, true] {
        let root_stop = CancellationToken::new();
        let root = capture_meter(boundary_budget(), &root_stop)?;
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
fn source_io_limit_cancellation_and_retirement_cases() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::{NativeFile, NativeFileMeter};
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let fixture = tempfile::tempdir()?;
    let path = fixture.path().join("payload");
    std::fs::write(&path, b"original")?;
    for cancelled in [false, true] {
        let root_stop = CancellationToken::new();
        let root = capture_meter(boundary_budget(), &root_stop)?;
        let volume = root.native_volume_work(WorkBudget {
            backend_read_operations: 1,
            ..crate::performance::BOUNDARY_BUDGET
        })?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let weak = Arc::downgrade(&source);
        let lease = source.native_source_io_lease()?;
        let last_lease = lease.clone();
        let (incoming, local_stop) = root.invocation_view()?;
        let job = incoming.with_source_io_lease(lease)?;
        let credit = job.reserve_work(crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let meter = job.runtime_meter();
        let file = NativeFile::from_file(
            std::fs::File::open(&path)?,
            Some(NativeFileMeter::Owned(meter.clone())),
        )
        .map_err(|(error, _)| error)?;
        let data = if cancelled {
            local_stop.cancel();
            let prefix = root.receipt();
            let error = file
                .read_bounded_async(0, 8, meter.clone(), false)
                .err()
                .ok_or("cancelled read admitted")?;
            assert!(matches!(
                job.runtime_error(error),
                crate::NativeTreeMaterializationError::Cancelled
            ));
            assert_eq!(root.receipt(), prefix);
            root.cancellation().check()?;
            None
        } else {
            let data = borrowed.runtime.block_on(
                || {
                    file.read_bounded_async(0, 8, meter.clone(), false)
                        .expect("read submission")
                },
                Some(meter.as_ref()),
            )??;
            assert_eq!(data.0.as_ref(), b"original");
            assert_eq!(volume.remaining()?.backend_read_operations, 0);
            assert!(root.remaining()?.backend_read_operations > 0);
            let prefix = root.receipt();
            let error = file
                .read_bounded_async(0, 8, meter.clone(), false)
                .and_then(|completion| {
                    borrowed
                        .runtime
                        .block_on(|| completion, Some(meter.as_ref()))?
                })
                .err()
                .ok_or("volume ceiling ignored")?;
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
            Some(data)
        };
        assert!(!root_stop.is_cancelled());
        assert!(!source.cancellation.is_cancelled());
        drop(file);
        drop(meter);
        drop(job);
        drop(incoming);
        drop(source);
        assert!(weak.upgrade().is_some());
        drop(last_lease);
        assert!(weak.upgrade().is_none());
        if let Some(data) = data {
            assert_eq!(data.0.as_ref(), b"original");
        }
        assert!(credit.remaining_work().items_examined > 0);
    }
    Ok(())
}

#[test]
fn native_source_io_projection_rejects_foreign_root_and_resource_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let cancellation = CancellationToken::new();
    let root = capture_meter(boundary_budget(), &cancellation)?;
    let volume = root.native_volume_work(crate::performance::BOUNDARY_BUDGET)?;
    let source = native_source_io_control(&borrowed, &volume)?;
    let lease = source.native_source_io_lease()?;
    let foreign_root = capture_meter(boundary_budget(), &cancellation)?;
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
    let root = capture_meter(boundary_budget(), &root_stop)?;
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
fn detached_construction_uses_the_callers_memory_limit() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let root = capture_meter(boundary_budget(), source.cancellation())?;
    let created = source.create_file(&native_test_path("detached"), metadata(), None, &root)?;
    let owner = ViewGate::callback_owner();
    let record = source.runtime.wait(|| async {
        source
            .record_by_id(
                created.node.file_id,
                owner,
                &mut NativeDemandCarrier::Native(&root),
            )
            .await
    })?;
    for limited in [true, false] {
        let mut budget = boundary_budget();
        if limited {
            budget.peak_allocation_bytes = 0;
        }
        let work = root.native_volume_work(budget)?;
        let result = source.runtime.wait(|| async {
            source
                .detached_file_from_record(
                    record,
                    created.metadata,
                    owner,
                    &mut NativeDemandCarrier::Native(&work),
                )
                .await
        });
        if limited {
            assert!(matches!(
                result,
                Err(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::Work(_)
                ))
            ));
        } else {
            assert!(result?.read_up_to_with_meter(0, 1, &work)?.bytes.is_empty());
        }
    }
    Ok(())
}

#[test]
fn native_source_qualified_guard_keeps_primary_and_actual_accounting_failure()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let borrowed = source(FilesystemProfile::Portable)?;
    let workspace_id = crate::WorkspaceId::from_bytes(borrowed.volume_id()?.into_bytes());
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
    let root_stop = CancellationToken::new();
    let root = capture_meter(boundary_budget(), &root_stop)?;
    let volume = root.native_volume_work(boundary_budget())?;
    let source = native_source_io_control(&borrowed, &volume)?;
    let (incoming, local_stop) = root.invocation_view()?;
    let job = incoming.with_source_io_lease(source.native_source_io_lease()?)?;
    file.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"native publication prefix"),
            0,
        ),
        &job,
    )?;
    let before = root.receipt();
    let stop = local_stop.clone();
    let expected_work = root.clone();
    let mut before_publication = move |work: &crate::NativeMaterializationWork| {
        assert!(work.shares_ledger(&expected_work));
        assert!(work.cancellation().same_state(&stop));
        let stop = stop.clone();
        async move {
            stop.cancel();
            Err::<(), MountSourceError>(MountSourceError::NotFound)
        }
    };
    let mut acknowledged = None;
    let failure = source
        .runtime
        .block_on(
            || {
                source.checkout.publish(
                    crate::PublicationPermit::Unrestricted,
                    &job,
                    &local_stop,
                    Some(source.cancellation()),
                    &mut before_publication,
                    Some(context),
                    &mut acknowledged,
                )
            },
            None,
        )?
        .expect_err("guard must reject DATA CAS");
    assert!(matches!(failure.0, MountSourceError::NotFound));
    assert!(matches!(failure.1, Some(MountSourceError::Cancelled)));
    assert!(acknowledged.is_none());
    assert_eq!(
        root.receipt().authority_records_appended,
        before.authority_records_appended
    );
    assert!(local_stop.is_cancelled());
    assert!(!root_stop.is_cancelled());
    assert!(!source.cancellation().is_cancelled());
    let fenced = native_test_path("fenced");
    assert!(matches!(
        borrowed.create_file(&fenced, metadata(), None, &root),
        Err(MountSourceError::Stale)
    ));
    let retained = source.runtime.wait(|| async {
        Ok(source
            .checkout
            .lock()
            .await
            .publication_operation
            .ok_or(MountSourceError::Stale)?)
    })?;
    drop(job);
    drop(incoming);
    let (retry, retry_stop) = root.invocation_view()?;
    let retry = retry.with_source_io_lease(source.native_source_io_lease()?)?;
    source
        .runtime
        .block_on(
            || async {
                source
                    .checkout
                    .publish(
                        crate::PublicationPermit::Unrestricted,
                        &retry,
                        &retry_stop,
                        Some(source.cancellation()),
                        &mut |_| std::future::ready(Ok::<(), MountSourceError>(())),
                        Some(context),
                        &mut acknowledged,
                    )
                    .await
            },
            None,
        )?
        .expect("retry retained publication");
    let published = acknowledged.ok_or("missing publication ACK")?;
    assert_eq!(published.operation(), Some(retained));
    assert_eq!(published.workspace_id(), workspace_id);
    assert_eq!(published.volume_config(), config);
    assert_eq!(
        root.receipt().authority_records_appended,
        before.authority_records_appended + 1
    );
    assert!(!retry_stop.is_cancelled());
    assert!(!root_stop.is_cancelled());
    assert!(!source.cancellation().is_cancelled());
    borrowed.create_file(&fenced, metadata(), None, &root)?;
    drop(retry);
    drop(file);
    source.shutdown_accepted_cleanup()?;
    Ok(())
}
