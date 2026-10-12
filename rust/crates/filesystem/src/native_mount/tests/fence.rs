use super::*;

#[cfg(windows)]
#[test]
fn projfs_cannot_shadow_an_existing_directory() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("existing.txt"), b"real")?;
    let request = NativeMountRequest {
        mount_id: MountId::new(),
        volume_id: VolumeId::new(),
        destination: root.path().to_path_buf(),
        writable: true,
    };
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancellation = CancellationToken::new();
    let checkout = runtime.block_on(async {
        let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
        private_checkout(&volume, &cancellation).await
    })?;
    let source = Arc::new(CheckoutMountSource::new(
        Arc::new(SharedCheckout::new(checkout)),
        config,
    )?);
    assert!(matches!(
        mount_native_over_existing(request, source),
        Err(NativeMountError::WritableUnavailable(_))
    ));
    assert_eq!(std::fs::read(root.path().join("existing.txt"))?, b"real");
    Ok(())
}

#[test]
fn administrative_last_drop_closes_real_fence_without_touching_residue()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("authored"), b"retain")?;
    let (lock_path, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let mut owner =
        NativeMountRecoveryOwner::acquire_with_original(&destination, original.clone())?;
    let identity = owner.cleanup_state().fence_identity;
    let borrowed_path = owner
        .cleanup_state()
        .destination
        .as_os_str()
        .as_encoded_bytes()
        .as_ptr();
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.backend_read_operations = 0;
    let (error, receipt) = owner
        .detach_measured(budget, &cancellation)
        .expect_err("the actual native observation is denied");
    assert!(matches!(
        error,
        NativeMountError::Work(_)
            | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))
    ));
    assert_eq!(receipt.backend_read_operations, 0);
    assert!(owner.cleanup_state().destination_fence_retained);
    assert!(owner.cleanup_state().measured_cleanup_failed);
    assert_eq!(owner.cleanup_state().fence_identity, identity);
    assert_eq!(
        owner
            .cleanup_state()
            .destination
            .as_os_str()
            .as_encoded_bytes()
            .as_ptr(),
        borrowed_path
    );
    assert!(matches!(
        acquire_existing_mount_lock(&lock_path),
        Err(NativeMountError::DestinationBusy)
    ));
    let before_close = original.receipt().backend_write_operations;
    await_recovery_worker(owner)?;
    assert_eq!(
        original.receipt().backend_write_operations,
        before_close + 1
    );
    assert!(
        lock_path.is_file(),
        "LastDrop must preserve the actual marker"
    );
    let unlocked = acquire_existing_mount_lock(&lock_path)?;
    let (after_identity, _) =
        crate::native_host::file_identity_and_length(&unlocked, &mut &original)?;
    assert_eq!(after_identity, identity);
    drop(unlocked);
    assert_eq!(std::fs::read(destination.join("authored"))?, b"retain");
    assert!(destination.is_dir());
    Ok(())
}

#[test]
fn administrative_post_lock_acquisition_denial_retires_actual_opened_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    let (lock_path, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
    let baseline =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let owner = NativeMountRecoveryOwner::acquire_with_original(&destination, baseline.clone())?;
    let writes = baseline.receipt().backend_write_operations;
    await_recovery_worker(owner)?;
    let mut budget = WorkBudget::UNBOUNDED;
    // Two guard cleanup writes and the original final-close write remain
    // reserved. This denies the real registry unlock AFTER fence locking.
    budget.backend_write_operations = writes + 2 + u64::from(cfg!(windows));
    let denied = crate::NativeMaterializationWork::new(budget, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let error = NativeMountRecoveryOwner::acquire_with_original(&destination, denied.clone())
        .expect_err("the actual post-lock registry unlock is denied");
    assert!(matches!(
        error,
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::BudgetExceeded {
                counter: "backend_write_operations",
                ..
            }
        ))
    ));
    assert_eq!(
        denied.receipt().backend_write_operations,
        writes,
        "actual prefix plus original physical fence close"
    );
    assert!(
        lock_path.exists(),
        "failed acquisition preserves its on-disk marker"
    );
    drop(acquire_existing_mount_lock(&lock_path)?);
    assert!(destination.is_dir());
    Ok(())
}

#[test]
fn administrative_retry_uses_same_fence_and_distinct_action_ledgers()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("authored"), b"retain")?;
    let mut owner = NativeMountRecoveryOwner::acquire(&destination)?;
    let identity = owner.cleanup_state().fence_identity;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.backend_read_operations = 0;
    assert!(
        owner
            .detach_measured(budget, &CancellationToken::new())
            .is_err()
    );
    let receipt = owner
        .detach_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    assert!(receipt.value);
    assert!(receipt.work.backend_read_operations > 0);
    assert_eq!(owner.cleanup_state().fence_identity, identity);
    let state = owner.cleanup_state();
    assert!(!state.destination_fence_retained && !state.controls_pending);
    await_recovery_worker(owner)?;
    assert_eq!(std::fs::read(destination.join("authored"))?, b"retain");
    Ok(())
}

#[test]
fn crash_recovery_never_removes_live_or_contaminated_destinations()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("native-destination");
    std::fs::create_dir(&destination)?;

    let live_guard = MountDestinationGuard::acquire(&destination)?;
    assert!(matches!(
        recover_native_mount_destination(&destination),
        Err(NativeMountError::DestinationBusy)
    ));
    assert!(destination.is_dir());
    drop(live_guard);

    std::fs::write(destination.join("unowned-content"), b"retain")?;
    let mut recovery = NativeMountRecoveryOwner::acquire(&destination)?;
    let original_fence = recovery.cleanup_state().fence_identity;
    assert!(recovery.recover().is_err());
    assert!(recovery.cleanup_state().destination_fence_retained);
    assert!(recovery.cleanup_state().measured_cleanup_failed);
    assert_eq!(
        std::fs::read(destination.join("unowned-content"))?,
        b"retain"
    );
    std::fs::remove_file(destination.join("unowned-content"))?;

    recovery.recover()?;
    assert_eq!(recovery.cleanup_state().fence_identity, original_fence);
    let state = recovery.cleanup_state();
    assert!(!state.destination_fence_retained && !state.controls_pending);
    assert!(!destination.exists());
    let residue = std::fs::read_dir(parent.path())?
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .filter(|name| name != ".acyclic-fs-mount-registry.lock")
        .collect::<Vec<_>>();
    assert!(residue.is_empty());
    Ok(())
}

#[cfg(windows)]
#[test]
fn preserving_recovery_fences_live_owner_and_retains_ordinary_residue()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    let live_guard = MountDestinationGuard::acquire(&destination)?;
    assert!(matches!(
        recover_native_mount_destination_preserving_residue(&destination),
        Err(NativeMountError::DestinationBusy)
    ));
    drop(live_guard);

    std::fs::write(destination.join("authored.txt"), b"retain")?;
    let preserved = recover_native_mount_destination_preserving_residue(&destination)?
        .ok_or("nonempty destination was not preserved")?;
    assert!(!destination.exists());
    assert_eq!(std::fs::read(preserved.join("authored.txt"))?, b"retain");
    assert!(recover_native_mount_destination_preserving_residue(&destination)?.is_none());
    Ok(())
}

#[test]
fn destination_ownership_is_process_external_and_recoverable()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let first = MountDestinationGuard::acquire(&destination)?;
    let second = MountDestinationGuard::acquire(&destination);
    assert!(
        matches!(second, Err(NativeMountError::DestinationBusy)),
        "unexpected second-owner result: {second:?}"
    );
    let independent = temporary.path().join("independent");
    std::fs::create_dir(&independent)?;
    let _independent = MountDestinationGuard::acquire(&independent)?;
    drop(first);
    let _recovered = MountDestinationGuard::acquire(&destination)?;
    Ok(())
}

#[test]
fn orderly_destination_release_removes_per_mount_fence() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let guard = MountDestinationGuard::acquire(&destination)?;
    drop(guard);

    let mut residue = std::fs::read_dir(temporary.path())?
        .map(|entry| entry.map(|value| value.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    residue.sort();
    assert_eq!(
        residue,
        vec![
            std::ffi::OsString::from(".acyclic-fs-mount-registry.lock"),
            std::ffi::OsString::from("mount"),
        ]
    );
    Ok(())
}

#[test]
fn repeated_admission_teardown_has_constant_persistent_resource_set()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destinations = [
        temporary.path().join("mount-a"),
        temporary.path().join("mount-b"),
    ];
    for destination in &destinations {
        std::fs::create_dir(destination)?;
    }
    for ordinal in 0..1_024 {
        let guard = MountDestinationGuard::acquire(&destinations[ordinal % 2])?;
        drop(guard);
    }
    let mut residue = std::fs::read_dir(temporary.path())?
        .map(|entry| entry.map(|value| value.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    residue.sort();
    assert_eq!(
        residue,
        vec![
            std::ffi::OsString::from(".acyclic-fs-mount-registry.lock"),
            std::ffi::OsString::from("mount-a"),
            std::ffi::OsString::from("mount-b"),
        ]
    );
    for destination in destinations {
        std::fs::remove_dir(destination)?;
    }
    Ok(())
}

#[test]
fn bounded_parent_sweep_reclaims_stale_fences_and_preserves_live_owners()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let live_destination = temporary.path().join("live");
    let stale_destination = temporary.path().join("stale");
    std::fs::create_dir(&live_destination)?;
    std::fs::create_dir(&stale_destination)?;
    let live = MountDestinationGuard::acquire(&live_destination)?;
    let mut stale = MountDestinationGuard::acquire(&stale_destination)?;
    let live_path = live.lock_path.clone();
    let stale_path = stale.lock_path.clone();
    stale.file.take();
    drop(stale);

    assert_eq!(
        reclaim_stale_native_mount_destination_fences(temporary.path())?,
        1
    );
    assert!(live_path.is_file());
    assert!(!stale_path.exists());
    drop(live);
    assert_eq!(
        reclaim_stale_native_mount_destination_fences(temporary.path())?,
        0
    );
    Ok(())
}

#[test]
fn parent_sweep_rejects_malformed_reserved_fence_names() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join(".acyclic-fs-mount-invalid.lock"), b"")?;
    assert!(matches!(
        reclaim_stale_native_mount_destination_fences(temporary.path()),
        Err(NativeMountError::InvalidDestination)
    ));
    Ok(())
}

#[test]
fn independent_supervisors_concurrently_reclaim_one_crash_left_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    let release = temporary.path().join("release");
    std::fs::create_dir(&destination)?;
    let mut crashed = MountDestinationGuard::acquire(&destination)?;
    let crash_left_path = crashed.lock_path.clone();
    crashed.file.take();
    drop(crashed);

    let executable = std::env::current_exe()?;
    let mut children = Vec::new();
    let mut readiness = Vec::new();
    for ordinal in 0..2 {
        let ready = temporary.path().join(format!("ready-{ordinal}"));
        let child = acyclic_native_runtime::ProcessTree::spawn(
            std::process::Command::new(&executable)
                .args([
                    "--exact",
                    "native_mount::tests::destination_reclaim_child_waits_for_external_release",
                    "--nocapture",
                ])
                .env("ACYCLIC_FS_RECLAIM_CHILD_DESTINATION", &destination)
                .env("ACYCLIC_FS_RECLAIM_CHILD_READY", &ready)
                .env("ACYCLIC_FS_RECLAIM_CHILD_RELEASE", &release),
        )?;
        readiness.push(ready);
        children.push(child);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while readiness.iter().any(|path| !path.is_file()) {
        for child in &mut children {
            if let Some(status) = child.try_wait(None)? {
                return Err(format!("reclaim child exited before release: {status}").into());
            }
        }
        if Instant::now() >= deadline {
            return Err("reclaim children did not become ready".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(&release, b"release")?;
    for child in &mut children {
        let status = child.wait(Duration::from_secs(5))?;
        if !status.success() {
            return Err(format!("reclaim child failed: {status}").into());
        }
    }
    children.clear();
    assert!(!crash_left_path.exists());
    assert!(destination.is_dir());
    Ok(())
}

#[test]
fn destination_ownership_is_reclaimed_after_process_death() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    let ready = temporary.path().join("ready");
    std::fs::create_dir(&destination)?;
    let child = acyclic_native_runtime::ProcessTree::spawn(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "native_mount::tests::destination_ownership_child_holds_lock_until_killed",
                "--nocapture",
            ])
            .env("ACYCLIC_FS_LOCK_CHILD_DESTINATION", &destination)
            .env("ACYCLIC_FS_LOCK_CHILD_READY", &ready),
    )?;
    let mut child = child;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.is_file() {
        if let Some(status) = child.try_wait(None)? {
            return Err(format!("lock child exited before readiness: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err("lock child readiness timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(matches!(
        MountDestinationGuard::acquire(&destination),
        Err(NativeMountError::DestinationBusy)
    ));
    assert!(matches!(
        reclaim_native_mount_destination_fence(&destination),
        Err(NativeMountError::DestinationBusy)
    ));
    child.terminate_descendants()?;
    let status = child.wait(Duration::from_secs(5))?;
    assert!(
        !status.success(),
        "killed lock child unexpectedly succeeded"
    );
    reclaim_native_mount_destination_fence(&destination)?;
    reclaim_native_mount_destination_fence(&destination)?;
    let _reclaimed = MountDestinationGuard::acquire(&destination)?;
    Ok(())
}

#[test]
fn shared_destination_fence_retains_original_owner_after_denial()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let cancellation = crate::CancellationToken::new();
    let original = Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
        crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation),
    )));
    let meter = NativeMaterializationWork::from_meter(Arc::clone(&original));
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &meter, &mut guard, false)?;
    let mut guard = guard.expect("actual fence was admitted");
    assert!(Arc::ptr_eq(meter.native_read_meter(), &original));
    assert!(guard.parent_directory.is_some());
    assert!(guard.file.is_some());
    let mut competing_guard = None;
    assert!(matches!(
        MountDestinationGuard::acquire_with_work(
            &destination,
            false,
            &meter,
            &mut competing_guard,
            false
        ),
        Err(NativeMountError::DestinationBusy)
    ));
    assert!(
        competing_guard.is_none(),
        "a competing lock never becomes an owned cleanup target"
    );
    {
        let mut work = original.lock();
        work.budget.backend_write_operations = work.work.backend_write_operations;
    }
    let before = original.lock().work;
    let error = guard
        .release(&mut &meter)
        .expect_err("original write allowance denies cleanup");
    assert!(matches!(
        error,
        NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations",
            ..
        })
    ));
    assert!(guard.file.is_some());
    assert_eq!(
        original.lock().work.backend_write_operations,
        before.backend_write_operations
    );
    original.lock().budget.backend_write_operations = u64::MAX;
    guard.release(&mut &meter)?;
    assert!(guard.file.is_none());
    assert_eq!(original.lock().live_allocation_bytes(), 0);
    Ok(())
}

#[test]
fn accepted_destination_fence_cleanup_survives_original_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let cancelled = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 64,
            backend_write_operations: 64,
            items_examined: 1_024,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancelled,
    )
    .map_err(|(error, _)| error)?;
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
    let mut guard = guard.expect("actual fence was admitted");
    let before = work.receipt();
    cancelled.cancel();
    let refused_wait = crate::CancellationToken::new();
    refused_wait.cancel();
    assert!(guard.release_cleanup(Some(&refused_wait)).is_err());
    assert!(
        guard.file.is_some(),
        "denied wait preserves the actual fence"
    );
    assert_eq!(
        work.receipt(),
        before,
        "no accepted operation starts on a denied wait"
    );
    guard.release_cleanup(Some(&crate::CancellationToken::new()))?;
    assert!(
        guard.file.is_none(),
        "the same held fence physically retired"
    );
    let after = work.receipt();
    assert!(after.backend_write_operations > before.backend_write_operations);
    assert_eq!(
        after.allocation_operations, before.allocation_operations,
        "accepted cleanup does not allocate a path or control"
    );
    assert!(
        work.cancellation().is_cancelled(),
        "original cancellation is never replaced"
    );
    drop(guard);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[test]
fn measured_fence_native_delete_failure_preserves_typed_cause_and_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mut guard = MountDestinationGuard::acquire(&destination)?;
    std::fs::remove_file(&guard.lock_path)?;
    std::fs::create_dir(&guard.lock_path)?;
    let cancellation = crate::CancellationToken::new();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    let error = guard
        .release(&mut work)
        .expect_err("native file deletion cannot remove a directory");
    assert!(matches!(error, NativeMountError::Io(_)));
    assert!(work.work.backend_read_operations > 0);
    assert!(work.work.backend_write_operations > 0);
    assert!(guard.file.is_some());
    assert!(guard.lock_path.is_dir());
    Ok(())
}

#[test]
fn measured_fence_cancellation_never_starts_a_native_leaf() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mut guard = MountDestinationGuard::acquire(&destination)?;
    let cancellation = crate::CancellationToken::new();
    cancellation.cancel();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
    let error = guard
        .release(&mut work)
        .expect_err("original cancellation must win before native work");
    assert!(matches!(
        error,
        NativeMountError::Demand(crate::DemandError::Cancelled)
    ));
    assert_eq!(work.work, crate::WorkCounters::default());
    assert!(guard.file.is_some());
    assert!(guard.lock_path.is_file());
    Ok(())
}
