use super::*;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn reported_control_paths_are_exact_destination_fences(name in "[a-z]{1,16}") {
        let parent = std::env::current_dir().unwrap();
        let destination = parent.join(name);
        let reported = super::super::native_mount_control_paths(&destination).unwrap();
        let actual = MountDestinationGuard::paths_for_canonical(&destination).unwrap();
        let restored: PathBuf = serde_json::from_slice(&serde_json::to_vec(&destination).unwrap()).unwrap();
        proptest::prop_assert_eq!(super::super::native_mount_control_paths(&restored).unwrap(), reported.clone());
        proptest::prop_assert_eq!(&reported[0], &actual.0);
        proptest::prop_assert_eq!(&reported[1], &actual.1);
        proptest::prop_assert_ne!(&reported[0], &reported[1]);
        proptest::prop_assert_eq!(reported[0].parent(), Some(parent.as_path()));
        proptest::prop_assert_eq!(&reported[1], &super::super::registry::path().unwrap());
        proptest::prop_assert_ne!(reported[0].clone(), destination.clone());
        proptest::prop_assert_ne!(reported[1].clone(), destination);
    }
}

#[cfg(windows)]
#[test]
fn projfs_cannot_shadow_an_existing_directory() -> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
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
    crate::native_mount::registry::initialize().unwrap();
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("authored"), b"retain")?;
    let (lock_path, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let mut owner = NativeMountRecoveryOwner::acquire_with_original(&destination, &original)?;
    let identity = owner.cleanup_state()?.fence_identity;
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.backend_read_operations = 0;
    let denied =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let error = owner
        .detach(&denied)
        .expect_err("the actual native observation is denied");
    let receipt = denied.receipt();
    assert!(matches!(
        error,
        NativeMountError::Work(_)
            | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))
    ));
    assert_eq!(receipt.backend_read_operations, 0);
    assert!(owner.cleanup_state()?.destination_fence_retained);
    assert!(owner.cleanup_state()?.measured_cleanup_failed);
    assert_eq!(owner.cleanup_state()?.fence_identity, identity);
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
    crate::native_mount::registry::initialize().unwrap();
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    let (lock_path, _) = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?;
    let baseline =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let owner = NativeMountRecoveryOwner::acquire_with_original(&destination, &baseline)?;
    let writes = baseline.receipt().backend_write_operations;
    await_recovery_worker(owner)?;
    let mut budget = WorkBudget::UNBOUNDED;
    // Successful acquisition spends its final write unlocking the registry.
    // Keep that one write unavailable while funding the accepted fence
    // retirement (lock, unlink, unlock) and the administrative final close.
    let retirement_writes = 3 + 1 + u64::from(cfg!(windows));
    budget.backend_write_operations = writes - 1 + retirement_writes;
    let denied = crate::NativeMaterializationWork::new(budget, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let error = NativeMountRecoveryOwner::acquire_with_original(&destination, &denied)
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
    crate::native_mount::registry::initialize().unwrap();
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("mount");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("authored"), b"retain")?;
    let mut owner = NativeMountRecoveryOwner::acquire(&destination)?;
    let identity = owner.cleanup_state()?.fence_identity;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.backend_read_operations = 0;
    let denied = crate::NativeMaterializationWork::new(budget, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    assert!(owner.detach(&denied).is_err());
    let retry =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    assert!(owner.detach(&retry)?);
    assert!(retry.receipt().backend_read_operations > 0);
    assert_eq!(owner.cleanup_state()?.fence_identity, identity);
    let state = owner.cleanup_state()?;
    assert!(!state.destination_fence_retained && !state.controls_pending);
    await_recovery_worker(owner)?;
    assert_eq!(std::fs::read(destination.join("authored"))?, b"retain");
    Ok(())
}

#[test]
fn exact_reclamation_preserves_live_owners() -> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
    let temporary = tempfile::tempdir()?;
    let live_destination = temporary.path().join("live");
    let stale_destination = temporary.path().join("stale");
    std::fs::create_dir(&live_destination)?;
    std::fs::create_dir(&stale_destination)?;
    let live = MountDestinationGuard::acquire_canonical(
        &live_destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?,
    )?;
    let mut stale = MountDestinationGuard::acquire_canonical(
        &stale_destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?,
    )?;
    let live_path = live.lock_path.clone();
    let stale_path = stale.lock_path.clone();
    stale.file.take();
    drop(stale);

    assert!(matches!(
        reclaim_native_mount_destination_fence(&live_destination),
        Err(NativeMountError::DestinationBusy)
    ));
    reclaim_native_mount_destination_fence(&stale_destination)?;
    assert!(live_path.is_file());
    assert!(!stale_path.exists());
    drop(live);
    assert!(!live_path.exists());
    Ok(())
}

#[test]
fn exact_reclamation_preserves_every_authored_sibling() -> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mut stale = MountDestinationGuard::acquire_canonical(&destination)?;
    stale.file.take();
    drop(stale);
    let authored = temporary.path().join(".acyclic-fs-mount-invalid.lock");
    std::fs::write(&authored, b"authored")?;
    // Beyond the former 4096-entry cap: reclamation must touch only the
    // explicitly tracked destination, independent of sibling population.
    for index in 0..4097 {
        std::fs::write(temporary.path().join(format!("user-{index}")), b"retain")?;
    }
    reclaim_native_mount_destination_fence(&destination)?;
    assert_eq!(std::fs::read(authored)?, b"authored");
    for index in 0..4097 {
        assert_eq!(
            std::fs::read(temporary.path().join(format!("user-{index}")))?,
            b"retain"
        );
    }
    Ok(())
}

#[test]
fn independent_supervisors_concurrently_reclaim_one_crash_left_fence()
-> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    let release = temporary.path().join("release");
    std::fs::create_dir(&destination)?;
    let mut crashed = MountDestinationGuard::acquire_canonical(
        &destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?,
    )?;
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
fn shared_destination_fence_retains_original_owner_after_denial()
-> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let cancellation = crate::CancellationToken::new();
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(
        crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation),
    ));
    let meter = NativeMaterializationWork::from_meter(Arc::clone(&original));
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &meter, &mut guard, false)?;
    let mut guard = guard.unwrap();
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
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::BudgetExceeded {
                counter: "backend_write_operations",
                ..
            }
        ))
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
    crate::native_mount::registry::initialize().unwrap();
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
    let baseline = work.live_allocation_bytes();
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
    let mut guard = guard.unwrap();
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
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn measured_fence_native_delete_failure_preserves_typed_cause_and_custody()
-> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mut guard = MountDestinationGuard::acquire_canonical(
        &destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?,
    )?;
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
    crate::native_mount::registry::initialize().unwrap();
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mut guard = MountDestinationGuard::acquire_canonical(
        &destination
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?,
    )?;
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

#[cfg(target_os = "linux")]
#[test]
fn recovery_reclaims_a_dead_owner_lock_but_rejects_a_held_lock()
-> Result<(), Box<dyn std::error::Error>> {
    crate::native_mount::registry::initialize().unwrap();
    let root = tempfile::tempdir()?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mut owner = MountDestinationGuard::acquire_canonical(&destination.canonicalize()?)?;
    let marker = owner.lock_path.clone();
    assert!(matches!(
        NativeMountRecoveryOwner::acquire(&destination),
        Err(NativeMountError::DestinationBusy)
    ));
    // A crashed process closes its file without deleting the on-disk marker.
    drop(owner.file.take());
    drop(owner);
    assert!(marker.exists());
    let mut recovery = NativeMountRecoveryOwner::acquire(&destination)?;
    assert!(recovery.recover()?);
    assert!(!recovery.cleanup_state()?.destination_fence_retained);
    assert!(recovery.cleanup_state()?.namespace_detach_confirmed);
    assert!(!destination.exists());
    await_recovery_worker(recovery)?;
    Ok(())
}

/// Another process may share a lock's open file description: a child spawned
/// concurrently holds a duplicate between fork and exec. Dropping the owner's
/// lock must still release ownership at once.
#[cfg(target_os = "linux")]
#[test]
fn a_dropped_lock_is_released_while_another_process_shares_its_description()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("shared.lock");
    let lock = super::fence::acquire_mount_lock(&path, false)?;
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .stdin(std::process::Stdio::from(lock.try_clone()?))
        .spawn()?;
    drop(lock);
    let reacquired = super::fence::acquire_existing_mount_lock(&path);
    child.kill()?;
    child.wait()?;
    drop(reacquired?);
    Ok(())
}
