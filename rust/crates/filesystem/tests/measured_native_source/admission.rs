use super::*;

#[tokio::test]
async fn native_root_admission_denies_owned_memory_before_native_io() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let reference = SourceReference {
        identity: [32; 16],
        epoch: 1,
    };
    let cancellation = CancellationToken::new();
    for memory_denial in [false, true] {
        let path = directory.path().to_path_buf();
        let input_bytes = path.capacity() as u64;
        let mut budget = FINITE_BUDGET;
        if memory_denial {
            budget.peak_allocation_bytes = 0;
        } else {
            budget.allocation_operations = 0;
        }
        let failure = NativeDemandSource::open(
            std::borrow::Cow::Owned(path),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(reference),
            None,
            budget,
            &cancellation,
        )
        .await
        .err()
        .ok_or("native root ignored its owned allocation bound")?;
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(
            failure.work.peak_allocation_bytes,
            if memory_denial { 0 } else { input_bytes }
        );
        assert_eq!(failure.work.source_entries_visited, 0);
        assert_eq!(failure.work.source_bytes_read, 0);
        failure.work.verify(budget)?;
    }
    let opened = NativeDemandSource::open(
        std::borrow::Cow::Owned(directory.path().to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        Some(reference),
        None,
        FINITE_BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(opened.value.reference(), reference);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn original_native_root_custody_and_current_reference_are_provider_owned() -> TestResult {
    use acyclic_fs::NativeMaterializationWork;
    acyclic_native_runtime::initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let budget = FINITE_BUDGET;
    let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let before = work.live_allocation_bytes();
    let (caller_path, caller_guard) = work.path(&root, None)?;
    let reference = SourceReference {
        identity: [92; 16],
        epoch: 18,
    };
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(std::path::Path::new(&caller_path)),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        Some(reference),
        None,
        &work,
        work.cancellation(),
    )
    .await?;
    drop(caller_path);
    drop(caller_guard);
    assert!(
        work.live_allocation_bytes() > before,
        "provider retains its own root/control custody"
    );
    let namespace = requested("")?;
    assert!(
        source
            .lookup(reference, &namespace, &work, work.cancellation())
            .await?
            .is_some()
    );
    let advanced = source.invalidate();
    assert_eq!(advanced.identity, reference.identity);
    assert_eq!(advanced.epoch, reference.epoch + 1);
    assert!(matches!(
        source
            .lookup(reference, &namespace, &work, work.cancellation())
            .await,
        Err(DemandError::StaleSource)
    ));
    assert!(
        source
            .lookup(advanced, &namespace, &work, work.cancellation())
            .await?
            .is_some()
    );
    std::fs::rename(&root, temporary.path().join("held-original"))?;
    std::fs::create_dir(&root)?;
    assert!(matches!(
        source
            .lookup(advanced, &namespace, &work, work.cancellation())
            .await,
        Err(DemandError::SourceUnavailable)
    ));
    let retained = work.live_allocation_bytes();
    drop(source);
    assert!(
        work.live_allocation_bytes() < retained,
        "the physical root spelling retires only with its provider"
    );
    let detached_source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(std::path::Path::new(&root)),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        Some(advanced),
        None,
        &work,
        work.cancellation(),
    )
    .await?;
    let original = work.runtime_meter();
    let original_weak = Arc::downgrade(&original);
    drop(original);
    work.receipt().verify(budget)?;
    drop(work);
    assert!(
        original_weak.upgrade().is_some(),
        "public provider outlives every caller accounting guard"
    );
    drop(detached_source);
    assert!(
        original_weak.upgrade().is_none(),
        "provider Drop retires its intrinsic original keeper"
    );
    Ok(())
}

// Faults occur only after the real companion operation ran. In particular,
// Binding is a lost acknowledgement of the real durable-in-memory CAS.
