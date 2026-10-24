#[tokio::test]
async fn observation_dependency_storage_is_admitted_with_cache_residency()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let cache_entries = maximum_cache_entries_for_components(path.depth(), config())?;
    let (cache, cache_work) = setup_cache(&store, cache_entries, WorkBudget::UNBOUNDED, None)?;
    let dependency_bytes = u64::try_from(path.depth().saturating_add(1))?
        .checked_mul(u64::try_from(size_of::<Dependency>())?)
        .ok_or("dependency byte count overflowed")?;
    let cache_bytes = cache_resident(&cache)?;
    drop(cache);
    // The same canonical path frame is a real simultaneous allocation too.
    let cancellation = CancellationToken::new();
    let zero_peak = observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .await
    .expect_err("the original path frame must be admitted before allocation");
    let PathLookupError::Work(WorkError::BudgetExceeded {
        counter: "peak_allocation_bytes",
        observed: frame_bytes,
        maximum: 0,
    }) = zero_peak.error
    else {
        return Err("zero-peak request did not reject the actual path frame".into());
    };
    assert_eq!(zero_peak.work.backend_read_operations, 0);
    let required_peak = frame_bytes
        .checked_add(cache_bytes)
        .and_then(|bytes| bytes.checked_add(dependency_bytes))
        .ok_or("combined resident byte count overflowed")?;

    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = required_peak - 1;
    let failure = (observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("unadmitted dependency allocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum,
        }) if observed == required_peak && maximum == required_peak - 1
    ));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(
        failure.work.allocation_operations,
        // The admitted frame and its ledger-lifetime retirement cell are
        // separate physical allocations, both preceding the cache backing.
        cache_work.allocation_operations + 2
    );

    // Isolate the actual name leaf, retaining the real cache/dependency backing.
    // A prior page's higher historical peak cannot disguise this field boundary.
    let (cache, _) = setup_cache(&store, cache_entries, WorkBudget::UNBOUNDED, None)?;
    let root = super::super::file_table::lookup_file_record(
        &cache,
        generation.file_table,
        generation.root_file_id,
        DecodeLimits::for_volume(config()),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert!(root.work.backend_read_operations > 0);
    let cache_bytes = cache_resident(&cache)?;
    let first_name_bytes = u64::try_from(path.components()[0].retained_bytes())?;
    let required_name_peak = cache_bytes
        .checked_add(dependency_bytes)
        .and_then(|bytes| bytes.checked_add(first_name_bytes))
        .ok_or("dependency name byte count overflowed")?;
    let name_budget = WorkBudget {
        peak_allocation_bytes: required_name_peak - 1,
        ..WorkBudget::UNBOUNDED
    };
    let mut accounting = KernelAccounting::standalone(name_budget, &cancellation);
    let (_cache_allocation, completed) = accounting.record_actual_owned(cache_bytes)?;
    completed?;
    let _dependencies =
        reserve_slots::<Dependency>(path.depth().saturating_add(1), &mut accounting, None)?;
    let failure = copy_name(
        &path.components()[0],
        config().limits.maximum_component_bytes,
        &mut accounting,
        None,
    )
    .err()
    .ok_or("unadmitted dependency name unexpectedly succeeded")?;
    assert!(matches!(
        failure,
        crate::FsError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed, maximum,
        }) if observed == required_name_peak && maximum == required_name_peak - 1
    ));
    assert_eq!(accounting.spent().bytes_copied, 0);
    Ok(())
}

#[tokio::test]
async fn batch_cancellation_after_authenticated_progress_preserves_prior_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = CancellingStore {
        inner: MemoryObjectStore::default(),
        reads: AtomicUsize::new(0),
    };
    let (generation, path) = fixture(&store.inner).await?;
    let cancellation = CancellationToken::new();
    let failure = (lookup_paths(
        &store,
        &generation,
        std::slice::from_ref(&path),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled batch unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert!(failure.work.backend_read_operations >= 1);
    assert!(failure.work.page_reads >= 1);
    assert!(failure.work.peak_allocation_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn copied_byte_budget_rejects_before_directory_frontier_read()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store).await?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = u64::try_from(main.components()[0].as_bytes().len())
        .unwrap_or(u64::MAX)
        .saturating_sub(1);
    let failure = lookup_paths(
        &store,
        &generation,
        std::slice::from_ref(&main),
        config(),
        budget,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("copy budget unexpectedly admitted")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        })
    ));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.page_reads, 1);
    assert!(failure.work.bytes_copied <= budget.bytes_copied);
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_single_path_operations_do_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let (generation, path) = fixture(&inner).await?;
    let store = CancellingStore {
        inner,
        reads: AtomicUsize::new(0),
    };
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let lookup = (lookup_path(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("pre-cancelled path lookup unexpectedly succeeded")?;
    assert!(matches!(
        lookup.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert_eq!(*lookup.work, WorkCounters::default());

    let observed = (observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("pre-cancelled path observation unexpectedly succeeded")?;
    assert!(matches!(
        observed.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert_eq!(*observed.work, WorkCounters::default());
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn empty_and_excessive_path_batches_reject_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let empty = lookup_paths(
        &store,
        &generation,
        &[],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("empty path batch succeeded")?;
    assert!(matches!(empty.error, PathLookupError::EmptyBatch));
    assert_eq!(*empty.work, WorkCounters::default());

    let mut bounded = config();
    bounded.limits.maximum_paths_per_batch = 1;
    let excessive = lookup_paths(
        &store,
        &generation,
        &[path.clone(), path],
        bounded,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("excessive path batch succeeded")?;
    assert!(matches!(excessive.error, PathLookupError::TooManyPaths));
    assert_eq!(*excessive.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn cancelled_and_unadmitted_batches_perform_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = (lookup_paths(
        &store,
        &generation,
        std::slice::from_ref(&path),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled path batch succeeded")?;
    assert!(matches!(
        cancelled.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert_eq!(*cancelled.work, WorkCounters::default());

    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 0;
    let unadmitted = lookup_paths(
        &store,
        &generation,
        &[path],
        config(),
        budget,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-allocation path batch succeeded")?;
    assert!(matches!(unadmitted.error, PathLookupError::Work(_)));
    assert_eq!(unadmitted.work.backend_read_operations, 0);
    Ok(())
}

#[tokio::test]
async fn single_path_cancellation_after_authenticated_progress_preserves_work()
-> Result<(), Box<dyn std::error::Error>> {
    for observe in [false, true] {
        let inner = MemoryObjectStore::default();
        let (generation, path) = fixture(&inner).await?;
        let store = CancellingStore {
            inner,
            reads: AtomicUsize::new(0),
        };
        let cancellation = CancellationToken::new();
        let failure = if observe {
            (observe_path_async(
                &store,
                &generation,
                &path,
                config(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .await
            .err()
            .ok_or("cancelled path observation unexpectedly succeeded")?
        } else {
            (lookup_path(
                &store,
                &generation,
                &path,
                config(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .await
            .err()
            .ok_or("cancelled path lookup unexpectedly succeeded")?
        };
        assert!(matches!(
            failure.error,
            PathLookupError::Tree(
                TreeReadError::Cancelled | TreeReadError::Storage(ObjectStoreError::Cancelled)
            )
        ));
        assert!(failure.work.backend_read_operations >= 1);
        assert!(failure.work.page_reads >= 1);
        assert!(failure.work.peak_allocation_bytes > 0);
    }
    Ok(())
}

#[tokio::test]
async fn path_bound_violations_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let mut shallow = config();
    shallow.limits.maximum_path_depth = 1;
    let mut small_component = config();
    small_component.limits.maximum_component_bytes = 3;
    for candidate in [shallow, small_component] {
        let examined = if path.depth() > usize::from(candidate.limits.maximum_path_depth) {
            0
        } else {
            u64::try_from(
                path.components()
                    .iter()
                    .position(|component| {
                        component.as_bytes().len()
                            > usize::try_from(candidate.limits.maximum_component_bytes)
                                .unwrap_or(usize::MAX)
                    })
                    .ok_or("component-bound fixture no longer rejects a reached component")?
                    + 1,
            )?
        };
        let failure = lookup_path(
            &store,
            &generation,
            &path,
            candidate,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("unsupported path semantics unexpectedly succeeded")?;
        assert!(matches!(failure.error, PathLookupError::PathBounds));
        assert_eq!(
            *failure.work,
            WorkCounters {
                items_examined: examined,
                ..WorkCounters::UNCHARGED
            }
        );
    }

    let large_component = LogicalName::new(NameEncoding::Utf8, vec![b'x'; 200], 255)?;
    let long_path = NamespacePath::new(
        vec![large_component.clone(), large_component],
        config().limits,
    )?;
    let mut short_path = config();
    short_path.limits.maximum_path_bytes = 255;
    let failure = lookup_path(
        &store,
        &generation,
        &long_path,
        short_path,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("oversized encoded path unexpectedly succeeded")?;
    assert!(matches!(failure.error, PathLookupError::PathBounds));
    assert_eq!(*failure.work, WorkCounters::default());

    let windows_name = LogicalName::new(NameEncoding::WindowsUtf16Le, vec![b'a', 0], 255)?;
    let windows_path = NamespacePath::new(vec![windows_name], config().limits)?;
    let failure = lookup_path(
        &store,
        &generation,
        &windows_path,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("Windows name in portable profile unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::UnsupportedNameEncoding
    ));
    assert_eq!(
        *failure.work,
        WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }
    );
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn cache_and_fixed_vector_admission_fail_before_impossible_allocations()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cache = setup_cache(&store, 1_usize << 61, WorkBudget::UNBOUNDED, None)
        .err()
        .ok_or("cache admitted an overflowing slot table")?;
    assert!(matches!(
        cache.error,
        PathLookupError::Work(WorkError::Overflow)
    ));

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let cancellation = CancellationToken::new();
    let fixed = {
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        reserve_slots::<u8>(usize::MAX, &mut accounting, None)
            .err()
            .ok_or("impossible fixed vector allocation succeeded")?
    };
    assert!(matches!(fixed, crate::FsError::HandleAllocationFailed));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}
