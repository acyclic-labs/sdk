use super::*;

#[tokio::test]
async fn immutable_summaries_are_cached_without_becoming_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None).await?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let first = probe
        .probe_async(
            generation,
            &DependencyRegion::Metadata(file_id),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(first.value, DependencyState::Present(metadata.digest));
    assert_eq!(first.work.backend_read_operations, 2);
    let cached = probe
        .probe_async(
            generation,
            &DependencyRegion::Metadata(file_id),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(cached.value, first.value);
    assert_eq!(cached.work.backend_read_operations, 0);
    let absent = probe
        .probe_async(
            generation,
            &DependencyRegion::DirectoryName {
                directory_id: file_id,
                name: LogicalName::new(NameEncoding::Utf8, b"absent".to_vec(), 255)?,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(absent.value, DependencyState::Absent);
    assert_eq!(absent.work.page_reads, 1);
    assert_eq!(absent.work.backend_read_operations, 1);

    let async_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancellation = CancellationToken::new();
    let metadata_region = DependencyRegion::Metadata(file_id);
    let mut future = std::pin::pin!(async_probe.probe_async(
        generation,
        &metadata_region,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory-backed asynchronous dependency probe remained pending".into());
    };
    assert_eq!(asynchronous?.value, first.value);

    let cancelled_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(cancelled_probe.probe_async(
        generation,
        &metadata_region,
        WorkBudget::UNBOUNDED,
        &cancelled,
    ));
    let Poll::Ready(cancelled_result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous dependency probe remained pending".into());
    };
    let failure = cancelled_result
        .err()
        .ok_or("pre-cancelled asynchronous dependency probe unexpectedly succeeded")?;
    assert!(matches!(failure.error, AuthenticatedProbeError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn poisoned_summary_caches_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, _) = fixture(&store, None).await?;
    let region = DependencyRegion::Metadata(file_id);

    let generation_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _async_guard = generation_probe
            .async_generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison generation summary cache"));
    }));
    assert!(poisoned.is_err());
    let cancellation = CancellationToken::new();
    let failure =
        (generation_probe.probe_async(generation, &region, WorkBudget::UNBOUNDED, &cancellation))
            .await
            .err()
            .ok_or("poisoned asynchronous generation cache probe succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::CachePoisoned
    ));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);

    let record_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _async_guard = record_probe
            .async_records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison record summary cache"));
    }));
    assert!(poisoned.is_err());
    let failure =
        (record_probe.probe_async(generation, &region, WorkBudget::UNBOUNDED, &cancellation))
            .await
            .err()
            .ok_or("poisoned asynchronous record cache probe succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::CachePoisoned
    ));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    Ok(())
}

#[tokio::test]
async fn inserted_directory_entry_conflicts_with_an_observed_empty_page()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (base, directory_id, _) = fixture(&store, None).await?;
    let (candidate, _, _) = fixture(&store, Some("phantom")).await?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let region = DependencyRegion::DirectoryRange {
        directory_id,
        after: None,
        maximum_entries: 8,
    };
    let captured = probe
        .probe_async(
            base,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    let dependencies = CheckoutDependencies::new(
        [Dependency {
            region: region.clone(),
            expected: captured.value,
        }],
        [],
        1,
    )?;
    let decision = classify_rebase_async(
        &probe,
        base,
        candidate,
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert!(matches!(
        decision.decision,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.len() == 1 && conflicts[0].region == region
    ));

    Ok(())
}

#[tokio::test]
async fn bounded_summary_cache_eviction_reauthenticates_without_changing_state()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation_a, file_id, metadata) = fixture(&store, None).await?;
    let (generation_b, _, _) = fixture(&store, Some("other")).await?;
    let probe = AuthenticatedGenerationProbe::new(
        &store,
        ProbeLimits {
            maximum_cached_generations: 1,
            maximum_cached_records: 1,
            ..ProbeLimits::default()
        },
    )?;
    let region = DependencyRegion::Metadata(file_id);
    let first = probe
        .probe_async(
            generation_a,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(first.value, DependencyState::Present(metadata.digest));
    assert!(first.work.backend_read_operations > 0);
    let cached = probe
        .probe_async(
            generation_a,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(cached.value, first.value);
    assert_eq!(cached.work.backend_read_operations, 0);
    let other = probe
        .probe_async(
            generation_b,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(other.value, first.value);
    assert!(other.work.backend_read_operations > 0);
    let reauthenticated = probe
        .probe_async(
            generation_a,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(reauthenticated.value, first.value);
    assert!(reauthenticated.work.backend_read_operations > 0);
    Ok(())
}

#[tokio::test]
async fn non_regular_dependency_regions_use_authenticated_record_state()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_ids) = non_regular_fixture(&store).await?;
    let name = LogicalName::new(NameEncoding::Utf8, b"not-a-directory-entry".to_vec(), 255)?;
    for file_id in file_ids {
        let baseline_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let baseline = baseline_probe
            .probe_async(
                generation,
                &DependencyRegion::FileRecord(file_id),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        let regions = [
            DependencyRegion::FileLength(file_id),
            DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 1,
            },
            DependencyRegion::SparseSeek {
                file_id,
                offset: 0,
                target: ExtentSeekTarget::Data,
            },
            DependencyRegion::DirectoryName {
                directory_id: file_id,
                name: name.clone(),
            },
            DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 1,
            },
        ];
        for region in regions {
            let asynchronous_probe =
                AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
            let asynchronous = (asynchronous_probe.probe_async(
                generation,
                &region,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            ))
            .await?;
            assert_eq!(asynchronous.value, baseline.value);
        }
    }
    Ok(())
}
