use super::*;

#[tokio::test]
async fn probe_limits_reject_oversized_dependencies_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(
        &store,
        ProbeLimits {
            maximum_content_payload_bytes: 8,
            maximum_directory_entries: 2,
            ..ProbeLimits::default()
        },
    )?;
    let generation = GenerationId::new(Digest::from_bytes([1; 32]));
    let file_id = FileId::from_bytes([2; 16]);
    let content = probe
        .probe_async(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 9,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized content dependency unexpectedly succeeded")?;
    assert!(matches!(
        content.error,
        AuthenticatedProbeError::ContentRangeTooLarge
    ));
    assert_eq!(*content.work, WorkCounters::default());

    let directory = probe
        .probe_async(
            generation,
            &DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 3,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized directory dependency unexpectedly succeeded")?;
    assert!(matches!(
        directory.error,
        AuthenticatedProbeError::DirectoryPageTooLarge
    ));
    assert_eq!(*directory.work, WorkCounters::default());

    let empty = probe
        .probe_async(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 0,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("empty content dependency unexpectedly succeeded")?;
    assert!(matches!(
        empty.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::EmptyContentRange)
    ));
    assert_eq!(*empty.work, WorkCounters::default());

    let overflow = probe
        .probe_async(
            generation,
            &DependencyRegion::ContentRange {
                file_id,
                offset: u64::MAX,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("overflowing content dependency unexpectedly succeeded")?;
    assert!(matches!(
        overflow.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::RangeOverflow)
    ));
    assert_eq!(*overflow.work, WorkCounters::default());

    let zero_page = probe
        .probe_async(
            generation,
            &DependencyRegion::DirectoryRange {
                directory_id: file_id,
                after: None,
                maximum_entries: 0,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("zero-entry directory dependency unexpectedly succeeded")?;
    assert!(matches!(
        zero_page.error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::ZeroDirectoryPageLimit)
    ));
    assert_eq!(*zero_page.work, WorkCounters::default());
    Ok(())
}

#[test]
fn every_zero_probe_bound_is_rejected_before_cache_or_backend_access() {
    let store = MemoryObjectStore::default();
    let defaults = ProbeLimits::default();
    let invalid = [
        ProbeLimits {
            maximum_cached_generations: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_cached_records: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_extent_spans: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_content_payload_bytes: 0,
            ..defaults
        },
        ProbeLimits {
            maximum_directory_entries: 0,
            ..defaults
        },
        ProbeLimits {
            decode: DecodeLimits {
                maximum_page_items: 0,
                ..defaults.decode
            },
            ..defaults
        },
    ];
    for limits in invalid {
        assert!(matches!(
            AuthenticatedGenerationProbe::new(&store, limits),
            Err(AuthenticatedProbeError::InvalidLimits)
        ));
    }
}

#[tokio::test]
async fn bounded_batch_capture_reuses_generation_and_record_frontiers()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None).await?;
    let regions = vec![
        DependencyRegion::Metadata(file_id),
        DependencyRegion::FileRecord(file_id),
        DependencyRegion::Metadata(file_id),
    ];
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let captured = (probe.capture_many_async(
        generation,
        regions.clone(),
        3,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;

    assert_eq!(captured.value.len(), 3);
    assert_eq!(captured.value[0].region, regions[0]);
    assert_eq!(captured.value[1].region, regions[1]);
    assert_eq!(captured.value[2], captured.value[0]);
    assert_eq!(
        captured.value[0].expected,
        DependencyState::Present(metadata.digest)
    );
    assert_eq!(captured.work.backend_read_operations, 2);
    assert!(captured.work.items_returned >= 3);
    assert!(captured.work.bytes_copied >= 3 * usize_to_u64(size_of::<Dependency>()));

    let rejected_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let rejected = (rejected_probe.capture_many_async(
        generation,
        regions,
        2,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("excessive batch unexpectedly succeeded")?;
    assert!(matches!(
        rejected.error,
        AuthenticatedProbeError::TooManyRegions { maximum: 2 }
    ));
    assert_eq!(*rejected.work, WorkCounters::UNCHARGED);
    Ok(())
}

#[tokio::test]
async fn authenticated_batch_admits_dependency_copy_before_output_push()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, metadata) = fixture(&store, None).await?;
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let record = probe
        .record_with_accounting(generation, file_id, &mut accounting, &cancellation)
        .await?;
    for budget in [
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            items_returned: 0,
            ..WorkBudget::UNBOUNDED
        },
    ] {
        let failure = (probe.capture_records_many_async(
            vec![(record, DependencyRegion::Metadata(file_id))],
            1,
            budget,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("output push exceeded its budget")?;
        assert!(matches!(
            failure.error,
            AuthenticatedProbeError::Work(WorkError::BudgetExceeded { .. })
        ));
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.items_returned, 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 1);
    }
    let copied = usize_to_u64(size_of::<Dependency>());
    let receipt = (probe.capture_records_many_async(
        vec![(record, DependencyRegion::Metadata(file_id))],
        1,
        WorkBudget {
            bytes_copied: copied,
            items_returned: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    ))
    .await?;
    assert_eq!(receipt.value.len(), 1);
    assert_eq!(
        receipt.value[0].expected,
        DependencyState::Present(metadata.digest)
    );
    assert_eq!(receipt.work.bytes_copied, copied);
    assert_eq!(receipt.work.items_returned, 1);
    assert_eq!(receipt.work.backend_read_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
struct PanicProbeStore<'a> {
    store: MemoryObjectStore,
    original: &'a crate::NativeMaterializationWork,
    reads: std::sync::atomic::AtomicU64,
    reached: Mutex<Option<(u64, WorkCounters)>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsyncObjectStore for PanicProbeStore<'_> {
    async fn put(
        &self,
        object: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = {
            let mut work = crate::AuthorityWork::new(budget, cancellation);
            let result = AsyncObjectStore::put(&self.store, object, bytes, &mut work).await;
            work.receipt(result)
        };
        context.accept(result)
    }

    async fn read(
        &self,
        object: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;
            return async {
                cancellation
                    .check()
                    .map_err(|_| ObjectStoreError::Cancelled)?;
                work.admit(WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(ObjectStoreError::Native)?;
                self.reads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if object.kind == ObjectKind::FileTablePage {
                    *self
                        .reached
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((
                        self.original.live_allocation_bytes(),
                        self.original.receipt(),
                    ));
                    std::panic::resume_unwind(Box::new("cold original authenticated probe read"));
                }
                {
                    let mut work = crate::AuthorityWork::native(work, cancellation);
                    AsyncObjectStore::read(&self.store, object, maximum_bytes, &mut work).await
                }
            }
            .await;
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            {
                let mut work = crate::AuthorityWork::new(budget, cancellation);
                let result =
                    AsyncObjectStore::read(&self.store, object, maximum_bytes, &mut work).await;
                work.receipt(result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, context).await
    }

    async fn contains(
        &self,
        object: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = {
            let mut work = crate::AuthorityWork::new(budget, cancellation);
            let result = AsyncObjectStore::contains(&self.store, object, &mut work).await;
            work.receipt(result)
        };
        context.accept(result)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_probe_denies_before_backend_and_retires_panic_temporaries()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, file_id, _) = fixture(&store, None).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            backend_read_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let store = PanicProbeStore {
        store,
        original: &original,
        reads: std::sync::atomic::AtomicU64::new(0),
        reached: Mutex::new(None),
    };
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
        &store,
        ProbeLimits::default(),
        &mut accounting,
    )?;
    let region = DependencyRegion::Metadata(file_id);
    let denied = (crate::kernel::rebase::AccountingRebaseProbe::probe_with_accounting(
        &probe,
        generation,
        &region,
        &mut accounting,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero read budget accepted")?;
    assert!(matches!(
        denied,
        crate::kernel::rebase::ClassifyRebaseFailure::Accounting(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert_eq!(store.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    probe.retire_with_accounting(&mut accounting)?;
    drop(constructor);
    assert_eq!(original.live_allocation_bytes(), baseline);

    let store = store.store;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let store = PanicProbeStore {
        store,
        original: &original,
        reads: std::sync::atomic::AtomicU64::new(0),
        reached: Mutex::new(None),
    };
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
        &store,
        ProbeLimits::default(),
        &mut accounting,
    )?;
    let panic = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
        (probe.probe_async_with_accounting(generation, &region, &mut accounting, &cancellation))
            .await
    }))
    .await;
    assert!(panic.is_err());
    let (reached_live, reached_work) = store
        .reached
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .ok_or("original file-table read was not reached")?;
    let cached = probe
        .async_generations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entries
        .bytes
        + probe
            .async_records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .bytes;
    assert_eq!(original.live_allocation_bytes(), baseline + cached);
    assert!(reached_live > original.live_allocation_bytes());
    assert_eq!(original.receipt(), reached_work);
    assert_eq!(original.receipt().backend_read_operations, 1);
    cancellation.cancel();
    probe.retire_with_accounting(&mut accounting)?;
    drop(constructor);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_probe_generation_caches_preserve_dependency_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (base, directory_id, _) = fixture(&store, None).await?;
    let (candidate, _, _) = fixture(&store, Some("phantom")).await?;
    let region = DependencyRegion::DirectoryRange {
        directory_id,
        after: None,
        maximum_entries: 8,
    };
    let ordinary = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let expected = ordinary
        .probe_async(
            base,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let dependencies = CheckoutDependencies::new(
        [Dependency {
            region: region.clone(),
            expected,
        }],
        [],
        1,
    )?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
        &store,
        ProbeLimits {
            maximum_cached_generations: 1,
            maximum_cached_records: 1,
            ..ProbeLimits::default()
        },
        &mut accounting,
    )?;
    let source =
        (probe.probe_async_with_accounting(base, &region, &mut accounting, &cancellation)).await?;
    let destination =
        (probe.probe_async_with_accounting(candidate, &region, &mut accounting, &cancellation))
            .await?;
    assert_eq!(source, expected);
    assert_ne!(destination, source);
    let source_again =
        (probe.probe_async_with_accounting(base, &region, &mut accounting, &cancellation)).await?;
    assert_eq!(source_again, source);
    let (decision, allocation) = (crate::kernel::rebase::classify_rebase_with_accounting(
        &probe,
        base,
        candidate,
        &dependencies,
        1,
        &mut accounting,
        &cancellation,
    ))
    .await
    .map_err(|failure| -> Box<dyn std::error::Error> {
        match failure {
            crate::kernel::rebase::ClassifyRebaseFailure::Accounting(error)
            | crate::kernel::rebase::ClassifyRebaseFailure::Probe(error) => Box::new(error),
            crate::kernel::rebase::ClassifyRebaseFailure::ZeroConflictLimit => {
                "zero conflict limit".into()
            }
        }
    })?;
    assert!(
        matches!(&decision, RebaseDecision::Conflicted { conflicts, truncated: false }
        if conflicts.len() == 1 && conflicts[0].region == region
            && conflicts[0].expected == source && conflicts[0].actual == destination)
    );
    drop(decision);
    drop(allocation);
    probe.retire_with_accounting(&mut accounting)?;
    drop(constructor);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn stack_dependency_denial_precedes_array_encoding() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    for budget in [
        WorkBudget {
            bytes_encoded: 0,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            bytes_hashed: 0,
            ..WorkBudget::UNBOUNDED
        },
    ] {
        let reached = std::cell::Cell::new(false);
        let mut accounting = KernelAccounting::standalone(budget, &cancellation);
        let denied = capture_stack_state_with_accounting(
            || {
                reached.set(true);
                [0_u8; 10]
            },
            b"acyclic-fs-dependency-sparse-seek-v1\0",
            &mut accounting,
            &cancellation,
            None,
        )
        .err()
        .ok_or("stack dependency exceeded its admission")?;
        assert!(matches!(
            denied,
            crate::FsError::Work(WorkError::BudgetExceeded { .. })
        ));
        assert!(!reached.get());
        assert_eq!(accounting.spent(), WorkCounters::UNCHARGED);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_stack_encoding_retains_completed_prefix_on_late_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = capture_stack_state_with_accounting(
        || {
            cancellation.cancel();
            [0_u8; 10]
        },
        b"acyclic-fs-dependency-sparse-seek-v1\0",
        &mut accounting,
        &cancellation,
        None,
    );
    assert!(result.is_err());
    assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded + 10);
    assert_eq!(original.receipt().bytes_hashed, before.bytes_hashed);
    assert_eq!(
        original.receipt().allocation_operations,
        before.allocation_operations
    );
    Ok(())
}
