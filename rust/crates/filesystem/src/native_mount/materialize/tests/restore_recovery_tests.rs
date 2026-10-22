use super::*;

pub(super) const FINITE_MATERIALIZATION_BUDGET: WorkBudget =
    crate::performance::work_budget(100_000, 64 * 1024 * 1024);

struct ObservedMaterializeStore {
    inner: crate::MemoryObjectStore,
    completed_reads: Arc<AtomicU64>,
}

impl AsyncObjectStore for ObservedMaterializeStore {
    fn supports_decoded_cache(&self) -> bool {
        false
    }

    async fn put(
        &self,
        id: crate::ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }

    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        let result = self.inner.read_many(requests, work).await?;
        self.completed_reads.fetch_add(1, Ordering::AcqRel);
        Ok(result)
    }

    async fn contains(
        &self,
        id: crate::ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        let result = self.inner.contains(id, work).await?;
        self.completed_reads.fetch_add(1, Ordering::AcqRel);
        Ok(result)
    }

    async fn read(
        &self,
        id: crate::ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        let result = self.inner.read(id, maximum, work).await?;
        // This observes a completed provider call independently of its receipt.
        self.completed_reads.fetch_add(1, Ordering::AcqRel);
        Ok(result)
    }
}

struct ObservedReadReceipt {
    work: WorkCounters,
    source_reads: u64,
}

fn materialization_reads_match_actual_source(
    reported: &ObservedReadReceipt,
    baseline: &ObservedReadReceipt,
) -> bool {
    // Both traversals finish exactly the same first file and host operations.
    // Only actual source reads, including cache/staged hits doing no backend
    // work, may change the aggregate native-plus-source read receipt.
    u128::from(reported.work.backend_read_operations) + u128::from(baseline.source_reads)
        == u128::from(baseline.work.backend_read_operations) + u128::from(reported.source_reads)
}

async fn observe_multipath_failure<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    first: &NamespacePath,
    missing: &NamespacePath,
    completed_reads: &AtomicU64,
    cancellation: &CancellationToken,
) -> Result<(ObservedReadReceipt, ObservedReadReceipt), Box<dyn std::error::Error>> {
    let baseline = tempfile::tempdir()?;
    completed_reads.store(0, Ordering::Release);
    let completed = materialize_checkout_paths(
        checkout,
        std::slice::from_ref(first),
        &MaterializeOptions::native(baseline.path()),
        FINITE_MATERIALIZATION_BUDGET,
        cancellation,
    )
    .await?;
    let completed = ObservedReadReceipt {
        work: completed.work,
        source_reads: completed_reads.swap(0, Ordering::AcqRel),
    };
    let output = tempfile::tempdir()?;
    let failed = materialize_checkout_paths(
        checkout,
        &[first.clone(), missing.clone()],
        &MaterializeOptions::native(output.path()),
        FINITE_MATERIALIZATION_BUDGET,
        cancellation,
    )
    .await
    .err()
    .ok_or("missing second path was accepted")?;
    let failed_source_reads = completed_reads.swap(0, Ordering::AcqRel);
    assert!(matches!(failed.error, MaterializeError::MissingPath));
    assert_eq!(std::fs::read(output.path().join("a-present"))?, b"first");
    let failed = ObservedReadReceipt {
        work: *failed.work,
        source_reads: failed_source_reads,
    };
    eprintln!(
        "materialize actual source reads baseline={} missing={}; reported total reads baseline={} missing={}",
        completed.source_reads,
        failed.source_reads,
        completed.work.backend_read_operations,
        failed.work.backend_read_operations,
    );
    assert!(materialization_reads_match_actual_source(&failed, &completed));
    assert_eq!(completed.work.output_bytes, 5);
    assert_eq!(failed.work.output_bytes, completed.work.output_bytes);
    failed.work.verify(FINITE_MATERIALIZATION_BUDGET)?;
    Ok((completed, failed))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn runtime_write_preserves_the_actual_os_error_and_unchanged_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("read-only-handle");
    std::fs::write(&path, b"before")?;
    let mut ordinary = File::open(&path)?;
    let expected = std::io::Write::write(&mut ordinary, b"after")
        .err()
        .ok_or("ordinary read-only handle accepted a write")?;
    let expected_code = expected.raw_os_error().ok_or("control did not reach the OS")?;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
            .map_err(|failure| MaterializeError::from(failure.0))?;
    let mut receipt = GenerationReceipt::from_work(&original)?;
    let native = acyclic_native_runtime::NativeFile::from_file(
        File::open(&path)?,
        Some(acyclic_native_runtime::NativeFileMeter::Owned(original.runtime_meter())),
    )
    .map_err(|(error, file)| {
        drop(file);
        original.runtime_error(error)
    })?;
    let mut before_effect =
        None::<&mut fn(_) -> std::future::Ready<Result<(), MaterializeError>>>;
    let failure = write_generation_bytes(
        &native,
        0,
        acyclic_stream::OwnedBytes::from_static(b"after"),
        0,
        &mut receipt,
        &mut before_effect,
    )
    .await
    .err()
    .ok_or("native read-only handle accepted a write")?;
    let MaterializeError::Io(actual) = failure else {
        return Err(format!("actual OS error was replaced: {failure:?}").into());
    };
    assert_eq!(actual.raw_os_error(), Some(expected_code));
    assert_eq!(actual.kind(), expected.kind());
    assert_eq!(std::fs::read(&path)?, b"before");
    assert_eq!(original.receipt().output_bytes, 0);
    original.receipt().verify(FINITE_MATERIALIZATION_BUDGET)?;
    Ok(())
}

#[tokio::test]
async fn metadata_offload_keeps_original_cancellation_without_starting_host_io()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"unchanged")?;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
            .map_err(|failure| MaterializeError::from(failure.0))?;
    let mut receipt = GenerationReceipt::from_work(&original)?;
    let root = HostRoot::open(directory.path(), &mut original.clone())?;
    let root = receipt.owned_arc(root, 0)?;
    let name = LogicalName::new(
        NameEncoding::Utf8,
        b"file".to_vec(),
        crate::VolumeLimits::default().maximum_component_bytes,
    )?;
    let path = materialize_host_path(None, std::slice::from_ref(&name), &mut receipt)?;
    let before = original.receipt();
    cancellation.cancel();
    let failure =
        apply_host_metadata_offloaded(&root, &path, FileMetadata::default(), &mut receipt)
            .await
            .err()
            .ok_or("metadata offload ignored original cancellation")?;
    assert!(matches!(failure, MaterializeError::Cancelled(_)), "{failure:?}");
    let after = original.receipt();
    assert_eq!(after.backend_read_operations, before.backend_read_operations);
    assert_eq!(after.backend_write_operations, before.backend_write_operations);
    assert_eq!(after.output_bytes, before.output_bytes);
    assert_eq!(std::fs::read(directory.path().join("file"))?, b"unchanged");
    after.verify(FINITE_MATERIALIZATION_BUDGET)?;
    Ok(())
}

#[test]
fn replaced_root_is_rejected_before_destination_enumeration()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("root");
    std::fs::create_dir(&destination)?;
    let original = HostRoot::open(&destination, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?
        .identity();
    std::fs::rename(&destination, temporary.path().join("original"))?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("foreign"), b"untouched")?;
    let actual = HostRoot::open(&destination, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?
        .identity();
    assert_ne!(original, actual);
    let cancellation = CancellationToken::new();
    let mut receipt = GenerationReceipt::from_work(
        &crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
            .map_err(|failure| MaterializeError::from(failure.0))?,
    )?;
    let failure = validate_options(
        &MaterializeOptions {
            expected_root_identity: Some(original),
            ..MaterializeOptions::native(&destination)
        },
        &mut receipt,
    )
    .err()
    .ok_or("replacement was accepted")?;
    assert!(
        matches!(failure, MaterializeError::AllocationChanged { expected, actual: found }
        if expected == original && found == actual)
    );
    assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
    Ok(())
}

#[tokio::test]
async fn multipath_failure_retains_completed_host_and_lookup_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{Lifecycle, VolumeConfig, VolumeLimits};
    let cancellation = CancellationToken::new();
    let limits = VolumeLimits::default();
    let completed_reads = Arc::new(AtomicU64::new(0));
    let fs = crate::Fs::new(
        crate::MemoryAuthorityStore::default(),
        ObservedMaterializeStore {
            inner: crate::MemoryObjectStore::default(),
            completed_reads: completed_reads.clone(),
        },
        crate::EmbeddedCapabilities::MEMORY,
    );
    let volume = fs
        .create_volume_with_id(
            crate::VolumeId::new(),
            VolumeConfig {
                limits,
                ..VolumeConfig::portable(Lifecycle::Ephemeral)
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
    let first = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"a-present".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let missing = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"z-missing".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    checkout
        .create(
            crate::AuthoredMutation::CreateFile {
                path: first.clone(),
                bytes: (Bytes::from_static(b"first")).into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let zero_output = tempfile::tempdir()?;
    let zero = materialize_checkout(
        &mut checkout,
        &MaterializeOptions::native(zero_output.path()),
        WorkBudget::default(),
        &cancellation,
    )
    .await
    .err()
    .ok_or("zero materialization budget was accepted")?;
    assert!(matches!(zero.error, MaterializeError::Work(_)));
    assert_eq!(*zero.work, WorkCounters::UNCHARGED);
    assert!(std::fs::read_dir(zero_output.path())?.next().is_none());

    let no_read_output = tempfile::tempdir()?;
    completed_reads.store(0, Ordering::Release);
    let no_read = materialize_checkout_paths(
        &mut checkout,
        &[first.clone(), missing.clone()],
        &MaterializeOptions::native(no_read_output.path()),
        WorkBudget {
            backend_read_operations: 0,
            ..FINITE_MATERIALIZATION_BUDGET
        },
        &cancellation,
    )
    .await
    .err()
    .ok_or("zero native-read allowance was accepted")?;
    assert!(matches!(no_read.error, MaterializeError::Work(_)));
    assert_eq!(no_read.work.backend_read_operations, 0);
    assert_eq!(no_read.work.backend_write_operations, 0);
    assert_eq!(no_read.work.output_bytes, 0);
    assert!(std::fs::read_dir(no_read_output.path())?.next().is_none());
    assert_eq!(completed_reads.load(Ordering::Acquire), 0);

    let cancelled_output = tempfile::tempdir()?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled = materialize_checkout(
        &mut checkout,
        &MaterializeOptions::native(cancelled_output.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancelled,
    )
    .await
    .err()
    .ok_or("cancelled materialization was accepted")?;
    assert!(matches!(cancelled.error, MaterializeError::Cancelled(_)));
    assert_eq!(*cancelled.work, WorkCounters::UNCHARGED);
    assert!(std::fs::read_dir(cancelled_output.path())?.next().is_none());

    let no_write_output = tempfile::tempdir()?;
    let no_write = materialize_checkout(
        &mut checkout,
        &MaterializeOptions::native(no_write_output.path()),
        WorkBudget {
            backend_write_operations: 0,
            ..FINITE_MATERIALIZATION_BUDGET
        },
        &cancellation,
    )
    .await
    .err()
    .ok_or("zero native-write budget was accepted")?;
    assert!(matches!(no_write.error, MaterializeError::Work(_)));
    assert_eq!(no_write.work.backend_write_operations, 0);
    assert_eq!(no_write.work.output_bytes, 0);
    assert!(no_write.work.backend_read_operations > 0);
    assert!(no_write.work.allocation_operations > 0);
    assert!(std::fs::read_dir(no_write_output.path())?.next().is_none());

    let no_sync_output = tempfile::tempdir()?;
    let no_sync = materialize_checkout_paths(
        &mut checkout,
        std::slice::from_ref(&first),
        &MaterializeOptions::native(no_sync_output.path()),
        WorkBudget {
            durability_operations: 0,
            ..FINITE_MATERIALIZATION_BUDGET
        },
        &cancellation,
    )
    .await
    .err()
    .ok_or("native file sync exceeded its original durability allowance")?;
    assert!(
        matches!(
            no_sync.error,
            MaterializeError::Work(WorkError::BudgetExceeded {
                counter: "durability_operations",
                maximum: 0,
                ..
            })
        ),
        "{:?}",
        no_sync.error
    );
    assert_eq!(std::fs::read(no_sync_output.path().join("a-present"))?, b"first");
    assert_eq!(no_sync.work.output_bytes, 5);
    assert_eq!(no_sync.work.durability_operations, 0);

    // First retain the original unpublished-candidate scenario: staged/cache
    // hits must not be charged as backend reads that never happened.
    observe_multipath_failure(
        &mut checkout,
        &first,
        &missing,
        &completed_reads,
        &cancellation,
    )
    .await?;

    // Publish and reopen, so this second scenario exercises actual backing-store
    // reads rather than assuming an unpublished mutation needs provider I/O.
    checkout
        .commit(
            crate::OperationId::new(),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, OperationFailure<crate::FsError>>(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?;
    drop(checkout);
    let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
    let (backed_baseline, backed_failure) = observe_multipath_failure(
        &mut checkout,
        &first,
        &missing,
        &completed_reads,
        &cancellation,
    )
    .await?;
    assert!(backed_failure.source_reads > backed_baseline.source_reads);

    // Negative control: omit the source work that the independent provider
    // observer actually saw. The identical accounting oracle must reject it.
    let mut omitted = backed_failure;
    omitted.work.backend_read_operations -= omitted.source_reads;
    assert!(!materialization_reads_match_actual_source(
        &omitted,
        &backed_baseline,
    ));

    // Cancellation after a real host write must not erase the write or charge
    // the missing path's unstarted lookup/output.
    let late_cancellation = CancellationToken::new();
    let late_work =
        crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &late_cancellation)
            .map_err(|failure| MaterializeError::from(failure.0))?;
    let late_output = tempfile::tempdir()?;
    let late_file = late_output.path().join("a-present");
    let mut cancelled_after_write = false;
    let mut cancel_after_write = |phase| {
        if phase == NativeMaterializationActionPhase::Completed
            && std::fs::read(&late_file).is_ok_and(|bytes| bytes == b"first")
        {
            cancelled_after_write = true;
            late_cancellation.cancel();
        }
        std::future::ready(Ok::<(), MaterializeError>(()))
    };
    let late_error = materialize_source(
        GenerationSource::Standalone(&mut checkout),
        &[first.clone(), missing.clone()],
        &MaterializeOptions::native(late_output.path()),
        MaterializeMode::DurableOutput,
        GenerationReceipt::from_work(&late_work)?,
        Some(&mut cancel_after_write),
    )
    .await
    .err()
    .ok_or("post-write cancellation was ignored")?;
    assert!(cancelled_after_write, "completed physical write was never observed");
    assert!(matches!(late_error, MaterializeError::Cancelled(_)), "{late_error:?}");
    assert_eq!(std::fs::read(late_output.path().join("a-present"))?, b"first");
    assert!(!late_output.path().join("z-missing").exists());
    let late_spent = late_work.receipt();
    assert_eq!(late_spent.output_bytes, 5);
    assert!(late_spent.backend_read_operations > 0);
    assert!(late_spent.backend_write_operations > 0);
    late_spent.verify(FINITE_MATERIALIZATION_BUDGET)?;
    let original = GenerationReceipt::from_work(
        &crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
            .map_err(|failure| MaterializeError::from(failure.0))?,
    )?;
    let candidate = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(
            original.custody.work(),
            &cancellation,
        ),
        &cancellation,
    );
    let candidate = candidate?;
    let reader =
        candidate.into_pinned_reader_with_native_work(original.custody.work(), &cancellation);
    let reader = reader?;
    let prefix = original.custody.work().receipt();
    let original_work = original.custody.work().clone();
    let original_output = tempfile::tempdir()?;
    let original_failed = materialize_reader_paths(
        &reader,
        &[first.clone(), missing.clone()],
        &MaterializeOptions::native(original_output.path()),
        MaterializeMode::DurableOutput,
        &original_work,
    )
    .await
    .err()
    .ok_or("original source accepted a missing second path")?;
    assert!(matches!(original_failed, MaterializeError::MissingPath));
    assert_eq!(
        std::fs::read(original_output.path().join("a-present"))?,
        b"first"
    );
    let original_spent = original.custody.work().receipt();
    assert!(original_spent.allocation_operations > prefix.allocation_operations);
    assert!(original_spent.backend_read_operations > prefix.backend_read_operations);
    assert_eq!(original_spent.output_bytes, 5);

    #[derive(Debug, Error)]
    enum TaskFenceFailure {
        #[error("task fence expired before the next logical action")]
        Owner,
        #[error(transparent)]
        Materialize(#[from] MaterializeError),
    }
    for deny_before_output in [true, false] {
        let guarded_output = tempfile::tempdir()?;
        let before = original.custody.work().receipt();
        let mut task_fence = |phase| {
            let denied = phase == NativeMaterializationActionPhase::Before
                && (deny_before_output
                    || original.custody.work().receipt().output_bytes > before.output_bytes);
            std::future::ready(if denied {
                Err(TaskFenceFailure::Owner)
            } else {
                Ok(())
            })
        };
        let error = materialize_checkout_host_paths_with_work_guarded(
            &reader,
            &["a-present".to_owned()],
            &MaterializeOptions::native(guarded_output.path()),
            &original_work,
            &mut task_fence,
        )
        .await
        .err()
        .ok_or("logical action bypassed the original task fence")?;
        assert!(matches!(error, TaskFenceFailure::Owner));
        let after = original.custody.work().receipt();
        after.verify(FINITE_MATERIALIZATION_BUDGET)?;
        assert!(after.backend_read_operations > before.backend_read_operations);
        if deny_before_output {
            assert!(!guarded_output.path().join("a-present").exists());
            assert_eq!(after.output_bytes, before.output_bytes);
        } else {
            assert_eq!(
                std::fs::read(guarded_output.path().join("a-present"))?,
                b"first"
            );
            assert!(after.backend_write_operations > before.backend_write_operations);
            assert!(after.output_bytes >= before.output_bytes + 5);
        }
    }

    let no_write = GenerationReceipt::from_work(
        &crate::NativeMaterializationWork::new(
            WorkBudget {
                backend_write_operations: 0,
                ..FINITE_MATERIALIZATION_BUDGET
            },
            &cancellation,
        )
        .map_err(|failure| MaterializeError::from(failure.0))?,
    )?;
    let candidate = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(
            no_write.custody.work(),
            &cancellation,
        ),
        &cancellation,
    );
    let candidate = candidate?;
    let reader =
        candidate.into_pinned_reader_with_native_work(no_write.custody.work(), &cancellation);
    let reader = reader?;
    let no_write_output = tempfile::tempdir()?;
    let no_write_work = no_write.custody.work().clone();
    let original_no_write = materialize_reader_paths(
        &reader,
        std::slice::from_ref(&first),
        &MaterializeOptions::native(no_write_output.path()),
        MaterializeMode::DurableOutput,
        &no_write_work,
    )
    .await
    .err()
    .ok_or("original source ignored zero native-write allowance")?;
    assert!(matches!(original_no_write, MaterializeError::Work(_)));
    let original_spent = no_write.custody.work().receipt();
    assert_eq!(original_spent.backend_write_operations, 0);
    assert_eq!(original_spent.output_bytes, 0);
    assert!(std::fs::read_dir(no_write_output.path())?.next().is_none());

    Ok(())
}

#[tokio::test]
async fn restore_removes_missing_path() -> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{Lifecycle, VolumeConfig};
    let cancellation = CancellationToken::new();
    let volume = super::super::tests::memory_volume(
        crate::VolumeId::new(),
        VolumeConfig::portable(Lifecycle::Ephemeral),
        &cancellation,
    )
    .await?;
    let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
    let temporary = tempfile::tempdir()?;
    let relative = Path::new("private-restore-path-sentinel");
    std::fs::write(temporary.path().join(relative), b"private-content-sentinel")?;
    let receipt = restore_checkout_host_path(
        &mut checkout,
        relative,
        HostPathReplacement::Atomic,
        &MaterializeOptions::native(temporary.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(receipt.value, HostPathRestore::Removed);
    assert!(!temporary.path().join(relative).exists());
    let invalid = restore_checkout_host_path(
        &mut checkout,
        Path::new(""),
        HostPathReplacement::Atomic,
        &MaterializeOptions::native(temporary.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await;
    assert!(
        matches!(invalid, Err(failure) if matches!(failure.error, MaterializeError::InvalidPath))
    );
    Ok(())
}

/// A file created through a Linux mount records "no flags" (and its own birth and change times); materializing it must not refuse any of those, or no fork's new file could ever reach the root.
#[cfg(unix)]
#[test]
fn host_projection_drops_what_linux_cannot_or_need_not_apply() {
    use crate::kernel::MetadataField;
    let created = FileMetadata {
        posix_flags: MetadataField::Value(0),
        created_ns: MetadataField::Value(1),
        changed_ns: MetadataField::Value(2),
        ..FileMetadata::default()
    };
    let projected = host_metadata(created).0;
    assert_eq!(projected.changed_ns, MetadataField::Unavailable);
    #[cfg(target_os = "linux")]
    {
        assert_eq!(projected.posix_flags, MetadataField::Unavailable);
        assert_eq!(projected.created_ns, MetadataField::Unavailable);
        let flagged = FileMetadata {
            posix_flags: MetadataField::Value(1),
            ..FileMetadata::default()
        };
        assert_eq!(
            host_metadata(flagged).0.posix_flags,
            MetadataField::Value(1)
        );
    }
}

#[tokio::test]
async fn native_materialization_rejects_unrepresentable_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::MetadataField;

    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"data")?;
    let cancellation = CancellationToken::new();
    let mut receipt = GenerationReceipt::from_work(
        &crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
            .map_err(|failure| MaterializeError::from(failure.0))?,
    )?;
    let root = HostRoot::open(temporary.path(), &mut receipt.custody.work().clone())?;
    let root = receipt.owned_arc(root, 0)?;
    let limits = crate::model::VolumeLimits::default();
    let namespace = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"file".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let host_path = materialize_host_path(None, namespace.components(), &mut receipt)?;
    #[cfg(unix)]
    for metadata in [
        FileMetadata {
            posix_flags: MetadataField::Value(1),
            ..FileMetadata::default()
        },
        FileMetadata {
            changed_ns: MetadataField::Value(123),
            ..FileMetadata::default()
        },
    ] {
        assert!(matches!(
            apply_host_metadata_offloaded(&root, &host_path, metadata, &mut receipt).await,
            Err(MaterializeError::UnsupportedMetadata(_))
        ));
    }
    #[cfg(windows)]
    let metadata = FileMetadata {
        posix_mode: MetadataField::Value(0o644),
        ..FileMetadata::default()
    };
    #[cfg(windows)]
    assert!(matches!(
        apply_host_metadata_offloaded(&root, &host_path, metadata, &mut receipt).await,
        Err(failure) if matches!(failure, MaterializeError::UnsupportedMetadata(_))
    ));
    Ok(())
}

fn restoration_work() -> crate::demand::DemandWork {
    crate::demand::DemandWork::new(
        WorkBudget {
            backend_read_operations: 10_000,
            backend_write_operations: 10_000,
            durability_operations: 10_000,
            bytes_hashed: 1_000_000,
            bytes_copied: 1_000_000,
            bytes_encoded: 1_000_000,
            source_bytes_read: 1_000_000,
            source_path_components: 10_000,
            source_entries_visited: 10_000,
            output_bytes: 1_000_000,
            items_examined: 10_000,
            allocation_operations: 10_000,
            peak_allocation_bytes: 1_000_000,
            ..WorkCounters::UNCHARGED
        },
        &CancellationToken::new(),
    )
}

#[test]
fn restore_preparation_rejects_a_swapped_root_before_any_write()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("root");
    std::fs::create_dir(&destination)?;
    let expected = HostRoot::open(&destination, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?
        .identity();
    std::fs::rename(&destination, temporary.path().join("original"))?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("foreign"), b"untouched")?;
    let original = Arc::new(NativeDemandReadMeter::new(restoration_work()));
    let native_work = crate::NativeMaterializationWork::from_meter(Arc::clone(&original));
    let allocation_work = native_work.clone();
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let failure = prepare_restore(
        allocation_work
            .path(&destination, None)
            .map_err(MaterializeError::from)?,
        allocation_work
            .path(Path::new("nested/entry"), None)
            .map_err(MaterializeError::from)?,
        Some(expected),
        HostPathReplacement::Atomic,
        &mut work,
    )
    .err()
    .ok_or("swapped restore root was accepted")?;
    assert!(
        matches!(failure, MaterializeError::AllocationChanged { expected: found, .. } if found == expected)
    );
    let spent = original.with_original_leaf(|work| work.work);
    assert!(spent.backend_read_operations > 0);
    assert_eq!(spent.backend_write_operations, 0);
    assert_eq!(spent.durability_operations, 0);
    assert_eq!(allocation_work.live_allocation_bytes(), 0);
    assert!(!destination.join("nested").exists());
    assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
    Ok(())
}

#[test]
fn restore_removal_retains_the_original_physical_parent() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("root");
    let original = temporary.path().join("original");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("entry"), b"old")?;
    let mut work = restoration_work();
    let root = HostRoot::open(&destination, &mut work)?;
    let parent = held_parent(&root, Path::new("entry"), &mut work)?;
    std::fs::rename(&destination, &original)?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("entry"), b"foreign")?;
    remove_restored_path(&parent, Path::new("entry"), Path::new("entry"), &mut work)?;
    assert!(!original.join("entry").exists());
    assert_eq!(std::fs::read(destination.join("entry"))?, b"foreign");
    assert!(work.work.backend_write_operations > 0);
    assert!(work.work.durability_operations > 0);
    Ok(())
}

#[test]
fn same_path_restores_are_serialized_by_a_process_crash_safe_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let relative = Path::new("entry");
    let mut work = restoration_work();
    let held_root = HostRoot::open(&root, &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;
    let first = acquire_restore_lock(&parent, relative, true, &mut work)?;

    assert!(acquire_restore_lock(&parent, relative, false, &mut work).is_err());
    drop(first);
    // Blocking: a child another test forks shares the lock's open file
    // until it execs, so a release can take a moment to be observed.
    let _next = acquire_restore_lock(&parent, relative, true, &mut work)?;
    Ok(())
}

#[test]
fn blocking_restore_retries_real_platform_lock_contention()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let relative = Path::new("entry");
    let mut work = restoration_work();
    let root = HostRoot::open(temporary.path(), &mut work)?;
    let parent = held_parent(&root, relative, &mut work)?;
    let mut held = Some(acquire_restore_lock(&parent, relative, true, &mut work)?);
    let name = restore_artifact_name(".acyclic-restore-lock-", relative, &mut work)?;
    let contender = crate::native_host::open_leaf(
        &parent.directory,
        &name,
        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
        &mut work,
    )?;
    let mut attempts = 0;
    let _acquired = lock_restore_file(contender, true, &mut work, |file| {
        attempts += 1;
        let result = fs2::FileExt::try_lock_exclusive(file);
        if attempts == 1 {
            assert_eq!(
                result.as_ref().err().and_then(std::io::Error::raw_os_error),
                fs2::lock_contended_error().raw_os_error()
            );
            // Release only after the OS has returned genuine contention.
            // An inherited descriptor must not extend the test's ownership.
            fs2::FileExt::unlock(held.as_ref().expect("first lock remains held"))?;
            drop(held.take());
        }
        result
    })?;
    assert_eq!(attempts, 2, "the contended attempt must be retried");
    Ok(())
}

#[tokio::test]
async fn cancelled_restore_waiter_keeps_lock_until_host_io_finishes()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let relative = Path::new("entry");
    let mut work = restoration_work();
    let held_root = HostRoot::open(temporary.path(), &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;
    let lock = acquire_restore_lock(&parent, relative, true, &mut work)?;
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let (finished, completion) = tokio::sync::oneshot::channel();
    let original = Arc::new(NativeDemandReadMeter::new(restoration_work()));
    let waiter = tokio::spawn(host_io(
        Some(lock),
        crate::NativeMaterializationWork::from_meter(Arc::clone(&original)),
        move |_| {
            let _ = started.send(());
            // Dropping release on an early test exit also unblocks the worker.
            blocked.recv().map_err(std::io::Error::other)?;
            let _ = finished.send(());
            Ok::<_, MaterializeError>(())
        },
    ));
    entered.await?;
    // Native waiting must not hold the original accounting leaf.
    original.with_original_leaf(|work| {
        work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })
    })?;
    assert_eq!(
        original.with_original_leaf(|work| work.work.items_examined),
        1
    );
    waiter.abort();
    let cancelled = waiter.await;
    let competing = acquire_restore_lock(&parent, relative, false, &mut work);
    let fenced = matches!(
        &competing,
        Err(MaterializeError::Io(error))
            if error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
    );
    drop(competing);
    // Release before assertions: even the broken-lock mutation must finish.
    release.send(())?;
    completion.await?;
    // Acquiring is the completion barrier for the worker's final lock drop.
    let _next = acquire_restore_lock(&parent, relative, true, &mut work)?;
    assert!(cancelled.is_err_and(|error| error.is_cancelled()));
    assert!(
        fenced,
        "cancelled waiter released a still-running restore's lock"
    );
    Ok(())
}

#[test]
fn live_mount_recovery_preserves_the_correct_entry() -> Result<(), Box<dyn std::error::Error>> {
    for published in [false, true] {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        let expected = if published {
            b"after".as_slice()
        } else {
            b"before"
        };
        std::fs::write(&destination, expected)?;
        let mut work = restoration_work();
        let held_root = HostRoot::open(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
        let witness = restore_artifact_name(".acyclic-restore-live-witness-", relative, &mut work)?;
        create_restore_witness(&parent, &witness, LIVE_RESTORE_WITNESS, &mut work)?;
        if published {
            std::fs::write(root.join(&backup), b"before")?;
        } else {
            rename_entry(&parent, relative, &parent, &backup, &mut work)?;
        }
        recover_live_mount_replacement(&parent, relative, relative, &mut work)?;
        assert_eq!(std::fs::read(&destination)?, expected);
        assert!(!root.join(backup).exists());
    }
    Ok(())
}

#[test]
fn live_mount_recovery_preserves_an_unowned_backup_collision()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let relative = Path::new("entry");
    let destination = root.join(relative);
    std::fs::write(&destination, b"live")?;
    let mut work = restoration_work();
    let held_root = HostRoot::open(&root, &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;
    let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
    std::fs::write(root.join(&backup), b"unrelated")?;

    assert!(recover_live_mount_replacement(&parent, relative, relative, &mut work).is_err());
    assert_eq!(std::fs::read(&destination)?, b"live");
    assert_eq!(std::fs::read(root.join(backup))?, b"unrelated");
    Ok(())
}

#[test]
fn absent_restore_moves_then_reclaims_the_old_entry() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let relative = Path::new("entry");
    std::fs::write(root.join(relative), b"old")?;
    let mut work = restoration_work();
    let held_root = HostRoot::open(&root, &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;

    remove_restored_path(&parent, relative, relative, &mut work)?;

    assert!(!root.join(relative).exists());
    assert!(
        !root
            .join(restore_artifact_name(
                ".acyclic-restore-removed-",
                relative,
                &mut work
            )?)
            .exists()
    );
    Ok(())
}
