use super::*;
use crate::demand::native::NativeDemandSource;
use crate::lazy_workspace::LazyReadonlyNode;
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::{Fs, MemoryLazyWorkspaceStore, PublicationPermit};

#[tokio::test]
async fn measured_customer_root_admission_keeps_unselected_source_cold()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_token = crate::CancellationToken::new();
    let fixture_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &fixture_token)
            .map_err(|(cause, _)| cause)?;
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let physical = tempfile::tempdir()?;
    std::fs::create_dir(physical.path().join("selected"))?;
    std::fs::write(
        physical.path().join("selected/cold.txt"),
        b"unread descendant",
    )?;
    std::fs::create_dir(physical.path().join("unselected"))?;
    std::fs::write(
        physical.path().join("unselected/cold.txt"),
        b"unread sibling",
    )?;
    let fs = Fs::memory();
    let demand = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(physical.path()).to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "measured-customer-root",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        source_bytes_read: 0,
        materializations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let options = MountOptions::read_write()
        .subdirectory("/selected")
        .publication(MountPublication::Manual);
    let work =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let (ordinary_source, volume, root) = lazy
        .prepare_authored_mount_source(&options, None, None, &work, None)
        .await?;
    assert_eq!(volume, lazy.workspace().id().volume_id());
    assert_eq!(root.0, "/selected");
    work.receipt().verify(budget)?;
    assert_eq!(work.receipt().source_bytes_read, 0);
    assert_eq!(work.receipt().materializations, 0);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(matches!(
        lazy.lookup("/selected").await?,
        LazyLookup::Authored { .. }
    ));
    assert!(matches!(
        lazy.lookup("/selected/cold.txt").await?,
        LazyLookup::Source(_)
    ));
    assert!(matches!(
        lazy.lookup("/unselected/cold.txt").await?,
        LazyLookup::Source(_)
    ));
    let pinned = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .id();
    let denied_budget = crate::WorkBudget {
        backend_read_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let denied = crate::NativeMaterializationWork::new(denied_budget, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(
        lazy.prepare_authored_mount_source(&options, Some(pinned), None, &denied, None)
            .await
            .is_err()
    );
    denied.receipt().verify(denied_budget)?;
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        pinned
    );
    let job = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(!job.shares_ledger(&work));
    let rejected_root = tempfile::tempdir()?;
    let before_binding = work.receipt();
    assert!(matches!(
        lazy.mount(
            rejected_root.path(),
            (options.clone()).native_context(&work, None, Some(&job))
        )
        .await,
        Err(MountLifecycleError::Source(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout,
        )))
    ));
    assert_eq!(work.receipt(), before_binding);
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        pinned
    );
    assert_eq!(std::fs::read_dir(rejected_root.path())?.count(), 0);
    let ordinary_job = job.with_source_io_lease(ordinary_source.native_source_io_lease()?)?;
    assert!(ordinary_job.shares_ledger(&job));
    assert!(ordinary_job.shares_resource_allowance(&job));
    assert!(!ordinary_job.shares_ledger(&work));
    drop(ordinary_job);
    ordinary_source.cancel_cleanup(Some(&crate::CancellationToken::new()))?;
    let (native_source, _, _) = lazy
        .prepare_authored_mount_source(&options, Some(pinned), None, &work, Some(&work))
        .await?;
    let before_foreign_job = job.receipt();
    assert!(matches!(
        job.with_source_io_lease(native_source.native_source_io_lease()?),
        Err(crate::NativeTreeMaterializationError::InvalidLayout)
    ));
    assert_eq!(job.receipt(), before_foreign_job);
    let native_job = work.with_source_io_lease(native_source.native_source_io_lease()?)?;
    assert!(native_job.shares_ledger(&work));
    assert!(native_job.shares_resource_allowance(&work));
    drop(native_job);
    native_source.cancel_cleanup(Some(&crate::CancellationToken::new()))?;
    assert!(ordinary_source.capture_retired());
    assert!(native_source.capture_retired());
    work.receipt().verify(budget)?;
    assert_eq!(work.receipt().source_bytes_read, 0);
    assert_eq!(work.receipt().materializations, 0);
    Ok(())
}

#[tokio::test]
async fn witnessed_attachment_preadmits_carriers_and_bounds_initial_capture_cumulatively()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_token = crate::CancellationToken::new();
    let fixture_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &fixture_token)
            .map_err(|(cause, _)| cause)?;
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "native-carrier-admission",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &fixture_work, &fixture_token)
        .await?;
    transaction
        .create_directory("/selected-Ã©", &fixture_work, &fixture_token)
        .await?;
    transaction
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let generation = workspace.head(&fixture_work, &fixture_token).await?;
    let directory = tempfile::tempdir()?;
    let options = crate::MaterializeOptions::native(directory.path());
    let mount = MountOptions::read_only().subdirectory("/selected-Ã©");
    let cancellation = crate::CancellationToken::new();
    let prepared = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = generation
            .prepare_native_working_set(
                &(&mount).clone().native_context(&original, None, None),
                &options,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(cause, _)| cause)?;
    let identity = prepared.value.source_identity;
    let rejected_capture = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = generation
            .attach_native_working_set(
                &(&mount).clone().native_context(&original, None, None),
                &options,
                identity,
                crate::WorkBudget::default(),
                &cancellation,
            )
            .await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .err()
    .ok_or("zero-budget attachment capture succeeded")?;
    assert!(matches!(
        rejected_capture.0,
        MountLifecycleError::Source(MountSourceError::Work(
            crate::WorkError::BudgetExceeded { .. }
        )) | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
    ));
    let preparation = rejected_capture.1;
    assert_eq!(preparation.materializations, 0);
    assert!(
        preparation.peak_allocation_bytes
            >= (std::mem::size_of_val(prepared.value.source.as_ref())) as u64
    );
    let mut finite_preparation = crate::WorkBudget::UNBOUNDED;
    finite_preparation.allocation_operations = preparation
        .allocation_operations
        .checked_sub(1)
        .ok_or("attachment omitted carrier allocations")?;
    let rejected_carrier = (async {
        let original = crate::NativeMaterializationWork::new(finite_preparation, &cancellation)
            .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = generation
            .attach_native_working_set(
                &(&mount).clone().native_context(&original, None, None),
                &options,
                identity,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .err()
    .ok_or("underfunded attachment carriers succeeded")?;
    assert!(matches!(
        rejected_carrier.0,
        MountLifecycleError::Workspace(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
            counter: "allocation_operations",
            ..
        })) | MountLifecycleError::Work(crate::WorkError::BudgetExceeded {
            counter: "allocation_operations",
            ..
        })
    ));
    assert_eq!(
        rejected_carrier.1.allocation_operations,
        finite_preparation.allocation_operations
    );
    assert_eq!(rejected_carrier.1.materializations, 0);
    let rejected_initial_capture = (async {
        let original = crate::NativeMaterializationWork::new(preparation, &cancellation)
            .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = generation
            .attach_native_working_set(
                &(&mount).clone().native_context(&original, None, None),
                &options,
                identity,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .err()
    .ok_or("initial capture reset its preparation allowance")?;
    assert!(matches!(
        rejected_initial_capture.0,
        MountLifecycleError::Source(MountSourceError::Work(
            crate::WorkError::BudgetExceeded { .. }
        )) | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
    ));
    rejected_initial_capture.1.verify(preparation)?;
    assert_eq!(rejected_initial_capture.1.materializations, 0);
    let admitted = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = generation
            .attach_native_working_set(
                &(&mount).clone().native_context(&original, None, None),
                &options,
                identity,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    let exact = admitted.work;
    drop(admitted.value);
    let attached = (async {
        let original = crate::NativeMaterializationWork::new(exact, &cancellation)
            .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = generation
            .attach_native_working_set(
                &(&mount).clone().native_context(&original, None, None),
                &options,
                identity,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    attached.work.verify(exact)?;
    assert_eq!(attached.value.source_identity, identity);
    assert!(attached.work.allocation_operations > preparation.allocation_operations);
    assert_eq!(attached.value.source_root, options.destination);
    assert_eq!(attached.value.selected_root, Path::new("selected-Ã©"));
    assert_eq!(
        std::fs::read_dir(directory.path().join("selected-Ã©"))?.count(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn pinned_native_sources_cannot_publish_and_destinations_preserve_concurrent_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_token = crate::CancellationToken::new();
    let fixture_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &fixture_token)
            .map_err(|(cause, _)| cause)?;
    let default_token = crate::CancellationToken::new();
    let default_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &default_token)
            .map_err(|(cause, _)| cause)?;
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "pinned-native-view",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/file.txt", "original").await?;
    let admitted = workspace.head(&fixture_work, &fixture_token).await?;
    workspace.write_text("/file.txt", "concurrent").await?;
    let concurrent = workspace.head(&fixture_work, &fixture_token).await?.id();
    let source_root = tempfile::tempdir()?;
    let source = admitted
        .prepare_native_working_set(
            &MountOptions::read_only(),
            &crate::MaterializeOptions::native(source_root.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    source.validate_for_presentation(&default_work).await?;
    source.verify_unchanged(&default_work).await?;
    let capture_token = crate::CancellationToken::new();
    let capture_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &capture_token)
            .map_err(|(error, _)| error)?;
    let caller_payload = capture_work.claim(64, 1, crate::WorkCounters::UNCHARGED)?;
    let mut caller_buffer = Vec::<u8>::new();
    caller_buffer.try_reserve_exact(64)?;
    assert_eq!(caller_payload.bytes(), caller_buffer.capacity() as u64);
    let before_capture = capture_work.receipt();
    source.verify_unchanged(&capture_work).await?;
    assert!(capture_work.receipt().source_bytes_read > before_capture.source_bytes_read);
    assert!(capture_work.live_allocation_bytes() >= caller_payload.bytes());
    let mut denied_budget = crate::WorkBudget::UNBOUNDED;
    denied_budget.items_examined = 0;
    let denied_work = crate::NativeMaterializationWork::new(denied_budget, &capture_token)
        .map_err(|(error, _)| error)?;
    assert!(source.verify_unchanged(&denied_work).await.is_err());
    assert_eq!(denied_work.receipt().source_bytes_read, 0);
    assert_eq!(
        std::fs::read(source_root.path().join("file.txt"))?,
        b"original",
    );
    assert_eq!(
        std::fs::read_to_string(source_root.path().join("file.txt"))?,
        "original"
    );
    assert_eq!(source.generation_id(), admitted.id());
    assert!(
        source
            .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            })
            .await
            .map_err(|failure| failure.cause)
            .is_err()
    );
    std::fs::write(source_root.path().join("file.txt"), "source mutation")?;
    assert!(source.verify_unchanged(&default_work).await.is_err());
    assert_eq!(
        workspace.head(&fixture_work, &fixture_token).await?.id(),
        concurrent
    );

    let destination_root = tempfile::tempdir()?;
    let write = MountOptions::read_write().publication(MountPublication::Manual);
    let rejected = admitted
        .prepare_native_working_set(
            &write,
            &crate::MaterializeOptions::native(destination_root.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("stale native preparation succeeded")?;
    assert!(matches!(
        rejected,
        MountLifecycleError::Workspace(WorkspaceError::StaleGeneration)
    ));
    let destination = workspace
        .head(&fixture_work, &fixture_token)
        .await?
        .prepare_native_working_set(
            &write,
            &crate::MaterializeOptions::native(destination_root.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    std::fs::write(destination_root.path().join("file.txt"), "native edit")?;
    workspace
        .write_text("/file.txt", "newer concurrent")
        .await?;
    assert!(
        destination
            .validate_for_presentation(&default_work)
            .await
            .is_err()
    );
    assert!(
        destination
            .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            })
            .await
            .map_err(|failure| failure.cause)
            .is_err()
    );
    assert_eq!(
        workspace
            .read("/file.txt", 64, &fixture_work, &fixture_token)
            .await?
            .as_ref(),
        b"newer concurrent"
    );
    Ok(())
}

#[tokio::test]
async fn prepared_subtree_captures_native_edit_and_rejects_stale_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_token = crate::CancellationToken::new();
    let fixture_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &fixture_token)
            .map_err(|(cause, _)| cause)?;
    let default_token = crate::CancellationToken::new();
    let default_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &default_token)
            .map_err(|(cause, _)| cause)?;
    let cancellation = crate::CancellationToken::new();
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("hot"))?;
    std::fs::write(source.path().join("hot/file.txt"), b"base")?;
    std::fs::create_dir(source.path().join("hot/sub"))?;
    std::fs::write(source.path().join("hot/sub/child.txt"), b"old child")?;
    let native_view = tempfile::tempdir()?;
    let fs = Fs::memory();
    let demand = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(source.path()).to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "prepared-native-working-set",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let before_invalid = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .id();
    assert!(matches!(
        lazy.prepare_native_working_set(
            "/hot",
            &crate::MaterializeOptions::native("relative-native-view"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await,
        Err(MountLifecycleError::NativePolicy(
            "native working-set root must be absolute"
        ))
    ));
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        before_invalid
    );
    let mut working_set = lazy
        .prepare_native_working_set(
            "/hot",
            &crate::MaterializeOptions::native(native_view.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    let generation = working_set.expected_generation;
    let working = native_view.path();
    working_set.validate_for_presentation(&default_work).await?;
    let denied_capture = default_work.native_volume_work(crate::WorkBudget::default())?;
    assert!(
        working_set
            .capture_host_subtree(&MountPath::root(), &denied_capture)
            .await
            .is_err()
    );
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        generation
    );

    let name = if cfg!(windows) {
        "file.txt"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect()
    } else {
        b"file.txt".to_vec()
    };
    let unchanged_head = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .id();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
            std::future::ready(Ok::<(), MountLifecycleError>(()))
        })
        .await
        .map_err(|failure| failure.cause)?;
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        unchanged_head
    );
    #[cfg(unix)]
    let canonical_before = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .stat("/hot/file.txt")
        .await?
        .metadata;
    #[cfg(unix)]
    {
        working_set
            .capture_host_paths(&[MountPath::root().child(name.clone())], &default_work)
            .await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            })
            .await
            .map_err(|failure| failure.cause)?;
        let canonical_after = lazy
            .workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .stat("/hot/file.txt")
            .await?
            .metadata;
        assert_eq!(canonical_after.changed_ns, canonical_before.changed_ns);
        #[cfg(target_os = "linux")]
        assert_eq!(canonical_after.created_ns, canonical_before.created_ns);
    }
    #[cfg(target_os = "linux")]
    {
        let accessed = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        std::fs::File::open(working.join("hot/file.txt"))?
            .set_times(std::fs::FileTimes::new().set_accessed(accessed))?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            })
            .await
            .map_err(|failure| failure.cause)?;
        let canonical = lazy
            .workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .stat("/hot/file.txt")
            .await?
            .metadata;
        assert_eq!(canonical.accessed_ns, Some(1_700_000_000_000_000_000));
    }
    std::fs::write(working.join("hot/file.txt"), b"native-edit")?;
    let before_guard = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .id();
    let denied_publication =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let denied = working_set
        .sync_with_permit(
            PublicationPermit::Unrestricted,
            &denied_publication,
            |_original: &crate::NativeMaterializationWork| {
                std::future::ready(Err::<(), _>(MountLifecycleError::NativePolicy(
                    "original materialized owner denied publication",
                )))
            },
        )
        .await
        .err()
        .ok_or("materialized publication ignored its original owner guard")?;
    assert!(!denied.source_acknowledged);
    assert!(denied.accounting_failure.is_none());
    assert!(denied.publication.is_none());
    assert!(matches!(
        denied.cause,
        MountLifecycleError::NativePolicy("original materialized owner denied publication",)
    ));
    assert!(denied_publication.receipt().items_examined > 0);
    denied_publication
        .receipt()
        .verify(crate::WorkBudget::UNBOUNDED)?;
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        before_guard
    );
    working_set
        .capture_host_paths(&[MountPath::root().child(name)], &default_work)
        .await?;
    let publication =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let guard_original = publication.clone();
    let published = working_set
        .sync_with_permit(
            PublicationPermit::Unrestricted,
            &publication,
            move |actual: &crate::NativeMaterializationWork| {
                assert!(actual.shares_ledger(&guard_original));
                assert!(actual.shares_resource_allowance(&guard_original));
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            },
        )
        .await?;
    assert!(published.operation().is_some());
    assert!(!published.snapshot().is_lazy());
    assert_eq!(
        published.snapshot().workspace_id(),
        working_set.workspace.id()
    );
    assert_eq!(
        published.snapshot().volume_config(),
        working_set.workspace.volume_config()
    );
    assert_eq!(
        published.snapshot().generation(),
        working_set
            .workspace
            .head(&fixture_work, &fixture_token)
            .await?
            .id()
    );
    assert!(publication.receipt().items_examined > 0);
    assert_eq!(
        lazy.workspace()
            .read("/hot/file.txt", 32, &fixture_work, &fixture_token)
            .await?
            .as_ref(),
        b"native-edit"
    );
    std::fs::write(working.join("hot/created.txt"), b"new")?;
    working_set
        .capture_host_subtree(&MountPath::root(), &default_work)
        .await?;
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
            std::future::ready(Ok::<(), MountLifecycleError>(()))
        })
        .await
        .map_err(|failure| failure.cause)?;
    assert_eq!(
        lazy.workspace()
            .read("/hot/created.txt", 16, &fixture_work, &fixture_token)
            .await?
            .as_ref(),
        b"new"
    );
    // The publication boundary must capture writes even when the caller
    // receives no change notification and supplies no path hint.
    std::fs::write(working.join("hot/unreported.txt"), b"unreported")?;
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
            std::future::ready(Ok::<(), MountLifecycleError>(()))
        })
        .await
        .map_err(|failure| failure.cause)?;
    assert_eq!(
        lazy.workspace()
            .read("/hot/unreported.txt", 32, &fixture_work, &fixture_token)
            .await?
            .as_ref(),
        b"unreported"
    );
    std::fs::remove_file(working.join("hot/unreported.txt"))?;
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
            std::future::ready(Ok::<(), MountLifecycleError>(()))
        })
        .await
        .map_err(|failure| failure.cause)?;
    assert!(
        lazy.workspace()
            .read("/hot/unreported.txt", 32, &fixture_work, &fixture_token)
            .await
            .is_err()
    );
    std::fs::remove_dir_all(working.join("hot/sub"))?;
    std::fs::write(working.join("hot/sub"), b"replacement")?;
    working_set
        .capture_host_subtree(&MountPath::root(), &default_work)
        .await?;
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
            std::future::ready(Ok::<(), MountLifecycleError>(()))
        })
        .await
        .map_err(|failure| failure.cause)?;
    assert_eq!(
        lazy.workspace()
            .read("/hot/sub", 32, &fixture_work, &fixture_token)
            .await?
            .as_ref(),
        b"replacement"
    );
    assert!(
        lazy.workspace()
            .read("/hot/sub/child.txt", 32, &fixture_work, &fixture_token)
            .await
            .is_err()
    );
    lazy.write("/later.txt", bytes::Bytes::from_static(b"later"))
        .await?;
    lazy.write("/hot/conflict.txt", bytes::Bytes::from_static(b"parent"))
        .await?;
    std::fs::write(working.join("hot/conflict.txt"), b"child")?;
    working_set
        .capture_host_subtree(&MountPath::root(), &default_work)
        .await?;
    assert!(matches!(
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted, &default_work, |_| {
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            })
            .await
            .map_err(|failure| failure.cause),
        Err(MountLifecycleError::Source(MountSourceError::Stale))
    ));
    assert_eq!(
        lazy.workspace()
            .read("/hot/conflict.txt", 16, &fixture_work, &fixture_token)
            .await?
            .as_ref(),
        b"parent"
    );
    assert!(matches!(
        working_set.validate_for_presentation(&default_work).await,
        Err(MountLifecycleError::Workspace(
            WorkspaceError::StaleGeneration
        ))
    ));
    assert_ne!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        generation
    );
    let latest_view = tempfile::tempdir()?;
    let ready = lazy
        .prepare_native_working_set(
            "/hot",
            &crate::MaterializeOptions::native(latest_view.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    ready.validate_for_presentation(&default_work).await?;
    assert_eq!(
        std::fs::read(latest_view.path().join("hot/file.txt"))?,
        b"native-edit"
    );
    std::fs::rename(
        latest_view.path(),
        source.path().join("retired-native-view"),
    )?;
    std::fs::create_dir(latest_view.path())?;
    std::fs::create_dir(latest_view.path().join("hot"))?;
    assert!(matches!(
        ready.validate_for_presentation(&default_work).await,
        Err(MountLifecycleError::Source(MountSourceError::Stale))
    ));
    let cancelled_wait = crate::CancellationToken::new();
    cancelled_wait.cancel();
    assert!(matches!(
        working_set.retire_with_work(&cancelled_wait),
        Err(MountLifecycleError::Source(MountSourceError::Cancelled))
    ));
    assert!(!working_set.source.capture_retired());
    let lease_owners = Arc::strong_count(&working_set.source);
    let source_io = working_set.native_source_io_lease()?;
    assert_eq!(Arc::strong_count(&working_set.source), lease_owners + 1);
    let source_io_tail = source_io.clone();
    assert_eq!(Arc::strong_count(&working_set.source), lease_owners + 1);
    drop(source_io);
    assert_eq!(Arc::strong_count(&working_set.source), lease_owners + 1);
    drop(source_io_tail);
    assert_eq!(Arc::strong_count(&working_set.source), lease_owners);
    // Revocation is not a new allowance: cleanup spends the source's
    // already accepted birth credit, independently of the wait token.
    working_set.source.cancellation().cancel();
    let owners = Arc::strong_count(&working_set.source);
    let drained = working_set.retire_with_work(&crate::CancellationToken::new())?;
    assert!(working_set.source.capture_retired());
    assert!(working_set.native_source_io_lease().is_err());
    assert_eq!(Arc::strong_count(&working_set.source), owners + 1);
    let repeated = working_set.retire_with_work(&cancelled_wait)?;
    assert_eq!(Arc::strong_count(&working_set.source), owners + 2);
    drop(repeated);
    drop(drained);
    assert_eq!(Arc::strong_count(&working_set.source), owners);
    let ready_drained = ready.retire_with_work(&crate::CancellationToken::new())?;
    assert!(ready.source.capture_retired());
    drop(ready_drained);
    Ok(())
}
