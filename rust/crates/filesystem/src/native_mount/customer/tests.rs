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
    let lazy =
        native_lazy_source(physical.path(), "measured-customer-root", &fixture_token).await?;
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
    let (ordinary_source, volume, root) =
        lazy.prepare_authored_mount_source(&options, &work).await?;
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
    let pinned = head(lazy.workspace(), &fixture_work).await?;
    let denied_budget = crate::WorkBudget {
        backend_read_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let denied = crate::NativeMaterializationWork::new(denied_budget, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(
        lazy.prepare_authored_mount_source(&options, &denied)
            .await
            .is_err()
    );
    denied.receipt().verify(denied_budget)?;
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(head(lazy.workspace(), &fixture_work).await?, pinned);
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
    assert_eq!(head(lazy.workspace(), &fixture_work).await?, pinned);
    assert_eq!(std::fs::read_dir(rejected_root.path())?.count(), 0);
    let ordinary_job = job.with_source_io_lease(ordinary_source.native_source_io_lease()?)?;
    assert!(ordinary_job.shares_ledger(&job));
    assert!(ordinary_job.shares_resource_allowance(&job));
    assert!(!ordinary_job.shares_ledger(&work));
    drop(ordinary_job);
    ordinary_source.cancel_cleanup(Some(&crate::CancellationToken::new()))?;
    let (native_source, _, _) = lazy
        .prepare_authored_mount_source(
            &options.clone().native_context(&work, None, Some(&work)),
            &work,
        )
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
async fn witnessed_attachment_keeps_the_original_root_and_rejects_unfunded_capture()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let token = crate::CancellationToken::new();
    let setup = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token)
        .map_err(|(cause, _)| cause)?;
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "native-carrier-admission",
            crate::WorkBudget::UNBOUNDED,
            &token,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &setup, &token)
        .await?;
    transaction
        .create_directory("/selected-Ã©", &setup, &token)
        .await?;
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &token,
            |_| async {
                Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    let generation = workspace.head(&setup, &token).await?;
    let directory = tempfile::tempdir()?;
    let options = crate::MaterializeOptions::native(directory.path());
    let mount = MountOptions::read_only().subdirectory("/selected-Ã©");
    let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token)
        .map_err(|(cause, _)| cause)?;
    let prepared = generation
        .prepare_native_working_set(
            &mount.clone().native_context(&original, None, None),
            &options,
            crate::WorkBudget::UNBOUNDED,
            &token,
        )
        .await?;
    let identity = prepared.source_identity;

    let capture = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token)
        .map_err(|(cause, _)| cause)?;
    let rejected = generation
        .attach_native_working_set(
            &mount.clone().native_context(&capture, None, None),
            &options,
            identity,
            crate::WorkBudget::default(),
            &token,
        )
        .await
        .err()
        .ok_or("zero-budget capture succeeded")?;
    assert!(matches!(
        rejected,
        MountLifecycleError::Source(MountSourceError::Work(
            crate::WorkError::BudgetExceeded { .. }
        )) | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
    ));
    let admitted_work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token)
        .map_err(|(cause, _)| cause)?;
    let admitted = generation
        .attach_native_working_set(
            &mount.clone().native_context(&admitted_work, None, None),
            &options,
            identity,
            crate::WorkBudget::UNBOUNDED,
            &token,
        )
        .await?;
    let attached = admitted;
    assert_eq!(attached.source_identity, identity);
    assert_eq!(attached.source_root, options.destination);
    assert_eq!(attached.selected_root, Path::new("selected-Ã©"));
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
    let concurrent = head(&workspace, &fixture_work).await?;
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
    assert_eq!(head(&workspace, &fixture_work).await?, concurrent);

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
    let lazy =
        native_lazy_source(source.path(), "prepared-native-working-set", &fixture_token).await?;
    let before_invalid = head(lazy.workspace(), &fixture_work).await?;
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
    assert_eq!(head(lazy.workspace(), &fixture_work).await?, before_invalid);
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
    assert_eq!(head(lazy.workspace(), &fixture_work).await?, generation);

    let name = if cfg!(windows) {
        "file.txt"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect()
    } else {
        b"file.txt".to_vec()
    };
    let unchanged_head = head(lazy.workspace(), &fixture_work).await?;
    publish(&working_set, &default_work).await?;
    assert_eq!(head(lazy.workspace(), &fixture_work).await?, unchanged_head);
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
        publish(&working_set, &default_work).await?;
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
        publish(&working_set, &default_work).await?;
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
    let before_guard = head(lazy.workspace(), &fixture_work).await?;
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
    assert_eq!(head(lazy.workspace(), &fixture_work).await?, before_guard);
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
    assert_read(
        lazy.workspace(),
        "/hot/file.txt",
        32,
        b"native-edit",
        &fixture_work,
    )
    .await?;
    std::fs::write(working.join("hot/created.txt"), b"new")?;
    working_set
        .capture_host_subtree(&MountPath::root(), &default_work)
        .await?;
    publish(&working_set, &default_work).await?;
    assert_read(
        lazy.workspace(),
        "/hot/created.txt",
        16,
        b"new",
        &fixture_work,
    )
    .await?;
    // The publication boundary must capture writes even when the caller
    // receives no change notification and supplies no path hint.
    std::fs::write(working.join("hot/unreported.txt"), b"unreported")?;
    publish(&working_set, &default_work).await?;
    assert_read(
        lazy.workspace(),
        "/hot/unreported.txt",
        32,
        b"unreported",
        &fixture_work,
    )
    .await?;
    std::fs::remove_file(working.join("hot/unreported.txt"))?;
    publish(&working_set, &default_work).await?;
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
    publish(&working_set, &default_work).await?;
    assert_read(
        lazy.workspace(),
        "/hot/sub",
        32,
        b"replacement",
        &fixture_work,
    )
    .await?;
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
    assert_read(
        lazy.workspace(),
        "/hot/conflict.txt",
        16,
        b"parent",
        &fixture_work,
    )
    .await?;
    assert!(matches!(
        working_set.validate_for_presentation(&default_work).await,
        Err(MountLifecycleError::Workspace(
            WorkspaceError::StaleGeneration
        ))
    ));
    assert_ne!(head(lazy.workspace(), &fixture_work).await?, generation);
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
        working_set.retire(&cancelled_wait),
        Err(MountLifecycleError::Source(MountSourceError::Cancelled))
    ));
    assert!(!working_set.source.capture_retired());
    let source_io = working_set.native_source_io_lease()?;
    let source_io_tail = source_io.clone();
    drop(source_io);
    drop(source_io_tail);
    // Revocation is not a new allowance: cleanup spends the source's
    // already accepted birth credit, independently of the wait token.
    working_set.source.cancellation().cancel();
    let drained = working_set.retire(&crate::CancellationToken::new())?;
    assert!(working_set.source.capture_retired());
    assert!(working_set.native_source_io_lease().is_err());
    let repeated = working_set.retire(&cancelled_wait)?;
    drop(repeated);
    drop(drained);
    let ready_drained = ready.retire(&crate::CancellationToken::new())?;
    assert!(ready.source.capture_retired());
    drop(ready_drained);
    Ok(())
}

type MemoryWorkspace =
    Workspace<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;

pub(super) async fn head(
    workspace: &MemoryWorkspace,
    work: &NativeMaterializationWork,
) -> Result<GenerationId, WorkspaceError> {
    Ok(workspace.head(work, work.cancellation()).await?.id())
}

async fn publish(
    view: &NativeWorkingSet<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >,
    work: &NativeMaterializationWork,
) -> Result<crate::NativePublicationSnapshot, MountLifecycleError> {
    view.sync_with_permit(PublicationPermit::Unrestricted, work, |_| {
        std::future::ready(Ok::<(), MountLifecycleError>(()))
    })
    .await
    .map_err(|failure| failure.cause)
}

async fn assert_read(
    workspace: &MemoryWorkspace,
    path: &str,
    maximum: u64,
    expected: &[u8],
    work: &NativeMaterializationWork,
) -> Result<(), WorkspaceError> {
    assert_eq!(
        workspace
            .read(path, maximum, work, work.cancellation())
            .await?
            .as_ref(),
        expected
    );
    Ok(())
}

type MemoryLazyWorkspace = LazyWorkspace<
    crate::facade::MemoryAuthorityBackend,
    crate::facade::MemoryObjectBackend,
    NativeDemandSource,
    MemoryLazyWorkspaceStore,
>;

pub(super) async fn native_lazy_source(
    root: &Path,
    name: &str,
    cancellation: &crate::CancellationToken,
) -> Result<MemoryLazyWorkspace, Box<dyn std::error::Error>> {
    let demand = NativeDemandSource::open(
        std::borrow::Cow::Owned(root.to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        crate::WorkBudget::UNBOUNDED,
        cancellation,
    )
    .await
    .map_err(|failure| failure.error)?
    .value;
    Ok(LazyWorkspace::attach(
        &Fs::memory(),
        name,
        Arc::new(demand),
        MemoryLazyWorkspaceStore::default(),
    )
    .await?)
}
