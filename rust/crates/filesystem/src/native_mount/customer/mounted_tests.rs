use super::*;
use crate::demand::native::NativeDemandSource;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::lazy_workspace::LazyReadonlyNode;
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::{CancellationToken, Fs, MemoryLazyWorkspaceStore, WorkBudget};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn lazy_mount_preserves_guarded_publication_import_and_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let cancellation = CancellationToken::new();
    let new_work =
        |budget| NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause);
    let setup = new_work(WorkBudget::UNBOUNDED)?;
    let physical = tempfile::tempdir()?;
    std::fs::write(physical.path().join("source.txt"), b"source")?;
    let destination = tempfile::tempdir()?;
    let fs = Fs::memory();
    let demand = NativeDemandSource::open(
        std::borrow::Cow::Owned(physical.path().to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?
    .value;
    let lazy = LazyWorkspace::attach(
        &fs,
        "measured-native-customer",
        Arc::new(demand),
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    lazy.workspace()
        .write_text("/authored.txt", "authored")
        .await?;
    let generation = lazy.workspace().head(&setup, &cancellation).await?.id();
    let options = MountOptions::read_write().publication(MountPublication::Manual);

    assert!(new_work(WorkBudget::default()).is_err());
    let path_budget = WorkBudget {
        bytes_copied: 0,
        ..WorkBudget::UNBOUNDED
    };
    let path_work = new_work(path_budget)?;
    assert!(
        lazy.mount(
            destination.path(),
            options.clone().native_context(&path_work, None, None)
        )
        .await
        .is_err()
    );
    path_work.receipt().verify(path_budget)?;
    assert_eq!(
        lazy.workspace().head(&setup, &cancellation).await?.id(),
        generation
    );
    assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);

    let startup_work = new_work(WorkBudget::UNBOUNDED)?;
    let mut startup = match lazy
        .mount(
            "relative-invalid-mount",
            options.clone().native_context(&startup_work, None, None),
        )
        .await
    {
        Err(MountLifecycleError::Native(NativeMountError::StartupFailed(failure))) => failure,
        _ => return Err("invalid destination lost its cleanup owner".into()),
    };
    assert!(startup.owner().cleanup_state().source_retained);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(
        startup
            .owner_mut()
            .stop_measured(WorkBudget::UNBOUNDED, &cancelled)
            .is_err()
    );
    assert!(startup.owner().cleanup_state().source_retained);
    startup
        .owner_mut()
        .stop_measured(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    assert!(startup.owner().cleanup_state().cleanup_complete());

    let mount_work = new_work(WorkBudget::UNBOUNDED)?;
    let mount = Arc::new(
        lazy.mount(
            destination.path(),
            options.native_context(&mount_work, None, None),
        )
        .await?,
    );
    let query = new_work(WorkBudget::UNBOUNDED)?;
    let before = mount.snapshot(&query, &cancellation).await?;
    assert_eq!(before.authored_generation, generation);
    lazy.validate_snapshot(
        before,
        &LazyTreapWork::native_with_work(&query, query.cancellation())?,
    )
    .await?;
    let (lookup, custody) = lazy
        .lookup_at_snapshot_with_work(before, "/source.txt", &query, query.cancellation())
        .await?;
    assert!(
        matches!(lookup, LazyLookup::Source(node) if node.kind == crate::demand::SourceNodeKind::RegularFile)
    );
    assert!(custody.work().shares_ledger(&query));
    drop(custody);
    assert!(NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled).is_err());

    let identity = mount
        .native_root_identity()
        .ok_or("mount lost its root identity")?;
    let io_work = new_work(WorkBudget::UNBOUNDED)?;
    let foreign = new_work(WorkBudget::UNBOUNDED)?;
    let original_scope = mount.begin_original_work(&io_work)?;
    assert!(matches!(
        mount.begin_original_work(&foreign),
        Err(MountLifecycleError::Native(
            NativeMountError::ControlPending
        ))
    ));
    let nested_scope = mount.begin_original_work(&io_work)?;
    let io_scope = nested_scope.try_clone_for_io()?;
    let bytes = io_work
        .read_working_file::<MountLifecycleError, _, _>(
            mount.path(),
            identity,
            Path::new("source.txt"),
            32,
            || std::future::ready(Ok(())),
        )
        .await?;
    drop(io_scope);
    drop(nested_scope);
    drop(original_scope);
    assert_eq!(bytes.as_ref(), b"source");
    assert_eq!(foreign.receipt().source_bytes_read, 0);
    drop(bytes);
    assert_eq!(
        std::fs::read(destination.path().join("authored.txt"))?,
        b"authored"
    );
    std::fs::write(destination.path().join("authored.txt"), b"native edit")?;
    let publication_work = new_work(WorkBudget::UNBOUNDED)?;
    let denied = mount
        .sync_with_permit(PublicationPermit::Unrestricted, &publication_work, |_| {
            std::future::ready(Err::<(), _>(MountLifecycleError::Source(
                MountSourceError::Stale,
            )))
        })
        .await
        .err()
        .ok_or("owner guard was bypassed")?;
    assert!(!denied.source_acknowledged);
    assert!(denied.publication.is_none());
    assert_eq!(
        lazy.workspace().head(&setup, &cancellation).await?.id(),
        generation
    );
    assert!(mount.snapshot(&query, &cancellation).await.is_err());
    let callback_work = publication_work.clone();
    let published = mount
        .sync_with_permit(
            PublicationPermit::Unrestricted,
            &publication_work,
            move |work: &NativeMaterializationWork| {
                assert!(work.shares_ledger(&callback_work));
                assert!(work.shares_resource_allowance(&callback_work));
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            },
        )
        .await?;
    assert!(published.operation().is_some());
    assert!(published.snapshot().is_lazy());
    assert_eq!(published.snapshot().workspace_id(), lazy.workspace().id());
    assert_eq!(
        lazy.read_range("/authored.txt", 0, 32).await?.as_ref(),
        b"native edit"
    );

    let snapshot_work = new_work(WorkBudget::UNBOUNDED)?;
    let actual = mount.snapshot(&snapshot_work, &cancellation).await?;
    let imported_path = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"selected-import.txt".to_vec(),
            VolumeLimits::default().maximum_component_bytes,
        )?],
        VolumeLimits::default(),
    )?;
    let operation = crate::OperationId::new();
    let child = lazy
        .fork("selected-import-child", IdempotencyKey::new())
        .await?;
    child
        .workspace()
        .write_text("/selected-import.txt", "selected SDK import")
        .await?;
    let child_snapshot = child
        .snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation))
        .await?;
    let imported_head = lazy.workspace().head(&setup, &cancellation).await?.id();
    assert!(
        mount
            .selected_import_snapshot(operation, &query, &cancellation)
            .await?
            .is_none()
    );

    let selected_work = new_work(WorkBudget::UNBOUNDED)?;
    let prepare = || async {
        let accounting = LazyTreapWork::native_with_work(&selected_work, &cancellation)?;
        match child
            .prepare_selected_import(
                child_snapshot,
                std::slice::from_ref(&imported_path),
                &accounting,
            )
            .await?
        {
            LazyReadonlyNode::Native(owned) => Ok::<_, LazyWorkspaceError>(owned.into_parts()),
            LazyReadonlyNode::Portable(_) => unreachable!("native plan retains its funding"),
        }
    };
    let (plan, custody) = prepare().await?;
    assert!(
        mount
            .import_selected(
                actual,
                plan,
                custody,
                operation,
                PublicationPermit::Unrestricted,
                &selected_work,
                &cancellation,
                |_| std::future::ready(Err::<(), _>(MountLifecycleError::Source(
                    MountSourceError::Stale
                ))),
            )
            .await
            .is_err()
    );
    assert_eq!(
        lazy.workspace().head(&setup, &cancellation).await?.id(),
        imported_head
    );
    let (plan, custody) = prepare().await?;
    let unpolled = mount.import_selected(
        actual,
        plan,
        custody,
        operation,
        PublicationPermit::Unrestricted,
        &selected_work,
        &cancellation,
        |_| std::future::ready(Ok::<(), MountLifecycleError>(())),
    );
    drop(unpolled);
    assert_eq!(
        lazy.workspace().head(&setup, &cancellation).await?.id(),
        imported_head
    );
    let (plan, custody) = prepare().await?;
    assert!(matches!(
        mount
            .import_selected(
                actual,
                plan,
                custody,
                operation,
                PublicationPermit::Unrestricted,
                &foreign,
                &cancellation,
                |_| std::future::ready(Ok::<(), MountLifecycleError>(())),
            )
            .await,
        Err(MountLifecycleError::Native(
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::InvalidLayout)
        ))
    ));
    assert_eq!(
        lazy.workspace().head(&setup, &cancellation).await?.id(),
        imported_head
    );
    let (plan, custody) = prepare().await?;
    let imported = mount
        .import_selected(
            actual,
            plan,
            custody,
            operation,
            PublicationPermit::Unrestricted,
            &selected_work,
            &cancellation,
            |_| std::future::ready(Ok::<(), MountLifecycleError>(())),
        )
        .await?;
    assert_eq!(
        imported.authored_generation,
        lazy.workspace().head(&setup, &cancellation).await?.id()
    );
    let completed_work = new_work(WorkBudget::UNBOUNDED)?;
    assert_eq!(
        mount
            .selected_import_snapshot(operation, &completed_work, &cancellation)
            .await?,
        Some(imported)
    );
    assert_eq!(
        lazy.read_range("/selected-import.txt", 0, 32)
            .await?
            .as_ref(),
        b"selected SDK import"
    );
    assert_eq!(
        lazy.read_range("/source.txt", 0, 32).await?.as_ref(),
        b"source"
    );

    let no_detach = WorkBudget {
        backend_write_operations: 0,
        ..WorkBudget::UNBOUNDED
    };
    match new_work(no_detach) {
        Ok(work) => {
            assert!(mount.abandon(&work).is_err());
            work.receipt().verify(no_detach)?;
        }
        Err(_) => {}
    }
    assert!(mount.native_root_identity().is_some());
    assert!(mount.retire_with_work(&cancelled).is_err());
    assert!(mount.native_root_identity().is_some());
    mount_work.cancellation().cancel();
    let drained = mount.retire_with_work(&CancellationToken::new())?;
    assert!(mount.source.physically_retired()?);
    assert!(mount.native_source_io_lease().is_err());
    let repeated = mount.retire_with_work(&cancelled)?;
    drop(repeated);
    drop(drained);
    Ok(())
}
