use super::tests::{head, native_lazy_source, test_work};
use super::*;
use crate::kernel::NamespacePath;
use crate::model::VolumeLimits;
use crate::{CancellationToken, WorkBudget};
use std::future::{Ready, ready};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn lazy_mount_preserves_guarded_publication_import_and_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let cancellation = CancellationToken::new();
    let new_work = |budget| test_work(budget, &cancellation);
    let accounting = || KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let setup = new_work(WorkBudget::UNBOUNDED)?;
    let physical = tempfile::tempdir()?;
    std::fs::write(physical.path().join("source.txt"), b"source")?;
    let destination = tempfile::tempdir()?;
    let lazy =
        native_lazy_source(physical.path(), "measured-native-customer", &cancellation).await?;
    lazy.workspace()
        .write_text("/authored.txt", "authored")
        .await?;
    let read = async |path: &str, expected: &[u8]| {
        assert_eq!(
            lazy.read_range(path, 0, 32, &mut accounting())
                .await?
                .bytes()
                .as_ref(),
            expected
        );
        Ok::<_, LazyWorkspaceError>(())
    };
    let generation = head(lazy.workspace(), &setup).await?;
    let options = MountOptions::read_write().publication(MountPublication::Manual);
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
    assert_eq!(head(lazy.workspace(), &setup).await?, generation);
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
    for (wait, completes) in [(&cancelled, false), (&cancellation, true)] {
        let result = startup
            .owner_mut()
            .stop_measured(WorkBudget::UNBOUNDED, wait);
        assert_eq!(result.is_ok(), completes);
        assert_eq!(startup.owner().cleanup_state().source_retained, !completes);
    }
    assert!(startup.owner().cleanup_state().cleanup_complete());

    let mount_work = new_work(WorkBudget::UNBOUNDED)?;
    let mount = lazy
        .mount(
            destination.path(),
            options.native_context(&mount_work, None, None),
        )
        .await?;
    let query = new_work(WorkBudget::UNBOUNDED)?;
    let before = mount.snapshot(&query, &cancellation).await?;
    assert_eq!(before.authored_generation, generation);
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
            || ready(Ok(())),
        )
        .await?;
    drop(io_scope);
    drop(nested_scope);
    drop(original_scope);
    assert_eq!(bytes.as_ref(), b"source");
    drop(bytes);
    assert_eq!(
        std::fs::read(destination.path().join("authored.txt"))?,
        b"authored"
    );
    std::fs::write(destination.path().join("authored.txt"), b"native edit")?;
    let publication_work = new_work(WorkBudget::UNBOUNDED)?;
    let denied = mount
        .sync_with_permit(PublicationPermit::Unrestricted, &publication_work, |_| {
            ready(Err::<(), _>(MountLifecycleError::Source(
                MountSourceError::Stale,
            )))
        })
        .await
        .err()
        .ok_or("owner guard was bypassed")?;
    assert!(!denied.source_acknowledged);
    assert!(denied.publication.is_none());
    assert_eq!(head(lazy.workspace(), &setup).await?, generation);
    assert!(mount.snapshot(&query, &cancellation).await.is_err());
    let callback_work = publication_work.clone();
    let published = mount
        .sync_with_permit(
            PublicationPermit::Unrestricted,
            &publication_work,
            move |work: &NativeMaterializationWork| {
                assert!(work.shares_ledger(&callback_work));
                assert!(work.shares_resource_allowance(&callback_work));
                ready(Ok::<(), MountLifecycleError>(()))
            },
        )
        .await?;
    assert!(published.operation().is_some());
    assert!(published.is_lazy());
    assert_eq!(published.workspace_id(), lazy.workspace().id());
    read("/authored.txt", b"native edit").await?;

    let snapshot_work = new_work(WorkBudget::UNBOUNDED)?;
    let actual = mount.snapshot(&snapshot_work, &cancellation).await?;
    let limits = VolumeLimits::default();
    let imported_path = NamespacePath::from_portable(
        &crate::path::PortablePath::parse("/selected-import.txt", limits)?,
        limits,
    )?;
    let operation = crate::OperationId::new();
    let (child, _) = lazy
        .fork(
            "selected-import-child",
            IdempotencyKey::new(),
            &mut accounting(),
        )
        .await?;
    child
        .workspace()
        .write_text("/selected-import.txt", "selected SDK import")
        .await?;
    let child_snapshot = child.snapshot(&mut accounting()).await?;
    let imported_head = head(lazy.workspace(), &setup).await?;
    assert!(
        mount
            .selected_import_snapshot(operation, &query, &cancellation)
            .await?
            .is_none()
    );
    let selected_work = new_work(WorkBudget::UNBOUNDED)?;
    let prepare = || async {
        let (plan, allocation) = child
            .prepare_selected_import(
                child_snapshot,
                std::slice::from_ref(&imported_path),
                &mut KernelAccounting::native(&selected_work, &cancellation),
            )
            .await?;
        let crate::kernel::KernelOwnedAllocation::NativeOriginal(custody) = allocation else {
            return Err(LazyWorkspaceError::NativeLedgerMismatch);
        };
        Ok::<_, LazyWorkspaceError>((plan, custody))
    };
    let allow = |_: &NativeMaterializationWork| ready(Ok::<(), MountLifecycleError>(()));
    let import = async |plan,
                        custody,
                        work: &NativeMaterializationWork,
                        before: fn(
        &NativeMaterializationWork,
    ) -> Ready<Result<(), MountLifecycleError>>| {
        mount
            .import_selected(
                actual,
                plan,
                custody,
                operation,
                PublicationPermit::Unrestricted,
                work,
                &cancellation,
                before,
            )
            .await
    };
    let (plan, custody) = prepare().await?;
    assert!(
        import(plan, custody, &selected_work, |_| ready(Err::<(), _>(
            MountLifecycleError::Source(MountSourceError::Stale)
        )))
        .await
        .is_err()
    );
    assert_eq!(head(lazy.workspace(), &setup).await?, imported_head);
    let (plan, custody) = prepare().await?;
    drop(import(plan, custody, &selected_work, allow));
    assert_eq!(head(lazy.workspace(), &setup).await?, imported_head);
    let (plan, custody) = prepare().await?;
    assert!(matches!(
        import(plan, custody, &foreign, allow).await,
        Err(MountLifecycleError::Materialization(
            crate::NativeTreeMaterializationError::InvalidLayout
        ))
    ));
    assert_eq!(head(lazy.workspace(), &setup).await?, imported_head);
    let (plan, custody) = prepare().await?;
    let imported = import(plan, custody, &selected_work, allow).await?;
    assert_eq!(
        imported.authored_generation,
        head(lazy.workspace(), &setup).await?
    );
    let completed_work = new_work(WorkBudget::UNBOUNDED)?;
    assert_eq!(
        mount
            .selected_import_snapshot(operation, &completed_work, &cancellation)
            .await?,
        Some(imported)
    );
    for (path, expected) in [
        ("/selected-import.txt", b"selected SDK import".as_slice()),
        ("/source.txt", b"source".as_slice()),
    ] {
        read(path, expected).await?;
    }

    assert!(mount.retire(&cancelled).is_err());
    assert!(mount.native_root_identity().is_some());
    mount_work.cancellation().cancel();
    let drained = mount.retire(&CancellationToken::new())?;
    assert!(mount.source.physically_retired()?);
    assert!(mount.native_source_io_lease().is_err());
    let repeated = mount.retire(&cancelled)?;
    drop(repeated);
    drop(drained);
    Ok(())
}
