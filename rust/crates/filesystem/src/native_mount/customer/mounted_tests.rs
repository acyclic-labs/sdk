use super::*;
use crate::demand::native::NativeDemandSource;
use crate::lazy_workspace::LazyReadonlyNode;
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::{Fs, MemoryLazyWorkspaceStore};
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn measured_lazy_customer_preserves_mixed_identity_guard_and_cleanup_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_token = crate::CancellationToken::new();
    let fixture_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &fixture_token)
            .map_err(|(cause, _)| cause)?;
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    let physical = tempfile::tempdir()?;
    std::fs::write(physical.path().join("source.txt"), b"source")?;
    let destination = tempfile::tempdir()?;
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
        "measured-native-customer",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    lazy.workspace()
        .write_text("/authored.txt", "authored")
        .await?;
    let generation = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .id();
    let cancellation = crate::CancellationToken::new();
    let options = MountOptions::read_write().publication(MountPublication::Manual);
    let denied = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::default(), &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = lazy
            .mount(
                destination.path(),
                (options.clone()).native_context(&original, None, None),
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
    .ok_or("zero mount allowance admitted a real provider")?;
    denied.1.verify(crate::WorkBudget::default())?;
    assert_eq!(denied.1, crate::WorkCounters::UNCHARGED);
    assert!(matches!(
        denied.0,
        MountLifecycleError::Materialization(crate::NativeTreeMaterializationError::Work(_))
    ));
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        generation
    );
    assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
    let path_budget = crate::WorkBudget {
        bytes_copied: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let path_work = crate::NativeMaterializationWork::new(path_budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let before_path_copy = path_work.receipt();
    let denied_path = lazy
        .mount(
            destination.path(),
            (options.clone()).native_context(&path_work, None, None),
        )
        .await
        .err()
        .ok_or("unfunded borrowed destination was copied before admission")?;
    assert!(matches!(
        denied_path,
        MountLifecycleError::Native(NativeMountError::Materialization(_))
    ));
    assert_eq!(path_work.receipt(), before_path_copy);
    path_work.receipt().verify(path_budget)?;
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        generation
    );
    assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
    let startup = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = lazy
            .mount(
                "relative-invalid-mount",
                (options.clone()).native_context(&original, None, None),
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
    .ok_or("invalid destination admitted a real provider")?;
    assert_ne!(startup.1, crate::WorkCounters::UNCHARGED);
    let mut startup = match startup.0 {
        MountLifecycleError::Native(NativeMountError::StartupFailed(startup)) => startup,
        error => return Err(Box::<dyn std::error::Error>::from(error)),
    };
    // The source watcher is real cleanup custody even before a driver or
    // destination fence exists.
    assert!(!startup.owner().cleanup_state().cleanup_complete());
    assert!(startup.owner().cleanup_state().source_retained);
    let cancelled_startup_cleanup = crate::CancellationToken::new();
    cancelled_startup_cleanup.cancel();
    let retained = startup
        .owner_mut()
        .stop_measured(crate::WorkBudget::UNBOUNDED, &cancelled_startup_cleanup)
        .err()
        .ok_or("cancelled startup cleanup discarded the source watcher")?;
    assert_eq!(retained.1, crate::WorkCounters::UNCHARGED);
    assert!(!startup.owner().cleanup_state().cleanup_complete());
    assert!(startup.owner().cleanup_state().source_retained);
    startup
        .owner_mut()
        .stop_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(startup.owner().cleanup_state().cleanup_complete());
    assert!(!startup.owner().cleanup_state().source_retained);
    let mounted = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = lazy
            .mount(
                destination.path(),
                (options).native_context(&original, None, None),
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
    assert!(mounted.work.backend_read_operations > 0);
    let mut mount = mounted.value;
    let payload_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    assert!(!payload_work.shares_ledger(mount._destination_custody.work()));
    let payload_bytes = std::mem::size_of_val(&mount) as u64;
    let mut payload_custody = payload_work.claim(
        payload_bytes,
        u64::from((payload_bytes) != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut slot = Arc::new_uninit();
    let before_transfer = payload_work.receipt();
    mount.retain_payload_allocation(&mut payload_custody)?;
    assert_eq!(payload_custody.bytes(), 0);
    assert_eq!(payload_work.receipt(), before_transfer);
    assert_eq!(
        mount
            .payload_custody
            .as_ref()
            .map(crate::NativeAllocation::bytes),
        Some(payload_bytes)
    );
    let foreign_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let mut foreign_custody = foreign_work.claim(8, 1, crate::WorkCounters::UNCHARGED)?;
    let foreign_data = Box::new([0u8; 8]);
    let foreign_prefix = foreign_work.receipt();
    assert!(matches!(
        mount.retain_payload_allocation(&mut foreign_custody),
        Err(MountLifecycleError::Materialization(
            crate::NativeTreeMaterializationError::InvalidLayout
        ))
    ));
    assert_eq!(foreign_custody.bytes(), 8);
    assert_eq!(foreign_work.receipt(), foreign_prefix);
    assert_eq!(payload_work.receipt(), before_transfer);
    assert_eq!(
        mount
            .payload_custody
            .as_ref()
            .map(crate::NativeAllocation::bytes),
        Some(payload_bytes)
    );
    drop(foreign_data);
    drop(foreign_custody);
    Arc::get_mut(&mut slot)
        .ok_or("mount payload slot was shared before initialization")?
        .write(mount);
    // SAFETY: the unique slot contains the complete real mount above.
    #[allow(
        unsafe_code,
        reason = "tests the actual pre-admitted mounted Arc payload transfer"
    )]
    let mount = unsafe { slot.assume_init() };
    let before = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount.snapshot(&original, &cancellation).await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    assert_eq!(before.value.authored_generation, generation);
    let query_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    assert!(!query_work.shares_ledger(mount._destination_custody.work()));
    let query_native = query_work.clone();
    mount
        .validate_snapshot_with_work(before.value, &query_native)
        .await?;
    let (lookup, lookup_custody) = mount
        .lookup_at_snapshot_with_work(before.value, "/source.txt", &query_native)
        .await?;
    assert!(
        matches!(&lookup, crate::LazyLookup::Source(node) if node.kind == crate::demand::SourceNodeKind::RegularFile)
    );
    assert!(lookup_custody.work().shares_ledger(&query_work));
    assert!(query_work.receipt().items_examined > 0);
    drop(lookup);
    drop(lookup_custody);
    let denied_query_cancellation = crate::CancellationToken::new();
    let denied_query = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &denied_query_cancellation,
    )
    .map_err(|(error, _)| error)?;
    let denied_query_prefix = denied_query.receipt();
    denied_query_cancellation.cancel();
    assert!(
        mount
            .validate_snapshot_with_work(before.value, &denied_query)
            .await
            .is_err()
    );
    assert_eq!(denied_query.receipt(), denied_query_prefix);
    mount
        .validate_snapshot_with_work(before.value, &query_native)
        .await?;
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        generation
    );
    let identity = mount
        .native_root_identity()
        .ok_or("mounted driver lost its original root witness")?;
    let io_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let unrelated =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let bytes = mount
        .with_original_work(&io_work, |_scope| async {
            let conflict = mount
                .with_original_work(&unrelated, |_scope| std::future::ready(()))
                .await;
            assert!(matches!(
                conflict,
                Err(MountLifecycleError::Native(
                    NativeMountError::ControlPending
                ))
            ));
            mount
                .with_original_work(&io_work, |scope| {
                    let scope = scope.try_clone_for_io();
                    let work = &io_work;
                    let mounted = &mount;
                    async move {
                        let scope = scope.map_err(MountLifecycleError::Native)?;
                        let result = work
                            .read_working_file::<MountLifecycleError, _, _>(
                                mounted.path(),
                                identity,
                                Path::new("source.txt"),
                                32,
                                || std::future::ready(Ok(())),
                            )
                            .await;
                        drop(scope);
                        result
                    }
                })
                .await?
        })
        .await??;
    assert_eq!(bytes.as_ref(), b"source");
    assert!(io_work.receipt().source_bytes_read >= 6);
    assert_eq!(unrelated.receipt().source_bytes_read, 0);
    drop(bytes);
    assert_eq!(
        std::fs::read(destination.path().join("source.txt"))?,
        b"source"
    );
    assert_eq!(
        std::fs::read(destination.path().join("authored.txt"))?,
        b"authored"
    );
    std::fs::write(destination.path().join("authored.txt"), b"native edit")?;
    let guarded = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| {
                    (
                        NativeMountPublicationFailure {
                            cause: MountLifecycleError::Materialization(cause),
                            accounting_failure: None,
                            source_acknowledged: false,
                            publication: None,
                        },
                        spent,
                    )
                })?;
        let result = mount
            .sync_with_permit(PublicationPermit::Unrestricted, &original, |_original| {
                std::future::ready(Err::<(), _>(MountLifecycleError::Source(
                    MountSourceError::Stale,
                )))
            })
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
    .ok_or("failed original-owner guard published")?;
    assert!(!guarded.0.source_acknowledged);
    assert!(guarded.0.accounting_failure.is_none());
    assert!(guarded.0.publication.is_none());
    assert!(guarded.1.items_examined > 0);
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        generation
    );
    assert!(
        (async {
            let original =
                crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, spent)| {
                        (MountLifecycleError::Materialization(cause), spent)
                    })?;
            let result = mount.snapshot(&original, &cancellation).await;
            result
                .map(|value| crate::OperationReceipt {
                    value,
                    work: original.receipt(),
                })
                .map_err(|cause| (cause, original.receipt()))
        })
        .await
        .is_err()
    );
    let original =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let callback_original = original.clone();
    let published = mount
        .sync_with_permit(
            PublicationPermit::Unrestricted,
            &original,
            move |meter: &crate::NativeMaterializationWork| {
                assert!(meter.shares_ledger(&callback_original));
                assert!(meter.shares_resource_allowance(&callback_original));
                assert_eq!(meter.receipt(), callback_original.receipt());
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            },
        )
        .await?;
    assert!(published.operation().is_some());
    assert!(published.snapshot().is_lazy());
    assert_eq!(published.snapshot().workspace_id(), lazy.workspace().id());
    assert_eq!(
        published.snapshot().volume_config(),
        lazy.workspace().volume_config()
    );
    assert!(original.receipt().items_examined > 0);
    assert_eq!(
        lazy.read_range("/authored.txt", 0, 32).await?.as_ref(),
        b"native edit"
    );
    let actual = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount.snapshot(&original, &cancellation).await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    (async {
        let original = crate::NativeMaterializationWork::new(actual.work, &cancellation)
            .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount.snapshot(&original, &cancellation).await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?
    .work
    .verify(actual.work)?;
    let imported_path = NamespacePath::new(
        vec![crate::kernel::LogicalName::new(
            crate::kernel::NameEncoding::Utf8,
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
        .snapshot(&LazyTreapWork::new(
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?;
    let plan = {
        let plan_cancellation = &cancellation;
        let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, plan_cancellation);
        let result = child
            .prepare_selected_import(
                child_snapshot,
                std::slice::from_ref(&imported_path),
                &accounting,
            )
            .await
            .map(|phase| match phase {
                LazyReadonlyNode::Portable(plan) => plan,
                #[cfg(not(target_arch = "wasm32"))]
                LazyReadonlyNode::Native(_) => unreachable!("ordinary plan uses ordinary funding"),
            });
        accounting.measured(result, crate::OperationFailure::new)
    }?
    .value;
    let actual = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount.snapshot(&original, &cancellation).await;
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    let imported_head = lazy
        .workspace()
        .head(&fixture_work, &fixture_token)
        .await?
        .id();
    let pending = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount
            .selected_import_snapshot(operation, &original, &cancellation)
            .await
            .map_err(MountLifecycleError::Native);
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    assert!(pending.value.is_none());
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        imported_head
    );
    let denied_import = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let plan = plan;
        let allocation = original
            .claim(
                plan.logical_owned_bytes(),
                0,
                crate::WorkCounters::default(),
            )
            .map_err(|cause| {
                (
                    MountLifecycleError::Materialization(cause),
                    original.receipt(),
                )
            })?;
        let result = mount
            .import_selected(
                actual.value,
                plan,
                allocation,
                operation,
                PublicationPermit::Unrestricted,
                &original,
                &cancellation,
                |_original| {
                    std::future::ready(Err::<(), _>(MountLifecycleError::Source(
                        MountSourceError::Stale,
                    )))
                },
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
    .ok_or("selected import bypassed its original owner guard")?;
    assert_ne!(denied_import.1, crate::WorkCounters::UNCHARGED);
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        imported_head
    );
    let selected_work =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    let (plan, plan_custody) = async {
        let accounting =
            LazyTreapWork::native_with_work(&selected_work, (&selected_work).cancellation())?;
        child
            .prepare_selected_import(
                child_snapshot,
                std::slice::from_ref(&imported_path),
                &accounting,
            )
            .await
    }
    .await
    .map(|phase| match phase {
        LazyReadonlyNode::Native(owned) => owned.into_parts(),
        LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding"),
    })
    .map_err(LazyWorkspaceError::into_native_failure)
    .map_err(|(error, _)| error)?;
    assert!(plan_custody.work().shares_ledger(&selected_work));
    assert!(plan_custody.bytes() >= plan.logical_owned_bytes());
    let selected_native = selected_work.clone();
    let before_unpolled = selected_work.receipt();
    let unpolled = mount.import_selected(
        actual.value,
        plan,
        plan_custody,
        operation,
        PublicationPermit::Unrestricted,
        &selected_native,
        &cancellation,
        |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
    );
    drop(unpolled);
    assert_eq!(selected_work.receipt(), before_unpolled);
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        imported_head
    );
    let (plan, plan_custody) = async {
        let accounting =
            LazyTreapWork::native_with_work(&selected_work, (&selected_work).cancellation())?;
        child
            .prepare_selected_import(
                child_snapshot,
                std::slice::from_ref(&imported_path),
                &accounting,
            )
            .await
    }
    .await
    .map(|phase| match phase {
        LazyReadonlyNode::Native(owned) => owned.into_parts(),
        LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding"),
    })
    .map_err(LazyWorkspaceError::into_native_failure)
    .map_err(|(error, _)| error)?;
    let foreign_native = foreign_work.clone();
    let before_foreign_import = foreign_work.receipt();
    assert!(matches!(
        mount
            .import_selected(
                actual.value,
                plan,
                plan_custody,
                operation,
                PublicationPermit::Unrestricted,
                &foreign_native,
                &cancellation,
                |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
            )
            .await,
        Err(MountLifecycleError::Native(
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::InvalidLayout,)
        ))
    ));
    assert_eq!(foreign_work.receipt(), before_foreign_import);
    assert_eq!(
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id(),
        imported_head
    );
    let (plan, plan_custody) = async {
        let accounting =
            LazyTreapWork::native_with_work(&selected_work, (&selected_work).cancellation())?;
        child
            .prepare_selected_import(
                child_snapshot,
                std::slice::from_ref(&imported_path),
                &accounting,
            )
            .await
    }
    .await
    .map(|phase| match phase {
        LazyReadonlyNode::Native(owned) => owned.into_parts(),
        LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding"),
    })
    .map_err(LazyWorkspaceError::into_native_failure)
    .map_err(|(error, _)| error)?;
    let imported = mount
        .import_selected(
            actual.value,
            plan,
            plan_custody,
            operation,
            PublicationPermit::Unrestricted,
            &selected_native,
            &cancellation,
            |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
        )
        .await?;
    selected_work
        .receipt()
        .verify(crate::WorkBudget::UNBOUNDED)?;
    assert_eq!(
        imported.authored_generation,
        lazy.workspace()
            .head(&fixture_work, &fixture_token)
            .await?
            .id()
    );
    let completed = (async {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount
            .selected_import_snapshot(operation, &original, &cancellation)
            .await
            .map_err(MountLifecycleError::Native);
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    assert_eq!(completed.value, Some(imported));
    let repeated = (async {
        let original = crate::NativeMaterializationWork::new(completed.work, &cancellation)
            .map_err(|(cause, spent)| (MountLifecycleError::Materialization(cause), spent))?;
        let result = mount
            .selected_import_snapshot(operation, &original, &cancellation)
            .await
            .map_err(MountLifecycleError::Native);
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: original.receipt(),
            })
            .map_err(|cause| (cause, original.receipt()))
    })
    .await
    .map_err(|(error, _)| error)?;
    repeated.work.verify(completed.work)?;
    assert_eq!(repeated.value, completed.value);
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
    let no_detach = crate::WorkBudget {
        backend_write_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let denied_detach = {
        let original = crate::NativeMaterializationWork::new(no_detach, &cancellation);
        match original {
            Ok(work) => mount
                .abandon(&work)
                .map(|value| crate::OperationReceipt {
                    value,
                    work: work.receipt(),
                })
                .map_err(|cause| (cause, work.receipt())),
            Err((cause, spent)) => {
                if let Some(session) = mount
                    .session
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_mut()
                {
                    session.record_measured_cleanup_failure();
                }
                Err((MountLifecycleError::Materialization(cause), spent))
            }
        }
    }
    .err()
    .ok_or("zero native-write allowance detached a live namespace")?;
    denied_detach.1.verify(no_detach)?;
    assert_eq!(denied_detach.1.backend_write_operations, 0);
    {
        let owner = mount
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = owner
            .as_ref()
            .ok_or("denied detach lost cleanup custody")?
            .cleanup_state();
        assert!(state.driver_retained);
        assert!(state.destination_fence_retained);
        assert!(!state.namespace_detach_confirmed);
    }
    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    let denied_cleanup = {
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancelled);
        match original {
            Ok(work) => mount
                .abandon(&work)
                .map(|value| crate::OperationReceipt {
                    value,
                    work: work.receipt(),
                })
                .map_err(|cause| (cause, work.receipt())),
            Err((cause, spent)) => {
                if let Some(session) = mount
                    .session
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_mut()
                {
                    session.record_measured_cleanup_failure();
                }
                Err((MountLifecycleError::Materialization(cause), spent))
            }
        }
    }
    .err()
    .ok_or("cancelled cleanup discarded the live session")?;
    assert_eq!(denied_cleanup.1, crate::WorkCounters::UNCHARGED);
    {
        let owner = mount
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = owner
            .as_ref()
            .ok_or("cleanup failure lost its session")?
            .cleanup_state();
        assert!(state.driver_retained);
        assert!(state.destination_fence_retained);
        assert!(state.measured_cleanup_failed);
        assert!(!state.cleanup_complete());
    }
    let original_mount_work = mount._destination_custody.work().clone();
    let denied_wait = mount
        .retire_with_work(&cancelled)
        .err()
        .ok_or("cancelled wait minted a physical drain proof")?;
    assert!(matches!(denied_wait, MountLifecycleError::Native(_)));
    {
        let owner = mount
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = owner
            .as_ref()
            .ok_or("cancelled wait lost physical cleanup ownership")?
            .cleanup_state();
        assert!(state.driver_retained);
        assert!(state.destination_fence_retained);
        assert!(!state.cleanup_complete());
    }
    original_mount_work.cancellation().cancel();
    let before_retire = original_mount_work.receipt();
    let drained = mount.retire_with_work(&crate::CancellationToken::new())?;
    assert!(
        original_mount_work.receipt().backend_write_operations
            > before_retire.backend_write_operations
    );
    assert!(mount.source.physically_retired()?);
    assert!(
        mount
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    );
    assert!(mount.native_source_io_lease().is_err());
    let owners = Arc::strong_count(&mount.source);
    let before_replay = original_mount_work.receipt();
    let repeated = mount.retire_with_work(&cancelled)?;
    assert_eq!(Arc::strong_count(&mount.source), owners + 1);
    assert_eq!(original_mount_work.receipt(), before_replay);
    drop(repeated);
    drop(drained);
    Ok(())
}
