use super::*;
use crate::source_authorization::{
    ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
};
pub(super) mod policy;
use policy::{MeasuredRootPolicy, runtime};

#[tokio::test]
async fn actual_created_descendant_project_is_operation_scoped_without_original_read_grant()
-> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let preparation = TaskId::from_bytes([83; 16]);
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.recover_work(preparation, &mut None).await?
    else {
        panic!("preparation lease")
    };
    fixture
        .host
        .settle_task(
            preparation,
            &(&lease.reservation).into(),
            Outcome::Succeeded(serde_json::to_value(&created.report).unwrap()),
            &mut None,
        )
        .await?;
    fixture.admit_child(created.request.child.clone()).await?;
    let child = TaskId::from_bytes([84; 16]);
    let project = created.project;
    let workspace = workspace_ref(
        fixture.filesystem_provider.clone(),
        &project.storage_name(&mut None)?,
    )?;
    let initial = fixture.filesystem.resolve(&workspace).await?.generation;
    let files = [
        WorkspaceMutation::PutFile {
            path: "/selected.txt".into(),
            bytes: b"frozen selected".to_vec().into(),
        },
        WorkspaceMutation::PutFile {
            path: "/second.txt".into(),
            bytes: b"second".to_vec().into(),
        },
    ];
    let generation = fixture
        .filesystem
        .apply(
            &workspace,
            Some(&initial),
            &files,
            &IdempotencyKey::new("source-inspection-files")?,
        )
        .await?;
    let parent = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "parent-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("created-project".into()),
        &mut None,
    )?;
    let parent_workspace = workspace_ref(
        fixture.filesystem_provider.clone(),
        &parent.storage_name(&mut None)?,
    )?;
    let policy = MeasuredRootPolicy {
        filesystem: fixture.filesystem.clone(),
        root: fixture.root,
        bootstrap: ProjectSourceSnapshot {
            project: parent,
            generation: fixture
                .filesystem
                .resolve(&parent_workspace)
                .await?
                .generation,
            native_snapshot: None,
        },
    };
    let runtime = runtime(&fixture).await?;
    let context = runtime
        .task_context(fixture.root, (&fixture.root_lease.reservation).into())
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let owner = Arc::new(
        fixture
            .host
            .journal_owner(fixture.root, context.owned_task_fence()?)
            .await?
            .with_invocation_cancellation(&cancellation),
    );
    let issuer = AuthorityIssuer::new(
        "created-owner",
        [80; 32],
        fixture.host.verifier.audience().clone(),
    );
    let operation = OperationId::from_bytes([120; 16]);
    let snapshot = ProjectSourceSnapshot {
        project: project.clone(),
        generation: generation.clone(),
        native_snapshot: None,
    };
    let authorize = |source, selected| {
        owner.authorize_project_source(
            &context,
            &issuer,
            &policy,
            source,
            operation,
            selected,
            1_000_000,
            128 * 1_024 * 1_024,
        )
    };
    let grants = context.scope().grants().clone();
    let handle = authorize(child, snapshot.clone()).await?;
    assert!(!grants.contains(&project.capability(VolumeOperation::Read, &mut None)?));
    assert_eq!(
        handle.authorization().policy_scope.capabilities(),
        &Capabilities::new([handle.authorization().policy_scope.id()])
    );
    for (volume, access, allowed) in [
        (&project, VolumeOperation::Read, true),
        (&project, VolumeOperation::Write, false),
        (&policy.bootstrap.project, VolumeOperation::Read, false),
        (&policy.bootstrap.project, VolumeOperation::Write, false),
    ] {
        assert_eq!(
            handle.grant().require(volume, access, None).is_ok(),
            allowed
        );
    }
    let encoded = crate::contract::canonical_json_bytes(handle.authorization())?;
    let record: crate::source_authorization::ProjectSourceAuthorization =
        serde_json::from_slice(&encoded).unwrap();
    record.verify_scope_with_work(&fixture.host.verifier, &mut None)?;
    assert!(
        crate::conversation::ContentGrant::verify(
            &fixture.host.verifier,
            &record.policy_scope,
            &project,
            VolumeOperation::Read,
            1_000_000,
            &mut None
        )
        .is_err()
    );
    let foreign = AuthorityIssuer::new(
        "foreign",
        [81; 32],
        fixture.host.verifier.audience().clone(),
    );
    for control in 0..12 {
        let mut forged = record.clone();
        match control {
            0 => forged.owner.id = "forged-root-owner".into(),
            1 => forged.caller_task = child,
            2 => forged.caller_admission_digest[0] ^= 1,
            3 => forged.caller_fence.reservation_id = "stale-owner".into(),
            4 => forged.operation_id = OperationId::from_bytes([121; 16]),
            5 => forged.source_task = fixture.root,
            6 => forged.project = policy.bootstrap.project.clone(),
            7 => forged.generation = initial.clone(),
            8 => forged.native_snapshot = Some(serde_json::json!({"forgedNativeProof":true})),
            9 => forged.graph_root = child,
            10 => forged.maximum_work += 1,
            _ => {
                forged.policy_scope = foreign.issue(
                    &forged.policy_scope.id(),
                    forged.policy_scope.capabilities(),
                    None,
                    None,
                    &mut None,
                )?
            }
        }
        assert!(
            forged
                .verify_scope_with_work(&fixture.host.verifier, &mut None)
                .is_err()
        );
        assert!(
            owner
                .restore_project_source_authorization(&context, &forged)
                .await
                .is_err()
        );
    }
    let mut other_purpose = record.clone();
    other_purpose.policy_scope = issuer.issue(
        "harness.other-purpose.v1",
        &Capabilities::new(["harness.other-purpose.v1"]),
        None,
        None,
        &mut None,
    )?;
    fixture
        .host
        .verifier
        .verify(&other_purpose.policy_scope, &mut None)?;
    assert!(
        other_purpose
            .verify_scope_with_work(&fixture.host.verifier, &mut None)
            .is_err()
    );
    assert!(
        owner
            .authorize_project_source(
                &context,
                &foreign,
                &policy,
                child,
                operation,
                snapshot.clone(),
                1_000_000,
                128 * 1_024 * 1_024
            )
            .await
            .is_err()
    );
    assert!(
        owner
            .authorize_project_source(
                &context,
                &issuer,
                &policy,
                child,
                operation,
                snapshot.clone(),
                1,
                1
            )
            .await
            .is_err()
    );
    let mut raw = snapshot.clone();
    raw.native_snapshot = Some(serde_json::json!({"raw":true}));
    assert!(authorize(child, raw).await.is_err());
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let inspect = |selected, op, source, budget| {
        runtime.inspect_project_source(
            &context,
            owner.clone(),
            selected,
            op,
            source,
            &snapshot,
            budget,
        )
    };
    let cancelled_guard = acyclic_fs::CancellationToken::new();
    let cancelled_owner = Arc::new(
        fixture
            .host
            .journal_owner(fixture.root, context.owned_task_fence()?)
            .await?
            .with_invocation_cancellation(&cancelled_guard),
    );
    cancelled_guard.cancel();
    assert!(
        cancelled_owner
            .authorize_project_source(
                &context,
                &issuer,
                &policy,
                child,
                operation,
                snapshot.clone(),
                1_000_000,
                128 * 1_024 * 1_024
            )
            .await
            .is_err()
    );
    assert!(
        cancelled_owner
            .restore_project_source_authorization(&context, &record)
            .await
            .is_err()
    );
    assert!(
        runtime
            .inspect_project_source(
                &context,
                cancelled_owner,
                handle.clone(),
                operation,
                child,
                &snapshot,
                budget
            )
            .await
            .is_err()
    );
    let inspected = inspect(handle.clone(), operation, child, budget).await?;
    let (file, bytes) = inspected
        .read_path(&project, "", "selected.txt", &generation)
        .await?;
    assert_eq!(bytes.as_ref(), b"frozen selected");
    assert!(matches!(
        context.read_file(&file).await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(
        inspected.read_file(&file).await?.as_ref(),
        b"frozen selected"
    );
    assert!(!inspected.diff(&file, &file).await?);
    let page = inspected
        .list(&project, "", "", &generation, None, 1)
        .await?;
    assert_eq!(page.entries.len(), 1);
    assert!(page.has_more);
    let next = inspected
        .list(
            &project,
            "",
            "",
            &generation,
            Some(&page.entries[0].name),
            1,
        )
        .await?;
    assert_eq!(next.entries.len(), 1);
    assert_ne!(page.entries[0].name, next.entries[0].name);
    for (volume, prefix, path, pinned) in [
        (&project, "", ".system/hidden", &generation),
        (&project, "selected", "selected.txt", &generation),
        (&policy.bootstrap.project, "", "selected.txt", &generation),
        (&project, "", "selected.txt", &initial),
    ] {
        assert!(
            inspected
                .read_path(volume, prefix, path, pinned)
                .await
                .is_err()
        );
    }
    for (op, source, limit) in [
        (OperationId::from_bytes([125; 16]), child, budget),
        (operation, fixture.root, budget),
        (operation, child, acyclic_fs::WorkBudget::UNBOUNDED),
    ] {
        assert!(inspect(handle.clone(), op, source, limit).await.is_err());
    }
    let mut denied = budget;
    denied.output_bytes = 1;
    assert!(
        inspect(handle.clone(), operation, child, denied)
            .await?
            .read_path(&project, "", "selected.txt", &generation)
            .await
            .is_err()
    );
    assert_eq!(context.scope().grants(), &grants);
    assert_eq!(
        fixture.filesystem.resolve(&workspace).await?.generation,
        generation
    );

    #[cfg(not(target_arch = "wasm32"))]
    {
        let native_cancel = acyclic_fs::CancellationToken::new();
        let work = test_work(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &native_cancel,
        )?;
        let mut prepared = created.request.clone();
        prepared.read_authorization = Some(record.clone());
        prepared.command = None;
        prepared.payload =
            serde_json::json!({"operation_id": operation, "authorized_source": record});
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        fixture
            .host
            .seal_prepared_project_source(&prepared, fixture.root, Some(&io))
            .await?
            .unwrap()
            .require_selection(operation, child, &project, &generation, None, Some(&work))?;
        let before = work.receipt();
        for field in ["operation_id", "authorized_source"] {
            let mut altered = prepared.clone();
            altered.payload[field] = Value::Null;
            assert!(
                fixture
                    .host
                    .seal_prepared_project_source(&altered, fixture.root, Some(&io))
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations
        );
        assert!(
            fixture
                .host
                .seal_prepared_project_source(&prepared, child, Some(&io))
                .await
                .is_err()
        );
        let restored = owner
            .restore_project_source_authorization_with_native_work(&context, &record, &work)?
            .await?;
        let selected = runtime
            .inspect_project_source_with_native_work(
                &context,
                owner.clone(),
                restored,
                operation,
                child,
                &snapshot,
                &work,
            )
            .await?;
        selected
            .require_native_project_selection_with_native_work(
                child,
                &project,
                &generation,
                None,
                &work,
            )
            .await?;
        let foreign_cancel = acyclic_fs::CancellationToken::new();
        let foreign_work = test_work(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &foreign_cancel,
        )?;
        assert!(
            selected
                .require_native_project_selection_with_native_work(
                    child,
                    &project,
                    &generation,
                    None,
                    &foreign_work
                )
                .await
                .is_err()
        );
        assert_eq!(foreign_work.receipt().backend_read_operations, 0);
        assert_eq!(foreign_work.receipt().backend_write_operations, 0);
        for (source, volume, pinned) in [
            (fixture.root, &project, &generation),
            (child, &policy.bootstrap.project, &generation),
            (child, &project, &initial),
        ] {
            assert!(
                selected
                    .require_native_project_selection_with_native_work(
                        source, volume, pinned, None, &work
                    )
                    .await
                    .is_err()
            );
        }
        let mut no_reads = acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024);
        no_reads.backend_read_operations = 0;
        let denied_work = test_work(no_reads, &native_cancel)?;
        let denied = async {
            let handle = owner
                .restore_project_source_authorization_with_native_work(
                    &context,
                    &record,
                    &denied_work,
                )?
                .await?;
            runtime
                .inspect_project_source_with_native_work(
                    &context,
                    owner.clone(),
                    handle,
                    operation,
                    child,
                    &snapshot,
                    &denied_work,
                )
                .await
        }
        .await;
        assert!(matches!(denied, Err(Error::NativeWork { .. })));
        let before = work.receipt();
        native_cancel.cancel();
        assert!(
            async {
                owner
                    .restore_project_source_authorization_with_native_work(
                        &context, &record, &work,
                    )?
                    .await
            }
            .await
            .is_err()
        );
        assert!(
            fixture
                .host
                .seal_prepared_project_source(&prepared, fixture.root, Some(&io))
                .await
                .is_err()
        );
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations
        );
    }

    authorize(fixture.root, policy.bootstrap.clone()).await?;
    let crate::distributed::WorkPull::Claimed(child_lease) =
        fixture.host.recover_work(child, &mut None).await?
    else {
        panic!("child lease")
    };
    let child_context = runtime
        .task_context(child, (&child_lease.reservation).into())
        .await?;
    let child_guard = acyclic_fs::CancellationToken::new();
    let child_owner = fixture
        .host
        .journal_owner(child, child_context.owned_task_fence()?)
        .await?
        .with_invocation_cancellation(&child_guard);
    child_owner
        .authorize_project_source(
            &child_context,
            &issuer,
            &policy,
            fixture.root,
            operation,
            policy.bootstrap.clone(),
            1_000_000,
            128 * 1_024 * 1_024,
        )
        .await?;
    let mut descendant = created.request.child.clone();
    descendant.operation_id = OperationId::from_bytes([123; 16]);
    descendant.parent = Some(child);
    descendant.grants = Capabilities::new([project.capability(VolumeOperation::Read, &mut None)?]);
    fixture
        .host
        .admit_owned(descendant, (&child_lease.reservation).into())
        .await?;
    fixture.start_next(TaskId::from_bytes([123; 16])).await?;
    authorize(TaskId::from_bytes([123; 16]), snapshot.clone()).await?;
    let sibling = TaskId::from_bytes([122; 16]);
    let mut sibling_admission = fixture.admission(122, serde_json::json!({}));
    sibling_admission.grants = Capabilities::new(["task:spawn:test.created@1"]);
    fixture.admit_child(sibling_admission).await?;
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.recover_work(sibling, &mut None).await?
    else {
        panic!("sibling lease")
    };
    let sibling_context = runtime
        .task_context(sibling, (&lease.reservation).into())
        .await?;
    let guard = acyclic_fs::CancellationToken::new();
    let sibling_owner = Arc::new(
        fixture
            .host
            .journal_owner(sibling, sibling_context.owned_task_fence()?)
            .await?
            .with_invocation_cancellation(&guard),
    );
    sibling_owner
        .authorize_project_source(
            &sibling_context,
            &issuer,
            &policy,
            child,
            operation,
            snapshot.clone(),
            1_000_000,
            128 * 1_024 * 1_024,
        )
        .await?;
    assert!(
        runtime
            .inspect_project_source(
                &sibling_context,
                sibling_owner,
                handle.clone(),
                operation,
                child,
                &snapshot,
                budget
            )
            .await
            .is_err()
    );
    let mut foreign_root = fixture.admission(124, serde_json::json!({}));
    foreign_root.parent = None;
    fixture.host.admit(foreign_root).await?;
    fixture.start_next(TaskId::from_bytes([124; 16])).await?;
    assert!(
        authorize(TaskId::from_bytes([124; 16]), policy.bootstrap.clone())
            .await
            .is_err()
    );
    let advanced = fixture
        .filesystem
        .apply(
            &workspace,
            Some(&generation),
            &[WorkspaceMutation::PutFile {
                path: "/selected.txt".into(),
                bytes: b"new mutable head".to_vec().into(),
            }],
            &IdempotencyKey::new("source-inspection-advanced-head")?,
        )
        .await?;
    assert_ne!(advanced, generation);
    assert_eq!(
        inspected.read_file(&file).await?.as_ref(),
        b"frozen selected"
    );
    assert_eq!(
        fixture.filesystem.resolve(&workspace).await?.generation,
        advanced
    );

    let mut request = fixture.sandbox_request()?;
    request.child.operation_id = OperationId::from_bytes([131; 16]);
    request.child.resource_proofs[0].preparation = TaskId::from_bytes([130; 16]);
    request.read_authorization = Some(record.clone());
    request.payload = serde_json::json!({"operation_id": operation, "authorized_source": record});
    fixture
        .host
        .admit_owned(
            fixture.admission(130, serde_json::to_value(&request).unwrap()),
            (&fixture.root_lease.reservation).into(),
        )
        .await?;
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.pull_work(&fixture.worker).await?
    else {
        panic!("helper lease")
    };
    fixture.host.start_task(&lease, None).await?;
    let helper_guard = acyclic_fs::CancellationToken::new();
    let helper = fixture
        .host
        .journal_owner(TaskId::from_bytes([130; 16]), (&lease.reservation).into())
        .await?
        .with_invocation_cancellation(&helper_guard);
    let prepared = helper
        .resource_preparation(request.clone(), &mut None)
        .await?;
    assert!(
        !prepared
            .grants()
            .contains(&project.capability(VolumeOperation::Read, &mut None)?)
    );
    assert_eq!(
        prepared.project_source().map(|s| s.authorization()),
        Some(&record)
    );
    let mut altered = request.clone();
    altered.read_authorization.as_mut().unwrap().operation_id = OperationId::from_bytes([132; 16]);
    assert!(
        helper
            .resource_preparation(altered, &mut None)
            .await
            .is_err()
    );
    fixture
        .host
        .coordinator
        .lock()
        .await
        .apply(
            IdempotencyKey::new("source-stale-owner")?,
            crate::scheduler::SchedulerEvent::LeaseReleased {
                operation_id: fixture.root_lease.operation.operation_id,
                fence: crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation),
            },
            &mut None,
        )
        .await?;
    assert!(
        authorize(fixture.root, policy.bootstrap.clone())
            .await
            .is_err()
    );
    assert!(
        owner
            .restore_project_source_authorization(&context, &record)
            .await
            .is_err()
    );
    assert!(inspected.read_file(&file).await.is_err());
    assert!(
        inspected
            .list(&project, "", "", &generation, None, 1)
            .await
            .is_err()
    );
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.recover_work(fixture.root, &mut None).await?
    else {
        panic!("recovered root")
    };
    fixture.host.start_task(&lease, None).await?;
    let recovered_context = runtime
        .task_context(fixture.root, (&lease.reservation).into())
        .await?;
    let guard = acyclic_fs::CancellationToken::new();
    let recovered = Arc::new(
        fixture
            .host
            .journal_owner(fixture.root, recovered_context.owned_task_fence()?)
            .await?
            .with_invocation_cancellation(&guard),
    );
    let restored = recovered
        .restore_project_source_authorization(&recovered_context, &record)
        .await?;
    assert_eq!(restored.authorization(), &record);
    let reopened = runtime
        .inspect_project_source(
            &recovered_context,
            recovered.clone(),
            restored,
            operation,
            child,
            &snapshot,
            budget,
        )
        .await?;
    assert_eq!(
        reopened.read_file(&file).await?.as_ref(),
        b"frozen selected"
    );
    assert!(
        helper
            .resource_preparation(request, &mut None)
            .await?
            .project_source()
            .is_some()
    );
    guard.cancel();
    assert!(reopened.read_file(&file).await.is_err());
    fixture
        .host
        .coordinator
        .lock()
        .await
        .cancel_operation(
            fixture.host.verifier.audience(),
            &fixture.host.owner_scope,
            &fixture.host.verifier,
            OperationId::from_bytes(fixture.root.into_bytes()),
            IdempotencyKey::new("source-cancel-original")?,
            true,
            None,
            &mut None,
        )
        .await?;
    assert!(
        recovered
            .restore_project_source_authorization(&recovered_context, &record)
            .await
            .is_err()
    );
    assert!(
        recovered
            .authorize_project_source(
                &recovered_context,
                &issuer,
                &policy,
                fixture.root,
                operation,
                policy.bootstrap.clone(),
                1_000_000,
                128 * 1_024 * 1_024
            )
            .await
            .is_err()
    );
    cancellation.cancel();
    assert!(
        owner
            .restore_project_source_authorization(&context, &record)
            .await
            .is_err()
    );
    Ok(())
}
