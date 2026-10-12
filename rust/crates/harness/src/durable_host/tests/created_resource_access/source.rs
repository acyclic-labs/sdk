use super::*;
use crate::scheduler::LeaseFence;
use crate::source_authorization::{
    ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
};
use acyclic_stream::BoxProviderFuture;

struct ProjectPolicy {
    filesystem: Arc<MemoryFilesystem>,
    root: TaskId,
}
impl RootProjectSourceProvider for ProjectPolicy {
    fn verify<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        selected: &'a ProjectSourceSnapshot,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if context.graph_root != self.root
                || !context.created_project_authenticated
                || context.maximum_work == 0
                || context.maximum_bytes == 0
                || context.cancellation.is_cancelled()
                || selected.native_snapshot.is_some()
            {
                return Err(Error::Unauthorized("source policy differs".into()));
            }
            let workspace = workspace_ref(
                selected.project.provider().clone(),
                &selected.project.storage_name(&mut None)?,
            )?;
            if self.filesystem.resolve(&workspace).await?.generation != selected.generation {
                return Err(Error::Unauthorized("source generation differs".into()));
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn created_project_authorization_keeps_scope_selection_and_original_owner() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.admit_child(created.request.child.clone()).await?;
    let child = TaskId::from_bytes([84; 16]);
    let workspace = workspace_ref(
        created.project.provider().clone(),
        &created.project.storage_name(&mut None)?,
    )?;
    let initial = fixture.filesystem.resolve(&workspace).await?.generation;
    let generation = fixture
        .filesystem
        .apply(
            &workspace,
            Some(&initial),
            &[
                WorkspaceMutation::PutFile {
                    path: "/selected.txt".into(),
                    bytes: b"frozen selected".to_vec().into(),
                },
                WorkspaceMutation::PutFile {
                    path: "/second.txt".into(),
                    bytes: b"second".to_vec().into(),
                },
            ],
            &IdempotencyKey::new("source-inspection-files")?,
        )
        .await?;
    let runtime = fixture.runtime(crate::tool::ToolRegistry::default())?;
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
    let policy = ProjectPolicy {
        filesystem: fixture.filesystem.clone(),
        root: fixture.root,
    };
    let operation = OperationId::from_bytes([120; 16]);
    let snapshot = ProjectSourceSnapshot {
        project: created.project.clone(),
        generation: generation.clone(),
        native_snapshot: None,
    };
    let handle = owner
        .authorize_project_source(
            &context,
            &issuer,
            &policy,
            child,
            operation,
            snapshot.clone(),
            1_000_000,
            128 * 1_024 * 1_024,
        )
        .await?;
    let read = created
        .project
        .capability(VolumeOperation::Read, &mut None)?;
    assert!(!context.scope().grants().contains(&read));
    assert!(
        handle
            .grant()
            .require(&created.project, VolumeOperation::Read, None)
            .is_ok()
    );
    assert!(
        handle
            .grant()
            .require(&created.project, VolumeOperation::Write, None)
            .is_err()
    );
    let record = handle.authorization().clone();
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
            6 => {
                forged.project = VolumeRef::new(
                    fixture.filesystem_provider.clone(),
                    "parent-project".into(),
                    VolumeClass::Project,
                    VolumeOwner::Project("created-project".into()),
                    &mut None,
                )?
            }
            7 => forged.generation = initial.clone(),
            8 => forged.native_snapshot = Some(serde_json::json!({"forged":true})),
            9 => forged.graph_root = child,
            10 => forged.maximum_work += 1,
            _ => {
                forged.policy_scope = foreign.issue(
                    forged.policy_scope.id(),
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
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let inspected = runtime
        .inspect_project_source(
            &context,
            owner.clone(),
            handle.clone(),
            operation,
            child,
            &snapshot,
            budget,
        )
        .await?;
    let (file, bytes) = inspected
        .read_path(&created.project, "", "selected.txt", &generation)
        .await?;
    assert_eq!(bytes.as_ref(), b"frozen selected");
    assert!(context.read_file(&file).await.is_err());
    assert_eq!(
        inspected.read_file(&file).await?.as_ref(),
        b"frozen selected"
    );
    assert!(!inspected.diff(&file, &file).await?);
    let page = inspected
        .list(&created.project, "", "", &generation, None, 1)
        .await?;
    assert!(page.has_more);
    let next = inspected
        .list(
            &created.project,
            "",
            "",
            &generation,
            Some(&page.entries[0].name),
            1,
        )
        .await?;
    assert_ne!(page.entries[0].name, next.entries[0].name);
    for (op, source, limit) in [
        (OperationId::from_bytes([125; 16]), child, budget),
        (operation, fixture.root, budget),
        (operation, child, acyclic_fs::WorkBudget::UNBOUNDED),
    ] {
        assert!(
            runtime
                .inspect_project_source(
                    &context,
                    owner.clone(),
                    handle.clone(),
                    op,
                    source,
                    &snapshot,
                    limit
                )
                .await
                .is_err()
        );
    }
    let mut denied = budget;
    denied.output_bytes = 1;
    let inspected = runtime
        .inspect_project_source(
            &context,
            owner.clone(),
            handle.clone(),
            operation,
            child,
            &snapshot,
            denied,
        )
        .await?;
    assert!(
        inspected
            .read_path(&created.project, "", "selected.txt", &generation)
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
    #[cfg(not(target_arch = "wasm32"))]
    {
        let native_cancel = acyclic_fs::CancellationToken::new();
        let work = test_work(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &native_cancel,
        )?;
        let fence = LeaseFence::from(&fixture.root_lease.reservation);
        let native_context = runtime
            .task_context_with_native_work(fixture.root, &fence, &work)
            .await?;
        let native_owner = fixture
            .host
            .journal_owner_with_native_work(fixture.root, &fence, &work)
            .await?
            .with_invocation_cancellation(&native_cancel)
            .into_native_shared(&work)?;
        let restored = native_owner
            .restore_project_source_authorization_with_native_work(
                native_context.as_ref(),
                &record,
                &work,
            )?
            .await?;
        let selected = runtime
            .inspect_project_source_with_native_work(
                native_context.as_ref(),
                native_owner.clone(),
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
                &created.project,
                &generation,
                None,
                &work,
            )
            .await?;
        let other = test_work(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &native_cancel,
        )?;
        assert!(
            selected
                .require_native_project_selection_with_native_work(
                    child,
                    &created.project,
                    &generation,
                    None,
                    &other
                )
                .await
                .is_err()
        );
        assert_eq!(other.receipt().backend_read_operations, 0);
        native_cancel.cancel();
        assert!(
            async {
                native_owner
                    .restore_project_source_authorization_with_native_work(
                        native_context.as_ref(),
                        &record,
                        &work,
                    )?
                    .await
            }
            .await
            .is_err()
        );
    }
    cancellation.cancel();
    assert!(
        owner
            .restore_project_source_authorization(&context, &record)
            .await
            .is_err()
    );
    assert!(!context.scope().grants().contains(&read));
    Ok(())
}
