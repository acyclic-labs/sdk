use super::*;
use crate::source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider};
use crate::filesystem::FilesystemTaskRuntime;

// This policy verifies the existing memory Filesystem's actual named workspace
// generation; it never treats an arbitrary native descriptor as capture proof.
struct MeasuredRootPolicy {
    filesystem: Arc<MemoryFilesystem>,
    root: TaskId,
    bootstrap: ProjectSourceSnapshot,
}
impl RootProjectSourceProvider for MeasuredRootPolicy {
    fn verify<'a>(&'a self, context: RootProjectSourceContext<'a>, snapshot: &'a ProjectSourceSnapshot)
        -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if context.graph_root != self.root || context.maximum_work == 0 || context.maximum_bytes == 0
                || context.cancellation.is_cancelled()
                || snapshot.native_snapshot.is_some()
                || (!context.created_project_authenticated && (context.source.operation_id.into_bytes() != self.root.into_bytes()
                    || snapshot != &self.bootstrap))
            {
                return Err(Error::Unauthorized("explicit source policy or genuine bootstrap differs".into()));
            }
            let workspace = workspace_ref(snapshot.project.provider().clone(), &snapshot.project.storage_name()?)?;
            let actual = self.filesystem.resolve(&workspace).await?;
            if actual.generation != snapshot.generation {
                return Err(Error::Unauthorized("selected filesystem generation is not authentic".into()));
            }
            Ok(())
        })
    }
}

async fn runtime(fixture: &CreationFixture) -> Result<FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>> {
    let volume = VolumeRef::new(fixture.filesystem_provider.clone(), "scheduler", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])))?;
    FilesystemTaskRuntime::open_with_creation_providers(
        fixture.host.stream.clone(), fixture.filesystem.clone(), volume,
        fixture.host.verifier.clone(), fixture.host.owner_scope.clone(), fixture.root_scope.clone(),
        fixture.host.tasks.clone(), fixture.host.machines.clone(), crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits { active_tasks:16, total_tasks:32, depth:4, model_steps:4 },
        16, 65_536, Arc::new(SystemUnixMillisClock), None, fixture.host.resource_creation.clone(),
        crate::durable_tool::ResumableToolRegistry::default(),
    ).await
}

#[tokio::test]
async fn actual_created_descendant_project_is_operation_scoped_without_original_read_grant() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let preparation_task = TaskId::from_bytes([83;16]);
    let crate::distributed::WorkPull::Claimed(preparation_lease) = fixture.host.recover_work(preparation_task).await? else {
        return Err(Error::NotFound("genuine source allocation preparation".into()));
    };
    fixture.host.settle_task(preparation_task, (&preparation_lease.reservation).into(),
        Outcome::Succeeded(serde_json::to_value(&created.report)
            .map_err(|error|Error::Invalid(error.to_string()))?)).await?;
    fixture.admit_child(created.request.child.clone()).await?;
    let child = TaskId::from_bytes([84; 16]);
    let project = created.project;
    let workspace = workspace_ref(fixture.filesystem_provider.clone(), &project.storage_name()?)?;
    let initial_generation = fixture.filesystem.resolve(&workspace).await?.generation;
    let generation = fixture.filesystem.apply(&workspace, Some(&initial_generation), &[
        crate::filesystem::WorkspaceMutation::PutFile { path:"/selected.txt".into(), bytes:b"frozen selected".to_vec() },
        crate::filesystem::WorkspaceMutation::PutFile { path:"/second.txt".into(), bytes:b"second".to_vec() },
    ], &IdempotencyKey::new("source-inspection-files")?).await?;
    let parent_project = VolumeRef::new(fixture.filesystem_provider.clone(), "parent-project", VolumeClass::Project,
        VolumeOwner::Project("created-project".into()))?;
    let parent_workspace = workspace_ref(fixture.filesystem_provider.clone(), &parent_project.storage_name()?)?;
    let policy = MeasuredRootPolicy { filesystem:fixture.filesystem.clone(), root:fixture.root,
        bootstrap:ProjectSourceSnapshot { project:parent_project,
            generation:fixture.filesystem.resolve(&parent_workspace).await?.generation, native_snapshot:None } };
    let runtime = runtime(&fixture).await?;
    let context = runtime.task_context(fixture.root, (&fixture.root_lease.reservation).into()).await?;
    let invocation_guard = acyclic_fs::CancellationToken::new();
    let owner = Arc::new(fixture.host.journal_owner(fixture.root, context.owned_task_fence()?).await?
        .with_invocation_cancellation(&invocation_guard));
    let issuer = AuthorityIssuer::new("created-owner", [80;32], fixture.host.owner.clone());
    let operation = OperationId::from_bytes([120;16]);
    let original = context.scope().grants().clone();
    assert!(!original.contains(&project.capability(VolumeOperation::Read)?));
    let handle = owner.authorize_project_source(&context, &issuer, &policy, child, operation,
        ProjectSourceSnapshot { project:project.clone(), generation:generation.clone(), native_snapshot:None },
        1_000_000, 128 * 1_024 * 1_024).await?;
    assert_eq!(context.scope().grants(), &original);
    handle.require_selection(operation, child, &project, &generation, None)?;
    let attestation = &handle.authorization().policy_scope;
    assert_eq!(attestation.capabilities(), &Capabilities::new([attestation.id()]));
    assert!(!attestation.capabilities().contains(&project.capability(VolumeOperation::Read)?));
    handle.grant().require(&project, VolumeOperation::Read)?;
    assert!(handle.grant().require(&policy.bootstrap.project, VolumeOperation::Read).is_err());
    assert!(handle.grant().require(&project, VolumeOperation::Write).is_err());
    assert!(handle.grant().require(&policy.bootstrap.project, VolumeOperation::Write).is_err());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(creation_control_budget(), &cancellation)
            .expect("finite original label operation");
        let baseline = work.remaining().expect("remaining source-label bytes").peak_allocation_bytes;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let mut observer = io.history_io();
        let label = handle.authorization().scope_label_with_work(&mut Some(&mut observer))?;
        assert_eq!(label, handle.authorization().scope_label()?);
        assert_eq!(label, handle.authorization().policy_scope.id());
        assert!(work.receipt().bytes_hashed > 0);
        assert!(work.remaining().expect("retained actual label").peak_allocation_bytes < baseline);
        drop(label);
        drop(observer);
        drop(io);
        assert_eq!(work.remaining().expect("released label").peak_allocation_bytes, baseline);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = creation_control_budget();
        budget.allocation_operations = 1;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("admitted original ledger with no remaining allocator attempt");
        handle.require_selection_with_native_work(operation, child, &project, &generation, None, &work)?;
        for (selected_operation, selected_task, selected_project, selected_generation) in [
            (OperationId::from_bytes([154;16]), child, &project, &generation),
            (operation, fixture.root, &project, &generation),
            (operation, child, &policy.bootstrap.project, &generation),
            (operation, child, &project, &initial_generation),
        ] {
            assert!(matches!(handle.require_selection_with_native_work(
                selected_operation, selected_task, selected_project, selected_generation, None, &work,
            ), Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unauthorized, .. })));
        }
        handle.grant().require_directory_path_with_native_work(&project, "", &work)?;
        for path in [".system", ".system/hidden", ".system/inherited-conversation/transcript"] {
            assert!(matches!(handle.grant().require_directory_path_with_native_work(
                &project, path, &work,
            ), Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unauthorized, .. })));
        }
        assert!(matches!(handle.grant().require_directory_path_with_native_work(
            &policy.bootstrap.project, "", &work,
        ), Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unauthorized, .. })));
        for path in ["", "../escape", "a/../escape", "/absolute", "back\\slash", "a//b"] {
            assert!(matches!(crate::conversation::validate_content_path_with_native_work(
                path, &work,
            ), Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Invalid, .. })));
        }
        crate::conversation::validate_content_path_with_native_work("selected.txt", &work)?;
        assert_eq!(work.receipt().allocation_operations, 1);
        assert_eq!(work.receipt().backend_read_operations, 0);
        cancellation.cancel();
        assert!(matches!(handle.grant().require_directory_path_with_native_work(
            &project, "", &work,
        ), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, 1);
    }
    let serialized = crate::contract::canonical_json_bytes(handle.authorization())?;
    let restored_record: crate::source_authorization::ProjectSourceAuthorization =
        serde_json::from_slice(&serialized).map_err(|error|Error::Invalid(error.to_string()))?;
    restored_record.verify_scope(&fixture.host.verifier)?;
    // The serialized record can be exposed in child-readable intent data. Even
    // its authentic original-issuer scope cannot become a raw Filesystem reader.
    assert!(crate::conversation::ContentGrant::verify(
        &fixture.host.verifier, &restored_record.policy_scope, &project, VolumeOperation::Read,
    ).is_err());
    let mut other_purpose = restored_record.clone();
    other_purpose.policy_scope = issuer.root("harness.other-purpose.v1", Capabilities::new(["harness.other-purpose.v1"]));
    fixture.host.verifier.verify(&other_purpose.policy_scope)?;
    assert!(other_purpose.verify_scope(&fixture.host.verifier).is_err());
    // The public selected boundary consumes only the sealed operation handle.
    // No donor READ capability is installed in the original caller context.
    let mut budget_value = serde_json::to_value(acyclic_fs::WorkBudget::default())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for allowance in budget_value.as_object_mut().ok_or_else(|| Error::Invalid("budget shape".into()))?.values_mut() {
        *allowance = serde_json::json!(1_000_000);
    }
    let budget: acyclic_fs::WorkBudget = serde_json::from_value(budget_value)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let snapshot = ProjectSourceSnapshot { project:project.clone(), generation:generation.clone(), native_snapshot:None };
    #[cfg(not(target_arch = "wasm32"))]
    {
        let native_cancellation = acyclic_fs::CancellationToken::new();
        let native = acyclic_fs::NativeMaterializationWork::new(
            creation_control_budget(), &native_cancellation,
        ).expect("finite original source invocation");
        let mut prepared = created.request.clone();
        prepared.read_authorization = Some(handle.authorization().clone());
        prepared.command = None;
        prepared.payload = serde_json::json!({
            "operation_id": operation,
            "authorized_source": handle.authorization(),
        });
        let (sealed, custody) = fixture.host.seal_prepared_project_source_with_native_work(
            &prepared, fixture.root, &native,
        ).await?;
        let sealed = sealed.expect("authentic retained foreign Project selection");
        sealed.require_selection(operation, child, &project, &generation, None)?;
        assert!(custody.matches_work(&native));
        let grant_bytes = sealed.grant().logical_owned_bytes()?;
        let minimum = (std::mem::size_of::<crate::source_authorization::ProjectSourceReadHandle>() as u64)
            .checked_add(sealed.authorization().logical_owned_bytes()?)
            .and_then(|bytes| bytes.checked_add(grant_bytes))
            .expect("finite complete source handle ownership");
        assert!(custody.bytes() >= minimum);
        assert_eq!(context.scope().grants(), &original);
        drop(sealed);
        drop(custody);
        for field in ["operation_id", "authorized_source"] {
            let mut forged = prepared.clone();
            forged.payload[field] = Value::Null;
            let before = native.receipt();
            assert!(matches!(fixture.host.seal_prepared_project_source_with_native_work(
                &forged, fixture.root, &native,
            ).await, Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unauthorized, .. })));
            assert_eq!(native.receipt().backend_read_operations, before.backend_read_operations);
            assert_eq!(native.receipt().backend_write_operations, before.backend_write_operations);
        }
        let before = native.receipt();
        assert!(matches!(fixture.host.seal_prepared_project_source_with_native_work(
            &prepared, child, &native,
        ).await, Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(native.receipt().backend_read_operations, before.backend_read_operations);
        let baseline = native.remaining().expect("source quota").peak_allocation_bytes;
        let native_handle = owner.restore_project_source_authorization_with_native_work(
            &context, handle.authorization(), &native)?.await?;
        let selected = runtime.inspect_project_source_with_native_work(
            &context, owner.clone(), native_handle, operation, child, &snapshot, &native,
        ).await?;
        assert_eq!(selected.source().task_id(), child);
        assert_eq!(selected.project()?.volume, project);
        assert_eq!(context.scope().grants(), &original);
        assert!(native.receipt().backend_read_operations > 0);
        assert!(native.remaining().expect("retained source quota").peak_allocation_bytes < baseline);
        selected.require_native_project_selection_with_native_work(
            child, &project, &generation, None, &native,
        ).await?;
        for (source, volume, pinned) in [
            (fixture.root, &project, &generation),
            (child, &policy.bootstrap.project, &generation),
            (child, &project, &initial_generation),
        ] {
            let before = native.receipt();
            assert!(matches!(selected.require_native_project_selection_with_native_work(
                source, volume, pinned, None, &native,
            ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
            assert_eq!(native.receipt().backend_read_operations, before.backend_read_operations);
            assert_eq!(native.receipt().backend_write_operations, before.backend_write_operations);
        }
        let foreign_cancellation = acyclic_fs::CancellationToken::new();
        let foreign = acyclic_fs::NativeMaterializationWork::new(
            creation_control_budget(), &foreign_cancellation,
        ).expect("independent caller ledger for the hostile selection");
        let before = foreign.receipt();
        assert!(matches!(selected.require_native_project_selection_with_native_work(
            child, &project, &generation, None, &foreign,
        ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(foreign.receipt().backend_read_operations, before.backend_read_operations);
        foreign_cancellation.cancel();
        assert!(matches!(selected.require_native_project_selection_with_native_work(
            child, &project, &generation, None, &foreign,
        ).await, Err(Error::NativeWork { .. })));
        assert_eq!(foreign.receipt().backend_read_operations, before.backend_read_operations);
        drop(selected);
        assert_eq!(native.remaining().expect("released source quota").peak_allocation_bytes, baseline);

        let mut denied_budget = creation_control_budget();
        denied_budget.backend_read_operations = 0;
        let denied = acyclic_fs::NativeMaterializationWork::new(
            denied_budget, &native_cancellation,
        ).expect("finite denied source invocation");
        let denied_read = async {
            let native_handle = owner.restore_project_source_authorization_with_native_work(
                &context, handle.authorization(), &denied)?.await?;
            runtime.inspect_project_source_with_native_work(
                &context, owner.clone(), native_handle, operation, child, &snapshot, &denied,
            ).await
        }.await;
        assert!(matches!(denied_read, Err(Error::NativeWork { .. })));
        assert_eq!(denied.receipt().backend_read_operations, 0);
        assert_eq!(denied.receipt().backend_write_operations, 0);

        let wrong_operation = OperationId::from_bytes([153;16]);
        let native_handle = owner.restore_project_source_authorization_with_native_work(
            &context, handle.authorization(), &native)?.await?;
        assert!(runtime.inspect_project_source_with_native_work(
            &context, owner.clone(), native_handle, wrong_operation, child, &snapshot, &native,
        ).await.is_err());
        assert_eq!(native.remaining().expect("rejected source quota").peak_allocation_bytes, baseline);
        native_cancellation.cancel();
        let before = native.receipt();
        assert!(matches!(fixture.host.seal_prepared_project_source_with_native_work(
            &prepared, fixture.root, &native,
        ).await, Err(Error::NativeWork { .. })));
        assert_eq!(native.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(native.receipt().backend_read_operations, before.backend_read_operations);
        assert_eq!(native.receipt().backend_write_operations, before.backend_write_operations);
    }
    let inspected = runtime.inspect_project_source(&context, owner.clone(), handle.clone(), operation, child, &snapshot, budget).await?;
    let (selected_file, selected_bytes) = inspected.read_path(&project, "", "selected.txt", &generation).await?;
    assert_eq!(selected_bytes, b"frozen selected");
    assert!(matches!(context.read_file(&selected_file).await, Err(Error::Unauthorized(_))));
    assert_eq!(inspected.read_file(&selected_file).await?, b"frozen selected");
    assert!(!inspected.diff(&selected_file, &selected_file).await?);
    let page = inspected.list(&project, "", "", &generation, None, 1).await?;
    assert_eq!(page.entries.len(), 1);
    assert!(page.has_more);
    let second_page = inspected.list(&project, "", "", &generation, Some(&page.entries[0].name), 1).await?;
    assert_eq!(second_page.entries.len(), 1);
    assert_ne!(page.entries[0].name, second_page.entries[0].name);
    assert!(inspected.read_path(&project, "", ".system/hidden", &generation).await.is_err());
    assert!(inspected.read_path(&project, "selected", "selected.txt", &generation).await.is_err());
    assert!(inspected.read_path(&policy.bootstrap.project, "", "selected.txt", &generation).await.is_err());
    assert!(inspected.read_path(&project, "", "selected.txt", &initial_generation).await.is_err());
    assert!(runtime.inspect_project_source(&context, owner.clone(), handle.clone(),
        OperationId::from_bytes([125;16]), child, &snapshot, budget).await.is_err());
    assert!(runtime.inspect_project_source(&context, owner.clone(), handle.clone(),
        operation, fixture.root, &snapshot, budget).await.is_err());
    assert!(runtime.inspect_project_source(&context, owner.clone(), handle.clone(),
        operation, child, &snapshot, acyclic_fs::WorkBudget::UNBOUNDED).await.is_err());
    let spent = inspected.project_work().ok_or_else(|| Error::Invalid("selected receipt absent".into()))?;
    assert!(spent.backend_read_operations > 0 && spent.output_bytes > 0);
    assert_eq!(context.scope().grants(), &original);
    let mut denied_budget = budget;
    denied_budget.output_bytes = 1;
    let denied = runtime.inspect_project_source(&context, owner.clone(), handle.clone(),
        operation, child, &snapshot, denied_budget).await?;
    assert!(denied.read_path(&project, "", "selected.txt", &generation).await.is_err());
    assert_eq!(fixture.filesystem.resolve(&workspace).await?.generation, generation);
    let cancelled_guard = acyclic_fs::CancellationToken::new();
    let cancelled_owner = Arc::new(fixture.host.journal_owner(fixture.root, context.owned_task_fence()?).await?
        .with_invocation_cancellation(&cancelled_guard));
    cancelled_guard.cancel();
    assert!(cancelled_owner.authorize_project_source(&context, &issuer, &policy, child, operation,
        ProjectSourceSnapshot {project:project.clone(),generation:generation.clone(),native_snapshot:None},
        1_000_000, 128 * 1_024 * 1_024).await.is_err());
    assert!(cancelled_owner.restore_project_source_authorization(&context, handle.authorization()).await.is_err());
    assert!(runtime.inspect_project_source(&context, cancelled_owner.clone(), handle.clone(),
        operation, child, &snapshot, budget).await.is_err());
    // Root, ancestor, sibling and deeper descendant use the same actual graph,
    // never an actor-supplied tree or a copied foreign scope.
    owner.authorize_project_source(&context, &issuer, &policy, fixture.root, operation,
        policy.bootstrap.clone(), 1_000_000, 128 * 1_024 * 1_024).await?;
    let crate::distributed::WorkPull::Claimed(child_lease) = fixture.host.recover_work(child).await? else {
        return Err(Error::NotFound("source child lease".into()));
    };
    let child_context = runtime.task_context(child, (&child_lease.reservation).into()).await?;
    let child_guard = acyclic_fs::CancellationToken::new();
    let child_owner = Arc::new(fixture.host.journal_owner(child, child_context.owned_task_fence()?).await?
        .with_invocation_cancellation(&child_guard));
    child_owner.authorize_project_source(&child_context, &issuer, &policy, fixture.root, operation,
        policy.bootstrap.clone(), 1_000_000, 128 * 1_024 * 1_024).await?;
    let sibling = TaskId::from_bytes([122;16]);
    let mut sibling_admission = fixture.admission(122, serde_json::json!({}));
    sibling_admission.grants = Capabilities::new(["task:spawn:test.created@1"]);
    fixture.admit_child(sibling_admission).await?;
    let crate::distributed::WorkPull::Claimed(sibling_lease) = fixture.host.recover_work(sibling).await? else {
        return Err(Error::NotFound("source sibling lease".into()));
    };
    let sibling_context = runtime.task_context(sibling, (&sibling_lease.reservation).into()).await?;
    let sibling_guard = acyclic_fs::CancellationToken::new();
    let sibling_owner = Arc::new(fixture.host.journal_owner(sibling, sibling_context.owned_task_fence()?).await?
        .with_invocation_cancellation(&sibling_guard));
    sibling_owner.authorize_project_source(&sibling_context, &issuer, &policy, child, operation,
        ProjectSourceSnapshot { project:project.clone(),generation:generation.clone(),native_snapshot:None },
        1_000_000, 128 * 1_024 * 1_024).await?;
    assert!(runtime.inspect_project_source(&sibling_context, sibling_owner.clone(), handle.clone(),
        operation, child, &snapshot, budget).await.is_err());
    let mut descendant = created.request.child.clone();
    descendant.operation_id = OperationId::from_bytes([123;16]);
    descendant.parent = Some(child);
    descendant.grants = Capabilities::new([project.capability(VolumeOperation::Read)?]);
    fixture.host.admit_owned(descendant, (&child_lease.reservation).into()).await?;
    fixture.start_next(TaskId::from_bytes([123;16])).await?;
    owner.authorize_project_source(&context, &issuer, &policy, TaskId::from_bytes([123;16]), operation,
        ProjectSourceSnapshot {project:project.clone(),generation:generation.clone(),native_snapshot:None},
        1_000_000, 128 * 1_024 * 1_024).await?;
    let advanced = fixture.filesystem.apply(&workspace, Some(&generation), &[
        crate::filesystem::WorkspaceMutation::PutFile { path:"/selected.txt".into(), bytes:b"new mutable head".to_vec() },
    ], &IdempotencyKey::new("source-inspection-advanced-head")?).await?;
    assert_ne!(advanced, generation);
    assert_eq!(inspected.read_path(&project, "", "selected.txt", &generation).await?.1, b"frozen selected");
    assert_eq!(fixture.filesystem.resolve(&workspace).await?.generation, advanced);
    let mut foreign_root = fixture.admission(124, serde_json::json!({}));
    foreign_root.parent = None;
    fixture.host.admit(foreign_root).await?;
    fixture.start_next(TaskId::from_bytes([124;16])).await?;
    assert!(owner.authorize_project_source(&context, &issuer, &policy, TaskId::from_bytes([124;16]), operation,
        policy.bootstrap.clone(), 1_000_000, 128 * 1_024 * 1_024).await.is_err());

    // A genuine ordinary preparation admission contains the complete immutable
    // authorization, yet retains no donor project grant in original input scope.
    let mut prepared_request = fixture.sandbox_request()?;
    prepared_request.child.operation_id = OperationId::from_bytes([131;16]);
    prepared_request.child.resource_proofs[0].preparation = TaskId::from_bytes([130;16]);
    prepared_request.read_authorization = Some(handle.authorization().clone());
    prepared_request.payload = serde_json::json!({
        "operation_id":operation, "authorized_source":handle.authorization(),
    });
    let helper = fixture.admission(130, serde_json::to_value(&prepared_request)
        .map_err(|error|Error::Invalid(error.to_string()))?);
    fixture.host.admit_owned(helper, (&fixture.root_lease.reservation).into()).await?;
    let crate::distributed::WorkPull::Claimed(helper_lease) = fixture.host.pull_work(&fixture.worker).await? else {
        return Err(Error::NotFound("authorized source preparation lease".into()));
    };
    fixture.host.start_task(&helper_lease).await?;
    let helper_guard = acyclic_fs::CancellationToken::new();
    let helper_owner = Arc::new(fixture.host.journal_owner(TaskId::from_bytes([130;16]), (&helper_lease.reservation).into()).await?
        .with_invocation_cancellation(&helper_guard));
    let prepared = helper_owner.resource_preparation(prepared_request.clone()).await?;
    assert!(!prepared.grants().contains(&project.capability(VolumeOperation::Read)?));
    assert_eq!(prepared.project_source().map(|handle|handle.authorization()), Some(handle.authorization()));
    let mut altered = prepared_request.clone();
    altered.read_authorization.as_mut().ok_or_else(||Error::Invalid("source control omitted".into()))?.operation_id = OperationId::from_bytes([132;16]);
    assert!(helper_owner.resource_preparation(altered).await.is_err());
    // A foreign original issuer cannot adopt a copied descriptor or capability.
    let foreign = AuthorityIssuer::new("foreign", [81;32], fixture.host.owner.clone());
    assert!(owner.authorize_project_source(&context, &foreign, &policy, child, operation,
        ProjectSourceSnapshot { project:project.clone(), generation:generation.clone(), native_snapshot:None },
        1_000_000, 128 * 1_024 * 1_024).await.is_err());
    for control in 0..12 {
        let mut record = handle.authorization().clone();
        match control {
            0 => record.owner.id = "forged-root-owner".into(),
            1 => record.caller_task = child,
            2 => record.caller_admission_digest[0] ^= 1,
            3 => record.caller_fence.reservation_id = "stale-owner".into(),
            4 => record.operation_id = OperationId::from_bytes([121;16]),
            5 => record.source_task = fixture.root,
            6 => record.project = policy.bootstrap.project.clone(),
            7 => record.generation = GenerationRef::new(fixture.filesystem_provider.clone(), b"forged-generation".to_vec(), None)?,
            8 => record.native_snapshot = Some(serde_json::json!({"forgedNativeProof":true})),
            9 => record.graph_root = child,
            10 => record.maximum_work += 1,
            _ => record.policy_scope = foreign.root(record.policy_scope.id(), record.policy_scope.capabilities().clone()),
        }
        assert!(record.verify_scope(&fixture.host.verifier).is_err(), "descriptor control {control}");
    }
    // Exhaustive bounded combinations of independent descriptor adversaries.
    for mask in 0_u16..256 {
        let mut record = handle.authorization().clone();
        if mask & 1 != 0 { record.owner.id.push('x'); }
        if mask & 2 != 0 { record.caller_task = child; }
        if mask & 4 != 0 { record.caller_admission_digest[0] ^= 1; }
        if mask & 8 != 0 { record.operation_id = OperationId::from_bytes([126;16]); }
        if mask & 16 != 0 { record.source_task = fixture.root; }
        if mask & 32 != 0 { record.project = policy.bootstrap.project.clone(); }
        if mask & 64 != 0 { record.graph_root = child; }
        if mask & 128 != 0 { record.native_snapshot = Some(serde_json::json!({"unverified":mask})); }
        assert_eq!(record.verify_scope(&fixture.host.verifier).is_ok(), mask == 0);
    }
    assert!(owner.authorize_project_source(&context, &issuer, &policy, child, operation,
        ProjectSourceSnapshot { project:project.clone(), generation:generation.clone(), native_snapshot:Some(serde_json::json!({"raw":true})) },
        1_000_000, 128 * 1_024 * 1_024).await.is_err());
    assert!(owner.authorize_project_source(&context, &issuer, &policy, child, operation,
        ProjectSourceSnapshot { project, generation, native_snapshot:None }, 1, 1).await.is_err());
    fixture.host.coordinator.lock().await.release_lease(&fixture.root_lease, IdempotencyKey::new("source-stale-owner")?).await?;
    assert!(owner.authorize_project_source(&context, &issuer, &policy, fixture.root, operation,
        policy.bootstrap.clone(), 1_000_000, 128 * 1_024 * 1_024).await.is_err());
    assert!(owner.restore_project_source_authorization(&context, handle.authorization()).await.is_err());
    assert!(inspected.read_file(&selected_file).await.is_err());
    assert!(inspected.list(&snapshot.project, "", "", &snapshot.generation, None, 1).await.is_err());
    // Issued immutable preparation survives a legitimate original caller
    // recovery. Fresh use through the old opaque context above did not.
    let crate::distributed::WorkPull::Claimed(recovered_root) = fixture.host.recover_work(fixture.root).await? else {
        return Err(Error::NotFound("recovered original source caller".into()));
    };
    fixture.host.start_task(&recovered_root).await?;
    let recovered_context = runtime.task_context(fixture.root, (&recovered_root.reservation).into()).await?;
    let recovered_guard = acyclic_fs::CancellationToken::new();
    let recovered_owner = Arc::new(fixture.host.journal_owner(fixture.root, recovered_context.owned_task_fence()?).await?
        .with_invocation_cancellation(&recovered_guard));
    assert_eq!(recovered_owner.restore_project_source_authorization(&recovered_context, handle.authorization())
        .await?.authorization(), handle.authorization());
    let recovered_inspection = runtime.inspect_project_source(&recovered_context, recovered_owner.clone(),
        handle.clone(), operation, child, &snapshot, budget).await?;
    assert_eq!(recovered_inspection.read_file(&selected_file).await?, b"frozen selected");
    let mut forged_intent = handle.authorization().clone();
    forged_intent.operation_id = OperationId::from_bytes([133;16]);
    assert!(recovered_owner.restore_project_source_authorization(&recovered_context, &forged_intent).await.is_err());
    assert_eq!(recovered_owner.restore_project_source(&recovered_context, &prepared_request).await?
        .as_ref().map(|handle|handle.authorization()), Some(handle.authorization()));
    assert!(helper_owner.resource_preparation(prepared_request).await?.project_source().is_some());
    recovered_guard.cancel();
    assert!(recovered_inspection.read_file(&selected_file).await.is_err());
    fixture.host.coordinator.lock().await.cancel_operation(
        &fixture.host.owner, &fixture.host.owner_scope, &fixture.host.verifier,
        OperationId::from_bytes(fixture.root.into_bytes()), IdempotencyKey::new("source-cancel-original")?, true,
    ).await?;
    assert!(recovered_owner.authorize_project_source(&recovered_context, &issuer, &policy, fixture.root, operation,
        policy.bootstrap.clone(), 1_000_000, 128 * 1_024 * 1_024).await.is_err());
    assert!(recovered_owner.restore_project_source_authorization(&recovered_context, handle.authorization()).await.is_err());
    Ok(())
}
