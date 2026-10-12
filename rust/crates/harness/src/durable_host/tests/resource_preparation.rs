use super::*;
use crate::runtime::TaskSpawner as _;
use crate::resource_preparation::ResourcePreparationRequest;
use crate::resources::{ResourceKind, ResourceRef};

#[tokio::test]
async fn preparation_binds_admitted_parent_input_budget_and_current_lease() -> Result<()> {
    check_preparation_budget(true, false).await
}

#[tokio::test]
async fn preparation_rejects_an_unconfigured_session_before_creation() -> Result<()> {
    check_preparation_budget(false, false).await
}

#[tokio::test]
async fn preparation_rejects_foreign_child_proof_owner_before_creation() -> Result<()> {
    check_preparation_budget(true, true).await
}

async fn check_preparation_budget(configured: bool, foreign_proof: bool) -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let payloads = Arc::new(MemoryPayloads::new()?);
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "resource-owner".into(),
    };
    let issuer = AuthorityIssuer::new("resource-owner", [61; 32], authority.clone());
    let grants = Capabilities::new([
        "operation:declare".to_owned(),
        "operation:observe".into(),
        "operation:cancel".into(),
        "task:spawn:test.resources@1".into(),
        "resource:create:custom".into(),
        payloads
            .volume
            .capability(crate::conversation::VolumeOperation::Read, &mut None)?,
    ]);
    let signed = issuer.issue("owner", &grants, None, None, &mut None).expect("scope proof");
    let scope = RuntimeScope::new(grants, Limits::default())?;
    let schema = serde_json::json!({"type":"object"});
    let implementation = Arc::new(BatchMachine {
        identity: MachineIdentity {
            name: "test.resources".into(),
            version: "1".into(),
            digest: [62; 32],
        },
        schema: schema.clone(),
    });
    let (identity, machine, tasks, machines) = test_registries::<Value>(
        implementation, schema.clone(), schema.clone(),
    )?;
    let mut host = CoordinatorTaskHost::new(DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?, stream.clone(), payloads.clone(), payloads, &(authority), signed, issuer.verifier(&mut None).expect("verifier copy"), scope.clone(), tasks.clone(), machines, Arc::new(SystemUnixMillisClock), None)?;
    if configured {
        host = host.with_session_limits(crate::scheduler::SessionLimits {
            active_tasks: 2,
            total_tasks: 2,
            depth: 1,
            model_steps: 1,
        }, None)?;
    }
    let host = Arc::new(host);
    #[cfg(not(target_arch = "wasm32"))]
    let harness = {
        let mut bindings = crate::runtime::Bindings::local();
        bindings.tasks = tasks;
        bindings.scope = scope.clone();
        bindings.durable_host = Some(host.clone());
        bindings.build(None)?
    };
    let admission = |operation_id, parent, input| TaskAdmissionRecord {
        resource_proofs: Vec::new(),
        operation_id,
        task: identity.clone(),
        machine: machine.clone(),
        input,
        input_schema: schema.clone(),
        output_schema: schema.clone(),
        parent,
        grants: scope.grants().clone(),
        limits: scope.limits(),
        run_limits: scope.run_limits(),
        policy: None,
        extensions: None,
        execution: None,
    };
    let root_op = OperationId::from_bytes([63; 16]);
    let root = TaskId::from_bytes(root_op.into_bytes());
    assert!(
        matches!(host.admit(admission(root_op,None,serde_json::json!({}))).await?,
        Admission::Accepted(id) if id==root)
    );
    let worker = crate::distributed::Worker {
        id: "resource-worker".into(),
        available: Default::default(),
        labels: BTreeMap::new(),
    };
    let crate::distributed::WorkPull::Claimed(root_lease) = host.pull_work(&worker).await? else {
        return Err(Error::NotFound("root resource lease".into()));
    };
    host.start_task(&root_lease, None).await?;
    let root_fence = crate::scheduler::LeaseFence::from(&root_lease.reservation);
    let prep_op = OperationId::from_bytes([64; 16]);
    let prep = TaskId::from_bytes(prep_op.into_bytes());
    let mut child = admission(
        OperationId::from_bytes([65; 16]),
        Some(root),
        serde_json::json!({}),
    );
    let provider = ProviderRef::new("custom".into(), "allocator".into(), "1".into(), &mut None)?;
    let resource = ResourceRef::new(
        ResourceKind::Sandbox,
        provider.clone(),
        b"one".to_vec().into(),
        None, &mut None
    )?;
    child = child.with_resource_proofs(vec![
        crate::resource_preparation::ResourceCreationProofRef {
            provider: provider.clone(),
            preparation: if foreign_proof {
                TaskId::from_bytes([67; 16])
            } else {
                prep
            },
            resource: resource.clone(),
            evidence: ResourceRef::new(
                ResourceKind::Stream,
                provider.clone(),
                b"allocation".to_vec().into(),
                None, &mut None
            )?,
        },
    ])?;
    let request = ResourcePreparationRequest {
provider: provider.clone(),
child: child.clone(),
creation_grant: "resource:create:custom".into(),
maximum_resources: 3,
maximum_work: 7,
maximum_bytes: 65_536,
payload: serde_json::json!({"allocation":"one"}),
command: None,
source_checkpoint: None,
input_files: Vec::new(),
read_authorization: None,
};
    let input = serde_json::to_value(&request).map_err(|e| Error::Invalid(e.to_string()))?;
    assert!(
        matches!(host.admit_owned(admission(prep_op,Some(root),input),root_fence.clone()).await?,
        Admission::Accepted(id) if id==prep)
    );
    let crate::distributed::WorkPull::Claimed(prep_lease) = host.pull_work(&worker).await? else {
        return Err(Error::NotFound("resource preparation lease".into()));
    };
    host.start_task(&prep_lease, None).await?;
    let fence = crate::scheduler::LeaseFence::from(&prep_lease.reservation);
    let owner = Arc::new(host.journal_owner(prep, fence.clone()).await?);
    if !configured {
        assert!(matches!(
            owner.resource_preparation(request, &mut None).await,
            Err(Error::Unsupported(_))
        ));
        return Ok(());
    }
    if foreign_proof {
        assert!(matches!(
            owner.resource_preparation(request, &mut None).await,
            Err(Error::Unauthorized(_))
        ));
        return Ok(());
    }
    let token = owner.resource_preparation(request.clone(), &mut None).await?;
    token.verify(false, None)?.await?;
    #[cfg(not(target_arch = "wasm32"))]
    check_native_preparation_authority(&host, &harness, prep, &fence, &token).await?;
    assert_eq!(token.origin().preparation_task, prep);
    assert_eq!(token.origin().parent_task, root);
    assert_eq!(token.origin().intended_child, child.operation_id);
    assert_eq!(
        token.origin().request_digest,
        crate::contract::canonical_json_digest(&request)?
    );
    for change in 0..4 {
        let mut changed = request.clone();
        match change {
            0 => changed.payload = serde_json::json!({"allocation":"other"}),
            1 => changed.child.parent = Some(TaskId::from_bytes([66; 16])),
            2 => changed.child.operation_id = prep_op,
            _ => changed.creation_grant = "resource:create:foreign".into(),
        }
        assert!(matches!(
            owner.resource_preparation(changed, &mut None).await,
            Err(Error::Unauthorized(_))
        ));
    }
    let unbound = crate::resource_preparation::ResourceCreationProofRef {
        provider: request.provider.clone(),
        preparation: prep,
        resource: request.resources().next().expect("created resource").clone(),
        evidence: ResourceRef::new(
            ResourceKind::Stream,
            request.provider.clone(),
            b"allocation".to_vec().into(),
            None, &mut None
        )?,
    };
    let proved_child = child.clone().with_resource_proofs(vec![unbound])?;
    assert!(matches!(
        host.admit_owned(proved_child, root_fence.clone()).await,
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        host.admit_owned(child.with_resource_proofs(Vec::new())?, root_fence)
            .await,
        Err(Error::Invalid(_))
    ));
    for wrong_proof in 0..3 {
        let mut changed = request.clone();
        match wrong_proof {
            0 => changed.child.resource_proofs.clear(),
            1 => {
                changed.child.resource_proofs[0].provider =
                    ProviderRef::new("foreign".into(), "creator".into(), "1".into(), &mut None)?
            }
            _ => {
                changed.child.resource_proofs[0].resource = ResourceRef::new(
                    ResourceKind::Sandbox,
                    request.provider.clone(),
                    b"other".to_vec().into(),
                    None, &mut None
                )?
            }
        }
        if wrong_proof == 2 {
            assert!(matches!(owner.resource_preparation(changed, &mut None).await, Err(Error::Unauthorized(_))));
        } else {
            assert!(matches!(changed.validate(&mut None), Err(Error::Invalid(_))));
        }
    }
    let mut duplicate = request.clone();
    duplicate.child.resource_proofs.push(duplicate.child.resource_proofs[0].clone());
    assert!(matches!(duplicate.validate(&mut None), Err(Error::Invalid(_))));
    let mut over_bytes = request.clone();
    over_bytes.maximum_bytes = 1;
    assert!(matches!(over_bytes.validate(&mut None), Err(Error::Invalid(_))));
    for unlimited in [false, true] {
        let mut changed = request.clone();
        if unlimited {
            changed.maximum_work = u64::MAX;
        } else {
            changed.maximum_bytes = u64::MAX;
        }
        assert!(matches!(changed.validate(&mut None), Err(Error::Invalid(_))));
        assert!(matches!(
            owner.resource_preparation(changed, &mut None).await,
            Err(Error::Invalid(_))
        ));
    }
    host.coordinator
        .lock()
        .await
        .cancel_operation(
            host.verifier.audience(),
            &host.owner_scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
            prep_op,
            IdempotencyKey::new("cancel-resource-preparation")?,
            false, None, &mut None,
        )
        .await?;
    assert!(token.verify(false, None)?.await.is_err());
    assert!(owner.resource_preparation(request, &mut None).await.is_err());
    token.verify(true, None)?.await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn check_native_preparation_authority(
    host: &Arc<CoordinatorTaskHost<MemoryStream>>,
    harness: &Arc<crate::runtime::AgentHarness>,
    task: TaskId,
    fence: &crate::scheduler::LeaseFence,
    ordinary: &crate::resource_preparation::TaskResourcePreparation,
) -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024), &cancellation)?;
    let before = work.receipt();
    assert!(matches!(ordinary.verify(false, Some(&crate::filesystem::PreparationIo::from_native_work(&work)?)),
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(work.receipt(), before);

    let owner = host.journal_owner_with_native_work(task, fence, &work).await?
        .into_native_shared(&work)?;
    let (context, custody) = harness.durable_context_with_native_work(
        task, OperationId::from_bytes(task.into_bytes()), &work,
    ).await?;
    let query = context.into_native_shared(custody, &work)?;
    let witness = host.task_preparation_readonly_witness_with_native_work(&query, &work)?.await?;
    let request = witness.original_preparation_request_with_native_work(&query, &work)?.await?;
    witness.verify_preparation_origin_with_native_work(
        &query, &request.0, ordinary.origin(), &work,
    )?;
    let parent = ordinary.origin().parent_task;
    let (other_context, other_custody) = harness.durable_context_with_native_work(
        parent, OperationId::from_bytes(parent.into_bytes()), &work,
    ).await?;
    let other_query = other_context.into_native_shared(other_custody, &work)?;
    let before = work.receipt();
    assert!(matches!(witness.original_preparation_request_with_native_work(&other_query, &work),
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(work.receipt(), before);
    let token = owner.resource_preparation_with_native_work(request, &work)?.await?;
    assert_eq!(token.origin(), ordinary.origin());
    token.verify(false, Some(&crate::filesystem::PreparationIo::from_native_work(&work)?))?.await?;

    let foreign_cancellation = acyclic_fs::CancellationToken::new();
    let foreign = test_work(
        acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024), &foreign_cancellation,
    )?;
    let before = foreign.receipt();
    assert!(matches!(token.verify(false, Some(&crate::filesystem::PreparationIo::from_native_work(&foreign)?)),
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert_eq!(foreign.receipt(), before);
    Ok(())
}
