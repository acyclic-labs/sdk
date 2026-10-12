use super::*;

#[tokio::test]
async fn partially_admitted_batch_reopens_and_fills_only_missing_slots() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let payloads = Arc::new(MemoryPayloads::new()?);
    let owner = Authority {
        kind: AggregateKind::Task,
        id: "batch-owner".into(),
    };
    let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
    let owner_scope = issuer.issue(
        "owner",
        &Capabilities::new([
            "operation:declare",
            "operation:observe",
            "operation:cancel",
            "task:spawn:test.batch@1",
        ]),
        None,
        None,
        &mut None,
    )?;
    let runtime_scope = RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
    let machine = MachineIdentity {
        name: "test.batch".into(),
        version: "1".into(),
        digest: [2; 32],
    };
    let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
        identity: machine.clone(),
        schema: serde_json::json!({"type": "integer"}),
    });
    let (task, machine, tasks, machines) = test_registries::<i64>(
        implementation,
        serde_json::json!({"type": "integer"}),
        serde_json::json!({"type": "integer"}),
    )?;
    let coordinator =
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
    let host = CoordinatorTaskHost::new(
        coordinator,
        stream.clone(),
        payloads.clone(),
        payloads.clone(),
        &owner,
        owner_scope.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        runtime_scope.clone(),
        tasks.clone(),
        machines.clone(),
        Arc::new(SystemUnixMillisClock),
        None,
    )?;
    let root_admission = |operation_id, input| TaskAdmissionRecord {
        resource_proofs: Vec::new(),
        operation_id,
        task: task.clone(),
        machine: machine.clone(),
        input,
        input_schema: serde_json::json!({"type": "integer"}),
        output_schema: serde_json::json!({"type": "integer"}),
        parent: None,
        grants: runtime_scope.grants().clone(),
        limits: runtime_scope.limits(),
        run_limits: runtime_scope.run_limits(),
        policy: None,
        extensions: runtime_scope.extensions().cloned(),
        execution: None,
    };
    assert!(matches!(
        host.admit(root_admission(
            OperationId::from_bytes([7; 16]),
            serde_json::json!("not an integer"),
        ))
        .await,
        Err(Error::Invalid(_))
    ));
    let parent_operation = OperationId::from_bytes([8; 16]);
    let parent = TaskId::from_bytes(parent_operation.into_bytes());
    assert!(matches!(host.admit(root_admission(
            parent_operation, serde_json::json!(0),
        )).await?, Admission::Accepted(id) if id == parent));
    let metadata_revision = host.coordinator.lock().await.revision();
    let unrelated_operation = OperationId::from_bytes([201; 16]);
    let mut unrelated = host
        .coordinator
        .lock()
        .await
        .scheduler()
        .operation(parent_operation)
        .ok_or_else(|| Error::NotFound("metadata parent".into()))?
        .spec
        .clone();
    unrelated.operation_id = unrelated_operation;
    let mut remote =
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
    remote
        .declare_operation(
            &owner,
            &owner_scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
            unrelated,
            IdempotencyKey::new("unrelated-metadata-observation")?,
            None,
            &mut None,
        )
        .await?;
    drop(remote);
    assert_eq!(
        host.admission_with_work(parent_operation, &mut None)
            .await?
            .operation_id,
        parent_operation
    );
    let cached = host.coordinator.lock().await;
    assert_eq!(cached.revision(), metadata_revision);
    assert!(
        cached.scheduler().operation(unrelated_operation).is_none(),
        "immutable metadata reads must not hydrate unrelated lifecycle history"
    );
    drop(cached);
    let request = DurableBatchRequest {
        group_id: GroupId::from_bytes([9; 16]),
        batch_id: BatchId::from_bytes([10; 16]),
        group_policy: crate::runtime::BatchGroupPolicy::CollectAll,
        task: task.clone(),
        machine: machine.clone(),
        inputs: vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ],
        input_schema: serde_json::json!({"type": "integer"}),
        output_schema: serde_json::json!({"type": "integer"}),
        parent: Some(parent),
        scope: runtime_scope.clone(),
        policy: None,
        execution: None,
    };
    let bounded_parent_operation = OperationId::from_bytes([27; 16]);
    let bounded_parent = TaskId::from_bytes(bounded_parent_operation.into_bytes());
    let mut bounded = root_admission(bounded_parent_operation, serde_json::json!(0));
    bounded.grants = Capabilities::new(["task:spawn:test.batch@1"]);
    bounded.limits.model_steps = 2;
    bounded.run_limits = crate::runtime::TaskRunLimits {
        concurrency: Some(2),
        max_steps: Some(2),
        deadline_epoch_ms: None,
    };
    assert!(matches!(
        host.admit(bounded.clone()).await?,
        Admission::Accepted(_)
    ));
    for kind in 0..5 {
        let mut child = bounded.clone();
        child.operation_id = OperationId::new();
        child.parent = Some(bounded_parent);
        match kind {
            0 => child.grants = runtime_scope.grants().clone(),
            1 => child.limits.model_steps = 3,
            2 => child.run_limits.max_steps = Some(3),
            3 => child.run_limits.max_steps = None,
            _ => child.run_limits.concurrency = None,
        }
        let operation_id = child.operation_id;
        assert!(matches!(
            host.admit(child).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(host.reconcile_admission(operation_id).await?.is_none());
    }
    let mut valid_child = bounded.clone();
    valid_child.operation_id = OperationId::new();
    valid_child.parent = Some(bounded_parent);
    valid_child.limits.model_steps = 1;
    valid_child.run_limits.max_steps = Some(1);
    valid_child.run_limits.concurrency = Some(1);
    assert!(matches!(
        host.admit(valid_child).await?,
        Admission::Accepted(_)
    ));
    let mut wide_batch = request.clone();
    wide_batch.batch_id = BatchId::from_bytes([28; 16]);
    wide_batch.parent = Some(bounded_parent);
    assert!(matches!(
        host.admit_batch(wide_batch.clone()).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(host.retained_batch(wide_batch.batch_id).await?.is_none());
    let mut forged = request.clone();
    forged.task.digest = [99; 32];
    assert!(matches!(
        host.admit_batch(forged).await,
        Err(Error::Conflict(_))
    ));
    let mut altered_schema = request.clone();
    altered_schema.output_schema = serde_json::json!({"type": "number"});
    assert!(matches!(
        host.admit_batch(altered_schema).await,
        Err(Error::Conflict(_))
    ));
    host.commit_batch(&request).await?;
    let first = request.operation_id(0);
    assert!(matches!(host.admit(request.member_admission(0)?).await?,
            Admission::Accepted(id) if id.into_bytes() == first.into_bytes()));
    let reopened = CoordinatorTaskHost::new(
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
        stream.clone(),
        payloads.clone(),
        payloads.clone(),
        &owner,
        owner_scope,
        issuer.verifier(&mut None).expect("verifier copy"),
        runtime_scope.clone(),
        tasks,
        machines,
        Arc::new(SystemUnixMillisClock),
        None,
    )?;
    let restored = reopened
        .load_batch(request.batch_id)
        .await?
        .ok_or_else(|| Error::NotFound("retained batch request".into()))?;
    assert_eq!(restored.canonical_value(), request.canonical_value());
    let observed = reopened
        .reconcile_batch(request.clone())
        .await?
        .ok_or_else(|| Error::NotFound("committed batch".into()))?;
    assert!(matches!(
        observed.as_slice(),
        [
            Admission::Accepted(_),
            Admission::Indeterminate { .. },
            Admission::Indeterminate { .. }
        ]
    ));
    let completed = reopened.admit_batch(request.clone()).await?;
    assert_eq!(completed.len(), 3);
    for (index, admission) in completed.iter().enumerate() {
        let Admission::Accepted(task_id) = admission else {
            return Err(Error::Conflict("batch slot was not admitted".into()));
        };
        assert_eq!(
            task_id.into_bytes(),
            request.operation_id(index).into_bytes()
        );
    }
    assert_eq!(reopened.admit_batch(request.clone()).await?, completed);
    let mut root_request = request.clone();
    root_request.batch_id = BatchId::from_bytes([15; 16]);
    root_request.parent = None;
    let root_members = reopened.admit_batch(root_request.clone()).await?;
    assert!(
        root_members
            .iter()
            .all(|entry| matches!(entry, Admission::Accepted(_)))
    );
    assert_eq!(
        reopened
            .load_batch(root_request.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("root batch request".into()))?
            .canonical_value(),
        root_request.canonical_value()
    );
    let manifest = reopened.batch_stream(request.batch_id)?;
    assert_eq!(manifest.bounds().await?.tail, 1);
    let records = manifest.read(0, 2).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(records.len(), 1);
    assert!(
        !std::str::from_utf8(&records[0].value)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .contains("inputs")
    );
    let mut collision = request.clone();
    collision.batch_id = BatchId::from_bytes([11; 16]);
    let mut claimed_admission = collision.member_admission(0)?;
    claimed_admission.input = serde_json::json!(999);
    assert!(matches!(
        reopened.admit(claimed_admission).await?,
        Admission::Accepted(_)
    ));
    reopened.commit_batch(&collision).await?;
    assert!(matches!(
        reopened.reconcile_batch(collision).await,
        Err(Error::Conflict(_))
    ));
    let mut root_collision = request.clone();
    root_collision.batch_id = BatchId::from_bytes([12; 16]);
    let mut claimed_admission = root_collision.member_admission(0)?;
    claimed_admission.parent = None;
    assert!(matches!(
        reopened.admit(claimed_admission).await?,
        Admission::Accepted(_)
    ));
    reopened.commit_batch(&root_collision).await?;
    assert!(matches!(
        reopened.reconcile_batch(root_collision).await,
        Err(Error::Conflict(_))
    ));
    let other_parent_operation = OperationId::from_bytes([13; 16]);
    let other_parent = TaskId::from_bytes(other_parent_operation.into_bytes());
    assert!(matches!(
        reopened
            .admit(root_admission(other_parent_operation, serde_json::json!(0),))
            .await?,
        Admission::Accepted(_)
    ));
    let mut foreign_parent = request.clone();
    foreign_parent.batch_id = BatchId::from_bytes([14; 16]);
    let mut claimed_admission = foreign_parent.member_admission(0)?;
    claimed_admission.parent = Some(other_parent);
    assert!(matches!(
        reopened.admit(claimed_admission).await?,
        Admission::Accepted(_)
    ));
    reopened.commit_batch(&foreign_parent).await?;
    assert!(matches!(
        reopened.reconcile_batch(foreign_parent).await,
        Err(Error::Conflict(_))
    ));
    let mut cancelled = request.clone();
    cancelled.batch_id = BatchId::from_bytes([16; 16]);
    cancelled.group_policy = crate::runtime::BatchGroupPolicy::CancelOnFailure;
    cancelled.parent = None;
    reopened.commit_batch(&cancelled).await?;
    let first_cancelled = cancelled.member_admission(0)?;
    assert!(matches!(
        reopened.admit(first_cancelled).await?,
        Admission::Accepted(_)
    ));
    let cancellation = reopened
        .cancel_batch(cancelled.batch_id)
        .await?
        .ok_or_else(|| Error::NotFound("retained cancellation".into()))?;
    assert_eq!(cancellation.group_id, cancelled.group_id);
    assert_eq!(cancellation.entries.len(), cancelled.inputs.len());
    assert!(matches!(
        &cancellation.entries[0].1,
        BatchCancellationStatus::Requested
    ));
    assert!(
        cancellation.entries[1..]
            .iter()
            .all(|(_, status)| matches!(status, BatchCancellationStatus::NotAdmitted { .. }))
    );
    assert_eq!(
        reopened
            .retained_batch_cancellation(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("cancellation declaration".into()))?
            .group_id,
        cancelled.group_id
    );
    let after_cancel = reopened.admit_batch(cancelled.clone()).await?;
    assert!(matches!(
        after_cancel.as_slice(),
        [
            Admission::Accepted(_),
            Admission::Rejected { .. },
            Admission::Rejected { .. }
        ]
    ));
    assert_eq!(
        reopened.reconcile_batch(cancelled.clone()).await?,
        Some(after_cancel)
    );
    let repeated = reopened
        .cancel_batch(cancelled.batch_id)
        .await?
        .ok_or_else(|| Error::NotFound("replayed cancellation".into()))?;
    assert_eq!(repeated.entries.len(), cancelled.inputs.len());
    assert!(matches!(
        &repeated.entries[0].1,
        BatchCancellationStatus::Requested
    ));
    assert_eq!(
        reopened
            .batch_cancellation_stream(cancelled.batch_id)?
            .bounds()
            .await?
            .tail,
        1
    );
    let mut changed = request.clone();
    changed.inputs.swap(0, 1);
    assert!(matches!(
        reopened.reconcile_batch(changed).await,
        Err(Error::Conflict(_))
    ));
    manifest
        .append_at(OwnedBytes::from_static(b"unexpected-second-record"), 1)
        .await?;
    assert!(matches!(
        reopened.retained_batch(request.batch_id).await,
        Err(Error::Storage(_))
    ));
    assert_eq!(
        manifest
            .read(0, 2)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        2
    );
    Ok(())
}
