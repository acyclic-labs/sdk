use super::*;
use fixture::fixture;

#[tokio::test]
async fn b01_registered_input_and_schema() -> Result<()> {
    let f = fixture().await?;
    let mut record = f.admission(1, json!("wrong type"));
    assert!(matches!(
        f.host.admit(record.clone()).await,
        Err(Error::Invalid(_))
    ));
    assert!(
        f.host
            .reconcile_admission(record.operation_id)
            .await?
            .is_none()
    );
    record.input = json!(0);
    assert_eq!(
        f.host.admit(record).await?,
        Admission::Accepted(TaskId::from_bytes([1; 16]))
    );
    Ok(())
}

#[tokio::test]
async fn b02_retry_preserves_signed_owner() -> Result<()> {
    for session in [
        None,
        Some(crate::scheduler::SessionLimits {
            active_tasks: 8,
            total_tasks: 16,
            depth: 4,
            model_steps: 100,
        }),
    ] {
        let f = Fixture::new(
            Arc::new(MemoryStream::default()),
            acyclic_fs::Fs::memory(),
            json!({"type":"integer"}),
            session,
            &[],
        )
        .await?;
        let record = f.admission(2, json!(0));
        f.host.admit(record.clone()).await?;
        let original = f
            .host
            .retained_declaration_with_work(record.operation_id, &mut None)
            .await?;
        let renewed = f.reopen(&f.issuer, "renewed-owner").await?;
        assert_eq!(
            renewed.admit(record.clone()).await?,
            Admission::Accepted(TaskId::from_bytes([2; 16]))
        );
        assert_eq!(
            renewed
                .retained_declaration_with_work(record.operation_id, &mut None)
                .await?,
            original
        );
        let impostor = AuthorityIssuer::new("fixture", [8; 32], f.issuer.audience().clone());
        assert!(matches!(
            f.reopen(&impostor, "owner")
                .await?
                .admit(record.clone())
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut changed = record;
        changed.input = json!(1);
        assert!(matches!(
            renewed.admit(changed).await,
            Err(Error::Conflict(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn b03_child_requires_original_live_fence() -> Result<()> {
    let f = fixture().await?;
    f.host.admit(f.admission(3, json!(0))).await?;
    let lease = f.claim(3, "parent").await?;
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let mut child = f.admission(4, json!(0));
    child.parent = Some(TaskId::from_bytes([3; 16]));
    f.host.admit_owned(child.clone(), fence.clone()).await?;
    let mut stale = fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(f.host.admit_owned(child.clone(), stale).await.is_err());
    f.host.cancel(child.parent.unwrap()).await?;
    assert!(f.host.admit_owned(child, fence).await.is_err());
    Ok(())
}

#[tokio::test]
async fn b04_descendants_only_narrow_authority() -> Result<()> {
    let f = fixture().await?;
    let mut parent = f.admission(5, json!(0));
    parent.grants = Capabilities::new(["task:spawn:test.fixture@1"]);
    parent.limits.model_steps = 2;
    parent.run_limits = crate::runtime::TaskRunLimits {
        concurrency: Some(2),
        max_steps: Some(2),
        deadline_epoch_ms: None,
    };
    f.host.admit(parent.clone()).await?;
    for change in 0..5 {
        let mut child = parent.clone();
        child.operation_id = OperationId::new();
        child.parent = Some(TaskId::from_bytes([5; 16]));
        match change {
            0 => child.grants = f.scope.grants().clone(),
            1 => child.limits.model_steps = 3,
            2 => child.run_limits.max_steps = Some(3),
            3 => child.run_limits.max_steps = None,
            _ => child.run_limits.concurrency = None,
        }
        let operation = child.operation_id;
        assert!(matches!(
            f.host.admit(child).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(f.host.reconcile_admission(operation).await?.is_none());
    }
    let mut child = parent;
    child.operation_id = OperationId::new();
    child.parent = Some(TaskId::from_bytes([5; 16]));
    child.limits.model_steps = 1;
    child.run_limits.max_steps = Some(1);
    child.run_limits.concurrency = Some(1);
    assert!(matches!(f.host.admit(child).await?, Admission::Accepted(_)));
    let request = f.batch(6, Some(TaskId::from_bytes([5; 16])));
    assert!(matches!(
        f.host.admit_batch(request.clone()).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(f.host.load_batch(request.batch_id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn b05_partial_batch_resumes_missing_members() -> Result<()> {
    let f = fixture().await?;
    let request = f.batch(7, None);
    f.host.commit_batch(&request).await?;
    f.host.admit(request.member_admission(0)?).await?;
    let reopened = f.reopen(&f.issuer, "owner").await?;
    assert_eq!(
        reopened
            .load_batch(request.batch_id)
            .await?
            .unwrap()
            .canonical_value(),
        request.canonical_value()
    );
    assert!(matches!(
        reopened
            .reconcile_batch(request.clone())
            .await?
            .unwrap()
            .as_slice(),
        [
            Admission::Accepted(_),
            Admission::Indeterminate { .. },
            Admission::Indeterminate { .. }
        ]
    ));
    let completed = reopened.admit_batch(request.clone()).await?;
    for (index, result) in completed.iter().enumerate() {
        assert_eq!(
            result,
            &Admission::Accepted(TaskId::from_bytes(request.operation_id(index).into_bytes()))
        );
    }
    assert_eq!(reopened.admit_batch(request).await?, completed);
    Ok(())
}

#[tokio::test]
async fn b06_batch_identity_and_parent_collisions() -> Result<()> {
    let f = fixture().await?;
    f.host.admit(f.admission(8, json!(0))).await?;
    f.host.admit(f.admission(9, json!(0))).await?;
    let request = f.batch(10, Some(TaskId::from_bytes([8; 16])));
    for change in 0..2 {
        let mut changed = request.clone();
        if change == 0 {
            changed.task.digest = [99; 32];
        } else {
            changed.output_schema = json!({"type":"number"});
        }
        assert!(matches!(
            f.host.admit_batch(changed).await,
            Err(Error::Conflict(_))
        ));
    }
    for change in 0..3 {
        let mut selected = request.clone();
        selected.batch_id = BatchId::from_bytes([11 + change; 16]);
        let mut member = selected.member_admission(0)?;
        match change {
            0 => member.input = json!(999),
            1 => member.parent = None,
            _ => member.parent = Some(TaskId::from_bytes([9; 16])),
        }
        f.host.admit(member).await?;
        f.host.commit_batch(&selected).await?;
        assert!(matches!(
            f.host.reconcile_batch(selected).await,
            Err(Error::Conflict(_))
        ));
    }
    f.host.admit_batch(request.clone()).await?;
    let mut reordered = request;
    reordered.inputs.swap(0, 1);
    assert!(matches!(
        f.host.reconcile_batch(reordered).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn b07_batch_cancellation_prevents_missing_admissions() -> Result<()> {
    let f = fixture().await?;
    let mut request = f.batch(14, None);
    request.group_policy = crate::runtime::BatchGroupPolicy::CancelOnFailure;
    f.host.commit_batch(&request).await?;
    f.host.admit(request.member_admission(0)?).await?;
    let cancellation = f.host.cancel_batch(request.batch_id).await?.unwrap();
    assert_eq!(cancellation.entries.len(), 3);
    assert!(matches!(
        cancellation.entries[0].1,
        BatchCancellationStatus::Requested
    ));
    assert!(
        cancellation.entries[1..]
            .iter()
            .all(|(_, state)| matches!(state, BatchCancellationStatus::NotAdmitted { .. }))
    );
    let after = f.host.admit_batch(request.clone()).await?;
    assert!(matches!(
        after.as_slice(),
        [
            Admission::Accepted(_),
            Admission::Rejected { .. },
            Admission::Rejected { .. }
        ]
    ));
    assert_eq!(f.host.reconcile_batch(request.clone()).await?, Some(after));
    assert_eq!(
        f.host
            .cancel_batch(request.batch_id)
            .await?
            .unwrap()
            .entries
            .len(),
        3
    );
    Ok(())
}
