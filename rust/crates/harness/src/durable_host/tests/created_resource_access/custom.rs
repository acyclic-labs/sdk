use super::*;

#[tokio::test]
async fn created_custom_operation_requires_canonical_admission_not_a_caller_receipt() -> Result<()>
{
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let label = created_resource_capability(&request.resources[0], "execute", &mut None)?;
    assert!(!fixture.root_scope.grants().contains(&label));
    let (preparation, _, _invocation) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    let raw = fixture
        .creator
        .verify(&request, &request.child.resource_proofs[0])
        .await?;
    assert_eq!(raw.origin, *preparation.origin());
    let unsealed = RuntimeScope::new(Capabilities::new([label.clone()]), request.child.limits)?;
    assert!(
        fixture
            .root_scope
            .narrow(unsealed.grants().clone(), unsealed.limits())
            .is_err()
    );
    assert!(
        !fixture
            .harness
            .durable_context(
                fixture.root,
                OperationId::from_bytes(fixture.root.into_bytes())
            )
            .await?
            .scope()
            .grants()
            .contains(&label)
    );
    let mut unrelated = request.child.clone();
    unrelated.operation_id = OperationId::from_bytes([85; 16]);
    assert!(fixture.admit_child(unrelated).await.is_err());
    assert!(fixture.host.admit(request.child.clone()).await.is_err());
    assert!(matches!(fixture.admit_child(request.child.clone()).await?,
        Admission::Accepted(id) if id == TaskId::from_bytes([84; 16])));
    let context = fixture
        .harness
        .durable_context(TaskId::from_bytes([84; 16]), request.child.operation_id)
        .await?;
    assert!(context.scope().grants().contains(&label));
    assert!(!context.scope().grants().contains("resource:create:custom"));
    assert!(!fixture.root_scope.grants().contains(&label));
    let narrowed = context.scoped(Capabilities::default(), context.scope().limits())?;
    assert!(!narrowed.scope().grants().contains(&label));
    assert!(
        narrowed
            .scoped(
                Capabilities::new([label.clone()]),
                narrowed.scope().limits()
            )
            .is_err()
    );
    let foreign = ResourceRef::new(
        ResourceKind::Sandbox,
        fixture.creator.provider.clone(),
        b"foreign".to_vec().into(),
        None,
        &mut None,
    )?;
    assert!(
        context
            .scoped(
                Capabilities::new([created_resource_capability(&foreign, "execute", &mut None)?]),
                context.scope().limits()
            )
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_genuine_allocation_provenance_never_derives_scope() -> Result<()> {
    for corruption in 0..6 {
        let fixture = CreationFixture::new().await?;
        let request = fixture.sandbox_request()?;
        let (preparation, _, _invocation) = fixture.prepare(&request).await?;
        fixture
            .creator
            .create(&preparation, |origin| match corruption {
                0 => origin.intended_child = OperationId::from_bytes([85; 16]),
                1 => origin.admission_digest[0] ^= 1,
                2 => origin.request_digest[0] ^= 1,
                3 => origin.owner.id = "foreign-owner".into(),
                4 => origin.parent_task = TaskId::from_bytes([85; 16]),
                _ => origin.creation_fence.reservation_id = "foreign-creation-lease".into(),
            })
            .await?;
        let receipt = fixture
            .creator
            .verify(&request, &request.child.resource_proofs[0])
            .await?;
        assert_ne!(receipt.origin, *preparation.origin());
        assert!(fixture.admit_child(request.child.clone()).await.is_err());
        assert!(
            fixture
                .harness
                .durable_context(TaskId::from_bytes([84; 16]), request.child.operation_id)
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn changed_child_request_evidence_and_provider_revision_are_rejected() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, _, _invocation) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    let mut changed_request = request.clone();
    changed_request.payload = serde_json::json!({"image":"foreign"});
    assert!(
        fixture
            .creator
            .verify(&changed_request, &request.child.resource_proofs[0])
            .await
            .is_err()
    );
    for change in 0..7 {
        let mut child = request.child.clone();
        match change {
            0 => child.operation_id = OperationId::from_bytes([85; 16]),
            1 => child.input = serde_json::json!({"action":"foreign"}),
            2 => child.grants = fixture.root_scope.grants().clone(),
            3 => {
                child.resource_proofs[0].evidence =
                    workspace_ref(fixture.filesystem_provider.clone(), "foreign-evidence")?
                        .as_resource()
                        .clone()
            }
            4 => {
                child.resource_proofs[0].provider = ProviderRef::new(
                    "created-controls".into(),
                    "sandbox".into(),
                    "2".into(),
                    &mut None,
                )?
            }
            5 => child.resource_proofs[0].preparation = TaskId::from_bytes([85; 16]),
            _ => child.parent = None,
        }
        assert!(fixture.admit_child(child).await.is_err());
    }
    let mut self_authorizing = fixture.admission(85, serde_json::json!({}));
    self_authorizing.parent = Some(preparation.origin().preparation_task);
    self_authorizing.grants = request.child.grants.clone();
    self_authorizing.resource_proofs = request.child.resource_proofs.clone();
    let prep_lease = fixture
        .host
        .recover_work(preparation.origin().preparation_task, &mut None)
        .await?;
    let crate::distributed::WorkPull::Claimed(prep_lease) = prep_lease else {
        return Err(Error::NotFound(
            "preparation lease for self-authorization control".into(),
        ));
    };
    assert!(
        fixture
            .host
            .admit_owned(self_authorizing, (&prep_lease.reservation).into())
            .await
            .is_err()
    );
    assert!(matches!(
        fixture.admit_child(request.child.clone()).await?,
        Admission::Accepted(_)
    ));
    Ok(())
}

#[tokio::test]
async fn original_creation_fence_survives_current_preparation_and_parent_recovery() -> Result<()> {
    let mut fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, original_prep, _invocation) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    fixture
        .host
        .coordinator
        .lock()
        .await
        .release_lease(
            &original_prep,
            IdempotencyKey::new("release-created-preparation")?,
        )
        .await?;
    let crate::distributed::WorkPull::Claimed(recovered_prep) =
        fixture.host.pull_work(&fixture.worker).await?
    else {
        return Err(Error::NotFound("recovered preparation lease".into()));
    };
    fixture.host.start_task(&recovered_prep, None).await?;
    assert_ne!(recovered_prep.reservation, original_prep.reservation);
    assert!(preparation.verify_creation().await.is_err());
    fixture
        .host
        .coordinator
        .lock()
        .await
        .release_lease(
            &fixture.root_lease,
            IdempotencyKey::new("release-created-parent")?,
        )
        .await?;
    let old_parent_fence = (&fixture.root_lease.reservation).into();
    let crate::distributed::WorkPull::Claimed(recovered_parent) =
        fixture.host.pull_work(&fixture.worker).await?
    else {
        return Err(Error::NotFound("recovered parent lease".into()));
    };
    fixture.host.start_task(&recovered_parent, None).await?;
    assert_ne!(recovered_parent.reservation, fixture.root_lease.reservation);
    fixture.root_lease = recovered_parent;
    let receipt = fixture
        .creator
        .verify(&request, &request.child.resource_proofs[0])
        .await?;
    assert_eq!(
        receipt.origin.creation_fence,
        (&original_prep.reservation).into()
    );
    assert!(
        fixture
            .host
            .admit_owned(request.child.clone(), old_parent_fence)
            .await
            .is_err()
    );
    assert!(matches!(
        fixture.admit_child(request.child.clone()).await?,
        Admission::Accepted(_)
    ));
    let context = fixture
        .harness
        .durable_context(TaskId::from_bytes([84; 16]), request.child.operation_id)
        .await?;
    assert!(
        context
            .scope()
            .grants()
            .contains(&created_resource_capability(
                &request.resources[0],
                "execute",
                &mut None
            )?)
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_parent_cannot_consume_existing_created_allocation() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, _, _invocation) = fixture.prepare(&request).await?;
    fixture.creator.create(&preparation, |_| {}).await?;
    assert!(
        !preparation
            .cancellation()
            .expect("caller invocation is sealed")
            .is_cancelled()
    );
    fixture.host.cancel(fixture.root, None).await?;
    assert!(preparation.verify_creation().await.is_err());
    assert!(
        preparation
            .cancellation()
            .expect("same caller invocation")
            .is_cancelled()
    );
    fixture
        .creator
        .verify(&request, &request.child.resource_proofs[0])
        .await?;
    assert!(fixture.admit_child(request.child.clone()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn sealed_preparation_borrows_the_actual_callers_drop_guard_token() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let request = fixture.sandbox_request()?;
    let (preparation, _, invocation) = fixture.prepare(&request).await?;
    assert!(
        !preparation
            .cancellation()
            .expect("actual owner invocation")
            .is_cancelled()
    );
    drop(invocation);
    assert!(
        preparation
            .cancellation()
            .expect("same owner invocation")
            .is_cancelled()
    );
    Ok(())
}
