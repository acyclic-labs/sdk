use super::*;

#[tokio::test]
async fn cancellation_is_owner_fenced_durable_and_exactly_replayable() -> Result<()> {
    use std::sync::atomic::Ordering;
    let client = Arc::new(LostSessionAck::<MemoryStream>::default());
    let mut coordinator = open(&client).await?;
    let parent = OperationId::from_bytes([44; 16]);
    let child = OperationId::from_bytes([45; 16]);
    let grandchild = OperationId::from_bytes([46; 16]);
    declare(&mut coordinator, spec(parent, 0)?, "cancel-parent-declare").await?;
    let worker = Worker {
        id: "cancel-parent-worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let lease = start(&mut coordinator, &worker, "cancel-parent-start").await?;
    assert_eq!(lease.operation.operation_id, parent);
    let fence = LeaseFence::from(&lease.reservation);
    for (operation_id, parent_id, key) in [
        (child, parent, "cancel-child-declare"),
        (grandchild, child, "cancel-grandchild-declare"),
    ] {
        let mut operation = spec(operation_id, 0)?;
        operation.parent = Some(ParentLink {
            operation_id: parent_id,
            slot: "child".into(),
        });
        declare(&mut coordinator, operation, key).await?;
    }
    assert_eq!(
        start(&mut coordinator, &worker, "cancel-child-start")
            .await?
            .operation
            .operation_id,
        child
    );
    let (owner, issuer) = owner_issuer();
    let scope = grant(
        &issuer,
        "cancel",
        &["operation:observe", "operation:cancel"],
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let foreign = Authority {
        kind: AggregateKind::Task,
        id: "foreign".into(),
    };
    let foreign_issuer = AuthorityIssuer::new("test-runtime", [9; 32], foreign.clone());
    let foreign_scope =
        foreign_issuer.issue("cancel", scope.capabilities(), None, None, &mut None)?;
    let foreign_verifier = foreign_issuer.verifier(&mut None)?;
    let mut stale = fence.clone();
    stale.placement.push_str("-stale");
    let before = coordinator.revision();
    for (authority, grant, verifier, target, fence, key, code) in [
        (
            &owner,
            &scope,
            &verifier,
            child,
            &stale,
            "stale",
            wire::ErrorCode::Unauthorized,
        ),
        (
            &owner,
            &scope,
            &verifier,
            grandchild,
            &fence,
            "indirect",
            wire::ErrorCode::Unauthorized,
        ),
        (
            &foreign,
            &foreign_scope,
            &foreign_verifier,
            child,
            &fence,
            "foreign",
            wire::ErrorCode::NotFound,
        ),
    ] {
        let error = coordinator
            .cancel_operation(
                authority,
                grant,
                verifier,
                target,
                IdempotencyKey::new(key)?,
                true,
                Some((parent, fence)),
                &mut None,
            )
            .await
            .err()
            .expect("denied cancellation");
        assert_eq!(error.code(), code);
    }
    assert_eq!(coordinator.revision(), before);
    client.lose_ack.store(true, Ordering::SeqCst);
    let (_, state) = coordinator
        .cancel_operation(
            &owner,
            &scope,
            &verifier,
            child,
            IdempotencyKey::new("owned-child-cancel")?,
            true,
            Some((parent, &fence)),
            &mut None,
        )
        .await?;
    assert!(state.cancellation_requested && state.outcome.is_none());
    assert_eq!(
        coordinator
            .scheduler()
            .operation(grandchild)
            .and_then(|state| state.outcome.as_ref()),
        Some(&crate::Outcome::Cancelled)
    );
    let writes = client.io.writes.load(Ordering::SeqCst);
    let (_, replay) = coordinator
        .cancel_operation(
            &owner,
            &scope,
            &verifier,
            child,
            IdempotencyKey::new("owned-child-cancel")?,
            true,
            Some((parent, &fence)),
            &mut None,
        )
        .await?;
    assert_eq!(replay, state);
    assert_eq!(client.io.writes.load(Ordering::SeqCst), writes);
    assert_eq!(
        read_coordinator_event_page(&client, 0, 64)
            .await?
            .iter()
            .filter(|event| matches!(event,
                SchedulerEvent::CancellationRequested { operation_id, recursive: true } if *operation_id == child
            ))
            .count(),
        1
    );

    let root = OperationId::from_bytes([31; 16]);
    let descendant = OperationId::from_bytes([32; 16]);
    declare(&mut coordinator, spec(root, 0)?, "declare-control-parent").await?;
    let mut child_spec = spec(descendant, 0)?;
    child_spec.parent = Some(ParentLink {
        operation_id: root,
        slot: "child".into(),
    });
    declare(&mut coordinator, child_spec, "declare-control-child").await?;
    let (applied, status) = coordinator
        .cancel_operation(
            &owner,
            &scope,
            &verifier,
            root,
            IdempotencyKey::new("cancel-control-tree")?,
            true,
            None,
            &mut None,
        )
        .await?;
    assert!(applied);
    assert_eq!(status.outcome, Some(crate::Outcome::Cancelled));
    assert_eq!(
        coordinator
            .scheduler()
            .operation(descendant)
            .and_then(|state| state.outcome.as_ref()),
        Some(&crate::Outcome::Cancelled)
    );
    assert!(
        !coordinator
            .cancel_operation(
                &owner,
                &scope,
                &verifier,
                root,
                IdempotencyKey::new("cancel-control-tree")?,
                true,
                None,
                &mut None
            )
            .await?
            .0
    );
    assert!(matches!(
        coordinator
            .cancel_operation(
                &owner,
                &scope,
                &verifier,
                root,
                IdempotencyKey::new("cancel-control-tree")?,
                false,
                None,
                &mut None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let mut reopened = open(&client).await?;
    assert_eq!(
        reopened
            .observe_operation(&owner, &scope, &verifier, descendant, &mut None)?
            .outcome,
        Some(crate::Outcome::Cancelled)
    );
    let observe_only = issuer.issue(
        "observe-only",
        &Capabilities::new(["operation:observe"]),
        None,
        None,
        &mut None,
    )?;
    assert!(matches!(
        reopened
            .cancel_operation(
                &owner,
                &observe_only,
                &verifier,
                descendant,
                IdempotencyKey::new("unauthorized-cancel")?,
                false,
                None,
                &mut None
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
