use super::*;

#[tokio::test]
async fn concurrent_session_claims_cannot_overspend_after_reopen() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut first = open(&client).await?;
    let root = OperationId::from_bytes([90; 16]);
    declare(&mut first, spec(root, 0)?, "budget-root").await?;
    let (owner, issuer) = owner_issuer();
    let scope = grant(&issuer, "budget", &["operation:declare"])?;
    let limits = crate::scheduler::SessionLimits {
        active_tasks: 2,
        total_tasks: 3,
        depth: 1,
        model_steps: 1,
    };
    assert!(
        publish(
            &mut first,
            "unscoped-budget",
            SchedulerEvent::SessionConfigured {
                operation_id: root,
                limits
            }
        )
        .await
        .is_err()
    );
    first
        .configure_session(
            &owner,
            &scope,
            &issuer.verifier(&mut None)?,
            root,
            limits,
            IdempotencyKey::new("configure-budget")?,
        )
        .await?;
    let mut claims = Vec::new();
    for index in [91, 92] {
        let id = OperationId::from_bytes([index; 16]);
        let mut child = spec(id, 0)?;
        child.parent = Some(ParentLink {
            operation_id: root,
            slot: index.to_string(),
        });
        declare(&mut first, child, &format!("budget-child-{index}")).await?;
        let lease = start(
            &mut first,
            &Worker {
                id: format!("worker-{index}"),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            },
            &format!("budget-start-{index}"),
        )
        .await?;
        let id = lease.operation.operation_id;
        let fence = LeaseFence::from(&lease.reservation);
        claims.push(SchedulerEvent::ModelDispatchClaimed {
            operation_id: id,
            attempt_id: id,
            step: 0,
            request_digest: [index; 32],
            fence,
            ceiling: 1,
        });
    }
    let mut second = open(&client).await?;
    let [one, two] = claims.as_slice() else {
        return Err(Error::Invalid("two budget claims required".into()));
    };
    let mut left_work = None;
    let mut right_work = None;
    let (left, right) = tokio::join!(
        first.apply(
            IdempotencyKey::new("budget-one")?,
            one.clone(),
            &mut left_work
        ),
        second.apply(
            IdempotencyKey::new("budget-two")?,
            two.clone(),
            &mut right_work
        )
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let (winner, key, loser) = if left.is_ok() {
        (one, "budget-one", two)
    } else {
        (two, "budget-two", one)
    };
    let mut reopened = open(&client).await?;
    assert_eq!(publish(&mut reopened, &(key), winner.clone()).await?, false);
    assert!(
        publish(&mut reopened, "budget-loser-retry", loser.clone())
            .await
            .is_err()
    );
    let mut changed = winner.clone();
    if let SchedulerEvent::ModelDispatchClaimed { request_digest, .. } = &mut changed {
        *request_digest = [99; 32];
    }
    assert!(
        publish(&mut reopened, "budget-changed", changed)
            .await
            .is_err()
    );
    assert!(
        reopened
            .configure_session(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                root,
                crate::scheduler::SessionLimits {
                    model_steps: 2,
                    ..limits
                },
                IdempotencyKey::new("budget-reset")?
            )
            .await
            .is_err()
    );
    let winner_id = winner.operation_id();
    publish(
        &mut reopened,
        "cancel-budget-winner",
        SchedulerEvent::CancellationRequested {
            operation_id: winner_id,
            recursive: false,
        },
    )
    .await?;
    assert!(
        publish(&mut reopened, &(key), winner.clone())
            .await
            .is_err(),
        "an exact accounting retry cannot bypass cancellation"
    );
    Ok(())
}

#[tokio::test]
async fn root_declaration_and_shared_limits_are_atomic() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut left = open(&client).await?;
    let mut right = open(&client).await?;
    let root = OperationId::from_bytes([89; 16]);
    let (owner, issuer) = owner_issuer();
    let signed = grant(&issuer, "owner", &["operation:declare"])?;
    let verifier = issuer.verifier(&mut None)?;
    let one = crate::scheduler::SessionLimits {
        active_tasks: 1,
        total_tasks: 2,
        depth: 1,
        model_steps: 1,
    };
    let two = crate::scheduler::SessionLimits {
        model_steps: 2,
        ..one
    };
    let unsigned = issuer
        .issue("reader", &Capabilities::default(), None, None, &mut None)
        .expect("scope proof");
    assert!(
        left.declare_session(
            &owner,
            &unsigned,
            &verifier,
            spec(root, 0)?,
            one,
            IdempotencyKey::new("unauthorized-root")?,
            &mut None
        )
        .await
        .is_err()
    );
    assert!(
        read_coordinator_event_page(&client, 0, 64)
            .await?
            .is_empty()
    );
    let mut observers = (None, None);
    let (first, second) = tokio::join!(
        left.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(root, 0)?,
            one,
            IdempotencyKey::new("atomic-one")?,
            &mut observers.0
        ),
        right.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(root, 0)?,
            two,
            IdempotencyKey::new("atomic-two")?,
            &mut observers.1
        ),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let (limits, key) = if first.is_ok() {
        (one, "atomic-one")
    } else {
        (two, "atomic-two")
    };
    let events = read_coordinator_event_page(&client, 0, 64).await?;
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events.first(),
        Some(SchedulerEvent::Declared { .. })
    ));
    assert!(
        matches!(events.last(), Some(SchedulerEvent::SessionConfigured { limits: retained, .. }) if *retained == limits)
    );
    let mut recovered = open(&client).await?;
    assert_eq!(
        recovered.scheduler().session_limits(root, &mut None)?,
        limits
    );
    let lease = recovered
        .pull(&Worker {
            id: "atomic-worker".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        })
        .await?
        .ok_or_else(|| Error::NotFound("atomic root lease".into()))?;
    assert_eq!(lease.operation.operation_id, root);
    assert_eq!(
        recovered
            .declare_session(
                &owner,
                &signed,
                &verifier,
                spec(root, 0)?,
                limits,
                IdempotencyKey::new(key)?,
                &mut None
            )
            .await?,
        false
    );
    assert!(
        recovered
            .declare_session(
                &owner,
                &signed,
                &verifier,
                spec(root, 0)?,
                crate::scheduler::SessionLimits {
                    model_steps: limits.model_steps + 1,
                    ..limits
                },
                IdempotencyKey::new(key)?,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 3);
    let identical_root = OperationId::from_bytes([88; 16]);
    let mut duplicate = open(&client).await?;
    let mut observers = (None, None);
    let (first, second) = tokio::join!(
        recovered.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(identical_root, 0)?,
            limits,
            IdempotencyKey::new("identical-root")?,
            &mut observers.0
        ),
        duplicate.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(identical_root, 0)?,
            limits,
            IdempotencyKey::new("identical-root")?,
            &mut observers.1
        ),
    );
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 5);
    Ok(())
}

#[tokio::test]
async fn session_retry_rejects_separately_committed_lookalike_events() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator = open(&client).await?;
    let root = OperationId::from_bytes([86; 16]);
    let (owner, issuer) = owner_issuer();
    let signed = grant(&issuer, "owner", &["operation:declare"])?;
    let limits = crate::scheduler::SessionLimits {
        active_tasks: 1,
        total_tasks: 1,
        depth: 0,
        model_steps: 1,
    };
    let key = "split-root";
    coordinator
        .declare_operation(
            &owner,
            &signed,
            &issuer.verifier(&mut None)?,
            spec(root, 0)?,
            IdempotencyKey::new(key)?,
            None,
            &mut None,
        )
        .await?;
    coordinator
        .configure_session(
            &owner,
            &signed,
            &issuer.verifier(&mut None)?,
            root,
            limits,
            IdempotencyKey::new(format!(
                "session:{root}:{}",
                blake3::hash(key.as_bytes()).to_hex()
            ))?,
        )
        .await?;
    let mut reopened = open(&client).await?;
    assert!(
        reopened
            .declare_session(
                &owner,
                &signed,
                &issuer.verifier(&mut None)?,
                spec(root, 0)?,
                limits,
                IdempotencyKey::new(key)?,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn root_limits_reconcile_a_lost_atomic_append_acknowledgement() -> Result<()> {
    for hide_receipt in [false, true] {
        let client = Arc::new(LostSessionAck::<MemoryStream> {
            lose_ack: std::sync::atomic::AtomicBool::new(true),
            hide_receipt: std::sync::atomic::AtomicBool::new(hide_receipt),
            ..Default::default()
        });
        let mut coordinator = open(&client).await?;
        let root = OperationId::from_bytes([87; 16]);
        let (owner, issuer) = owner_issuer();
        let signed = grant(&issuer, "owner", &["operation:declare"])?;
        let limits = crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        };
        let result = coordinator
            .declare_session(
                &owner,
                &signed,
                &issuer.verifier(&mut None)?,
                spec(root, 0)?,
                limits,
                IdempotencyKey::new("lost-root")?,
                &mut None,
            )
            .await;
        if hide_receipt {
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == root));
        } else {
            assert!(result.is_ok());
        }
        drop(coordinator);
        let mut recovered = open(&client).await?;
        assert_eq!(
            recovered.scheduler().session_limits(root, &mut None)?,
            limits
        );
        assert_eq!(
            recovered
                .declare_session(
                    &owner,
                    &signed,
                    &issuer.verifier(&mut None)?,
                    spec(root, 0)?,
                    limits,
                    IdempotencyKey::new("lost-root")?,
                    &mut None
                )
                .await?,
            false
        );
        assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 2);
    }
    Ok(())
}
