use super::*;

#[tokio::test]
async fn concurrent_session_claims_cannot_overspend_after_reopen() -> Result<()> {
    session_claim_race(Arc::new(MemoryStream::default())).await?;
    Ok(())
}

#[tokio::test]
async fn root_declaration_and_shared_limits_are_atomic() -> Result<()> {
    atomic_session_race(Arc::new(MemoryStream::default())).await?;
    Ok(())
}

#[tokio::test]
async fn session_retry_rejects_separately_committed_lookalike_events() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let root = OperationId::from_bytes([86; 16]);
    let (owner, issuer) = owner_issuer();
    let signed = issuer.issue("owner", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof");
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
            &issuer.verifier(&mut None).expect("verifier copy"),
            spec(root, 0)?,
            IdempotencyKey::new(key)?,
            None, &mut None,
        )
        .await?;
    coordinator
        .configure_session(
            &owner,
            &signed,
            &issuer.verifier(&mut None).expect("verifier copy"),
            root,
            limits,
            IdempotencyKey::new(format!(
                "session:{root}:{}",
                blake3::hash(key.as_bytes()).to_hex()
            ))?,
        )
        .await?;
    let mut reopened =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    assert!(
        reopened
            .declare_session(
                &owner,
                &signed,
                &issuer.verifier(&mut None).expect("verifier copy"),
                spec(root, 0)?,
                limits,
                IdempotencyKey::new(key)?
            , &mut None)
            .await
            .is_err()
    );
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn root_limits_reconcile_a_lost_atomic_append_acknowledgement() -> Result<()> {
    for hide_receipt in [false, true] {
        let client = Arc::new(LostSessionAck {
            lose_ack: std::sync::atomic::AtomicBool::new(true),
            hide_receipt: std::sync::atomic::AtomicBool::new(hide_receipt),
            ..Default::default()
        });
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
        let root = OperationId::from_bytes([87; 16]);
        let (owner, issuer) = owner_issuer();
        let signed = issuer.issue("owner", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof");
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
                &issuer.verifier(&mut None).expect("verifier copy"),
                spec(root, 0)?,
                limits,
                IdempotencyKey::new("lost-root")?,
             &mut None,)
            .await;
        if hide_receipt {
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == root));
        } else {
            assert!(result.is_ok());
        }
        drop(coordinator);
        let mut recovered =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
        assert_eq!(recovered.scheduler().session_limits(root, &mut None)?, limits);
        assert_eq!(
            recovered
                .declare_session(
                    &owner,
                    &signed,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    spec(root, 0)?,
                    limits,
                    IdempotencyKey::new("lost-root")?
                , &mut None)
                .await?,
            false
        );
        assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 2);
    }
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn atomic_root_limits_survive_local_stream_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let client = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let (root, limits) = atomic_session_race(client).await?;
    let client = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    assert_eq!(coordinator.scheduler().session_limits(root, &mut None)?, limits);
    assert_eq!(
        coordinator
            .scheduler()
            .operation(root)
            .map(|state| state.phase),
        Some(crate::scheduler::OperationPhase::Admitted)
    );
    Ok(())
}

async fn atomic_session_race<P: StreamProvider>(
    client: std::sync::Arc<P>,
) -> Result<(OperationId, crate::scheduler::SessionLimits)> {
    let mut left = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let mut right =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let root = OperationId::from_bytes([89; 16]);
    let (owner, issuer) = owner_issuer();
    let signed = issuer.issue("owner", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof");
    let verifier = issuer.verifier(&mut None).expect("verifier copy");
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
    let unsigned = issuer.issue("reader", &Capabilities::default(), None, None, &mut None).expect("scope proof");
    assert!(
        left.declare_session(
            &owner,
            &unsigned,
            &verifier,
            spec(root, 0)?,
            one,
            IdempotencyKey::new("unauthorized-root")?
        , &mut None)
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
            IdempotencyKey::new("atomic-one")?
        , &mut observers.0),
        right.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(root, 0)?,
            two,
            IdempotencyKey::new("atomic-two")?
        , &mut observers.1),
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
    let mut recovered =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    assert_eq!(recovered.scheduler().session_limits(root, &mut None)?, limits);
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
                IdempotencyKey::new(key)?
            , &mut None)
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
                IdempotencyKey::new(key)?
            , &mut None)
            .await
            .is_err()
    );
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 3);
    let identical_root = OperationId::from_bytes([88; 16]);
    let mut duplicate =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let mut observers = (None, None);
    let (first, second) = tokio::join!(
        recovered.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(identical_root, 0)?,
            limits,
            IdempotencyKey::new("identical-root")?
        , &mut observers.0),
        duplicate.declare_session(
            &owner,
            &signed,
            &verifier,
            spec(identical_root, 0)?,
            limits,
            IdempotencyKey::new("identical-root")?
        , &mut observers.1),
    );
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 5);
    Ok((root, limits))
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn local_session_budget_survives_provider_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let loser = session_claim_race(provider.clone()).await?;
    drop(provider);
    let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let client = Arc::new(provider);
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    assert!(
        coordinator
            .apply(
                IdempotencyKey::new("local-budget-loser")?,
                loser
            , &mut None)
            .await
            .is_err()
    );
    Ok(())
}

async fn session_claim_race<P: StreamProvider>(
    client: std::sync::Arc<P>,
) -> Result<SchedulerEvent> {
    let mut first =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let root = OperationId::from_bytes([90; 16]);
    declare(&mut first, spec(root, 0)?, "budget-root").await?;
    let (owner, issuer) = owner_issuer();
    let scope = issuer.issue("budget", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof");
    let limits = crate::scheduler::SessionLimits {
        active_tasks: 2,
        total_tasks: 3,
        depth: 1,
        model_steps: 1,
    };
    assert!(
        first
            .apply(
                IdempotencyKey::new("unscoped-budget")?,
                SchedulerEvent::SessionConfigured {
                    operation_id: root,
                    limits
                }
            , &mut None)
            .await
            .is_err()
    );
    first
        .configure_session(
            &owner,
            &scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
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
        let lease = start(&mut first, &Worker {
            id: format!("worker-{index}"), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
        }, &format!("budget-start-{index}")).await?;
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
    let mut second =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let [one, two] = claims.as_slice() else {
        return Err(Error::Invalid("two budget claims required".into()));
    };
    let mut left_work = None;
    let mut right_work = None;
    let (left, right) = tokio::join!(
        first.apply(
            IdempotencyKey::new("budget-one")?,
            one.clone()
        , &mut left_work),
        second.apply(
            IdempotencyKey::new("budget-two")?,
            two.clone()
        , &mut right_work)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let (winner, key, loser) = if left.is_ok() {
        (one, "budget-one", two)
    } else {
        (two, "budget-two", one)
    };
    let mut reopened =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    assert_eq!(
        reopened
            .apply(
                IdempotencyKey::new(key)?,
                winner.clone()
            , &mut None)
            .await?,
        false
    );
    assert!(
        reopened
            .apply(
                IdempotencyKey::new("budget-loser-retry")?,
                loser.clone()
            , &mut None)
            .await
            .is_err()
    );
    let mut changed = winner.clone();
    if let SchedulerEvent::ModelDispatchClaimed { request_digest, .. } = &mut changed {
        *request_digest = [99; 32];
    }
    assert!(
        reopened
            .apply(
                IdempotencyKey::new("budget-changed")?,
                changed
            , &mut None)
            .await
            .is_err()
    );
    assert!(
        reopened
            .configure_session(
                &owner,
                &scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
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
    reopened
        .apply(
            IdempotencyKey::new("cancel-budget-winner")?,
            SchedulerEvent::CancellationRequested {
                operation_id: winner_id,
                recursive: false,
            }, &mut None,
        )
        .await?;
    assert!(
        reopened
            .apply(IdempotencyKey::new(key)?, winner.clone(), &mut None,)
            .await
            .is_err(),
        "an exact accounting retry cannot bypass cancellation"
    );
    Ok(loser.clone())
}
