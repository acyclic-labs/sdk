use super::*;

#[tokio::test]
async fn pull_enforces_operation_placement_labels() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator = open(&client).await?;
    let operation_id = OperationId::from_bytes([53; 16]);
    let mut declaration = spec(operation_id, 0)?;
    declaration.placement.insert("region".into(), "eu".into());
    declare(&mut coordinator, declaration, "placed").await?;
    let mut worker = Worker {
        id: "worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    assert!(coordinator.pull(&worker).await?.is_none());
    worker.labels.insert("region".into(), "us".into());
    assert!(coordinator.pull(&worker).await?.is_none());
    worker.labels.insert("region".into(), "eu".into());
    assert_eq!(
        coordinator
            .pull(&worker)
            .await?
            .map(|lease| lease.operation.operation_id),
        Some(operation_id)
    );
    Ok(())
}

#[tokio::test]
async fn owned_pull_filters_foreign_operations_but_shares_worker_capacity() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator = open(&client).await?;
    let foreign = OperationId::from_bytes([1; 16]);
    let owned = OperationId::from_bytes([2; 16]);
    let mut other = spec(foreign, 1)?;
    other.owner = DurableOwner::Attached {
        authority: Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        },
    };
    other.owner_scope = declaration_scope(other.owner.authority());
    declare(&mut coordinator, other, "foreign-pull").await?;
    declare(&mut coordinator, spec(owned, 1)?, "owned-pull").await?;
    let (owner, issuer) = owner_issuer();
    let scope = grant(
        &issuer,
        "admit",
        &["operation:observe", "operation:declare"],
    )?;
    let worker = Worker {
        id: "shared-worker".into(),
        available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
        labels: BTreeMap::new(),
    };
    let WorkPull::Claimed(lease) = coordinator
        .pull_owned(
            &owner,
            &scope,
            &issuer.verifier(&mut None)?,
            &worker,
            None,
            &mut None,
        )
        .await?
    else {
        return Err(Error::NotFound("owned pull".into()));
    };
    assert_eq!(lease.operation.operation_id, owned);
    assert!(
        coordinator
            .scheduler()
            .operation(foreign)
            .is_some_and(|state| state.reservation.is_none())
    );
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                None,
                &mut None
            )
            .await?,
        WorkPull::Idle
    ));
    assert!(coordinator.pull(&worker).await?.is_none());
    let fence = LeaseFence::from(&lease.reservation);
    publish(
        &mut coordinator,
        "owned-start",
        SchedulerEvent::Started {
            operation_id: owned,
            fence: fence.clone(),
        },
    )
    .await?;
    publish(
        &mut coordinator,
        "owned-failed",
        SchedulerEvent::Completed {
            operation_id: owned,
            fence: Some(fence),
            execution_duration_ns: None,
            outcome: crate::Outcome::Failed {
                message: "dependency failed".into(),
            },
        },
    )
    .await?;
    let blocked = OperationId::from_bytes([3; 16]);
    let mut other = spec(blocked, 1)?;
    other.owner = DurableOwner::Attached {
        authority: Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        },
    };
    other.owner_scope = declaration_scope(other.owner.authority());
    other.dependencies.insert(owned);
    declare(&mut coordinator, other, "foreign-blocked").await?;
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                None,
                &mut None
            )
            .await?,
        WorkPull::Idle
    ));
    assert!(
        coordinator
            .scheduler()
            .operation(blocked)
            .is_some_and(|state| state.phase != crate::scheduler::OperationPhase::Terminal)
    );
    let before = Stream::new(client.clone(), COORDINATOR_PATH)?
        .tail()
        .await?;
    let denied = grant(&issuer, "denied", &["operation:observe"])?;
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &denied,
                &issuer.verifier(&mut None)?,
                &worker,
                None,
                &mut None
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(
        Stream::new(client.clone(), COORDINATOR_PATH)?
            .tail()
            .await?,
        before
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn exact_target_pull_never_claims_or_rejects_an_unrelated_task() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator = open(&client).await?;
    let first = OperationId::from_bytes([61; 16]);
    let target = OperationId::from_bytes([62; 16]);
    declare(&mut coordinator, spec(first, 1)?, "target-first").await?;
    declare(&mut coordinator, spec(target, 1)?, "target-second").await?;
    let (owner, issuer) = owner_issuer();
    let scope = grant(
        &issuer,
        "admit",
        &["operation:observe", "operation:declare"],
    )?;
    let worker = Worker {
        id: "target-worker".into(),
        available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
        labels: BTreeMap::new(),
    };
    let first_before = coordinator.scheduler().operation(first).cloned();
    let WorkPull::Claimed(lease) = coordinator
        .pull_owned(
            &owner,
            &scope,
            &issuer.verifier(&mut None)?,
            &worker,
            Some(target),
            &mut None,
        )
        .await?
    else {
        return Err(Error::NotFound("target admission".into()));
    };
    assert_eq!(lease.operation.operation_id, target);
    assert_eq!(
        coordinator.scheduler().operation(first),
        first_before.as_ref()
    );
    let tail = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?
        .tail()
        .await?;
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                Some(target),
                &mut None
            )
            .await?,
        WorkPull::Idle
    ));
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                Some(first),
                &mut None
            )
            .await?,
        WorkPull::Idle
    ));
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                Some(OperationId::from_bytes([63; 16])),
                &mut None
            )
            .await,
        Err(Error::NotFound(_))
    ));
    let denied = grant(&issuer, "denied", &["operation:observe"])?;
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &denied,
                &issuer.verifier(&mut None)?,
                &worker,
                Some(first),
                &mut None
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?
            .tail()
            .await?,
        tail
    );
    let fence = LeaseFence::from(&lease.reservation);
    publish(
        &mut coordinator,
        "target-start",
        SchedulerEvent::Started {
            operation_id: target,
            fence: fence.clone(),
        },
    )
    .await?;
    publish(
        &mut coordinator,
        "target-fail",
        SchedulerEvent::Completed {
            operation_id: target,
            fence: Some(fence),
            execution_duration_ns: None,
            outcome: crate::Outcome::Failed {
                message: "failed dependency".into(),
            },
        },
    )
    .await?;
    let blocked = OperationId::from_bytes([64; 16]);
    let ready = OperationId::from_bytes([65; 16]);
    let mut dependent = spec(blocked, 1)?;
    dependent.dependencies.insert(target);
    declare(&mut coordinator, dependent, "target-blocked").await?;
    declare(&mut coordinator, spec(ready, 1)?, "target-ready").await?;
    let blocked_before = coordinator.scheduler().operation(blocked).cloned();
    let WorkPull::Claimed(lease) = coordinator
        .pull_owned(
            &owner,
            &scope,
            &issuer.verifier(&mut None)?,
            &worker,
            Some(ready),
            &mut None,
        )
        .await?
    else {
        return Err(Error::NotFound("ready target behind blocked task".into()));
    };
    assert_eq!(lease.operation.operation_id, ready);
    assert_eq!(
        coordinator.scheduler().operation(blocked),
        blocked_before.as_ref()
    );
    assert_eq!(
        coordinator.scheduler().operation(first),
        first_before.as_ref()
    );
    assert!(matches!(
        coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                Some(blocked),
                &mut None
            )
            .await?,
        WorkPull::Idle
    ));
    assert_eq!(
        coordinator
            .scheduler()
            .operation(blocked)
            .expect("blocked")
            .phase,
        crate::scheduler::OperationPhase::Terminal
    );
    assert_eq!(
        coordinator.scheduler().operation(first),
        first_before.as_ref()
    );
    Ok(())
}

#[tokio::test]
async fn owned_pull_retains_exact_attempt_after_unresolved_admission() -> Result<()> {
    use std::sync::atomic::Ordering;
    for (fault, targeted) in [(1, false), (2, false), (1, true), (2, true)] {
        let client = Arc::new(LostSessionAck::<MemoryStream>::default());
        let mut coordinator = open(&client).await?;
        let operation = OperationId::from_bytes([31; 16]);
        declare(&mut coordinator, spec(operation, 1)?, "fault-pull-root").await?;
        let (owner, issuer) = owner_issuer();
        let scope = grant(
            &issuer,
            "admit",
            &["operation:observe", "operation:declare"],
        )?;
        let worker = Worker {
            id: "fault-worker".into(),
            available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
            labels: BTreeMap::new(),
        };
        if fault == 1 {
            client.lose_ack.store(true, Ordering::SeqCst);
            client.hide_receipt.store(true, Ordering::SeqCst);
        } else {
            client.location_fault.store(4, Ordering::SeqCst);
        }
        let WorkPull::Unresolved { lease, error } = coordinator
            .pull_owned(
                &owner,
                &scope,
                &issuer.verifier(&mut None)?,
                &worker,
                targeted.then_some(operation),
                &mut None,
            )
            .await?
        else {
            return Err(Error::NotFound("unresolved admission attempt".into()));
        };
        assert!(matches!(error, Error::Indeterminate(id) if id == operation));
        assert_eq!(lease.operation.operation_id, operation);
        drop(coordinator);
        let mut reopened = open(&client).await?;
        let retained = reopened
            .scheduler()
            .operation(operation)
            .ok_or_else(|| Error::NotFound("retained admission".into()))?;
        if fault == 2 {
            assert!(retained.reservation.is_none());
            assert!(
                crate::scheduler::require_execution_owner(
                    retained,
                    &LeaseFence::from(&lease.reservation),
                    false,
                    &mut None
                )
                .is_err()
            );
            let WorkPull::Claimed(retried) = reopened
                .pull_owned(
                    &owner,
                    &scope,
                    &issuer.verifier(&mut None)?,
                    &worker,
                    targeted.then_some(operation),
                    &mut None,
                )
                .await?
            else {
                return Err(Error::NotFound("uncommitted admission retry".into()));
            };
            assert_eq!(retried, lease);
            continue;
        }
        assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
        assert_eq!(retained.revision, lease.operation_revision);
        assert_eq!(retained.phase, crate::scheduler::OperationPhase::Admitted);
        let tail = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?
            .tail()
            .await?;
        assert!(matches!(
            reopened
                .pull_owned(
                    &owner,
                    &scope,
                    &issuer.verifier(&mut None)?,
                    &worker,
                    targeted.then_some(operation),
                    &mut None
                )
                .await?,
            WorkPull::Idle
        ));
        assert_eq!(
            acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?
                .tail()
                .await?,
            tail
        );
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_work_does_not_starve_the_pull_queue() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator = open(&client).await?;
    let mut follower = open(&client).await?;
    let cancelled = OperationId::from_bytes([5; 16]);
    let runnable = OperationId::from_bytes([6; 16]);
    for (index, operation_id) in [cancelled, runnable].into_iter().enumerate() {
        declare(
            &mut coordinator,
            spec(operation_id, 0)?,
            &format!("declare-cancel-{index}"),
        )
        .await?;
        assert!(
            !declare(
                &mut follower,
                spec(operation_id, 0)?,
                &format!("declare-cancel-{index}")
            )
            .await?
        );
    }
    publish(
        &mut coordinator,
        "cancel-before-pull",
        SchedulerEvent::CancellationRequested {
            operation_id: cancelled,
            recursive: false,
        },
    )
    .await?;
    let worker = Worker {
        id: "worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    assert_eq!(
        follower
            .pull(&worker)
            .await?
            .map(|lease| lease.operation.operation_id),
        Some(runnable)
    );
    assert_eq!(
        follower
            .scheduler()
            .operation(cancelled)
            .and_then(|state| state.outcome.as_ref()),
        Some(&crate::Outcome::Cancelled)
    );
    Ok(())
}
