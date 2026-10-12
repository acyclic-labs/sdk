use super::*;

#[tokio::test]
async fn owned_child_declaration_rechecks_parent_and_recovers_tail_conflict() -> Result<()> {
    for cancel in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let gate = Arc::new(PausedChildVerifier::default());
        let mut first = DistributedCoordinator::open(&client, gate.clone(), None, &mut None).await?;
        let parent = OperationId::from_bytes([41; 16]);
        declare(&mut first, spec(parent, 0)?, "fenced-parent").await?;
        let lease = start(&mut first, &Worker {
            id: "parent-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
        }, "parent-start").await?;
        assert_eq!(lease.operation.operation_id, parent);
        let fence = LeaseFence::from(&lease.reservation);
        let mut second =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
        let (owner, issuer) = owner_issuer();
        let signed = issuer.issue("declare", &Capabilities::new(["operation:declare", "operation:observe"]), None, None, &mut None).expect("scope proof");
        let verifier = issuer.verifier(&mut None).expect("verifier copy");
        let child_id = OperationId::from_bytes([42; 16]);
        let mut child = spec(child_id, 0)?;
        child.parent = Some(ParentLink {
            operation_id: parent,
            slot: "child".into(),
        });
        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let disturbance = async {
            gate.entered.notified().await;
            let result = if cancel {
                second
                    .apply(
                        IdempotencyKey::new("parent-cancel")?,
                        SchedulerEvent::CancellationRequested {
                            operation_id: parent,
                            recursive: false,
                        }, &mut None,
                    )
                    .await
            } else {
                declare(
                    &mut second,
                    spec(OperationId::from_bytes([43; 16]), 0)?,
                    "unrelated-tail",
                )
                .await
            };
            gate.release.notify_one();
            result
        };
        let (attempt, disturbance) = tokio::join!(
            first.declare_operation(
                        &owner,
                        &signed,
                        &verifier,
                        child.clone(),
                        IdempotencyKey::new("owned-child")?,
                        Some(fence.clone())
                    , &mut None),
            disturbance
        );
        disturbance?;
        assert!(matches!(attempt, Err(Error::Conflict(_))));
        assert!(first.scheduler().operation(child_id).is_none());
        let retry = first
            .declare_operation(
                        &owner,
                        &signed,
                        &verifier,
                        child.clone(),
                        IdempotencyKey::new("owned-child")?,
                        Some(fence.clone())
                    , &mut None)
            .await;
        if cancel {
            assert!(retry.is_err());
            assert!(first.scheduler().operation(child_id).is_none());
        } else {
            assert_eq!(retry?, true);
            assert_eq!(
                first
                    .declare_operation(
                        &owner,
                        &signed,
                        &verifier,
                        child.clone(),
                        IdempotencyKey::new("owned-child")?,
                        Some(fence.clone())
                    , &mut None)
                    .await?,
                false
            );
            assert_eq!(first.scheduler().children(parent).count(), 1);
            first
                .apply(
                    IdempotencyKey::new("parent-cancel-after")?,
                    SchedulerEvent::CancellationRequested {
                        operation_id: parent,
                        recursive: false,
                    }, &mut None,
                )
                .await?;
            assert!(
                first
                    .declare_operation(
                        &owner,
                        &signed,
                        &verifier,
                        child,
                        IdempotencyKey::new("owned-child")?,
                        Some(fence)
                    , &mut None)
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_is_owner_fenced_durable_and_exactly_replayable() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(LostSessionAck::default());
    let client = provider.clone();
    let mut coordinator = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let parent = OperationId::from_bytes([44; 16]);
    let child = OperationId::from_bytes([45; 16]);
    let grandchild = OperationId::from_bytes([46; 16]);
    declare(&mut coordinator, spec(parent, 0)?, "cancel-parent-declare").await?;
    let worker = Worker {
        id: "cancel-parent-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
    };
    let lease = start(&mut coordinator, &worker, "cancel-parent-start").await?;
    assert_eq!(lease.operation.operation_id, parent);
    let fence = LeaseFence::from(&lease.reservation);
    for (operation_id, parent_id, key) in [
        (child, parent, "cancel-child-declare"), (grandchild, child, "cancel-grandchild-declare"),
    ] {
        let mut operation = spec(operation_id, 0)?;
        operation.parent = Some(ParentLink { operation_id: parent_id, slot: "child".into() });
        declare(&mut coordinator, operation, key).await?;
    }
    assert_eq!(start(&mut coordinator, &worker, "cancel-child-start").await?.operation.operation_id, child);
    let (owner, issuer) = owner_issuer();
    let scope = issuer.issue("cancel", &Capabilities::new(["operation:observe", "operation:cancel"]), None, None, &mut None).expect("scope proof");
    let verifier = issuer.verifier(&mut None)?;
    let foreign = Authority { kind: AggregateKind::Task, id: "foreign".into() };
    let foreign_issuer = AuthorityIssuer::new("test-runtime", [9; 32], foreign.clone());
    let foreign_scope = foreign_issuer.issue("cancel", scope.capabilities(), None, None, &mut None)?;
    let foreign_verifier = foreign_issuer.verifier(&mut None)?;
    let mut stale = fence.clone();
    stale.placement.push_str("-stale");
    let before = coordinator.revision();
    for (authority, grant, verifier, target, fence, key, code) in [
        (&owner, &scope, &verifier, child, &stale, "stale", wire::ErrorCode::Unauthorized),
        (&owner, &scope, &verifier, grandchild, &fence, "indirect", wire::ErrorCode::Unauthorized),
        (&foreign, &foreign_scope, &foreign_verifier, child, &fence, "foreign", wire::ErrorCode::NotFound),
    ] {
        let error = coordinator.cancel_operation(authority, grant, verifier, target,
            IdempotencyKey::new(key)?, true, Some((parent, fence)), &mut None).await.err().expect("denied cancellation");
        assert_eq!(error.code(), code);
    }
    assert_eq!(coordinator.revision(), before);
    provider.lose_ack.store(true, Ordering::SeqCst);
    let (_, state) = coordinator.cancel_operation(&owner, &scope, &verifier, child,
        IdempotencyKey::new("owned-child-cancel")?, true, Some((parent, &fence)), &mut None).await?;
    assert!(state.cancellation_requested && state.outcome.is_none());
    assert_eq!(coordinator.scheduler().operation(grandchild).and_then(|state| state.outcome.as_ref()), Some(&crate::Outcome::Cancelled));
    let writes = provider.io.writes.load(Ordering::SeqCst);
    let (_, replay) = coordinator.cancel_operation(&owner, &scope, &verifier, child,
        IdempotencyKey::new("owned-child-cancel")?, true, Some((parent, &fence)), &mut None).await?;
    assert_eq!(replay, state);
    assert_eq!(provider.io.writes.load(Ordering::SeqCst), writes);
    assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.iter().filter(|event| matches!(event,
        SchedulerEvent::CancellationRequested { operation_id, recursive: true } if *operation_id == child
    )).count(), 1);

    let root = OperationId::from_bytes([31; 16]);
    let descendant = OperationId::from_bytes([32; 16]);
    declare(&mut coordinator, spec(root, 0)?, "declare-control-parent").await?;
    let mut child_spec = spec(descendant, 0)?;
    child_spec.parent = Some(ParentLink { operation_id: root, slot: "child".into() });
    declare(&mut coordinator, child_spec, "declare-control-child").await?;
    let (applied, status) = coordinator.cancel_operation(&owner, &scope, &verifier, root,
        IdempotencyKey::new("cancel-control-tree")?, true, None, &mut None).await?;
    assert!(applied);
    assert_eq!(status.outcome, Some(crate::Outcome::Cancelled));
    assert_eq!(coordinator.scheduler().operation(descendant).and_then(|state| state.outcome.as_ref()), Some(&crate::Outcome::Cancelled));
    assert!(!coordinator.cancel_operation(&owner, &scope, &verifier, root,
        IdempotencyKey::new("cancel-control-tree")?, true, None, &mut None).await?.0);
    assert!(matches!(coordinator.cancel_operation(&owner, &scope, &verifier, root,
        IdempotencyKey::new("cancel-control-tree")?, false, None, &mut None).await, Err(Error::Conflict(_))));
    let mut reopened = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    assert_eq!(reopened.observe_operation(&owner, &scope, &verifier, descendant, &mut None)?.outcome, Some(crate::Outcome::Cancelled));
    let observe_only = issuer.issue("observe-only", &Capabilities::new(["operation:observe"]), None, None, &mut None)?;
    assert!(matches!(reopened.cancel_operation(&owner, &observe_only, &verifier, descendant,
        IdempotencyKey::new("unauthorized-cancel")?, false, None, &mut None).await, Err(Error::Unauthorized(_))));
    Ok(())
}

#[tokio::test]
async fn selected_command_wake_rechecks_wait_and_recovers_tail_conflict() -> Result<()> {
    for cancel in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let gate = Arc::new(PausedChildVerifier::default());
        let mut first = DistributedCoordinator::open(&client, gate.clone(), None, &mut None).await?;
        let task = OperationId::from_bytes([51; 16]);
        let selected = OperationId::from_bytes([52; 16]);
        declare(&mut first, spec(task, 0)?, "wake-task").await?;
        let worker = Worker {
            id: "wake-worker".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        };
        let lease = start(&mut first, &worker, "wake-start").await?;
        assert_eq!(lease.operation.operation_id, task);
        let fence = LeaseFence::from(&lease.reservation);
        first
            .apply(
                IdempotencyKey::new("wake-suspend")?,
                SchedulerEvent::WorkflowSuspended {
                    operation_id: task,
                    fence,
                    workflow_revision: 1,
                    waiting_command: Some(selected),
                }, &mut None,
            )
            .await?;
        let mut second =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
        let (owner, issuer) = owner_issuer();
        let signed = issuer.issue("wake", &Capabilities::new(["operation:wake", "operation:observe"]), None, None, &mut None).expect("scope proof");
        let verifier = issuer.verifier(&mut None).expect("verifier copy");
        let input = state_ref()?;
        assert!(
            first
                .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("wrong-selected-wake")?, Some(OperationId::from_bytes([53; 16])), &mut None)
                .await
                .is_err()
        );
        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let disturbance = async {
            gate.entered.notified().await;
            let result = if cancel {
                second
                    .apply(
                        IdempotencyKey::new("cancel-wake")?,
                        SchedulerEvent::CancellationRequested {
                            operation_id: task,
                            recursive: false,
                        }, &mut None,
                    )
                    .await
            } else {
                declare(
                    &mut second,
                    spec(OperationId::from_bytes([54; 16]), 0)?,
                    "wake-unrelated-tail",
                )
                .await
            };
            gate.release.notify_one();
            result
        };
        let (attempt, disturbance) = tokio::join!(
            first.resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected), &mut None),
            disturbance
        );
        disturbance?;
        assert!(matches!(attempt, Err(Error::Conflict(_))));
        let retry = first
            .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected), &mut None)
            .await;
        if cancel {
            assert!(retry.is_err());
        } else {
            assert!(retry?);
            assert!(!first.resume_workflow(&owner, &signed, &verifier, task, 1,
                input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected), &mut None).await?);
            assert!(
                first
                    .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("another-wake")?, Some(selected), &mut None)
                    .await
                    .is_err()
            );
            let lease = start(&mut first, &worker, "second-wait-start").await?;
            assert_eq!(lease.operation.operation_id, task);
            let fence = LeaseFence::from(&lease.reservation);
            let next = OperationId::from_bytes([55; 16]);
            first
                .apply(
                    IdempotencyKey::new("second-wait-suspend")?,
                    SchedulerEvent::WorkflowSuspended {
                        operation_id: task,
                        fence,
                        workflow_revision: 1,
                        waiting_command: Some(next),
                    }, &mut None,
                )
                .await?;
            assert!(
                first
                    .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected), &mut None)
                    .await
                    .is_err()
            );
            assert_eq!(
                first
                    .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("second-selected-wake")?, Some(next), &mut None)
                    .await?,
                true
            );
            first
                .apply(
                    IdempotencyKey::new("cancel-published-wake")?,
                    SchedulerEvent::CancellationRequested {
                        operation_id: task,
                        recursive: false,
                    }, &mut None,
                )
                .await?;
            assert!(
                first
                    .resume_workflow(&owner, &signed, &verifier, task, 1, input, IdempotencyKey::new("second-selected-wake")?, Some(next), &mut None)
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn passive_suspension_atomically_fences_execution_and_recovers_commit_ack() -> Result<()>
{
    for existing_tail in [None, Some(0), Some(70)] {
        let expected_tail = existing_tail.unwrap_or(0);
        for mode in 0..4 {
            let provider = Arc::new(LostSessionAck::default());
            let client = provider.clone();
            let mut coordinator =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
            let task = OperationId::from_bytes([91; 16]);
            let execution = OperationId::from_bytes([92; 16]);
            declare(&mut coordinator, spec(task, 0)?, "idle-task").await?;
            let lease = start(&mut coordinator, &Worker {
                id: "idle-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
            }, "idle-start").await?;
            assert_eq!(lease.operation.operation_id, task);
            let fence = LeaseFence::from(&lease.reservation);
            if let Some(tail) = existing_tail {
                acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?
                    .fork(execution_path(execution, &mut None)?.as_str(), Some(0), None)
                    .await?;
                if tail != 0 {
                    acyclic_stream::Stream::new(std::sync::Arc::clone(&client), execution_path(execution, &mut None)?.as_str())?
                        .append_batch(
                            vec![
                                OwnedBytes::from_static(b"settled observation");
                                usize::try_from(tail).map_err(|_| Error::Invalid(
                                    "fixture tail is not representable".into()
                                ))?
                            ],
                            Some(0),
                            None,
                        )
                        .await?;
                }
            }
            provider
                .commit_lose_ack
                .store(mode == 1 || mode == 2, std::sync::atomic::Ordering::SeqCst);
            provider
                .hide_receipt
                .store(mode == 2, std::sync::atomic::Ordering::SeqCst);
            provider
                .execution_race
                .store(mode == 3, std::sync::atomic::Ordering::SeqCst);
            let event = SchedulerEvent::WorkflowSuspended {
                operation_id: task,
                fence: fence.clone(),
                workflow_revision: 1,
                waiting_command: Some(execution),
            };
            let result = coordinator
                .apply_if_execution_quiescent(
                    IdempotencyKey::new("idle-suspend")?,
                    event.clone(),
                    execution,
                    expected_tail, None, &mut None
                )
                .await;
            if mode == 3 {
                assert!(matches!(result, Err(Error::Conflict(_))));
            } else if mode == 2 {
                assert!(matches!(result, Err(Error::Indeterminate(id)) if id == task));
            } else {
                assert_eq!(result?, true);
            }
            let reopened =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
            let retained = reopened
                .scheduler()
                .operation(task)
                .ok_or_else(|| Error::NotFound("idle task".into()))?;
            if mode == 3 {
                assert_eq!(retained.phase, OperationPhase::Running);
                assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
                assert_eq!(
                    acyclic_stream::Stream::new(std::sync::Arc::clone(&client), execution_path(execution, &mut None)?.as_str())?
                        .tail()
                        .await?,
                    expected_tail + 1
                );
                assert!(
                    coordinator
                        .apply_if_execution_quiescent(
                            IdempotencyKey::new("idle-suspend")?,
                            event,
                            execution,
                            expected_tail, None, &mut None
                        )
                        .await
                        .is_err()
                );
            } else {
                assert_eq!(retained.phase, OperationPhase::Suspended);
                assert!(retained.reservation.is_none());
                assert_eq!(
                    retained
                        .workflow
                        .as_ref()
                        .and_then(|slot| slot.waiting_command),
                    Some(execution)
                );
                let outcome = client
                    .commit(acyclic_stream::CommitRequest {
                        conditions: vec![
                            acyclic_stream::CommitCondition::Tail {
                                path: acyclic_stream::StreamPath::new(COORDINATOR_PATH)?,
                                expected: reopened.revision() - 1,
                            },
                            if existing_tail.is_some() {
                                acyclic_stream::CommitCondition::Tail {
                                    path: execution_path(execution, &mut None)?,
                                    expected: expected_tail,
                                }
                            } else {
                                acyclic_stream::CommitCondition::Absent {
                                    path: execution_path(execution, &mut None)?,
                                }
                            },
                        ],
                        mutations: vec![acyclic_stream::CommitMutation::Append {
                            path: execution_path(execution, &mut None)?,
                            records: vec![OwnedBytes::from_static(b"stale dispatch")],
                        }],
                        idempotency_key: stream_key("stale-idle-dispatch".len(), format_args!("stale-idle-dispatch"), &mut None).map(|(key, _)| key)?,
                    })
                    .await?;
                assert!(matches!(
                    outcome,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn leases_are_fenced_and_recoverable() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator =
        DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let operation_id = OperationId::from_bytes([7; 16]);
    declare(&mut coordinator, spec(operation_id, 0)?, "declare-recovery").await?;
    coordinator = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier), None, &mut None).await?;
    let worker = Worker {
        id: "worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let first = coordinator
        .pull(&worker)
        .await?
        .ok_or_else(|| Error::NotFound("lease".into()))?;
    assert!(coordinator.pull(&worker).await?.is_none());
    let forged = LeaseFence {
        reservation_id: "foreign".into(),
        placement: worker.id.clone(),
    };
    assert!(matches!(
        coordinator
            .apply(
                IdempotencyKey::new("forged-start")?,
                SchedulerEvent::Started {
                    operation_id,
                    fence: forged
                }
            , &mut None)
            .await,
        Err(Error::Unauthorized(_))
    ));
    coordinator
        .apply(
            IdempotencyKey::new("valid-start")?,
            SchedulerEvent::Started {
                operation_id,
                fence: LeaseFence::from(&first.reservation),
            }, &mut None,
        )
        .await?;
    let checkpoint = crate::resources::CheckpointRef::new(
        crate::resources::ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?,
        vec![1],
        Some("checkpoint-1".into()),
    )?;
    coordinator
        .apply(
            IdempotencyKey::new("checkpoint-before-crash")?,
            SchedulerEvent::Checkpointed {
                operation_id,
                checkpoint: checkpoint.clone(),
                fence: LeaseFence::from(&first.reservation),
            }, &mut None,
        )
        .await?;
    coordinator
        .release_lease(&first, IdempotencyKey::new("release-crashed-worker")?)
        .await?;
    let second = coordinator
        .pull(&worker)
        .await?
        .ok_or_else(|| Error::NotFound("replacement lease".into()))?;
    assert_ne!(first.reservation.id, second.reservation.id);
    assert_eq!(second.checkpoint, Some(checkpoint));
    assert!(second.operation_revision > first.operation_revision);
    assert!(matches!(
        coordinator
            .apply(
                IdempotencyKey::new("stale-complete")?,
                SchedulerEvent::Completed {
                    operation_id,
                    outcome: crate::Outcome::Succeeded(state_ref()?),
                    fence: Some(LeaseFence::from(&first.reservation)),
                    execution_duration_ns: None,
                }
            , &mut None)
            .await,
        Err(Error::Conflict(_) | Error::Unauthorized(_))
    ));
    Ok(())
}
