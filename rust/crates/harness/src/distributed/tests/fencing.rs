use super::*;

#[tokio::test]
async fn owned_child_declaration_rechecks_parent_and_recovers_tail_conflict() -> Result<()> {
    for cancel in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let gate = Arc::new(TestContentVerifier::default());
        let mut first =
            DistributedCoordinator::open(&client, gate.clone(), None, &mut None).await?;
        let parent = OperationId::from_bytes([41; 16]);
        declare(&mut first, spec(parent, 0)?, "fenced-parent").await?;
        let lease = start(
            &mut first,
            &Worker {
                id: "parent-worker".into(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            },
            "parent-start",
        )
        .await?;
        assert_eq!(lease.operation.operation_id, parent);
        let fence = LeaseFence::from(&lease.reservation);
        let mut second = DistributedCoordinator::open(
            &client,
            Arc::new(TestContentVerifier::default()),
            None,
            &mut None,
        )
        .await?;
        let (owner, issuer) = owner_issuer();
        let signed = grant(
            &issuer,
            "declare",
            &["operation:declare", "operation:observe"],
        )?;
        let verifier = issuer.verifier(&mut None)?;
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
                publish(
                    &mut second,
                    "parent-cancel",
                    SchedulerEvent::CancellationRequested {
                        operation_id: parent,
                        recursive: false,
                    },
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
        let mut declare_work = None;
        let (attempt, disturbance) = tokio::join!(
            first.declare_operation(
                &owner,
                &signed,
                &verifier,
                child.clone(),
                IdempotencyKey::new("owned-child")?,
                Some(fence.clone()),
                &mut declare_work
            ),
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
                Some(fence.clone()),
                &mut None,
            )
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
                        Some(fence.clone()),
                        &mut None
                    )
                    .await?,
                false
            );
            assert_eq!(first.scheduler().children(parent).count(), 1);
            publish(
                &mut first,
                "parent-cancel-after",
                SchedulerEvent::CancellationRequested {
                    operation_id: parent,
                    recursive: false,
                },
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
                        Some(fence),
                        &mut None
                    )
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn selected_command_wake_rechecks_wait_and_recovers_tail_conflict() -> Result<()> {
    for cancel in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let gate = Arc::new(TestContentVerifier::default());
        let mut first =
            DistributedCoordinator::open(&client, gate.clone(), None, &mut None).await?;
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
        publish(
            &mut first,
            "wake-suspend",
            SchedulerEvent::WorkflowSuspended {
                operation_id: task,
                fence,
                workflow_revision: 1,
                waiting_command: Some(selected),
            },
        )
        .await?;
        let mut second = DistributedCoordinator::open(
            &client,
            Arc::new(TestContentVerifier::default()),
            None,
            &mut None,
        )
        .await?;
        let (owner, issuer) = owner_issuer();
        let signed = grant(&issuer, "wake", &["operation:wake", "operation:observe"])?;
        let verifier = issuer.verifier(&mut None)?;
        let input = state_ref()?;
        assert!(
            first
                .resume_workflow(
                    &owner,
                    &signed,
                    &verifier,
                    task,
                    1,
                    input.clone(),
                    IdempotencyKey::new("wrong-selected-wake")?,
                    Some(OperationId::from_bytes([53; 16])),
                    &mut None
                )
                .await
                .is_err()
        );
        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let disturbance = async {
            gate.entered.notified().await;
            let result = if cancel {
                publish(
                    &mut second,
                    "cancel-wake",
                    SchedulerEvent::CancellationRequested {
                        operation_id: task,
                        recursive: false,
                    },
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
        let mut resume_work = None;
        let (attempt, disturbance) = tokio::join!(
            first.resume_workflow(
                &owner,
                &signed,
                &verifier,
                task,
                1,
                input.clone(),
                IdempotencyKey::new("selected-wake")?,
                Some(selected),
                &mut resume_work
            ),
            disturbance
        );
        disturbance?;
        assert!(matches!(attempt, Err(Error::Conflict(_))));
        let retry = first
            .resume_workflow(
                &owner,
                &signed,
                &verifier,
                task,
                1,
                input.clone(),
                IdempotencyKey::new("selected-wake")?,
                Some(selected),
                &mut None,
            )
            .await;
        if cancel {
            assert!(retry.is_err());
        } else {
            assert!(retry?);
            assert!(
                !first
                    .resume_workflow(
                        &owner,
                        &signed,
                        &verifier,
                        task,
                        1,
                        input.clone(),
                        IdempotencyKey::new("selected-wake")?,
                        Some(selected),
                        &mut None
                    )
                    .await?
            );
            assert!(
                first
                    .resume_workflow(
                        &owner,
                        &signed,
                        &verifier,
                        task,
                        1,
                        input.clone(),
                        IdempotencyKey::new("another-wake")?,
                        Some(selected),
                        &mut None
                    )
                    .await
                    .is_err()
            );
            let lease = start(&mut first, &worker, "second-wait-start").await?;
            assert_eq!(lease.operation.operation_id, task);
            let fence = LeaseFence::from(&lease.reservation);
            let next = OperationId::from_bytes([55; 16]);
            publish(
                &mut first,
                "second-wait-suspend",
                SchedulerEvent::WorkflowSuspended {
                    operation_id: task,
                    fence,
                    workflow_revision: 1,
                    waiting_command: Some(next),
                },
            )
            .await?;
            assert!(
                first
                    .resume_workflow(
                        &owner,
                        &signed,
                        &verifier,
                        task,
                        1,
                        input.clone(),
                        IdempotencyKey::new("selected-wake")?,
                        Some(selected),
                        &mut None
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                first
                    .resume_workflow(
                        &owner,
                        &signed,
                        &verifier,
                        task,
                        1,
                        input.clone(),
                        IdempotencyKey::new("second-selected-wake")?,
                        Some(next),
                        &mut None
                    )
                    .await?,
                true
            );
            publish(
                &mut first,
                "cancel-published-wake",
                SchedulerEvent::CancellationRequested {
                    operation_id: task,
                    recursive: false,
                },
            )
            .await?;
            assert!(
                first
                    .resume_workflow(
                        &owner,
                        &signed,
                        &verifier,
                        task,
                        1,
                        input,
                        IdempotencyKey::new("second-selected-wake")?,
                        Some(next),
                        &mut None
                    )
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn passive_suspension_atomically_fences_execution_and_recovers_commit_ack() -> Result<()> {
    for existing_tail in [None, Some(0), Some(70)] {
        let expected_tail = existing_tail.unwrap_or(0);
        for mode in 0..4 {
            let client = Arc::new(LostSessionAck::<MemoryStream>::default());
            let mut coordinator = DistributedCoordinator::open(
                &client,
                Arc::new(TestContentVerifier::default()),
                None,
                &mut None,
            )
            .await?;
            let task = OperationId::from_bytes([91; 16]);
            let execution = OperationId::from_bytes([92; 16]);
            declare(&mut coordinator, spec(task, 0)?, "idle-task").await?;
            let lease = start(
                &mut coordinator,
                &Worker {
                    id: "idle-worker".into(),
                    available: ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                },
                "idle-start",
            )
            .await?;
            assert_eq!(lease.operation.operation_id, task);
            let fence = LeaseFence::from(&lease.reservation);
            if let Some(tail) = existing_tail {
                acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?
                    .fork(
                        execution_path(execution, &mut None)?.as_str(),
                        Some(0),
                        None,
                    )
                    .await?;
                if tail != 0 {
                    acyclic_stream::Stream::new(
                        std::sync::Arc::clone(&client),
                        execution_path(execution, &mut None)?.as_str(),
                    )?
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
            client
                .commit_lose_ack
                .store(mode == 1 || mode == 2, std::sync::atomic::Ordering::SeqCst);
            client
                .hide_receipt
                .store(mode == 2, std::sync::atomic::Ordering::SeqCst);
            client
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
                    expected_tail,
                    None,
                    &mut None,
                )
                .await;
            if mode == 3 {
                assert!(matches!(result, Err(Error::Conflict(_))));
            } else if mode == 2 {
                assert!(matches!(result, Err(Error::Indeterminate(id)) if id == task));
            } else {
                assert_eq!(result?, true);
            }
            let reopened = DistributedCoordinator::open(
                &client,
                Arc::new(TestContentVerifier::default()),
                None,
                &mut None,
            )
            .await?;
            let retained = reopened
                .scheduler()
                .operation(task)
                .ok_or_else(|| Error::NotFound("idle task".into()))?;
            if mode == 3 {
                assert_eq!(retained.phase, OperationPhase::Running);
                assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
                assert_eq!(
                    acyclic_stream::Stream::new(
                        std::sync::Arc::clone(&client),
                        execution_path(execution, &mut None)?.as_str()
                    )?
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
                            expected_tail,
                            None,
                            &mut None
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
                        idempotency_key: stream_key(
                            "stale-idle-dispatch".len(),
                            format_args!("stale-idle-dispatch"),
                            &mut None,
                        )
                        .map(|(key, _)| key)?,
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
