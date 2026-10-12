use super::*;

#[tokio::test]
async fn ambiguous_append_is_durable_and_retry_resolves_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityAppend,
        occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([7; 16]);
    let created = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .create_authority(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let proposed = proposal(3, 4);
    let ambiguous_result = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            head,
            proposed.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.4, __authority_args.5);
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: __authority_args.0,
                    epoch: __authority_args.1,
                    expected: __authority_args.2,
                    commit: __authority_args.3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            ;
        __authority_work.receipt(__authority_result)
    })
    .await;
    let Err(ambiguous) = ambiguous_result else {
        return Err("scheduled ambiguous append returned success".into());
    };
    assert!(matches!(
        ambiguous.error,
        AuthorityStoreError::Indeterminate { .. }
    ));
    let retry = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            head,
            proposed,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.4, __authority_args.5);
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: __authority_args.0,
                    epoch: __authority_args.1,
                    expected: __authority_args.2,
                    commit: __authority_args.3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            ;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(matches!(retry.value, AppendOutcome::AlreadyCommitted(_)));
    Ok(())
}

#[tokio::test]
async fn duplicate_replay_and_fence_change_are_observable_without_hidden_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityAppend,
        occurrence: 2,
        fault: SimulationFault::FenceBeforeAuthorityAppend,
    })?;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReplay,
        occurrence: 1,
        fault: SimulationFault::DuplicateAuthorityReplay,
    })?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([8; 16]);
    let created = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .create_authority(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let committed = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            head,
            proposal(5, 6),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.4, __authority_args.5);
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: __authority_args.0,
                    epoch: __authority_args.1,
                    expected: __authority_args.2,
                    commit: __authority_args.3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            ;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let AppendOutcome::Committed(commit) = committed.value else {
        return Err("first simulated append did not commit".into());
    };
    let replay = (async {
        let __authority_args = (
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 8,
                payload_bytes: 1_024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.3, __authority_args.4);
        let __authority_result = authority
            .replay(
                __authority_args.0,
                __authority_args.1,
                __authority_args.2,
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert_eq!(replay.value, vec![commit.clone(), commit.clone()]);
    let fenced = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            Head {
                epoch: commit.epoch,
                sequence: commit.sequence,
                digest: commit.digest,
            },
            proposal(7, 8),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.4, __authority_args.5);
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: __authority_args.0,
                    epoch: __authority_args.1,
                    expected: __authority_args.2,
                    commit: __authority_args.3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            ;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(matches!(fenced.value, AppendOutcome::Fenced { .. }));
    Ok(())
}

#[tokio::test]
async fn fence_before_append_preserves_nested_budget_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityAppend,
        occurrence: 1,
        fault: SimulationFault::FenceBeforeAuthorityAppend,
    })?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([81; 16]);
    let created = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .create_authority(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let result = (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            head,
            proposal(82, 83),
            WorkBudget {
                authority_records_appended: 1,
                backend_read_operations: 1,
                backend_write_operations: 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.4, __authority_args.5);
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: __authority_args.0,
                    epoch: __authority_args.1,
                    expected: __authority_args.2,
                    commit: __authority_args.3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            ;
        __authority_work.receipt(__authority_result)
    })
    .await;
    let failure = result
        .err()
        .ok_or("zero append budget unexpectedly published after fencing")?;
    assert!(
        matches!(
            failure.error,
            AuthorityStoreError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                maximum: 1,
                ..
            })
        ),
        "unexpected failure: {failure:?}"
    );
    assert_eq!(failure.work.backend_write_operations, 1);
    assert_eq!(failure.work.authority_records_appended, 1);
    Ok(())
}

#[tokio::test]
async fn stale_head_and_batch_read_faults_are_budgeted_and_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let stale = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([51; 32]),
    };
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityHead,
        occurrence: 1,
        fault: SimulationFault::StaleAuthorityHead(stale),
    })?;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityHead,
        occurrence: 2,
        fault: SimulationFault::StaleAuthorityHead(stale),
    })?;
    let (authority, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([52; 16]);
    let observed = (async {
        let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .head(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert_eq!(observed.value, stale);
    assert_eq!(observed.work.authority_records_read, 1);
    let too_small = (async {
        let __authority_args = (authority_id, WorkBudget::default(), &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .head(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })
    .await;
    assert!(matches!(
        too_small,
        Err(AuthorityFailure {
            error: AuthorityStoreError::Work(_),
            ..
        })
    ));

    let (object_id, bytes) = object(b"batch-fault");
    (async { let __object_args = (object_id, bytes, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 64,
    }];
    for (occurrence, fault) in [
        (1, SimulationFault::CorruptObjectRead),
        (2, SimulationFault::PartialObjectRead),
    ] {
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectReadMany,
            occurrence,
            fault,
        })?;
    }
    let corrupt = (async { let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = objects.read_many(__object_args.0, &mut __object_work).await.map(|(values, _allocation)| values); __object_work.receipt(__object_result) }).await?;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value[0].bytes),
        object_id.digest
    );
    let partial = (async { let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = objects.read_many(__object_args.0, &mut __object_work).await.map(|(values, _allocation)| values); __object_work.receipt(__object_result) }).await?;
    assert_eq!(partial.value[0].bytes.len(), b"batch-fault".len() - 1);
    Ok(())
}
