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
    let created = (authority
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity)))?;
    let CreateAuthorityOutcome::Created(head) = created else {
        return Err("fresh simulated authority already existed".into());
    };
    let proposed = proposal(3, 4);
    let ambiguous_result = (async {
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: proposed.clone(),
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value);
        __authority_result
    })
    .await;
    let Err(ambiguous) = ambiguous_result else {
        return Err("scheduled ambiguous append returned success".into());
    };
    assert!(matches!(
        ambiguous,
        AuthorityStoreError::Indeterminate { .. }
    ));
    let retry = (authority
        .compare_and_append(
            crate::GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: head,
                commit: proposed,
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))
        .map(|(value, _allocation)| value))?;
    assert!(matches!(retry, AppendOutcome::AlreadyCommitted(_)));
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
    let created = (authority
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity)))?;
    let CreateAuthorityOutcome::Created(head) = created else {
        return Err("fresh simulated authority already existed".into());
    };
    let committed = ({
        authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: proposal(5, 6),
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value)
    })?;
    let AppendOutcome::Committed(commit) = committed else {
        return Err("first simulated append did not commit".into());
    };
    let replay = (async {
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority
            .replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 8,
                    payload_bytes: 1_024,
                },
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert_eq!(replay.value, vec![commit.clone(), commit.clone()]);
    let fenced = ({
        authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: Head {
                        epoch: commit.epoch,
                        sequence: commit.sequence,
                        digest: commit.digest,
                    },
                    commit: proposal(7, 8),
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value)
    })?;
    assert!(matches!(fenced, AppendOutcome::Fenced { .. }));
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
    let created = (authority
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity)))?;
    let CreateAuthorityOutcome::Created(head) = created else {
        return Err("fresh simulated authority already existed".into());
    };
    let result = (async {
        let mut __authority_work = crate::AuthorityWork::new(
            WorkBudget {
                authority_records_appended: 1,
                backend_read_operations: 1,
                backend_write_operations: 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let __authority_result = authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: proposal(82, 83),
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value);
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
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority.head(authority_id, &mut __authority_work).await;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert_eq!(observed.value, stale);
    assert_eq!(observed.work.authority_records_read, 1);
    let too_small = (async {
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::default(), &cancellation);
        let __authority_result = authority.head(authority_id, &mut __authority_work).await;
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
    (objects
        .put(
            object_id,
            bytes,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
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
    let corrupt = (objects
        .read_many(
            &requests,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await
        .map(|(values, _allocation)| values))?;
    assert_ne!(
        object_digest(object_id.kind, &corrupt[0].bytes),
        object_id.digest
    );
    let partial = (objects
        .read_many(
            &requests,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await
        .map(|(values, _allocation)| values))?;
    assert_eq!(partial[0].bytes.len(), b"batch-fault".len() - 1);
    Ok(())
}
