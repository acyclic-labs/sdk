use super::*;

#[tokio::test]
async fn ambiguous_append_is_durable_and_retry_resolves_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(
        sites::AUTHORITY_APPEND,
        0,
        SimulationFault::AmbiguousAuthorityAppend,
    );
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([7; 16]);
    let created = (work!(
        authority,
        create_authority(authority_id, Epoch::GENESIS),
        B,
        &cancellation
    ))
    .await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let proposed = proposal(3, 4);
    let ambiguous_result = (work!(
        authority,
        compare_and_append(authority_id, Epoch::GENESIS, head, proposed.clone()),
        B,
        &cancellation
    ))
    .await;
    let Err(ambiguous) = ambiguous_result else {
        return Err("scheduled ambiguous append returned success".into());
    };
    assert!(matches!(
        ambiguous.error,
        AuthorityStoreError::Indeterminate { .. }
    ));
    let retry = (work!(
        authority,
        compare_and_append(authority_id, Epoch::GENESIS, head, proposed),
        B,
        &cancellation
    ))
    .await?;
    assert!(matches!(retry.value, AppendOutcome::AlreadyCommitted(_)));
    Ok(())
}

#[tokio::test]
async fn duplicate_replay_and_fence_change_are_observable_without_hidden_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(
        sites::AUTHORITY_APPEND,
        1,
        SimulationFault::FenceBeforeAuthorityAppend,
    );
    schedule.inject(
        sites::AUTHORITY_REPLAY,
        0,
        SimulationFault::DuplicateAuthorityReplay,
    );
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([8; 16]);
    let created = (work!(
        authority,
        create_authority(authority_id, Epoch::GENESIS),
        B,
        &cancellation
    ))
    .await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let committed = (work!(
        authority,
        compare_and_append(authority_id, Epoch::GENESIS, head, proposal(5, 6)),
        B,
        &cancellation
    ))
    .await?;
    let AppendOutcome::Committed(commit) = committed.value else {
        return Err("first simulated append did not commit".into());
    };
    let replay = (work!(
        authority,
        replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 8,
                payload_bytes: 1_024,
            }
        ),
        B,
        &cancellation
    ))
    .await?;
    assert_eq!(replay.value, vec![commit.clone(), commit.clone()]);
    let fenced = (work!(
        authority,
        compare_and_append(
            authority_id,
            Epoch::GENESIS,
            Head {
                epoch: commit.epoch,
                sequence: commit.sequence,
                digest: commit.digest,
            },
            proposal(7, 8)
        ),
        B,
        &cancellation
    ))
    .await?;
    assert!(matches!(fenced.value, AppendOutcome::Fenced { .. }));
    Ok(())
}

#[tokio::test]
async fn fence_before_append_preserves_nested_budget_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(
        sites::AUTHORITY_APPEND,
        0,
        SimulationFault::FenceBeforeAuthorityAppend,
    );
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([81; 16]);
    let created = (work!(
        authority,
        create_authority(authority_id, Epoch::GENESIS),
        B,
        &cancellation
    ))
    .await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let result = (work!(
        authority,
        compare_and_append(authority_id, Epoch::GENESIS, head, proposal(82, 83)),
        WorkBudget {
            authority_records_appended: 1,
            backend_read_operations: 1,
            backend_write_operations: 1,
            ..B
        },
        &cancellation
    ))
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
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(
        sites::AUTHORITY_HEAD,
        0,
        SimulationFault::StaleAuthorityHead(stale),
    );
    schedule.inject(
        sites::AUTHORITY_HEAD,
        1,
        SimulationFault::StaleAuthorityHead(stale),
    );
    let (authority, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([52; 16]);
    let observed = (work!(authority, head(authority_id), B, &cancellation)).await?;
    assert_eq!(observed.value, stale);

    let too_small = (work!(
        authority,
        head(authority_id),
        WorkBudget::default(),
        &cancellation
    ))
    .await;
    assert!(matches!(
        too_small,
        Err(AuthorityFailure {
            error: AuthorityStoreError::Work(_),
            ..
        })
    ));

    let (object_id, bytes) = object(b"batch-fault");
    (work!(objects, put(object_id, bytes), B, &cancellation)).await?;
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 64,
    }];
    schedule.inject(
        sites::OBJECT_READ_MANY,
        0,
        SimulationFault::CorruptObjectRead,
    );
    schedule.inject(
        sites::OBJECT_READ_MANY,
        1,
        SimulationFault::PartialObjectRead,
    );
    let corrupt = (work!(objects, read_many(&requests), B, &cancellation)).await?;
    assert_ne!(object_digest(&corrupt.value[0].bytes), object_id.digest);
    let partial = (work!(objects, read_many(&requests), B, &cancellation)).await?;
    assert_eq!(partial.value[0].bytes.len(), b"batch-fault".len() - 1);
    Ok(())
}
