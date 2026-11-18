use super::*;

#[tokio::test]
async fn simulation_is_exactly_idempotent_and_rejects_key_rebinding() {
    let provider = SimulatedMachines::default();
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000007")
        .unwrap_or_else(|_| unreachable!());
    let first = provider.create(request(key)).await;
    let second = provider.create(request(key)).await;
    assert_eq!(first, second);
    let mut changed = request(key);
    changed.network_policy_digest = [9; 32];
    assert!(matches!(
        provider.create(changed).await,
        Err(ProviderError::Conflict(_))
    ));
    let mut default = request(key);
    default.image = None;
    assert!(matches!(
        provider.create(default.clone()).await,
        Err(ProviderError::Conflict(_))
    ));
    default.idempotency_key = IdempotencyKey::new();
    assert!(matches!(
        provider.create(default).await,
        Err(ProviderError::Unsupported(_))
    ));
}

#[tokio::test]
async fn failed_policy_mutation_does_not_advance_the_simulation_clock() {
    let provider = SimulatedMachines::default();
    let first = provider
        .create(request(IdempotencyKey::new()))
        .await
        .unwrap_or_else(|_| unreachable!());
    let MutationOutcome::Created(first) = first else {
        unreachable!()
    };
    assert_eq!(first.created_at_unix_ms, 2);
    assert!(matches!(
        provider
            .set_suspension_policy(
                MachineId::new(),
                SuspensionPolicy::Manual,
                IdempotencyKey::new()
            )
            .await,
        Err(ProviderError::NotFound(_))
    ));
    let second = provider
        .create(request(IdempotencyKey::new()))
        .await
        .unwrap_or_else(|_| unreachable!());
    let MutationOutcome::Created(second) = second else {
        unreachable!()
    };
    assert_eq!(second.created_at_unix_ms, 3);
}

#[tokio::test]
async fn retained_transition_receipts_and_event_pages_survive_long_histories()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = SimulatedMachines::default();
    let MutationOutcome::Created(machine) = provider.create(request(key(1))).await? else {
        return Err("create returned another outcome".into());
    };
    let transition_key =
        |index: u32| IdempotencyKey::from_digest(Sha256::digest(index.to_be_bytes()).into());
    for index in 0..4_096_u32 {
        if index % 2 == 0 {
            provider.suspend(machine.id, transition_key(index)).await?;
        } else {
            provider.wake(machine.id, transition_key(index)).await?;
        }
    }
    let original = MutationOutcome::Suspended(machine.id);
    assert_eq!(provider.recover(transition_key(0)).await?, original);
    assert_eq!(
        provider.suspend(machine.id, transition_key(0)).await?,
        original
    );
    assert!(matches!(
        provider.wake(machine.id, transition_key(0)).await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(
        provider.inspect_machine(machine.id).await?.state,
        MachineState::Running
    );
    let last = provider.events(machine.id, Some(4_096), 1).await?;
    assert_eq!(last.events.len(), 1);
    let event = last
        .events
        .first()
        .ok_or("the terminal event page is empty")?;
    assert_eq!(event.sequence, 4_097);
    assert_eq!(event.fact, EventFact::State(MachineState::Running));
    assert_eq!(last.next_sequence, None);
    assert!(
        provider
            .events(machine.id, Some(4_097), 1)
            .await?
            .events
            .is_empty()
    );
    Ok(())
}
