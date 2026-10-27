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
async fn simulation_replays_the_canonical_settlement_after_later_destruction()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = SimulatedMachines::default();
    let key = IdempotencyKey::new();
    let original = provider.create(request(key)).await?;
    let MutationOutcome::Created(value) = &original else {
        return Err("expected machine".into());
    };
    let machine = value.id;
    let settled = provider.ops.settled(key).await?;
    provider
        .destroy_machine(machine, IdempotencyKey::new())
        .await?;
    assert_eq!(provider.recover(key).await, Ok(original.clone()));
    assert_eq!(provider.create(request(key)).await, Ok(original));
    assert_eq!(provider.ops.settled(key).await?, settled);
    assert_eq!(
        provider.recover_operation(key).await?,
        crate::core::operation_id(key)
    );
    assert_eq!(
        provider.inspect_machine(machine).await?.state,
        MachineState::Destroyed
    );
    assert_eq!(provider.events(machine, None, 10).await?.events.len(), 2);
    Ok(())
}
