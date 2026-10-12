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
