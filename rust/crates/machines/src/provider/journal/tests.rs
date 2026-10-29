use super::*;

#[tokio::test]
async fn abandoned_preparation_does_not_close_unsubmitted_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let MutationOutcome::Created(machine) = SimulatedMachines::default()
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            Some(Image::custom([7; 32])?),
            [8; 32],
        ))
        .await?
    else {
        panic!("create must return its original observation");
    };
    let journal = Journal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{}", machine.id))?,
        machine.id,
    );
    journal.enroll(machine.clone()).await?;
    let mut pending = machine.clone();
    pending.state = MachineState::Suspending;
    let state = std::cell::Cell::new(machine.state);
    let key = IdempotencyKey::new();
    let digest = [9; 32];
    // Withhold the actual preparation lock. This is before the original
    // Streams submission, so abandoning this wait must preserve availability.
    let preparation = journal.producer.lock().await;
    let mut operation = Box::pin(journal.begin(key, digest, Some(&pending), || {
        Ok(|| state.set(pending.state))
    }));
    assert!(futures::poll!(&mut operation).is_pending());
    assert_eq!(state.get(), MachineState::Running);
    drop(operation);
    drop(preparation);
    assert_eq!(journal.machine().await?, machine);
    assert!(matches!(
        journal.recover(key).await,
        Err(ProviderError::NotFound(_))
    ));

    // No mutation was submitted: the same original key can still make progress.
    let Admission::Original(publication) = journal
        .begin(key, digest, Some(&pending), || {
            Ok(|| state.set(pending.state))
        })
        .await?
    else {
        panic!("unsubmitted preparation cannot create a replay");
    };
    assert_eq!(state.get(), MachineState::Suspending);
    pending.state = MachineState::Suspended;
    let outcome = MutationOutcome::Suspended(machine.id);
    assert_eq!(
        journal
            .finish(key, digest, publication, &pending, outcome.clone())
            .await?,
        outcome
    );
    assert_eq!(journal.recover(key).await?, outcome);
    Ok(())
}
