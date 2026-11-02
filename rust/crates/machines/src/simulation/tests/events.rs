use super::*;

#[tokio::test]
async fn event_follow_keeps_original_cursor_and_ends_on_terminal_or_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = SimulatedMachines::default();
    let MutationOutcome::Created(machine) = provider.create(request(key(1))).await? else {
        panic!("original create observation");
    };
    assert!(matches!(
        provider.follow_events(machine.id, Some(2)).await,
        Err(ProviderError::Invalid(_))
    ));
    assert!(matches!(
        provider.follow_events(MachineId::new(), None).await,
        Err(ProviderError::NotFound(_))
    ));
    let mut events = provider.follow_events(machine.id, Some(1)).await?;
    assert!(futures::poll!(events.next()).is_pending());

    // Another original producer must not become this Machine's event source.
    let MutationOutcome::Created(other) = provider.create(request(key(2))).await? else {
        panic!("second create observation");
    };
    assert!(futures::poll!(events.next()).is_pending());
    provider.suspend(machine.id, key(3)).await?;
    let suspended = events.next().await.unwrap()?;
    assert_eq!(suspended.sequence, 2);
    assert_eq!(suspended.fact, EventFact::State(MachineState::Suspended));
    assert_eq!(suspended.machine, machine.id);
    assert!(futures::poll!(events.next()).is_pending());

    // Discarding one subscription must not discard the Machine or its history.
    drop(events);
    assert_eq!(
        provider.inspect_machine(machine.id).await?.state,
        MachineState::Suspended
    );
    let mut events = provider
        .follow_events(machine.id, Some(suspended.sequence))
        .await?;
    provider.destroy_machine(machine.id, key(4)).await?;
    let terminal = events.next().await.unwrap()?;
    assert_eq!(terminal.sequence, 3);
    assert_eq!(terminal.fact, EventFact::State(MachineState::Destroyed));
    assert!(events.next().await.is_none());
    assert!(matches!(
        provider
            .follow_events(machine.id, Some(terminal.sequence))
            .await,
        Err(ProviderError::Conflict(_))
    ));
    let mut lost = provider.follow_events(other.id, Some(1)).await?;
    assert!(futures::poll!(lost.next()).is_pending());
    drop(provider);
    assert!(matches!(
        futures::poll!(lost.next()),
        std::task::Poll::Ready(Some(Err(ProviderError::Unavailable)))
    ));
    assert!(lost.next().await.is_none());

    // Corrupt only actual producer output for the protocol negative controls.
    let mut wrong = suspended.clone();
    wrong.machine = other.id;
    for event in [wrong, terminal] {
        let mut rejected = checked_output(machine.id, Some(1), event);
        assert!(matches!(
            rejected.next().await,
            Some(Err(ProviderError::Rejected(_)))
        ));
        assert!(rejected.next().await.is_none());
    }
    let mut truncated = checked_output(machine.id, Some(1), suspended);
    assert!(truncated.next().await.unwrap().is_ok());
    assert!(matches!(
        truncated.next().await,
        Some(Err(ProviderError::Unavailable))
    ));
    assert!(truncated.next().await.is_none());
    Ok(())
}

fn checked_output(machine: MachineId, after: Option<u64>, event: MachineEvent) -> EventStream {
    crate::provider::events::checked(machine, after, Some(event), |event| async move {
        Ok(event.map(|event| (event, None)))
    })
}
