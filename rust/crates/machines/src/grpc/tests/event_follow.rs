use super::*;

// Encode original simulator output; no manually fabricated provider event fixture.
pub(super) fn encode_event(event: MachineEvent) -> wire::MachineEvent {
    let mut value = wire::MachineEvent {
        machine: Some(encode_machine(event.machine)),
        sequence: event.sequence,
        observed_at_unix_ms: event.observed_at_unix_ms,
        ..Default::default()
    };
    match event.fact {
        EventFact::State(state) => {
            value.kind = wire::EventKind::State as i32;
            value.state = match state {
                MachineState::Starting => wire::MachineStatus::Starting,
                MachineState::Running => wire::MachineStatus::Running,
                MachineState::Suspending => wire::MachineStatus::Suspending,
                MachineState::Suspended => wire::MachineStatus::Suspended,
                MachineState::Waking => wire::MachineStatus::Waking,
                MachineState::Destroying => wire::MachineStatus::Destroying,
                MachineState::Destroyed => wire::MachineStatus::Destroyed,
                MachineState::Failed => wire::MachineStatus::Failed,
                MachineState::Indeterminate => wire::MachineStatus::Indeterminate,
                MachineState::Offline => wire::MachineStatus::Offline,
            } as i32;
        }
        EventFact::Pressure(pressure) => {
            value.kind = wire::EventKind::Pressure as i32;
            value.pressure = match pressure {
                Pressure::CustomerBudget => wire::PressureKind::CustomerBudget,
                Pressure::MachineLimit => wire::PressureKind::MachineLimit,
                Pressure::ServiceSaturation => wire::PressureKind::ServiceSaturation,
            } as i32;
        }
        EventFact::CapacityChanged => value.kind = wire::EventKind::Capacity as i32,
    }
    value
}

#[tokio::test]
async fn event_follow_retains_cursor_and_original_terminal_over_authenticated_rpc()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for terminate in [true, false] {
        let provider = SimulatedMachines::default();
        let MutationOutcome::Created(machine) = provider
            .create(CreateMachine::new(
                IdempotencyKey::new(),
                Some(Image::custom([7; 32])?),
                [8; 32],
            ))
            .await?
        else {
            panic!("original create observation");
        };
        let (mut service, _, _) = scripted_operation_service(Vec::new(), Vec::new());
        service.watch = WatchReply::Events {
            machine: machine.id,
            after: Some(1),
            events: Arc::new(std::sync::Mutex::new(Some(
                provider.follow_events(machine.id, Some(1)).await?,
            ))),
        };
        let (client, shutdown, server) = serve_operation_service(service).await?;
        let mut events = client.follow_events(machine.id, Some(1)).await?;
        provider.suspend(machine.id, IdempotencyKey::new()).await?;
        let suspended = events.next().await.unwrap()?;
        assert_eq!(suspended.machine, machine.id);
        assert_eq!(suspended.sequence, 2);
        assert_eq!(suspended.fact, EventFact::State(MachineState::Suspended));
        if terminate {
            provider
                .destroy_machine(machine.id, IdempotencyKey::new())
                .await?;
            let terminal = events.next().await.unwrap()?;
            assert_eq!(terminal.sequence, 3);
            assert_eq!(terminal.fact, EventFact::State(MachineState::Destroyed));
        }
        drop(provider);
        if !terminate {
            assert!(matches!(
                events.next().await,
                Some(Err(ProviderError::Unavailable))
            ));
        }
        assert!(events.next().await.is_none());
        drop(events);
        let _ = shutdown.send(());
        server.await??;
    }
    Ok(())
}
