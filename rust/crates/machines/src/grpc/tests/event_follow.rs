use super::*;

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
        let original = provider
            .events(machine.id, Some(1), 1)
            .await?
            .events
            .remove(0);
        #[cfg(feature = "test-support")]
        codec_fuzz::capture_event(&original);
        let suspended = events.next().await.unwrap()?;
        assert_eq!(suspended, original);
        if terminate {
            provider
                .destroy_machine(machine.id, IdempotencyKey::new())
                .await?;
            let original = provider
                .events(machine.id, Some(suspended.sequence), 1)
                .await?
                .events
                .remove(0);
            #[cfg(feature = "test-support")]
            codec_fuzz::capture_event(&original);
            let terminal = events.next().await.unwrap()?;
            assert_eq!(terminal, original);
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
