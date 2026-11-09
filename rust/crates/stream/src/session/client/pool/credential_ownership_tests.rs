use super::*;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Drive the actual client admission, pool dispatch and final-owner Drop.
/// The negative control releases custody without its physical withdrawal.
async fn custody(broken: bool) -> Result {
    let channel =
        tonic::transport::Endpoint::from_static("http://credential.invalid").connect_lazy();
    let (mut pool, _) = pool::fixture::fixture(&channel);
    pool.links.clear();
    let (sender, mut commands) = mpsc::channel(4);
    let mut client = pool::fixture::client(&pool);
    client.pool.commands = sender;
    let credential = crate::session::testing::credential();
    client.register_credential(0, credential.clone())?;
    pool.command(commands.recv().await.ok_or("registration producer lost")?)?;
    for _ in 0..100 {
        let mut pending = Box::pin(client.with_credential(credential.clone()));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        pool.command(commands.recv().await.ok_or("admission producer lost")?)?;
        let (owned, slot) = pending.await?;
        assert_eq!(slot, 1, "slot use must depend on live owners, not history");
        let clone = owned.clone();
        drop(owned);
        assert!(matches!(
            commands.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        // Fill every unreserved credit. Final-owner Drop must still publish
        // withdrawal, while another admission gets an immediate typed refusal.
        for _ in 0..3 {
            client.register_credential(0, credential.clone())?;
        }
        drop(clone);
        assert!(matches!(
            client.with_credential(credential.clone()).await,
            Err(StreamError::Capacity)
        ));
        for _ in 0..3 {
            pool.command(commands.recv().await.ok_or("renewal producer lost")?)?;
        }
        let withdrawal = commands.recv().await.ok_or("withdrawal producer lost")?;
        assert!(pool.credential_slots.owned(slot));
        if broken {
            // Reintroduce clear-before-physical-withdrawal on the real slot.
            pool.credential_slots.release(slot);
            drop(withdrawal);
        } else {
            pool.command(withdrawal)?;
        }
        if pool.credentials.contains_key(&slot) {
            return Err("withdrawal must precede slot reuse".into());
        }
        assert!(!pool.credential_slots.owned(slot));
        assert_eq!(pool.credentials.len(), 1);
    }
    // Losing the named reply producer ends admission, without a readiness timer.
    let mut pending = Box::pin(client.with_credential(credential.clone()));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    drop(commands.recv().await.ok_or("missing admitted command")?);
    assert!(matches!(pending.await, Err(StreamError::Unavailable)));

    // Cancellation before dispatch creates no slot or retained queue credit.
    let mut pending = Box::pin(client.with_credential(credential.clone()));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    drop(pending);
    pool.command(commands.recv().await.ok_or("missing cancelled command")?)?;
    assert_eq!(pool.credentials.len(), 1);
    assert_eq!(client.pool.commands.capacity(), 4);

    // Cancellation after dispatch drops the undelivered owner and uses exactly
    // the same withdrawal as the final live client clone.
    let mut pending = Box::pin(client.with_credential(credential));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    pool.command(commands.recv().await.ok_or("missing dispatched command")?)?;
    drop(pending);
    pool.command(
        commands
            .recv()
            .await
            .ok_or("missing cancelled owner withdrawal")?,
    )?;
    assert_eq!(pool.credentials.len(), 1);
    assert_eq!(client.pool.commands.capacity(), 4);
    Ok(())
}

#[tokio::test]
async fn credential_owners_reuse_only_withdrawn_slots_and_end_on_producer_loss() -> Result {
    custody(false).await?;
    let error = custody(true)
        .await
        .expect_err("the omitted withdrawal must fail the same oracle");
    assert_eq!(error.to_string(), "withdrawal must precede slot reuse");
    Ok(())
}
