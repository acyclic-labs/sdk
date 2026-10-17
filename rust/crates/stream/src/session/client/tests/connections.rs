use super::*;

#[tokio::test]
async fn reconnect_pushes_a_generation_without_new_records() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "idle", 1).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = Client::with_connector(
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
    );
    let connections = client.connections();
    let _idle = client.follow(StreamPath::new("idle")?, 1).await?;
    let first = connections.changed(0).await?;
    fixture.reset()?;
    let second = tokio::time::timeout(Duration::from_secs(1), connections.changed(first)).await??;
    assert!(second > first);
    Ok(())
}
