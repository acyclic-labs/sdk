use super::*;

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn owned_task_journal_guards_survive_local_stream_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let payloads = model_claim_host(Arc::new(provider)).await?;
    let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stream = Arc::new(provider);
    assert_eq!(
        acyclic_stream::Stream::new(
            std::sync::Arc::clone(&stream),
            format!("harness/v2/workflows/{}", OperationId::from_bytes([77; 16]))
        )?
        .tail()
        .await?,
        1
    );
    assert_eq!(
        acyclic_stream::Stream::new(
            std::sync::Arc::clone(&stream),
            format!("harness/v2/execution/{}", OperationId::from_bytes([78; 16]))
        )?
        .tail()
        .await?,
        75
    );
    let coordinator = DistributedCoordinator::open(&stream, payloads, None, &mut None).await?;
    let operation = coordinator
        .scheduler()
        .operation(OperationId::from_bytes([77; 16]))
        .ok_or_else(|| Error::NotFound("reopened owned task".into()))?;
    assert!(operation.cancellation_requested);
    assert!(operation.reservation.is_some());
    Ok(())
}
