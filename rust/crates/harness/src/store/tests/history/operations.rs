use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn operation_lookup_counts_atomic_records_under_one_byte_budget() -> Result<()> {
    let client = projection_fixture(2).await?;
    let reader = with_content(&client).await?.history_reader()?;
    let operation = projection_command(1, 0)?.operation_id;
    let path = crate::store::operations::publication_path(
        &authority(),
        "harness/v2/aggregate-operations/",
        Some(&operation),
        &mut None,
    )?
    .0;
    let mut total = 0;
    for path in [path.as_str(), &authority().stream_path(&mut None)?] {
        let records = acyclic_stream::Stream::new(client.clone(), path)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        total += records[0].value.len() as u64;
    }
    let (found, bytes) = reader.operation_event(operation, total, &mut None).await?;
    assert_eq!(found.unwrap().operation_id, operation);
    assert_eq!(bytes, total);
    assert!(matches!(
        reader
            .operation_event(operation, total - 1, &mut None)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        reader
            .operation_event(OperationId::from_bytes([254; 16]), total, &mut None)
            .await?,
        (None, 0)
    );
    Ok(())
}

#[tokio::test]
async fn operation_index_is_atomic_and_recovers_without_duplicate_publication() -> Result<()> {
    let client = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let mut aggregate = with_content(&client).await?;
    let submitted = command(1)?;
    client.aggregate_commit_fault.store(3, Ordering::SeqCst);
    assert!(
        matches!(aggregate.execute(submitted.clone(), #[cfg(feature = "filesystem")] None, &mut None).await,
        Err(Error::Indeterminate(id)) if id == submitted.operation_id)
    );
    aggregate
        .execute(
            submitted.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert_eq!(aggregate.reducer().revision(), 1);
    let mut later = command(2)?;
    later.expected_revision = 1;
    aggregate
        .execute(
            later,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    client.forbid_writes.store(true, Ordering::SeqCst);
    assert!(matches!(
        aggregate
            .execute(
                submitted.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await?,
        ApplyResult::Replayed { .. }
    ));
    let mut changed = submitted.clone();
    changed.idempotency_key = IdempotencyKey::new("changed-retry")?;
    assert!(matches!(
        aggregate
            .execute(
                changed,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    client.history_read_fault.store(3, Ordering::SeqCst);
    assert!(
        aggregate
            .execute(
                submitted.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert!(matches!(
        aggregate.reconcile(&submitted).await?,
        Some(ApplyResult::Replayed { .. })
    ));
    assert_eq!(aggregate.reducer().revision(), 2);
    Ok(())
}

#[tokio::test]
async fn operation_location_must_share_the_canonical_events_atomic_commit() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None)?, schemas());
    let ApplyResult::Applied { event } = reducer.apply(&command(1)?)? else {
        panic!("fresh event");
    };
    acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
        .append(encode_event(&authority(), &event, &mut None)?)
        .await?;
    let location = json!({"authority":authority(), "operation_id":event.operation_id,
        "revision":event.revision, "intent_digest":event.intent_digest});
    let path = crate::store::operations::publication_path(
        &authority(),
        "harness/v2/aggregate-operations/",
        Some(&event.operation_id),
        &mut None,
    )?
    .0;
    acyclic_stream::Stream::new(client.clone(), path.as_str())?
        .append(crate::contract::canonical_json_bytes(&location)?)
        .await?;
    let reader = HistoryReader::new(
        &client,
        &authority(),
        &issuer().verifier(&mut None)?,
        &mut None,
    )?;
    assert!(matches!(
        reader
            .operation_event(
                event.operation_id,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None
            )
            .await,
        Err(Error::Storage(_))
    ));
    Ok(())
}

#[tokio::test]
async fn reconciliation_observes_a_commit_without_redispatch() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = with_content(&client).await?;
    let mut uncertain = with_content(&client).await?;
    let submitted = command(1)?;
    writer
        .execute(
            submitted.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(matches!(
        uncertain.reconcile(&submitted).await?,
        Some(ApplyResult::Applied { .. })
    ));
    assert!(matches!(
        uncertain.reconcile(&submitted).await?,
        Some(ApplyResult::Replayed { .. })
    ));
    assert_eq!(uncertain.reducer().revision(), 1);
    Ok(())
}
