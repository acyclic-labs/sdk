use super::*;

#[tokio::test]
async fn operation_lookup_counts_atomic_records_under_one_byte_budget() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    writer.set_resident_event_limit(1)?;
    let original = command(1)?;
    let first = writer
        .execute(
            original.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let ApplyResult::Applied { event } = first else {
        return Err(Error::Invalid("fixture event was not published".into()));
    };
    let mut later = command(2)?;
    later.expected_revision = 1;
    writer
        .execute(
            later,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(
        writer
            .reducer()
            .operation_revision(original.operation_id)
            .is_none()
    );
    let index = acyclic_stream::Stream::new(
        Arc::clone(&client),
        crate::store::operations::publication_path(
            &authority(),
            "harness/v2/aggregate-operations/",
            Some((&original.operation_id, 36)),
            &mut None,
        )?
        .0
        .as_str(),
    )?
    .read(0, 1)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    let canonical =
        acyclic_stream::Stream::new(Arc::clone(&client), authority().stream_path(&mut None)?)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
    let index_bytes = index
        .first()
        .ok_or_else(|| Error::Storage("fixture index missing".into()))?
        .value
        .len() as u64;
    let total_bytes = index_bytes
        + canonical
            .first()
            .ok_or_else(|| Error::Storage("fixture canonical event missing".into()))?
            .value
            .len() as u64;
    let reader = writer.history_reader()?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .operation_event(original.operation_id, 0, &mut None)
            .await,
        Err(Error::Invalid(_))
    ));

    for maximum in [index_bytes - 1, index_bytes] {
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .operation_event(original.operation_id, maximum, &mut None)
                .await,
            Err(Error::Invalid(_))
        ));
    }
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .operation_event(original.operation_id, total_bytes - 1, &mut None)
            .await,
        Err(Error::Invalid(_))
    ));

    provider.observation_reads.store(0, Ordering::SeqCst);
    assert_eq!(
        reader
            .operation_event(original.operation_id, total_bytes, &mut None)
            .await?,
        (Some(event.clone()), total_bytes)
    );

    assert_eq!(
        reader
            .operation_event(
                original.operation_id,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None
            )
            .await?
            .0,
        Some(event)
    );

    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        reader
            .operation_event(OperationId::from_bytes([254; 16]), total_bytes, &mut None)
            .await?,
        (None, 0)
    );
    Ok(())
}

#[tokio::test]
async fn operation_index_is_atomic_and_recovers_without_duplicate_publication() -> Result<()> {
    use std::sync::atomic::Ordering;
    for fault in 1..=5 {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut aggregate = with_content(&client).await?;
        let submitted = command(1)?;
        provider
            .aggregate_commit_fault
            .store(fault, Ordering::SeqCst);
        let result = aggregate
            .execute(
                submitted.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await;
        if fault == 2 {
            assert!(matches!(result, Ok(ApplyResult::Applied { .. })));
        } else {
            assert!(
                matches!(result, Err(Error::Indeterminate(id)) if id == submitted.operation_id)
            );
        }

        let indexed = aggregate.operation_event(submitted.operation_id).await?;
        assert_eq!(indexed.is_some(), fault != 1);
        let canonical = match acyclic_stream::Stream::new(
            Arc::clone(&client),
            authority().stream_path(&mut None)?,
        )?
        .read(0, 1)
        .await
        {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(acyclic_stream::StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        assert_eq!(canonical.len(), usize::from(fault != 1));
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
        provider.forbid_writes.store(true, Ordering::SeqCst);
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
        for read_fault in [1, 3, 4] {
            provider
                .history_read_fault
                .store(read_fault, Ordering::SeqCst);
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
        }
        assert!(matches!(
            aggregate.reconcile(&submitted).await?,
            Some(ApplyResult::Replayed { .. })
        ));

        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert_eq!(aggregate.reducer().revision(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn operation_location_must_share_the_canonical_events_atomic_commit() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None)?, schemas());
    let ApplyResult::Applied { event } = reducer.apply(command(1)?)? else {
        return Err(Error::Invalid("fixture needs admitted event".into()));
    };
    acyclic_stream::Stream::new(Arc::clone(&client), authority().stream_path(&mut None)?)?
        .append_batch(
            vec![OwnedBytes::from(encode_event(
                &authority(),
                &event,
                &mut None,
            )?)],
            Some(0),
            None,
        )
        .await?;
    let location = json!({"authority": authority(), "operation_id": event.operation_id,
            "revision": event.revision, "intent_digest": event.intent_digest});
    acyclic_stream::Stream::new(
        Arc::clone(&client),
        crate::store::operations::publication_path(
            &authority(),
            "harness/v2/aggregate-operations/",
            Some((&event.operation_id, 36)),
            &mut None,
        )?
        .0
        .as_str(),
    )?
    .append_batch(
        vec![OwnedBytes::from(crate::contract::canonical_json_bytes(
            &location,
        )?)],
        Some(0),
        None,
    )
    .await?;
    let reader = HistoryReader::new(
        &client,
        &authority(),
        &(issuer().verifier(&mut None)?),
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
    let provider = Arc::new(MemoryStream::default());
    let client = provider;
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
