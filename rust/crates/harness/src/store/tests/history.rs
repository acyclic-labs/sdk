use super::*;
use futures::TryStreamExt as _;

#[tokio::test]
async fn empty_history_cursor_stays_terminal_after_first_publication() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let reader = HistoryReader::new(
        &client,
        &authority(),
        &(issuer().verifier(&mut None)?),
        &mut None,
    )?;
    let cursor = reader.pin(0, None).await?;
    let limits = HistoryReadLimits {
        maximum_events: 1,
        maximum_bytes: 65_536,
    };
    assert_eq!(cursor.through_revision, 0);
    assert!(
        reader
            .read_page(&cursor, limits, None)
            .await?
            .events
            .is_empty()
    );
    assert!(matches!(reader.pin(1, None).await, Err(Error::Invalid(_))));
    let future = HistoryCursor {
        through_revision: 1,
        ..cursor.clone()
    };
    assert!(matches!(
        reader.read_page(&future, limits, None).await,
        Err(Error::Invalid(_))
    ));

    provider.forbid_writes.store(false, Ordering::SeqCst);
    let mut writer = with_content(&client).await?;
    writer
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    assert!(
        reader
            .read_page(&cursor, limits, None)
            .await?
            .events
            .is_empty()
    );

    let current = reader.pin(0, None).await?;
    let page = reader.read_page(&current, limits, None).await?;
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.cursor.after_revision, 1);
    Ok(())
}

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
        operations::publication_path(
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
async fn bounded_resident_suffix_preserves_cold_retries_and_recovery() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = with_content(&client).await?;
    writer.set_resident_event_limit(2)?;
    let original = command(1)?;
    let first = writer
        .execute(
            original.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    for identity in 2..=5 {
        let mut next = command(identity)?;
        next.expected_revision = writer.reducer().revision();
        writer
            .execute(
                next,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    assert_eq!(writer.reducer().archived_through_revision(), 3);
    assert!(matches!(
        writer.reducer().events_after(0, 10),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(writer.reducer().events_after(3, 10)?.len(), 2);
    let snapshot = writer.reducer().snapshot(usize::MAX, &mut None)?.0;
    assert_eq!(snapshot.events.len(), 2);
    assert_eq!(writer.read_event_at(1, &mut None).await?.0.revision, 1);
    let mut reused = original.clone();
    reused.expected_revision = writer.reducer().revision();
    assert!(matches!(
        writer.reducer().plan(&reused),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        writer
            .execute(
                original.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await?,
        ApplyResult::Replayed { .. }
    ));
    let mut conflicting = original.clone();
    conflicting.causal_parent = Some(EventReference {
        authority: authority(),
        revision: 1,
    });
    assert!(matches!(
        writer
            .execute(
                conflicting,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let mut restored = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None)?,
        schemas(),
        Some(snapshot),
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(Arc::new(TestContent));
    restored.set_resident_event_limit(2)?;
    assert_eq!(restored.reducer().archived_through_revision(), 3);
    let ApplyResult::Replayed { event } = restored
        .execute(
            original,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?
    else {
        return Err(Error::Invalid("cold retry was not replayed".into()));
    };
    let ApplyResult::Applied {
        event: original_event,
    } = first
    else {
        return Err(Error::Invalid("fixture first event was not applied".into()));
    };
    assert_eq!(event, original_event);
    assert_eq!(restored.reducer().revision(), 5);
    let mut next = command(6)?;
    next.expected_revision = 5;
    restored
        .execute(
            next,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert_eq!(restored.reducer().archived_through_revision(), 4);
    assert_eq!(
        restored
            .reducer()
            .snapshot(usize::MAX, &mut None)?
            .0
            .events
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn cold_history_reader_bounds_ten_thousand_events_without_reducer_restore() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    for identity in 1_u64..=10_000 {
        let mut next = command(1)?;
        let mut bytes = [0; 16];
        bytes
            .get_mut(..8)
            .ok_or_else(|| Error::Invalid("fixture identity missing".into()))?
            .copy_from_slice(&identity.to_le_bytes());
        next.operation_id = OperationId::from_bytes(bytes);
        next.idempotency_key = IdempotencyKey::new(format!("history-{identity}"))?;
        next.expected_revision = writer.reducer().revision();
        writer
            .execute(
                next,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let last = writer.read_event_at(10_000, &mut None).await?.0;
    let mut later = command(2)?;
    later.expected_revision = writer.reducer().revision();

    provider.forbid_writes.store(true, Ordering::SeqCst);
    let reader = HistoryReader::new(
        &client,
        &authority(),
        &(issuer().verifier(&mut None)?),
        &mut None,
    )?;
    let cursor = reader.pin(9_997, None).await?;

    provider.forbid_writes.store(false, Ordering::SeqCst);
    writer
        .execute(
            later,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let limits = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: 131_072,
    };
    let first = reader.read_page(&cursor, limits, None).await?;
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.cursor.after_revision, 9_999);
    let second = reader.read_page(&first.cursor, limits, None).await?;
    assert_eq!(second.events, vec![last.clone()]);
    assert_eq!(second.cursor.after_revision, 10_000);
    assert!(
        reader
            .read_page(&second.cursor, limits, None)
            .await?
            .events
            .is_empty()
    );

    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
    assert_eq!(cursor.after_revision, 9_997);
    let encoded =
        serde_json::to_vec(&first.cursor).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        serde_json::from_slice::<HistoryCursor>(&encoded)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        first.cursor
    );
    for fault in 1..=6 {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(reader.read_page(&cursor, limits, None).await.is_err());
        assert_eq!(reader.read_page(&cursor, limits, None).await?, first);
    }
    assert!(
        reader
            .read_page(
                &cursor,
                HistoryReadLimits {
                    maximum_bytes: 1,
                    ..limits
                },
                None
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .read_page(
                &cursor,
                HistoryReadLimits {
                    maximum_events: 0,
                    ..limits
                },
                None
            )
            .await
            .is_err()
    );
    let mut invalid = cursor.clone();
    invalid.through_revision = 10_002;
    assert!(reader.read_page(&invalid, limits, None).await.is_err());
    invalid = cursor;
    invalid.authority.id = "other".into();
    assert!(matches!(
        reader.read_page(&invalid, limits, None).await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(
        reader
            .operation_event(
                last.operation_id,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None
            )
            .await?
            .0,
        Some(last)
    );
    assert!(
        reader
            .operation_event(
                OperationId::new(),
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None
            )
            .await?
            .0
            .is_none()
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
        operations::publication_path(
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
async fn history_reader_rejects_unattested_events_and_wrong_authority() -> Result<()> {
    for wrong_authority in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None)?, schemas());
        let ApplyResult::Applied { mut event } = reducer.apply(command(1)?)? else {
            return Err(Error::Invalid("fixture needs fresh event".into()));
        };
        let mut wire_authority = authority();
        if wrong_authority {
            wire_authority.id = "other".into();
        } else {
            event.intent_digest = [0; 32];
        }
        acyclic_stream::Stream::new(Arc::clone(&client), authority().stream_path(&mut None)?)?
            .append_batch(
                vec![OwnedBytes::from(encode_event(
                    &wire_authority,
                    &event,
                    &mut None,
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
        let cursor = reader.pin(0, None).await?;
        assert!(
            reader
                .read_page(
                    &cursor,
                    HistoryReadLimits {
                        maximum_events: 1,
                        maximum_bytes: 65_536,
                    },
                    None
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn incremental_refresh_pins_tail_and_advances_only_bounded_pages() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut reader = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None)?,
        schemas(),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let mut writer = with_content(&client).await?;
    for identity in 1..=5 {
        let mut next = command(identity)?;
        next.expected_revision = writer.reducer().revision();
        writer
            .execute(
                next,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let through = reader.tail_revision().await?;
    assert_eq!(through, 5);
    let mut later = command(6)?;
    later.expected_revision = writer.reducer().revision();
    writer
        .execute(
            later,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(reader.refresh_through(through, 0).await.is_err());
    assert!(!reader.refresh_through(through, 2).await?);
    assert_eq!(reader.reducer().revision(), 2);
    assert!(!reader.refresh_through(through, 2).await?);
    assert_eq!(reader.reducer().revision(), 4);
    assert!(reader.refresh_through(through, 2).await?);
    assert_eq!(reader.reducer().revision(), 5);
    assert!(reader.refresh_through(through, 2).await?);
    assert!(reader.refresh_through(4, 2).await.is_err());
    assert!(reader.refresh_through(7, 2).await.is_err());
    assert!(reader.refresh_through(6, 2).await?);
    assert_eq!(reader.reducer().revision(), 6);
    let reopened = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None)?,
        schemas(),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(
        reader.reducer().events_after(0, 6)?,
        reopened.reducer().events_after(0, 6)?
    );
    Ok(())
}

#[tokio::test]
async fn verifier_must_match_the_exact_aggregate_audience() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let foreign = AuthorityIssuer::new(
        "test",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: authority().id,
        },
    );
    assert!(matches!(
        StreamAggregate::open(
            &client,
            authority(),
            foreign.verifier(&mut None)?,
            schemas(),
            None,
            default_projection_read_limits(),
            DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None
        )
        .await,
        Err(Error::Unauthorized(_))
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

#[tokio::test]
async fn canonical_paths_bind_aggregate_authority() -> Result<()> {
    assert_eq!(
        authority().stream_path(&mut None)?,
        "harness/v2/conversations/conversation-1"
    );
    assert_ne!(
        authority().stream_path(&mut None)?,
        Authority {
            kind: AggregateKind::Task,
            id: "conversation-1".into(),
        }
        .stream_path(&mut None)?
    );
    assert!(
        Authority {
            kind: AggregateKind::Task,
            id: "../escape".into(),
        }
        .stream_path(&mut None)
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_reopens_with_full_stream_history() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let client = provider;
    let mut aggregate = with_content(&client).await?;
    aggregate
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let mut second = command(2)?;
    second.expected_revision = 1;
    aggregate
        .execute(
            second,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let snapshot = aggregate.reducer().snapshot(usize::MAX, &mut None)?.0;
    let mut third = command(3)?;
    third.expected_revision = 2;
    aggregate
        .execute(
            third,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let history =
        acyclic_stream::Stream::new(Arc::clone(&client), authority().stream_path(&mut None)?)?;
    assert_eq!(history.bounds().await?.tail, 3);
    assert_eq!(
        history
            .read(0, 3)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        3
    );

    let reopened = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None)?,
        schemas(),
        Some(snapshot),
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(reopened.reducer().revision(), 3);
    Ok(())
}
