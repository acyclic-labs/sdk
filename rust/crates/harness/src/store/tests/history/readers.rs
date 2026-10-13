use super::*;

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
