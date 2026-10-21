use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn empty_history_cursor_stays_terminal_after_first_publication() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = with_content(&client).await?;
    let reader = writer.history_reader()?;
    let cursor = reader.pin(0, None).await?;
    let limits = HistoryReadLimits {
        maximum_events: 1,
        maximum_bytes: 65_536,
    };
    assert!(
        reader
            .read_page(&cursor, limits, None)
            .await?
            .events
            .is_empty()
    );
    assert!(matches!(reader.pin(1, None).await, Err(Error::Invalid(_))));
    writer
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(
        reader
            .read_page(&cursor, limits, None)
            .await?
            .events
            .is_empty()
    );
    let current = reader.pin(0, None).await?;
    assert_eq!(
        reader.read_page(&current, limits, None).await?.events.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn cold_history_reader_bounds_pages_across_checkpoints_without_reducer_restore() -> Result<()> {
    const EVENTS: u64 = 2 * DEFAULT_PROJECTION_EVENTS as u64 + 2;
    let client = projection_fixture(EVENTS).await?;
    let mut writer = with_content(&client).await?;
    let last = writer.read_event_at(EVENTS, &mut None).await?.0;
    let reader = writer.history_reader()?;
    let cursor = reader.pin(EVENTS - 3, None).await?;
    client.forbid_writes.store(false, Ordering::SeqCst);
    writer
        .execute(
            projection_command(u128::from(EVENTS + 1), EVENTS)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    client.forbid_writes.store(true, Ordering::SeqCst);
    let limits = HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: 131_072,
    };
    client.reset_io();
    client.observation_maximum.store(0, Ordering::SeqCst);
    let first = reader.read_page(&cursor, limits, None).await?;
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.cursor.after_revision, EVENTS - 1);
    let work = client.io_snapshot();
    let page_work = (
        work.reads,
        work.tails,
        client.observation_maximum.load(Ordering::SeqCst),
    );
    assert_eq!(page_work, (1, 1, 2));
    let second = reader.read_page(&first.cursor, limits, None).await?;
    assert_eq!(second.events, vec![last.clone()]);
    assert!(
        reader
            .read_page(&second.cursor, limits, None)
            .await?
            .events
            .is_empty()
    );
    client.history_read_fault.store(3, Ordering::SeqCst);
    assert!(reader.read_page(&cursor, limits, None).await.is_err());
    assert_eq!(reader.read_page(&cursor, limits, None).await?, first);
    // A scan of the canonical prefix returns the same suffix, but must fail
    // the same exact page-work detector even with this smaller fixture.
    let mut full = cursor.clone();
    full.after_revision = 0;
    client.reset_io();
    client.observation_maximum.store(0, Ordering::SeqCst);
    let scanned = reader
        .read_page(
            &full,
            HistoryReadLimits {
                maximum_events: EVENTS as u32,
                maximum_bytes: u64::MAX,
            },
            None,
        )
        .await?;
    assert_eq!(scanned.events.len(), EVENTS as usize);
    assert_eq!(scanned.events.last(), Some(&last));
    assert_eq!(
        &scanned.events[EVENTS as usize - 3..EVENTS as usize - 1],
        first.events
    );
    let work = client.io_snapshot();
    assert_ne!(
        (
            work.reads,
            work.tails,
            client.observation_maximum.load(Ordering::SeqCst),
        ),
        page_work,
        "full-prefix scan evaded the exact page-work detector"
    );
    for bounds in [
        HistoryReadLimits {
            maximum_bytes: 1,
            ..limits
        },
        HistoryReadLimits {
            maximum_events: 0,
            ..limits
        },
    ] {
        assert!(reader.read_page(&cursor, bounds, None).await.is_err());
    }
    let mut invalid = cursor.clone();
    invalid.through_revision = EVENTS + 2;
    assert!(reader.read_page(&invalid, limits, None).await.is_err());
    invalid = cursor;
    invalid.authority.id = "other".into();
    assert!(matches!(
        reader.read_page(&invalid, limits, None).await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn history_reader_rejects_unattested_events_and_wrong_authority() -> Result<()> {
    for wrong_authority in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None)?, schemas());
        let ApplyResult::Applied { mut event } = reducer.apply(&command(1)?)? else {
            panic!("fresh event");
        };
        let mut wire_authority = authority();
        if wrong_authority {
            wire_authority.id = "other".into();
        } else {
            event.intent_digest = [0; 32];
        }
        acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
            .append(encode_event(&wire_authority, &event, &mut None)?)
            .await?;
        let reader = HistoryReader::new(
            &client,
            &authority(),
            &issuer().verifier(&mut None)?,
            &mut None,
        )?;
        let cursor = reader.pin(0, None).await?;
        assert!(
            reader
                .read_page(
                    &cursor,
                    HistoryReadLimits {
                        maximum_events: 1,
                        maximum_bytes: 65_536
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
    let mut reader = with_content(&client).await?;
    let mut writer = with_content(&client).await?;
    for revision in 0..5 {
        writer
            .execute(
                projection_command(u128::from(revision) + 1, revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let through = reader.tail_revision().await?;
    writer
        .execute(
            projection_command(6, 5)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(reader.refresh_through(through, 0).await.is_err());
    for revision in [2, 4, 5] {
        assert_eq!(reader.refresh_through(through, 2).await?, revision == 5);
        assert_eq!(reader.reducer().revision(), revision);
    }
    assert!(reader.refresh_through(4, 2).await.is_err());
    assert!(reader.refresh_through(7, 2).await.is_err());
    assert!(reader.refresh_through(6, 2).await?);
    let reopened = with_content(&client).await?;
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
            (authority(), foreign.verifier(&mut None)?, schemas()),
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
