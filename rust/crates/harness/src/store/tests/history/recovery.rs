use super::*;

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
