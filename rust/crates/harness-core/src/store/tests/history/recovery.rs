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
    let mut restored = StreamAggregate::open(
        &client,
        (authority(), issuer().verifier(&mut None)?, schemas()),
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
        panic!("fresh fixture event");
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
