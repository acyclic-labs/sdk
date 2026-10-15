use super::*;

#[tokio::test]
async fn exact_historical_open_keeps_admitted_origin_after_later_parent_appends() -> Result<()> {
    let provider = projection_fixture(69).await?;
    let limits = default_projection_read_limits();
    let cancellation = CancellationToken::new();
    let open =
        |cut, limits| open_history(&provider, authority(), cut, limits, budget(), &cancellation);
    let original = open(Some(65), limits)
        .await
        .map_err(|failure| failure.error)?
        .value
        .reducer()
        .snapshot(usize::MAX, &mut None)?
        .0;
    provider.forbid_writes.store(false, Ordering::SeqCst);
    let mut writer = with_content(&provider).await?;
    for revision in 69..134 {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider
        .stale_projection_head_tail
        .store(2, Ordering::SeqCst);
    let admitted = open(Some(65), limits)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        admitted.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        original
    );
    provider
        .stale_projection_head_tail
        .store(0, Ordering::SeqCst);
    for cut in [0, 63, 64, 127, 128, 134] {
        let historical = open(Some(cut), limits)
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(historical.value.reducer().revision(), cut);
    }
    let future = open(Some(135), limits)
        .await
        .err()
        .expect("uncommitted cut");
    assert_eq!(future.work.output_bytes, 0);
    let narrow = open(
        Some(65),
        HistoryReadLimits {
            maximum_events: 1,
            ..limits
        },
    )
    .await
    .err()
    .expect("bounded suffix");
    assert_eq!(narrow.work.output_bytes, 0);
    provider.history_read_fault.store(3, Ordering::SeqCst);
    let failed = open(Some(65), limits)
        .await
        .err()
        .expect("unavailable checkpoint");
    assert_eq!(failed.work.output_bytes, 0);
    let restored = open(Some(65), limits)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        restored.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        original
    );
    let latest = open(None, limits).await.map_err(|failure| failure.error)?;
    let exact = open(Some(134), limits)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        exact.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        latest.value.reducer().snapshot(usize::MAX, &mut None)?.0
    );
    Ok(())
}
