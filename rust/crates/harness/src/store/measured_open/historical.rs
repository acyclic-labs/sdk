use super::*;

#[tokio::test]
async fn exact_historical_open_keeps_admitted_origin_after_later_parent_appends() -> Result<()> {
    let provider = fixture().await?;
    let limits = default_projection_read_limits();
    let original = StreamAggregate::open(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        limits,
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        Some(65),
        &mut None,
    )
    .await?
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
    let cancellation = CancellationToken::new();
    provider.reset_io();
    let admitted = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        Some(65),
        limits,
        budget(),
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    assert_eq!(
        admitted.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        original
    );
    assert_eq!(admitted.value.reducer().revision(), 65);

    assert_eq!(
        admitted.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert_eq!(provider.io_snapshot().writes, 0);

    provider
        .stale_projection_head_tail
        .store(0, Ordering::SeqCst);
    for cut in [0_u64, 63, 64, 127, 128, 134] {
        provider.reset_io();
        let historical = open_history(
            &provider,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            Some(cut),
            limits,
            budget(),
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?;
        assert_eq!(historical.value.reducer().revision(), cut);

        let io = provider.io_snapshot();

        assert_eq!(historical.work.authority_bytes_read, io.read_bytes);
        assert_eq!(
            io.writes + io.inspections + io.commit_reads + io.other_calls,
            0
        );
    }
    provider.reset_io();
    let future = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        Some(135),
        limits,
        budget(),
        &cancellation,
    )
    .await
    .err()
    .expect("uncommitted original revision cannot use latest projection");
    assert_eq!(future.work.authority_records_read, 0);
    assert_eq!(future.work.output_bytes, 0);
    assert_eq!(provider.io_snapshot().reads, 0);
    provider.reset_io();
    let narrow = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        Some(65),
        HistoryReadLimits {
            maximum_events: 1,
            ..limits
        },
        budget(),
        &cancellation,
    )
    .await
    .err()
    .expect("exact historical suffix must share original finite event allowance");
    assert_eq!(narrow.work.output_bytes, 0);
    for fault in [3, 4] {
        provider.reset_io();
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        let failed = open_history(
            &provider,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            Some(65),
            limits,
            budget(),
            &cancellation,
        )
        .await
        .err()
        .expect(
            "unavailable/tampered original checkpoint cannot fall forward to latest checkpoint",
        );
        assert_eq!(failed.work.output_bytes, 0);
        assert_eq!(failed.work.items_returned, 0);
        assert_eq!(
            StreamAggregate::open(
                &provider,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                limits,
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                Some(65),
                &mut None
            )
            .await?
            .reducer()
            .snapshot(usize::MAX, &mut None)?
            .0,
            original
        );
    }
    let exact = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        Some(134),
        limits,
        budget(),
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    let latest = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    assert_eq!(
        exact.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        latest.value.reducer().snapshot(usize::MAX, &mut None)?.0
    );
    assert_eq!(exact.work, latest.work);
    Ok(())
}
