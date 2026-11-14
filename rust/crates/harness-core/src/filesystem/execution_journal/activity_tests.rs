use super::*;
use acyclic_stream::StreamProviderExt as _;

#[tokio::test]
async fn latest_activity_reads_one_authenticated_cut_independent_of_history() -> Result<()> {
    let stream = Arc::new(acyclic_stream::testing::Counted::new(
        acyclic_stream::MemoryStream::default(),
    ));
    let ledger = stream.ledger();
    let (journal, _issuer) =
        test_support::journal_with_backends(stream, acyclic_fs::Fs::memory()).await?;
    let operation = OperationId::new();
    let journal = journal.with_turn(operation, operation);
    let (clock, _) = acyclic_actors::local::ControlledClock::new(0);
    assert_eq!(journal.activity(operation, &clock).await?, None);
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let projection = journal
        .stage(operation, "context", b"{}", "application/json", None)?
        .await?;
    for step in 0..32_u32 {
        journal
            .append(
                operation,
                &format!("context:{step}"),
                &ExecutionEvent::ContextPrepared {
                    step,
                    projection: projection.clone(),
                    steering: None,
                    configuration: None,
                },
                None,
            )?
            .await?;
        if ![0, 31].contains(&step) {
            continue;
        }
        let original = journal
            .replay(operation, u64::from(step) + 1, 1, None)?
            .await?
            .pop()
            .ok_or_else(|| Error::Storage("fixture observation is absent".into()))?;
        let at = original
            .committed_at_micros
            .ok_or_else(|| Error::Storage("fixture timestamp is absent".into()))?;
        let (clock, _) = acyclic_actors::local::ControlledClock::new(at / 1000 + 73);
        // A cold binding cannot hide a history scan behind a warm cache.
        let cold = FilesystemExecutionJournal::new(
            Arc::clone(&journal.stream),
            Arc::clone(&journal.host),
            crate::filesystem::FilesystemJournalBinding {
                volume: &journal.volume,
                verifier: &journal.verifier,
                scope: &journal.scope,
                io: None,
            },
        )?
        .with_turn(operation, operation);
        let sample = ledger.start_sample();
        let activity = cold
            .activity(operation, &clock)
            .await?
            .ok_or_else(|| Error::Storage("fixture activity is absent".into()))?;
        let requests = sample.finish().requests;
        assert_eq!(
            requests.data(),
            2,
            "only checkpoint and atomic anchor are read"
        );
        assert_eq!(requests.control(), 1, "activity repeated tail discovery");
        assert_eq!(activity.sequence, original.sequence);
        assert_eq!(activity.step, Some(step));
        assert_eq!(activity.committed_at_micros, Some(at));
        assert_eq!(activity.age_ms, Some(73));
        let sample = ledger.start_sample();
        assert!(matches!(
            cold.activity(OperationId::new(), &clock).await,
            Err(Error::Unauthorized(_))
        ));
        let refused = sample.finish().requests;
        assert_eq!(refused.data(), 0, "foreign operation read data");
        assert_eq!(refused.control(), 0, "foreign operation reached storage");
    }
    // An unattested raw suffix is not progress from a published execution cut.
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: crate::distributed::execution_path(operation, &mut None)?,
            records: vec![
                acyclic_stream::OwnedBytes::from_static(b"not a published observation").into(),
            ],
            if_tail: Some(33),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    let activity = journal
        .activity(operation, &clock)
        .await?
        .ok_or_else(|| Error::Storage("authenticated activity disappeared".into()))?;
    assert_eq!(activity.sequence, 33);
    assert_eq!(activity.step, Some(31));
    Ok(())
}
