use super::*;

#[tokio::test]
async fn native_append_lost_ack_reopens_exact_append_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let request = keyed_append("native-append-lost-ack", 0)?;
    let key = request
        .idempotency_key
        .clone()
        .ok_or("append fixture lacks a key")?;
    let meter = Arc::new(Meter::local());
    meter.cancel_on_write.store(true, Ordering::SeqCst);
    let error = provider
        .native_append(request.clone(), meter.original())
        .await
        .err()
        .ok_or("accepted cancelled append fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    let accepted = provider
        .inspect_idempotency(key.clone())
        .await?
        .ok_or("accepted append identity disappeared")?;
    let crate::IdempotencyOutcome::Append(outcome) = &accepted.outcome else {
        return Err("native append was rewritten into the Commit family".into());
    };
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(
        reopened.inspect_idempotency(key.clone()).await?,
        Some(accepted.clone())
    );
    assert_eq!(reopened.append(request.clone()).await?, *outcome);
    assert_eq!(reopened.tail(request.path.clone()).await?, 1);
    // Reusing the same bytes/key through Commit is a different operation.
    let wrong_family = CommitRequest {
        conditions: vec![crate::CommitCondition::Tail {
            path: request.path.clone(),
            expected: 1,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: request.path,
            records: request.records,
        }],
        idempotency_key: key,
    };
    meter.cancelled.store(false, Ordering::SeqCst);
    let failure = reopened
        .native_commit(wrong_family, meter.original())
        .await?
        .err()
        .ok_or("Commit incorrectly reused an Append identity")?;
    assert_eq!(&failure, &StreamError::IdempotencyMismatch);
    drop(failure);
    drop(reopened);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_append_keeps_keyed_conflict_and_does_not_journal_unkeyed_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let path = StreamPath::new("native-append-conflict")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"seed")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let request = keyed_append(path.as_str(), 0)?;
    let key = request
        .idempotency_key
        .clone()
        .ok_or("append fixture lacks a key")?;
    let meter = Arc::new(Meter::local());
    let expected = AppendOutcome::TailConflict { actual_tail: 1 };
    let conflict = provider
        .native_append(request.clone(), meter.original())
        .await??;
    assert_eq!(conflict.value(), &expected);
    drop(conflict);
    let after_keyed = std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len();
    let mut unkeyed = request.clone();
    unkeyed.idempotency_key = None;
    let conflict = provider.native_append(unkeyed, meter.original()).await??;
    assert_eq!(conflict.value(), &expected);
    drop(conflict);
    assert_eq!(
        std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(),
        after_keyed
    );
    let accepted = provider
        .inspect_idempotency(key.clone())
        .await?
        .ok_or("keyed conflict lost its original identity")?;
    assert_eq!(
        accepted.outcome,
        crate::IdempotencyOutcome::Append(expected.clone())
    );
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
    assert_eq!(reopened.append(request).await?, expected);
    assert_eq!(reopened.tail(path).await?, 1);
    drop(reopened);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_commit_before_checks_actual_cas_and_preserves_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicU64::default());
    clock.store(99, Ordering::SeqCst);
    let provider = LocalStream::open_with_clock(
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    let request = keyed_commit("native-expired", 0)?;
    let journal_before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
    let meter = Arc::new(Meter::local());
    *meter
        .expire_after_hash
        .lock()
        .map_err(|_| "native clock seam was poisoned")? = Some((clock.clone(), 100));
    let expired = provider
        .native_commit_before(request.clone(), 100, meter.original())
        .await?;
    assert!(matches!(
        expired.as_ref(),
        Err(StreamError::DeadlineElapsed)
    ));
    assert_eq!(clock.load(Ordering::SeqCst), 100);
    assert_eq!(
        std::fs::read(directory.path().join(JOURNAL_FILE))?,
        journal_before
    );
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    drop(expired);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    let accepted = provider.commit(request.clone()).await?;
    let replay = provider
        .native_commit_before(request, 0, meter.original())
        .await??;
    assert_eq!(replay.value(), &accepted);
    drop(replay);
    assert_eq!(provider.tail(StreamPath::new("native-expired")?).await?, 1);
    meter.assert_refunded();
    Ok(())
}
