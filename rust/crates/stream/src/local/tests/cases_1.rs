#[tokio::test]
async fn cancelled_local_read_records_error_and_closes_origin_parent()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let publication = provider.inner.publication.write().await;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let future = tracing::subscriber::with_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
        || {
            let parent = obs::OperationSpan::new(tracing::info_span!("origin_parent"));
            parent.scope(async {
                provider
                    .read(ReadRequest {
                        key: None,
                        path: StreamPath::new("pending")?,
                        from: 0,
                        limit: 1,
                        at_least: Default::default(),
                    })
                    .await
            })
        },
    );
    let mut future = Box::pin(future);
    assert!(futures::poll!(future.as_mut()).is_pending());
    let other = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(other.clone()),
    );
    let parent = tracing::info_span!("foreign_parent");
    let _entered = parent.enter();
    drop(future);
    assert_eq!(
        captured
            .fields("acyclic.stream.read")
            .get("error.kind")
            .ok_or("missing span field")?,
        "cancelled"
    );
    assert_eq!(captured.closed("acyclic.stream.read"), 1);
    assert_eq!(captured.closed("origin_parent"), 1);
    assert_eq!(other.closed("foreign_parent"), 0);
    drop(publication);
    Ok(())
}

#[tokio::test]
async fn local_append_emits_its_span() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let capture = captured.clone();
    let _default =
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(capture));
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider
        .append(AppendRequest {
            path: StreamPath::new("traced/path")?,
            records: vec![
                (Bytes::from_static(b"body")).into(),
                (Bytes::from_static(b"more")).into(),
            ],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let append = captured.fields("acyclic.stream.append");
    for (field, value) in [
        ("items", "2"),
        ("bytes", "8"),
        ("rev", "2"),
        ("outcome", "ok"),
    ] {
        assert_eq!(append.get(field).ok_or("missing span field")?, value);
    }
    assert_eq!(
        captured
            .fields("acyclic.stream.open")
            .get("outcome")
            .ok_or("missing span field")?,
        "ok"
    );
    let evidence = captured.0.lock().unwrap();
    assert!(evidence.events.iter().any(|(_, fields)| {
        fields
            .iter()
            .any(|(_, field, value)| field == "fsyncs" && value == "1")
    }));
    for span in &evidence.spans {
        for field in &span.declared_fields {
            assert!(!["path", "token", "content", "body", "authorization"].contains(field));
        }
        for value in span.fields.values() {
            assert!(!value.contains("traced"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_provider_reopens_and_passes_public_conformance()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    conformance::verify(&crate::conformance::Fixture::new(provider.clone()))
        .await
        .map_err(std::io::Error::other)?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(
        reopened
            .tail(StreamPath::new("conformance/source")?)
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn deadline_is_evaluated_once_and_only_accepted_commands_are_replayed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(TestClock::default());
    clock.0.store(10, Ordering::SeqCst);
    let provider = LocalStream::open_with_clock(
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    let accepted = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![crate::CommitCondition::Absent {
            path: StreamPath::new("deadline/accepted")?,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: StreamPath::new("deadline/accepted")?,
            records: vec![(Bytes::from_static(b"accepted")).into()],
        }],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(b"accepted"))?,
    };
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: Some(11),
                ..accepted
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    let expired = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![crate::CommitCondition::Absent {
            path: StreamPath::new("deadline/expired")?,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: StreamPath::new("deadline/expired")?,
            records: vec![(Bytes::from_static(b"expired")).into()],
        }],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(b"expired"))?,
    };
    assert_eq!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: Some(10),
                ..expired
            })
            .await,
        Err(StreamError::DeadlineElapsed)
    );
    drop(provider);

    clock.0.store(100, Ordering::SeqCst);
    let reopened =
        LocalStream::open_with_clock(directory.path(), LocalStreamLimits::default(), clock).await?;
    assert_eq!(
        reopened.tail(StreamPath::new("deadline/accepted")?).await?,
        1
    );
    assert_eq!(
        reopened.tail(StreamPath::new("deadline/expired")?).await,
        Err(StreamError::NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn emitted_history_drains_replay_and_refuses_repair_after_semantic_failure()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::TryStreamExt as _;

    async fn history(
        provider: &LocalStream,
        path: &StreamPath,
        count: u32,
    ) -> Result<Vec<crate::Record>, StreamError> {
        provider
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit: count,
                key: None,
                at_least: Default::default(),
            })
            .await?
            .try_collect()
            .await
    }

    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::AlreadyOpen)
    ));
    let paths = ["source", "branch", "branch/child"]
        .map(StreamPath::new)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let journal_path = directory.path().join(JOURNAL_FILE);
    let header = std::fs::read(&journal_path)?;
    provider.append(keyed_append("source", 0)?).await?;
    let seed_end = usize::try_from(std::fs::metadata(&journal_path)?.len())?;
    for pair in paths.windows(2) {
        provider
            .fork(ForkRequest {
                source: pair[0].clone(),
                destination: pair[1].clone(),
                at_tail: Some(1),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
    }
    let count = u32::try_from(REPLAY_PIPELINE_COMMANDS * 4)?;
    for index in 1..u64::from(count) {
        provider
            .append(AppendRequest {
                path: paths[2].clone(),
                records: vec![(Bytes::copy_from_slice(&index.to_le_bytes())).into()],
                if_tail: Some(index),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
    }
    let mut expected = Vec::new();
    for path in &paths {
        expected.push(history(&provider, path, count).await?);
    }
    let prefix = std::fs::read(&journal_path)?;
    provider
        .append(keyed_append("branch/child", u64::from(count))?)
        .await?;
    let mut torn = std::fs::read(&journal_path)?;
    torn.pop().ok_or("emitted journal is empty")?;
    drop(provider);

    // Remove the emitted seed command, keeping the original header, forks and
    // later commands. The first fork now has no source. Its refusal must drain
    // the bounded channel without authorizing repair of the torn final frame.
    let mut missing_seed = header;
    missing_seed.extend_from_slice(&torn[seed_end..]);
    std::fs::write(&journal_path, &missing_seed)?;
    for _ in 0..2 {
        assert!(matches!(
            LocalStream::open(directory.path(), limits).await,
            Err(LocalStreamError::Replay(StreamError::NotFound))
        ));
        assert_eq!(
            std::fs::read(&journal_path)?,
            missing_seed,
            "refused replay repaired emitted journal"
        );
    }
    std::fs::write(&journal_path, torn)?;
    let reopened = LocalStream::open(directory.path(), limits).await?;
    for (path, expected) in paths.iter().zip(expected) {
        assert_eq!(
            history(&reopened, path, count + 1).await?,
            expected,
            "recovered pipeline history changed"
        );
    }
    drop(reopened);
    assert_eq!(std::fs::read(&journal_path)?, prefix);
    Ok(())
}

#[tokio::test]
async fn compaction_targets_never_refuse_an_atomic_append() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 1,
        compaction_bytes: u64::try_from(HEADER_BYTES)?,
        ..LocalStreamLimits::default()
    };
    let request = AppendRequest {
        path: StreamPath::new("capacity")?,
        records: vec![(Bytes::from_static(b"larger-than-the-compaction-target")).into()],
        if_tail: Some(0),
        idempotency_key: crate::IdempotencyKey::unique(),
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let original = provider.append(request.clone()).await?;
    assert_eq!(provider.tail(request.path.clone()).await?, 1);
    drop(provider);
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(provider.tail(request.path.clone()).await?, 1);
    assert_eq!(provider.append(request).await?, original);
    Ok(())
}

/// A damaged frame with an intact one after it was committed, so the
#[tokio::test]
async fn a_damaged_frame_before_an_intact_one_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let journal = directory.path().join("stream.journal");
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider.append(keyed_append("first", 0)?).await?;
    provider.append(keyed_append("second", 0)?).await?;
    drop(provider);

    let mut file = OpenOptions::new().read(true).write(true).open(&journal)?;
    let body_offset = u64::try_from(HEADER_BYTES + 5).map_err(std::io::Error::other)?;
    // The byte is part of the first frame's commit time, so any fixed value
    // may already be there; its complement never is.
    let mut byte = [0];
    file.seek(SeekFrom::Start(body_offset))?;
    std::io::Read::read_exact(&mut file, &mut byte)?;
    file.seek(SeekFrom::Start(body_offset))?;
    file.write_all(&[!byte[0]])?;
    file.sync_all()?;
    drop(file);
    let damaged = std::fs::read(&journal)?;

    assert!(matches!(
        LocalStream::open(directory.path(), LocalStreamLimits::default()).await,
        Err(LocalStreamError::Corrupt)
    ));
    assert_eq!(
        std::fs::read(&journal)?,
        damaged,
        "recovery left it untouched"
    );
    Ok(())
}

/// Power loss can leave the last append whole in length but zero-filled
#[tokio::test]
async fn a_zero_filled_or_garbage_last_frame_is_a_torn_tail()
-> Result<(), Box<dyn std::error::Error>> {
    for fill in [0x00_u8, 0x5a] {
        let directory = tempfile::tempdir()?;
        let journal = directory.path().join("stream.journal");
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        provider.append(keyed_append("kept", 0)?).await?;
        let prefix = std::fs::read(&journal)?;
        let kept = u64::try_from(prefix.len())?;
        provider.append(keyed_append("torn", 0)?).await?;
        let torn = std::fs::metadata(&journal)?.len();
        drop(provider);

        let mut file = OpenOptions::new().read(true).write(true).open(&journal)?;
        file.seek(SeekFrom::Start(kept))?;
        file.write_all(&vec![fill; usize::try_from(torn - kept)?])?;
        file.sync_all()?;
        drop(file);

        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(
            reopened.tail(StreamPath::new("kept")?).await?,
            1,
            "{fill:#x}"
        );
        assert_eq!(
            reopened.tail(StreamPath::new("torn")?).await,
            Err(StreamError::NotFound),
            "torn final frame must be absent: {fill:#x}"
        );
        drop(reopened);
        assert_eq!(
            std::fs::read(&journal)?,
            prefix,
            "repaired prefix changed: {fill:#x}"
        );
    }
    Ok(())
}

fn compacting_limits() -> LocalStreamLimits {
    LocalStreamLimits {
        compaction_operations: 8,
        ..LocalStreamLimits::default()
    }
}

async fn append_keyed(
    provider: &LocalStream,
    index: u64,
) -> Result<AppendOutcome, Box<dyn std::error::Error>> {
    Ok(provider
        .append(AppendRequest {
            path: StreamPath::new("compacted")?,
            records: vec![(Bytes::copy_from_slice(&index.to_le_bytes())).into()],
            if_tail: Some(index),
            idempotency_key: IdempotencyKey::new(Bytes::copy_from_slice(&index.to_le_bytes()))?,
        })
        .await?)
}

/// A journal half full becomes a snapshot and starts again; reopening
#[tokio::test]
async fn a_half_full_journal_is_compacted_into_a_snapshot() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let limits = compacting_limits();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let mut outcomes = Vec::new();
    for index in 0..21 {
        outcomes.push(append_keyed(&provider, index).await?);
    }
    let journal = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal poisoned")?
        .operations;
    assert!(journal < 4, "the journal restarted: {journal} frames");
    drop(provider);
    assert!(directory.path().join(SNAPSHOT_FILE).exists());

    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(StreamPath::new("compacted")?).await?, 21);
    let history = reopened
        .read(ReadRequest {
            key: None,
            path: StreamPath::new("compacted")?,
            from: 0,
            limit: 21,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(history.len(), 21);
    for (index, record) in (0_u64..21).zip(&history) {
        assert_eq!(record.sequence, index);
        assert_eq!(record.value.as_ref(), index.to_le_bytes());
    }
    reopened
        .fork(ForkRequest {
            exclude: Vec::new(),
            source: StreamPath::new("compacted")?,
            destination: StreamPath::new("compacted-fork")?,
            at_tail: Some(21),
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let fork_history = reopened
        .read(ReadRequest {
            key: None,
            path: StreamPath::new("compacted-fork")?,
            from: 0,
            limit: 21,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(fork_history, history);
    for (index, outcome) in (0..).zip(&outcomes) {
        assert_eq!(&append_keyed(&reopened, index).await?, outcome, "{index}");
    }
    conformance::verify(&crate::conformance::Fixture::new(reopened.clone()))
        .await
        .map_err(std::io::Error::other)?;
    Ok(())
}

/// A crash after the snapshot landed but before the journal restarted
#[tokio::test]
async fn a_journal_the_snapshot_already_holds_is_discarded()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = compacting_limits();
    let journal = directory.path().join(JOURNAL_FILE);
    let provider = LocalStream::open(directory.path(), limits).await?;
    for index in 0..3 {
        append_keyed(&provider, index).await?;
    }
    let before = std::fs::read(&journal)?;
    append_keyed(&provider, 3).await?;
    drop(provider);
    assert!(std::fs::metadata(&journal)?.len() < u64::try_from(before.len())?);
    std::fs::write(&journal, &before)?;
    std::fs::write(directory.path().join(SNAPSHOT_TEMPORARY), b"torn")?;

    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(StreamPath::new("compacted")?).await?, 4);
    drop(reopened);
    assert_eq!(
        std::fs::read(directory.path().join(SNAPSHOT_TEMPORARY))?,
        b"torn"
    );
    assert_eq!(
        std::fs::metadata(&journal)?.len(),
        u64::try_from(HEADER_BYTES)?
    );
    Ok(())
}

/// A damaged snapshot fails closed.
#[tokio::test]
async fn a_damaged_snapshot_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = compacting_limits();
    let provider = LocalStream::open(directory.path(), limits).await?;
    for index in 0..4 {
        append_keyed(&provider, index).await?;
    }
    drop(provider);
    let snapshot = directory.path().join(SNAPSHOT_FILE);
    let mut damaged = std::fs::read(&snapshot)?;
    let middle = damaged.len() / 2;
    *damaged.get_mut(middle).ok_or("snapshot is empty")? ^= 1;
    std::fs::write(&snapshot, damaged)?;
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::Corrupt)
    ));
    Ok(())
}
