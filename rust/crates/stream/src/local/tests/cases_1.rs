#[tokio::test]
async fn cancelled_local_read_records_error_and_closes_origin_parent()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let visibility = provider.inner.visibility.write().await;
    let captured = obs::tests::Capture::default();
    let future = tracing::subscriber::with_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
        || {
            let parent = obs::OwnedSpan::new(tracing::info_span!("origin_parent"));
            parent.scope(async {
                provider
                    .read(ReadRequest {
                        path: StreamPath::new("pending")?,
                        from: 0,
                        limit: 1,
                    })
                    .await
            })
        },
    );
    let mut future = Box::pin(future);
    assert!(futures::poll!(future.as_mut()).is_pending());
    let other = obs::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(other.clone()),
    );
    let parent = tracing::info_span!("foreign_parent");
    let _entered = parent.enter();
    drop(future);
    assert_eq!(
        captured.fields("acyclic.stream.read")["error.kind"],
        "cancelled"
    );
    assert_eq!(captured.closed("acyclic.stream.read"), 1);
    assert_eq!(captured.closed("origin_parent"), 1);
    assert_eq!(other.closed("foreign_parent"), 0);
    drop(visibility);
    Ok(())
}

#[tokio::test]
async fn local_append_emits_its_span() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    // With one live dispatcher, a callsite that a concurrent test reaches
    let _second = tracing::Dispatch::new(tracing_subscriber::Registry::default());
    let captured = Captured::default();
    let capture = Capture(Arc::clone(&captured), Mutex::default());
    let _default =
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(capture));
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider
        .append(AppendRequest {
            path: StreamPath::new("traced/path")?,
            records: vec![Bytes::from_static(b"body"), Bytes::from_static(b"more")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let captured = captured.lock().unwrap();
    let fields = |name: &str| {
        captured
            .iter()
            .find(|(found, _)| *found == name)
            .map(|(_, fields)| fields.clone())
            .unwrap_or_default()
    };
    let append = fields("acyclic.stream.append");
    for field in [
        ("items", "2"),
        ("bytes", "8"),
        ("rev", "2"),
        ("outcome", "ok"),
    ] {
        assert!(
            append.contains(&(field.0, field.1.to_owned())),
            "{append:?}"
        );
    }
    assert!(fields("acyclic.stream.open").contains(&("outcome", "ok".to_owned())));
    assert!(captured.iter().any(|(name, fields)| {
        *name == "event" && fields.contains(&("fsyncs", "1".to_owned()))
    }));
    for (_, fields) in captured.iter() {
        for (field, value) in fields {
            assert!(!["path", "token", "content", "body", "authorization"].contains(field));
            assert!(!value.contains("traced"), "{field} leaks a stream path");
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_provider_reopens_and_passes_public_conformance()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    conformance::verify(&provider)
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
        conditions: vec![crate::CommitCondition::Absent {
            path: StreamPath::new("deadline/accepted")?,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: StreamPath::new("deadline/accepted")?,
            records: vec![Bytes::from_static(b"accepted")],
        }],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(b"accepted"))?,
    };
    assert!(matches!(
        provider.commit_before(accepted, 11).await?,
        CommitOutcome::Committed(_)
    ));
    let expired = CommitRequest {
        conditions: vec![crate::CommitCondition::Absent {
            path: StreamPath::new("deadline/expired")?,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: StreamPath::new("deadline/expired")?,
            records: vec![Bytes::from_static(b"expired")],
        }],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(b"expired"))?,
    };
    assert_eq!(
        provider.commit_before(expired, 10).await,
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
async fn local_provider_excludes_a_second_process_owner_and_repairs_a_torn_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert!(matches!(
        LocalStream::open(directory.path(), LocalStreamLimits::default()).await,
        Err(LocalStreamError::AlreadyOpen)
    ));
    provider
        .append(AppendRequest {
            path: StreamPath::new("durable")?,
            records: vec![Bytes::from_static(b"one")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"append"))?),
        })
        .await?;
    drop(provider);
    let journal = directory.path().join("stream.journal");
    let valid_length = std::fs::metadata(&journal)?.len();
    OpenOptions::new()
        .append(true)
        .open(&journal)?
        .write_all(&[9, 8, 7])?;
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(reopened.tail(StreamPath::new("durable")?).await?, 1);
    drop(reopened);
    assert_eq!(std::fs::metadata(journal)?.len(), valid_length);
    Ok(())
}

#[tokio::test]
async fn reopen_streams_more_commands_than_the_pipeline_window()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let path = StreamPath::new("bounded-replay")?;
    let count = u64::try_from(REPLAY_PIPELINE_COMMANDS * 4)?;
    for index in 0..count {
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::copy_from_slice(&index.to_le_bytes())],
                if_tail: Some(index),
                idempotency_key: None,
            })
            .await?;
    }
    drop(provider);

    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(reopened.tail(path).await?, count);
    Ok(())
}

#[tokio::test]
async fn replay_failure_drains_the_bounded_pipeline_and_releases_the_root()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    drop(provider);

    let path = StreamPath::new("failed-replay")?;
    let invalid = PreparedFrame::encode(
        &Command::Fork(ForkRequest {
            source: StreamPath::new("missing-source")?,
            destination: path.clone(),
            at_tail: None,
            idempotency_key: None,
        }),
        0,
        None,
    )?;
    let valid = PreparedFrame::encode(
        &Command::Append(AppendRequest {
            path,
            records: vec![Bytes::from_static(b"later")],
            if_tail: Some(0),
            idempotency_key: None,
        }),
        0,
        None,
    )?;
    let journal_path = directory.path().join("stream.journal");
    let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
    journal.write_all(&invalid.encoded)?;
    for _ in 0..REPLAY_PIPELINE_COMMANDS * 4 {
        journal.write_all(&valid.encoded)?;
    }
    journal.write_all(&[9, 8, 7])?;
    journal.sync_all()?;
    drop(journal);
    let before = std::fs::read(&journal_path)?;

    for _ in 0..2 {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            LocalStream::open(directory.path(), limits),
        )
        .await?;
        assert!(matches!(
            result,
            Err(LocalStreamError::Replay(StreamError::NotFound))
        ));
        assert_eq!(std::fs::read(&journal_path)?, before);
    }
    Ok(())
}

#[tokio::test]
async fn journal_capacity_rejects_before_mutating_visible_state()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        journal_bytes: u64::try_from(HEADER_BYTES).map_err(std::io::Error::other)?,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert!(matches!(
        provider
            .append(AppendRequest {
                path: StreamPath::new("capacity")?,
                records: vec![Bytes::from_static(b"must-not-appear")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await,
        Err(StreamError::Capacity)
    ));
    assert!(matches!(
        provider.tail(StreamPath::new("capacity")?).await,
        Err(StreamError::NotFound)
    ));
    Ok(())
}

/// Appends one record to `path` and returns the journal's length after.
async fn append_one(
    provider: &LocalStream,
    path: &str,
    journal: &Path,
) -> Result<u64, Box<dyn std::error::Error>> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![Bytes::from_static(b"body")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    Ok(std::fs::metadata(journal)?.len())
}

/// The tail of `path` in the journal reopened, if the path exists.
async fn tail_of(directory: &Path, path: &str) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let provider = LocalStream::open(directory, LocalStreamLimits::default()).await?;
    Ok(provider.tail(StreamPath::new(path)?).await.ok())
}

/// A damaged frame with an intact one after it was committed, so the
#[tokio::test]
async fn a_damaged_frame_before_an_intact_one_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let journal = directory.path().join("stream.journal");
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    append_one(&provider, "first", &journal).await?;
    append_one(&provider, "second", &journal).await?;
    drop(provider);

    let mut file = OpenOptions::new().read(true).write(true).open(&journal)?;
    let body_offset = u64::try_from(HEADER_BYTES + 5).map_err(std::io::Error::other)?;
    file.seek(SeekFrom::Start(body_offset))?;
    file.write_all(&[0xff])?;
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
        let kept = append_one(&provider, "kept", &journal).await?;
        let torn = append_one(&provider, "torn", &journal).await?;
        drop(provider);

        let mut file = OpenOptions::new().read(true).write(true).open(&journal)?;
        file.seek(SeekFrom::Start(kept))?;
        file.write_all(&vec![fill; usize::try_from(torn - kept)?])?;
        file.sync_all()?;
        drop(file);

        assert_eq!(
            tail_of(directory.path(), "kept").await?,
            Some(1),
            "{fill:#x}"
        );
        assert_eq!(
            tail_of(directory.path(), "torn").await?.unwrap_or(0),
            0,
            "{fill:#x}"
        );
        assert_eq!(std::fs::metadata(&journal)?.len(), kept, "{fill:#x}");
    }
    Ok(())
}

fn compacting_limits() -> LocalStreamLimits {
    LocalStreamLimits {
        journal_operations: 8,
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
            records: vec![Bytes::copy_from_slice(&index.to_le_bytes())],
            if_tail: Some(index),
            idempotency_key: Some(IdempotencyKey::new(Bytes::copy_from_slice(
                &index.to_le_bytes(),
            ))?),
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
            path: StreamPath::new("compacted")?,
            from: 0,
            limit: 21,
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
            source: StreamPath::new("compacted")?,
            destination: StreamPath::new("compacted-fork")?,
            at_tail: Some(21),
            idempotency_key: None,
        })
        .await?;
    let fork_history = reopened
        .read(ReadRequest {
            path: StreamPath::new("compacted-fork")?,
            from: 0,
            limit: 21,
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
    conformance::verify(&reopened)
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
