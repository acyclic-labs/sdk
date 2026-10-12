use super::*;

#[tokio::test]
async fn every_partial_current_frame_recovers_only_the_accepted_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let path = directory.path().join(JOURNAL_FILE);
    let command = Command::Append(AppendRequest {
        path: StreamPath::new("partial")?,
        records: vec![OwnedBytes::from_static(b"record")],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"partial"))?),
    });
    let frame = encode_frame(&command, 42, None)?;
    for cut in 0..=frame.value.len() {
        let mut bytes = encode_header(limits, 0)?.to_vec();
        bytes.extend_from_slice(&frame.value[..cut]);
        std::fs::write(&path, &bytes)?;
        let reopened = LocalStream::open(directory.path(), limits).await?;
        if cut == frame.value.len() {
            assert_eq!(reopened.tail(StreamPath::new("partial")?).await?, 1);
        } else {
            assert_eq!(
                reopened.tail(StreamPath::new("partial")?).await,
                Err(StreamError::NotFound)
            );
        }
        drop(reopened);
        assert_eq!(
            std::fs::metadata(&path)?.len(),
            u64::try_from(if cut == frame.value.len() {
                bytes.len()
            } else {
                HEADER_BYTES
            })?
        );
    }
    // A whole checksummed noncanonical command is never a torn append.
    let mut payload = 42_u64.to_le_bytes().to_vec();
    payload
        .extend_from_slice(&crate::wire_codec::CanonicalWire::Journal(&command).encode_to_vec()?);
    payload.extend_from_slice(&[0x78, 0]);
    let length = u32::try_from(payload.len())?.to_le_bytes();
    let mut bytes = encode_header(limits, 0)?.to_vec();
    bytes.extend_from_slice(&length);
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&checksum(FRAME_DOMAIN, &[&length, &payload], None, None)?);
    std::fs::write(&path, &bytes)?;
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::Corrupt)
    ));
    assert_eq!(std::fs::read(&path)?, bytes);
    Ok(())
}

#[tokio::test]
async fn invalid_headers_and_snapshot_state_fail_without_repairing_disk()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let journal_path = directory.path().join(JOURNAL_FILE);
    let snapshot_path = directory.path().join(SNAPSHOT_FILE);
    let mut obsolete = encode_header(limits, 0)?;
    obsolete[22] = b'4';
    for header in [obsolete, [0; HEADER_BYTES], encode_header(limits, 2)?] {
        std::fs::write(&journal_path, &header)?;
        for _ in 0..2 {
            assert!(matches!(
                LocalStream::open(directory.path(), limits).await,
                Err(LocalStreamError::Corrupt)
            ));
            assert_eq!(std::fs::read(&journal_path)?, header);
        }
    }
    // Keep the checksum valid so rejection must enforce the snapshot version.
    let header = encode_header(limits, 0)?;
    std::fs::write(&journal_path, &header)?;
    let state = MemoryStream::new(limits.memory)
        .encode_state(None)
        .await
        .expect("unmetered snapshot encoding")
        .value;
    for version in *b"024" {
        let mut obsolete = encode_snapshot(limits, 1, &state, None)?.value;
        obsolete[SNAPSHOT_MAGIC.len() - 2] = version;
        let checksum_at = obsolete.len() - 32;
        let checksum = checksum(SNAPSHOT_DOMAIN, &[&obsolete[..checksum_at]], None, None)?;
        obsolete[checksum_at..].copy_from_slice(&checksum);
        std::fs::write(&snapshot_path, &obsolete)?;
        for _ in 0..2 {
            assert!(matches!(
                LocalStream::open(directory.path(), limits).await,
                Err(LocalStreamError::Corrupt)
            ));
            assert_eq!(std::fs::read(&journal_path)?, header);
            assert_eq!(std::fs::read(&snapshot_path)?, obsolete);
        }
    }
    let header = encode_header(limits, 0)?;
    std::fs::write(&journal_path, &header)?;
    let invalid = encode_snapshot(limits, 1, b"invalid state", None)?.value;
    std::fs::write(&snapshot_path, &invalid)?;
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::Corrupt)
    ));
    assert_eq!(std::fs::read(&journal_path)?, header);
    assert_eq!(std::fs::read(&snapshot_path)?, invalid);
    Ok(())
}

#[tokio::test]
async fn recovery_rejects_receipt_overcapacity_without_rewriting_the_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        memory: MemoryLimits {
            commits: 1,
            idempotency_results: 1,
            ..MemoryLimits::default()
        },
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let first = AppendRequest {
        path: StreamPath::new("capacity/replay")?,
        records: vec![OwnedBytes::from_static(b"first")],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"first"))?),
    };
    let outcome = provider.append(first.clone()).await?;
    let state = provider
        .inner
        .provider
        .encode_state(None)
        .await
        .expect("unmetered snapshot encoding")
        .value;
    let (physical, observed, _) = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "poisoned")?
        .compact(&state, None);
    physical?;
    observed?;
    drop(provider);
    let second = AppendRequest {
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"second"))?),
        if_tail: Some(1),
        ..first.clone()
    };
    let path = directory.path().join(JOURNAL_FILE);
    let before = std::fs::read(&path)?;
    let frame = encode_frame(&Command::Append(second), 1, None)?;
    let mut damaged = before.clone();
    damaged.extend_from_slice(&frame.value);
    damaged.extend_from_slice(&[9, 8, 7]);
    std::fs::write(&path, &damaged)?;
    for _ in 0..2 {
        assert!(matches!(
            LocalStream::open(directory.path(), limits).await,
            Err(LocalStreamError::Replay(StreamError::Capacity))
        ));
        assert_eq!(std::fs::read(&path)?, damaged);
    }
    std::fs::write(&path, before)?;
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.append(first).await?, outcome);
    Ok(())
}

#[tokio::test]
async fn retry_outcomes_survive_clock_exhaustion_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicU64::default());
    let provider = LocalStream::open_with_clock(
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    let request = AppendRequest {
        path: StreamPath::new("retained/local")?,
        records: vec![OwnedBytes::from_static(b"once")],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"local-once"))?),
    };
    let first = provider.append(request.clone()).await?;
    clock.store(u64::MAX, Ordering::SeqCst);
    assert_eq!(provider.append(request.clone()).await?, first);
    drop(provider);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(reopened.append(request.clone()).await?, first);
    assert_eq!(reopened.tail(request.path.clone()).await?, 1);
    let mut changed = request;
    changed.path = StreamPath::new("retained/another")?;
    assert_eq!(
        reopened.append(changed).await,
        Err(StreamError::IdempotencyMismatch)
    );
    Ok(())
}

#[tokio::test]
async fn journal_replay_preserves_monotonic_commit_times() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicU64::default());
    let path = StreamPath::new("accounting/local")?;
    let provider = LocalStream::open_with_clock(
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    for (time, body) in [(100, b"first".as_slice()), (90, b"rollback")] {
        clock.store(time, Ordering::SeqCst);
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![OwnedBytes::copy_from_slice(body)],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
    }
    drop(provider);
    clock.store(80, Ordering::SeqCst);
    let reopened =
        LocalStream::open_with_clock(directory.path(), LocalStreamLimits::default(), clock).await?;
    reopened
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"after-restart")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let records = reopened
        .read(ReadRequest {
            path,
            from: 0,
            limit: 3,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        records
            .iter()
            .map(|record| record.committed_at_micros)
            .collect::<Vec<_>>(),
        vec![100_000, 100_000, 100_000]
    );
    Ok(())
}

#[cfg(not(target_vendor = "apple"))]
#[tokio::test]
async fn barrier_policy_rejects_targets_without_exact_barrier_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        durability: LocalDurability::Barrier,
        ..LocalStreamLimits::default()
    };
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::Io(error))
            if error.kind() == std::io::ErrorKind::Unsupported
    ));
    Ok(())
}

#[tokio::test]
async fn a_journal_torn_before_its_header_completed_is_created_again()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join(JOURNAL_FILE);
    let limits = LocalStreamLimits::default();
    let header = encode_header(limits, 0)?;
    for cut in 0..header.len() {
        std::fs::write(&path, &header[..cut])?;
        let provider = LocalStream::open(directory.path(), limits).await?;
        drop(provider);
        assert_eq!(std::fs::read(&path)?, header);
    }
    let provider = LocalStream::open(directory.path(), limits).await?;
    conformance::verify(&provider)
        .await
        .map_err(std::io::Error::other)?;
    drop(provider);
    LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
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
    let clock = Arc::new(AtomicU64::default());
    clock.store(10, Ordering::SeqCst);
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
            records: vec![OwnedBytes::from_static(b"accepted")],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"accepted"))?,
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
            records: vec![OwnedBytes::from_static(b"expired")],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"expired"))?,
    };
    assert_eq!(
        provider.commit_before(expired, 10).await,
        Err(StreamError::DeadlineElapsed)
    );
    drop(provider);

    clock.store(100, Ordering::SeqCst);
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
            records: vec![OwnedBytes::from_static(b"one")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"append"))?),
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
                records: vec![OwnedBytes::copy_from_slice(&index.to_le_bytes())],
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
    let invalid = encode_frame(
        &Command::Fork(ForkRequest {
            source: StreamPath::new("missing-source")?,
            destination: path.clone(),
            at_tail: None,
            idempotency_key: None,
        }),
        0,
        None,
    )?;
    let valid = encode_frame(
        &Command::Append(AppendRequest {
            path,
            records: vec![OwnedBytes::from_static(b"later")],
            if_tail: Some(0),
            idempotency_key: None,
        }),
        0,
        None,
    )?;
    let journal_path = directory.path().join("stream.journal");
    let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
    journal.write_all(&invalid.value)?;
    for _ in 0..REPLAY_PIPELINE_COMMANDS * 4 {
        journal.write_all(&valid.value)?;
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
                records: vec![OwnedBytes::from_static(b"must-not-appear")],
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
