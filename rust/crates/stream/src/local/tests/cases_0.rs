use super::*;

#[test]
fn cancelled_real_open_retains_ownership_until_journal_open_stops()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = obs::tests::Capture::default();
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let (blocking_started_tx, blocking_started_rx) = std::sync::mpsc::sync_channel(0);
    let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
    let blocker = runtime.spawn_blocking(move || {
        let _ = blocking_started_tx.send(());
        let _ = release_blocker_rx.recv();
    });
    blocking_started_rx.recv()?;

    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
        let ownership = Arc::clone(&lifecycle).lock_owned().await;
        let anchor = OwnershipAnchor::new(ownership);
        let root = directory.path().to_path_buf();
        let (submitted_tx, submitted_rx) = tokio::sync::oneshot::channel();
        {
            let mut hook = JOURNAL_OPEN_SUBMITTED
                .lock()
                .map_err(|_| "journal-open test hook was poisoned")?;
            *hook = Some((root.clone(), submitted_tx));
        }
        let opening = tokio::spawn(
            async move {
                LocalStream::open_with_ownership_anchor(root, LocalStreamLimits::default(), anchor)
                    .await
            }
            .with_subscriber(dispatch),
        );

        // The only blocking worker is occupied, so the real Journal::open submitted by the
        submitted_rx.await?;
        opening.abort();
        let cancelled = opening.await;
        assert!(
            cancelled
                .as_ref()
                .is_err_and(tokio::task::JoinError::is_cancelled)
        );
        let ownership_retained = Arc::clone(&lifecycle).try_lock_owned().is_err();

        release_blocker_tx.send(())?;
        blocker.await?;
        // The single blocking worker runs its queue in order, so this
        tokio::task::spawn_blocking(|| ()).await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&lifecycle).lock_owned(),
        )
        .await?;
        assert!(
            ownership_retained,
            "caller cancellation must not release ownership from active Journal::open"
        );
        let fields = captured.fields("acyclic.stream.open");
        assert_eq!(
            fields["outcome"], "ok",
            "detached work owns its successful completion"
        );
        assert!(!fields.contains_key("error.kind"));
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn cancelled_persist_retains_ownership_until_the_journal_task_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = obs::tests::Capture::default();
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
        let ownership = Arc::clone(&lifecycle).lock_owned().await;
        let provider = LocalStream::open_with_ownership_anchor(
            directory.path(),
            LocalStreamLimits::default(),
            OwnershipAnchor::new(ownership),
        )
        .await?;

        let journal_identity = Arc::as_ptr(&provider.inner.journal) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        {
            let mut hook = JOURNAL_PERSIST_BLOCKER
                .lock()
                .map_err(|_| "journal-persist test hook was poisoned")?;
            *hook = Some((journal_identity, started_tx, release_rx));
        }
        let appending = tokio::spawn(
            {
                let provider = provider.clone();
                async move {
                    provider
                        .append(AppendRequest {
                            path: StreamPath::new("cancelled-persist")?,
                            records: vec![Bytes::from_static(b"record")],
                            if_tail: Some(0),
                            idempotency_key: None,
                        })
                        .await
                }
            }
            .with_subscriber(dispatch),
        );

        started_rx.recv()?;
        appending.abort();
        assert!(appending.await.is_err_and(|error| error.is_cancelled()));
        drop(provider);
        assert!(
            Arc::clone(&lifecycle).try_lock_owned().is_err(),
            "queued journal persistence must retain local-root ownership after cancellation"
        );

        release_tx.send(())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&lifecycle).lock_owned(),
        )
        .await?;
        let fields = captured.fields("acyclic.stream.append");
        assert_eq!(
            fields["outcome"], "ok",
            "detached work owns its successful completion"
        );
        assert!(!fields.contains_key("error.kind"));
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

/// Serializes the tests that install [`JOURNAL_PERSIST_BLOCKER`], which is
static PERSIST_BLOCKER_TESTS: Mutex<()> = Mutex::new(());

/// A cancelled mutation stays invisible until its frame is durable: a
#[test]
fn a_cancelled_mutation_is_invisible_until_its_frame_is_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let journal_identity = Arc::as_ptr(&provider.inner.journal) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .map_err(|_| "journal-persist test hook was poisoned")? =
            Some((journal_identity, started_tx, release_rx));
        let path = StreamPath::new("cancelled-visibility")?;
        let appending = tokio::spawn({
            let (provider, path) = (provider.clone(), path.clone());
            async move {
                provider
                    .append(AppendRequest {
                        path,
                        records: vec![Bytes::from_static(b"record")],
                        if_tail: Some(0),
                        idempotency_key: None,
                    })
                    .await
            }
        });
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        appending.abort();
        assert!(appending.await.is_err_and(|error| error.is_cancelled()));

        let reading = tokio::spawn({
            let (provider, path) = (provider.clone(), path.clone());
            async move { provider.tail(path).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!reading.is_finished(), "the unsynced record is not visible");
        release_tx.send(())?;
        assert_eq!(reading.await??, 1, "the persisted record is visible");
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

/// One record appended to `path` at `tail` under a key naming both.
fn keyed_commit(path: &str, tail: u64) -> Result<CommitRequest, StreamError> {
    let path = StreamPath::new(path)?;
    Ok(CommitRequest {
        conditions: vec![if tail == 0 {
            crate::CommitCondition::Absent { path: path.clone() }
        } else {
            crate::CommitCondition::Tail {
                path: path.clone(),
                expected: tail,
            }
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: path.clone(),
            records: vec![Bytes::from(format!("{path}-{tail}"))],
        }],
        idempotency_key: IdempotencyKey::new(Bytes::from(format!("{path}@{tail}")))?,
    })
}

/// While one mutation's flush is under way, a mutation on another path
#[test]
fn mutations_on_other_paths_write_while_a_flush_is_under_way()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let journal_identity = Arc::as_ptr(&provider.inner.journal) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .map_err(|_| "journal-persist test hook was poisoned")? =
            Some((journal_identity, started_tx, release_rx));
        let commit = |path: &'static str| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.commit(keyed_commit(path, 0)?).await })
        };
        let first = commit("first");
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        let second = commit("second");
        let appended = |provider: &LocalStream| {
            provider
                .inner
                .journal
                .lock()
                .map(|journal| journal.appended)
                .unwrap_or_default()
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while appended(&provider) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert!(!first.is_finished() && !second.is_finished());
        release_tx.send(())?;
        for (outcome, path) in [(first.await??, "first"), (second.await??, "second")] {
            assert!(matches!(outcome, CommitOutcome::Committed(_)), "{path}");
            assert_eq!(provider.tail(StreamPath::new(path)?).await?, 1, "{path}");
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

/// Once the store is poisoned, waiting for a frame fails even when that
#[tokio::test(flavor = "multi_thread")]
async fn a_poisoned_store_fails_waits_for_frames_already_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider.commit(keyed_commit("durable", 0)?).await?;
    let required = provider.inner.required()?;
    assert_eq!(required, 1);
    provider.await_durable(required).await?;
    provider.inner.poison();
    assert_eq!(
        provider.await_durable(required).await,
        Err(StreamError::Unavailable)
    );
    assert_eq!(
        provider.tail(StreamPath::new("durable")?).await,
        Err(StreamError::Unavailable)
    );
    Ok(())
}

/// Many writers commit concurrently to their own paths, some joined by a
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_commits_on_distinct_paths_replay_after_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITERS: usize = 8;
    const COMMITS: u64 = 25;
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let provider = provider.clone();
        writers.push(tokio::spawn(async move {
            let path = format!("agents/{writer}/memory");
            let mut outcomes = Vec::new();
            for tail in 0..COMMITS {
                let outcome = provider.commit(keyed_commit(&path, tail)?).await?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
                outcomes.push(outcome);
            }
            Ok::<_, StreamError>(outcomes)
        }));
    }
    // Cross-path atomic commits race the writers on paths of their own.
    let joint = {
        let provider = provider.clone();
        tokio::spawn(async move {
            let (left, right) = (
                StreamPath::new("joint/left")?,
                StreamPath::new("joint/right")?,
            );
            for tail in 0..COMMITS {
                let condition = |path: &StreamPath| {
                    if tail == 0 {
                        crate::CommitCondition::Absent { path: path.clone() }
                    } else {
                        crate::CommitCondition::Tail {
                            path: path.clone(),
                            expected: tail,
                        }
                    }
                };
                let outcome = provider
                    .commit(CommitRequest {
                        conditions: vec![condition(&left), condition(&right)],
                        mutations: [&left, &right]
                            .into_iter()
                            .map(|path| crate::CommitMutation::Append {
                                path: path.clone(),
                                records: vec![Bytes::from(tail.to_le_bytes().to_vec())],
                            })
                            .collect(),
                        idempotency_key: IdempotencyKey::new(Bytes::from(format!("joint@{tail}")))?,
                    })
                    .await?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
            }
            Ok::<_, StreamError>(())
        })
    };
    let mut outcomes = Vec::new();
    for writer in writers {
        outcomes.push(writer.await??);
    }
    joint.await??;
    let frames = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal poisoned")?
        .appended;
    assert_eq!(frames, (WRITERS as u64 + 1) * COMMITS);
    // Nothing else flushes or closes the provider before it reopens.
    drop(provider);

    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    for (writer, outcomes) in outcomes.iter().enumerate() {
        let path = format!("agents/{writer}/memory");
        assert_eq!(reopened.tail(StreamPath::new(&path)?).await?, COMMITS);
        let records = reopened
            .read(ReadRequest {
                path: StreamPath::new(&path)?,
                from: 0,
                limit: 100,
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        for (tail, record) in (0..).zip(&records) {
            assert_eq!(record.value, Bytes::from(format!("{path}-{tail}")));
        }
        for (tail, outcome) in (0..).zip(outcomes) {
            assert_eq!(&reopened.commit(keyed_commit(&path, tail)?).await?, outcome);
        }
    }
    for path in ["joint/left", "joint/right"] {
        assert_eq!(reopened.tail(StreamPath::new(path)?).await?, COMMITS);
    }
    Ok(())
}

use crate::conformance;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct TestClock(AtomicU64);

impl UnixMillisClock for TestClock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[tokio::test]
async fn every_partial_current_frame_recovers_only_the_accepted_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let path = directory.path().join(JOURNAL_FILE);
    let command = Command::Append(AppendRequest {
        path: StreamPath::new("partial")?,
        records: vec![Bytes::from_static(b"record")],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"partial"))?),
    });
    let frame = PreparedFrame::encode(&command, 42, None)?;
    for cut in 0..=frame.encoded.len() {
        let mut bytes = encode_header(limits, 0)?;
        bytes.extend_from_slice(&frame.encoded[..cut]);
        std::fs::write(&path, &bytes)?;
        let reopened = LocalStream::open(directory.path(), limits).await?;
        if cut == frame.encoded.len() {
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
            u64::try_from(if cut == frame.encoded.len() {
                bytes.len()
            } else {
                HEADER_BYTES
            })?
        );
    }
    // A whole checksummed noncanonical command is never a torn append.
    let mut payload = 42_u64.to_le_bytes().to_vec();
    payload.extend_from_slice(&command.encode_to_vec());
    payload.extend_from_slice(&[0x78, 0]);
    let length = u32::try_from(payload.len())?.to_le_bytes();
    let mut bytes = encode_header(limits, 0)?;
    bytes.extend_from_slice(&length);
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&frame_checksum(&length, &payload));
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
    for header in [obsolete, vec![0; HEADER_BYTES], encode_header(limits, 2)?] {
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
    let state = MemoryStream::new(limits.memory).encode_state().await;
    for version in *b"024" {
        let mut obsolete = encode_snapshot(limits, 1, &state)?;
        obsolete[SNAPSHOT_MAGIC.len() - 2] = version;
        let checksum_at = obsolete.len() - 32;
        let checksum = snapshot_checksum(&obsolete[..checksum_at]);
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
    // A checksummed snapshot with invalid semantic state must not authorize
    let header = encode_header(limits, 0)?;
    std::fs::write(&journal_path, &header)?;
    let invalid = encode_snapshot(limits, 1, b"invalid state")?;
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
        records: vec![Bytes::from_static(b"first")],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"first"))?),
    };
    let outcome = provider.append(first.clone()).await?;
    let state = provider.inner.provider.encode_state().await;
    provider
        .inner
        .journal
        .lock()
        .map_err(|_| "poisoned")?
        .compact(&state, None)?;
    drop(provider);
    let second = AppendRequest {
        idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"second"))?),
        if_tail: Some(1),
        ..first.clone()
    };
    let path = directory.path().join(JOURNAL_FILE);
    let before = std::fs::read(&path)?;
    let frame = PreparedFrame::encode(&Command::Append(second), 1, None)?;
    let mut damaged = before.clone();
    damaged.extend_from_slice(&frame.encoded);
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
    let clock = Arc::new(TestClock::default());
    let provider = LocalStream::open_with_clock(
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    let request = AppendRequest {
        path: StreamPath::new("retained/local")?,
        records: vec![Bytes::from_static(b"once")],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"local-once"))?),
    };
    let first = provider.append(request.clone()).await?;
    clock.0.store(u64::MAX, Ordering::SeqCst);
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
    let clock = Arc::new(TestClock::default());
    let path = StreamPath::new("accounting/local")?;
    let provider = LocalStream::open_with_clock(
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    for (time, body) in [(100, b"first".as_slice()), (90, b"rollback")] {
        clock.0.store(time, Ordering::SeqCst);
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::copy_from_slice(body)],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
    }
    drop(provider);
    clock.0.store(80, Ordering::SeqCst);
    let reopened =
        LocalStream::open_with_clock(directory.path(), LocalStreamLimits::default(), clock).await?;
    reopened
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"after-restart")],
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
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
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

/// A mutation made under `deferring_durability` is visible at once and
#[tokio::test(flavor = "multi_thread")]
async fn deferred_mutations_wait_for_the_next_flush() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let unflushed = |provider: &LocalStream| -> Result<bool, Box<dyn std::error::Error>> {
        Ok(provider
            .inner
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .unflushed())
    };
    let append = |provider: LocalStream, key: &'static [u8]| async move {
        provider
            .append(AppendRequest {
                path: StreamPath::new("deferred")?,
                records: vec![Bytes::from_static(b"record")],
                if_tail: None,
                idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(key))?),
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };

    deferring_durability(append(provider.clone(), b"first")).await?;
    assert_eq!(provider.tail(StreamPath::new("deferred")?).await?, 1);
    assert!(unflushed(&provider)?);
    provider.flush()?;
    assert!(!unflushed(&provider)?);

    deferring_durability(append(provider.clone(), b"second")).await?;
    assert!(unflushed(&provider)?);
    append(provider.clone(), b"third").await?;
    assert!(
        !unflushed(&provider)?,
        "a flushed mutation flushes those before it"
    );
    assert_eq!(provider.tail(StreamPath::new("deferred")?).await?, 3);
    Ok(())
}

/// Spans and events by name, each with its fields as text.
type Captured = Arc<Mutex<Vec<(&'static str, Vec<(&'static str, String)>)>>>;

struct Capture(Captured, Mutex<std::collections::HashMap<u64, usize>>);

struct Fields<'a>(&'a mut Vec<(&'static str, String)>);

impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push((field.name(), value.to_owned()));
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name(), format!("{value:?}")));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Vec::new();
        attributes.record(&mut Fields(&mut fields));
        let names = attributes
            .metadata()
            .fields()
            .iter()
            .map(|field| field.name());
        fields.extend(names.map(|name| (name, String::new())));
        let mut captured = self.0.lock().unwrap();
        self.1.lock().unwrap().insert(id.into_u64(), captured.len());
        captured.push((attributes.metadata().name(), fields));
    }
    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let index = self.1.lock().unwrap().get(&id.into_u64()).copied();
        let mut captured = self.0.lock().unwrap();
        let span = index.and_then(|index| captured.get_mut(index)).unwrap();
        values.record(&mut Fields(&mut span.1));
    }
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Vec::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().push(("event", fields));
    }
}

#[tokio::test]
async fn filtered_local_operations_preserve_caller() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = obs::tests::Capture::default();
    let subscriber = tracing_subscriber::Registry::default()
        .with(captured.clone())
        .with(tracing_subscriber::filter::filter_fn(|metadata| {
            !metadata.name().starts_with("acyclic.stream")
        }));
    let _default = tracing::subscriber::set_default(subscriber);
    let parent = tracing::info_span!(
        "caller",
        outcome = "caller",
        error.kind = "caller",
        frames = 77,
        bytes = 88,
        batch_len = 99,
        rev = 66
    );
    async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = AppendRequest {
            path: StreamPath::new("traced")?,
            records: vec![Bytes::from_static(b"record")],
            if_tail: None,
            idempotency_key: None,
        };
        provider.append(request).await?;
        provider.flush()?;
        assert!(
            provider
                .read(ReadRequest {
                    path: StreamPath::new("missing")?,
                    from: 0,
                    limit: 1
                })
                .await
                .is_err()
        );
        drop(provider);
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("traced")?).await?, 1);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .instrument(parent)
    .await?;
    let fields = captured.fields("caller");
    for (field, value) in [
        ("outcome", "caller"),
        ("error.kind", "caller"),
        ("frames", "77"),
        ("bytes", "88"),
        ("batch_len", "99"),
        ("rev", "66"),
    ] {
        assert_eq!(fields[field], value);
    }
    Ok(())
}

#[tokio::test]
async fn local_follow_observes_lifetime_drop_and_late_error()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    for late_error in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("follow")?;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"record")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let captured = obs::tests::Capture::default();
        let mut records = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = obs::OwnedSpan::new(tracing::info_span!("origin_parent"));
                parent.scope(async { provider.follow(path, 1).await })
            },
        )
        .await?;
        assert!(
            !captured
                .fields("acyclic.stream.follow")
                .contains_key("outcome")
        );
        assert_eq!(captured.closed("acyclic.stream.follow"), 0);
        let other = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_parent");
        let _entered = parent.enter();
        if late_error {
            provider.inner.poison();
            assert_eq!(
                records.next().await.unwrap().unwrap_err(),
                StreamError::Unavailable
            );
            assert!(records.next().await.is_none());
        } else {
            assert!(futures::poll!(records.next()).is_pending());
        }
        drop(records);
        let fields = captured.fields("acyclic.stream.follow");
        assert_eq!(
            fields["terminal"],
            if late_error { "error" } else { "dropped" }
        );
        if late_error {
            assert_eq!(fields["outcome"], "err");
            assert_eq!(fields["error.kind"], "unavailable");
        } else {
            assert!(!fields.contains_key("outcome"));
            assert!(!fields.contains_key("error.kind"));
        }
        assert_eq!(captured.closed("acyclic.stream.follow"), 1);
        assert_eq!(captured.closed("origin_parent"), 1);
        assert_eq!(other.closed("foreign_parent"), 0);
    }
    Ok(())
}
