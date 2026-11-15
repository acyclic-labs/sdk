use super::*;
use crate::{CommitMutation, Record};
use futures::TryFutureExt as _;
use std::time::Instant;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The compacted snapshot alone restores both appended paths in order.
async fn assert_snapshot_restores(
    directory: &std::path::Path,
    limits: LocalStreamLimits,
) -> Result {
    let snapshot = read_snapshot(directory, limits)?.ok_or("snapshot missing")?;
    let restored = MemoryStream::default();
    restored.install_state(&snapshot.state).await?;
    assert_eq!(restored.tail(StreamPath::new("agents/a")?).await?, 2);
    assert_eq!(restored.tail(StreamPath::new("agents/b")?).await?, 1);
    let replay = restored
        .read(ReadRequest {
            key: None,
            path: StreamPath::new("agents/a")?,
            from: 0,
            limit: 2,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        replay
            .iter()
            .map(|record| record.value.as_slice())
            .collect::<Vec<_>>(),
        [b"seed".as_slice(), b"accepted".as_slice()]
    );
    Ok(())
}

#[test]
fn accepted_follow_discovery_and_relay_watermarks_progress_while_compaction_is_held() -> Result {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits { compaction_operations: 4, ..LocalStreamLimits::default() };
        let provider = LocalStream::open(directory.path(), limits).await?;
        let path = StreamPath::new("agents/a")?;
        let AppendOutcome::Committed(seed) = provider.append(AppendRequest {
            path: path.clone(), records: vec![(Bytes::from_static(b"seed")).into()],
            if_tail: None, idempotency_key: crate::IdempotencyKey::unique(),
        }).await? else { return Err("seed conflict".into()) };
        assert_eq!(provider.watermarks(path.clone()).await?.durable, 1);
        let mut records = provider.follow(crate::FollowRequest::new(path.clone(), 1)).map_ok(crate::records_only).await?;
        assert!(futures::poll!(records.next()).is_pending());
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_COMPACT_BLOCKER.lock().map_err(|_| "compaction hook")? = Some((
            Arc::as_ptr(&provider.inner.journal) as usize, started, released,
        ));
        let started = Instant::now();
        let mut writing = tokio::spawn({
            let provider = provider.clone();
            async move {
                provider.commit(CommitRequest {
                    deadline_unix_millis: None,
                    effects: vec![crate::CommitEffect::hard(vec![
                        crate::CommitCondition::Tail { path: StreamPath::new("agents/a")?, expected: 1 },
                        crate::CommitCondition::Absent { path: StreamPath::new("agents/b")? },
                    ], ["agents/a", "agents/b"].into_iter().map(|path| Ok(CommitMutation::Append {
                        path: StreamPath::new(path)?, records: vec![(Bytes::from_static(b"accepted")).into()],
                    })).collect::<std::result::Result<Vec<_>, StreamError>>()?)],
                    idempotency_key: IdempotencyKey::new(Bytes::from_static(b"compacting"))?,
                }).await
            }
        });
        journal_started(waiting, &mut writing).await?;
        // One poll at a known held device cut establishes progress without a
        // timer or guessed number of idle polls. Drain/release before failing.
        let record = futures::poll!(records.next());
        let opened = futures::poll!(provider.follow(crate::FollowRequest {
            bootstrap_head: false,
            until: None, when_absent: Default::default(), path: path.clone(), from: 1, preceding: Some(seed.commit_id), filter: crate::FollowFilter::default(), progress: false, at_least: Default::default() }).map_ok(crate::records_only));
        let ready = [record.is_ready(), opened.is_ready()].into_iter().filter(|ready| *ready).count();
        eprintln!("accepted_compaction_probes ready_before_device_release={ready}/2");
        if ready != 2 {
            release.send(())?;
            writing.await??;
            return Err("compaction delayed accepted publication".into());
        }
        let std::task::Poll::Ready(Some(record)) = record else { return Err("follow ended".into()) };
        let record: Record = record?;
        let std::task::Poll::Ready(opened) = opened else { unreachable!() };
        let mut resumed = opened?;
        drop(records);
        let mut observing = tokio::spawn({
            let provider = provider.clone();
            let path = path.clone();
            async move {
                assert_eq!(record.sequence, 1);
                assert_eq!(resumed.next().await.ok_or("resume ended")??, record);
                let marks = provider.watermarks(path.clone()).await?;
                // Rotation flushed the sealed prefix before the active file
                // could certify any suffix, independently of snapshot I/O.
                assert_eq!(marks, crate::StreamWatermarks { accepted: 2, durable: 2 });
                let mut head = provider.read(ReadRequest {
 key: None, path: path.clone(), from: 1, limit: 1, at_least: Default::default() }).await?;
                assert_eq!(head.next().await.ok_or("head ended")??, record);
                                {
                    let relay = crate::grpc::tests::client(provider.clone())?;
                    assert_eq!(relay.watermarks(path.clone()).await?, marks);
                    let mut followed = relay.follow(crate::FollowRequest {
            bootstrap_head: false,
            until: None, when_absent: Default::default(), path: path, from: 1, preceding: Some(seed.commit_id), filter: crate::FollowFilter::default(), progress: false, at_least: Default::default() }).map_ok(crate::records_only).await?;
                    assert_eq!(followed.next().await.ok_or("relay ended")??, record);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(started.elapsed())
            }
        });
        let observed = tokio::select! {
            observed = &mut observing => observed??,
            terminal = &mut writing => {
                terminal??;
                return Err("compaction completed before its held device cut".into());
            }
        };
        let was_writing = !writing.is_finished();
        release.send(())?;
        writing.await??;
        eprintln!("accepted_compaction_observations before_device_release=6 append_to_all_observers={observed:?}");
        assert!(was_writing, "compaction completed before the device gate was released");
        assert_eq!(provider.watermarks(path).await?, crate::StreamWatermarks { accepted: 2, durable: 2 });
        assert_snapshot_restores(directory.path(), limits).await
    })
}
#[test]
fn append_to_follow_progresses_across_repeated_compactions() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for transport in ["local", "grpc"] {
            let directory = tempfile::tempdir()?;
            let provider = LocalStream::open(
                directory.path(),
                LocalStreamLimits {
                    compaction_operations: 4,
                    ..LocalStreamLimits::default()
                },
            )
            .await?;
            let path = StreamPath::new("latency")?;
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![(Bytes::from_static(b"seed")).into()],
                    if_tail: None,
                    idempotency_key: crate::IdempotencyKey::unique(),
                })
                .await?;
            let reader: Box<dyn StreamProvider> = if transport == "grpc" {
                Box::new(crate::grpc::tests::client(provider.clone())?)
            } else {
                Box::new(provider.clone())
            };
            let mut records = reader
                .follow(crate::FollowRequest::new(path.clone(), 1))
                .map_ok(crate::records_only)
                .await?;
            // The two-frame threshold is crossed repeatedly; more identical
            // rounds would measure latency rather than add a state transition.
            for sequence in 1..=6 {
                let writing = tokio::spawn({
                    let provider = provider.clone();
                    let path = path.clone();
                    async move {
                        provider
                            .append(AppendRequest {
                                path,
                                records: vec![(Bytes::from_static(b"delta")).into()],
                                if_tail: Some(sequence),
                                idempotency_key: crate::IdempotencyKey::unique(),
                            })
                            .await
                    }
                });
                let (next, ()) = tokio::try_join!(
                    async {
                        let next = records.next().await.ok_or("latency follow ended")??;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(next)
                    },
                    async {
                        writing.await??;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    },
                )?;
                assert_eq!(next.sequence, sequence);
            }
            let snapshot = read_snapshot(
                directory.path(),
                LocalStreamLimits {
                    compaction_operations: 4,
                    ..LocalStreamLimits::default()
                },
            )?
            .ok_or("compaction snapshot missing")?;
            assert!(
                snapshot.epoch >= 3,
                "fewer than three compactions completed"
            );
            eprintln!("compacting_append_to_follower transport={transport} rounds=6");
        }
        Ok(())
    })
}

/// Snapshot device work cannot own the admission of unrelated durable writes.
#[test]
fn unrelated_mutations_complete_before_snapshot_device_release() -> Result {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 4,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let path = StreamPath::new("volumes/project/commits")?;
    let AppendOutcome::Committed(_) = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?
    else {
        return Err("seed was not committed".into());
    };
    // Prepared appends reserve existing streams. Empty forks provide those
    // original namespace entries without adding records to the measured cut.
    for index in [2, 5] {
        let destination = StreamPath::new(format!("agents/worker-{index}/execution"))?;
        provider.fork(ForkRequest {
            source: path.clone(),
            destination: destination.clone(),
            at_tail: Some(0),
            exclude: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        }).await?;
        assert_eq!(provider.tail(destination).await?, 0);
    }
    let (entered, held) = tokio::sync::oneshot::channel();
    let (release, device) = std::sync::mpsc::channel();
    *JOURNAL_COMPACT_BLOCKER
        .lock()
        .map_err(|_| "snapshot gate")? = Some((
        Arc::as_ptr(&provider.inner.journal) as usize,
        entered,
        device,
    ));
    let mut compacting = tokio::spawn({
        let provider = provider.clone();
        async move {
            provider
                .append(AppendRequest {
                    path,
                    records: vec![Bytes::from_static(b"published").into()],
                    if_tail: Some(1),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await
        }
    });
    journal_started(held, &mut compacting).await?;
    let observed = async {
        let mut latency = Vec::new();
        // These are the original mutation-admission locks, not a mock queue.
        // One poll at the known held snapshot cut makes the old dependency
        // fail immediately, without a readiness timeout or a parked test.
        for _ in 0..8 {
            let mut admitted = Box::pin(provider.inner.visibility.write());
            let started = Instant::now();
            match futures::poll!(admitted.as_mut()) {
                std::task::Poll::Ready(permit) => drop(permit),
                std::task::Poll::Pending => return Err("snapshot held unrelated mutation admission".into()),
            }
            latency.push(started.elapsed());
        }
        let barrier = tokio::sync::Barrier::new(8);
        let writes = futures::future::join_all((0..8).map(|index| {
            let (provider, barrier) = (&provider, &barrier);
            async move {
                barrier.wait().await;
                let started = Instant::now();
                let path = StreamPath::new(format!("agents/worker-{index}/execution"))?;
                if index % 3 == 0 {
                    let outcome = provider.append(AppendRequest {
                        path,
                        records: vec![Bytes::from_static(b"tool intent").into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::unique(),
                    }).await?;
                    assert!(matches!(outcome, AppendOutcome::Committed(_)));
                } else {
                    let meter = Arc::new(crate::test_meter::Meter::local());
                    let prepared = index % 3 == 2;
                    let request = CommitRequest {
                        deadline_unix_millis: None,
                        effects: vec![crate::CommitEffect::hard(
                            if prepared {
                                vec![
                                    crate::CommitCondition::Tail {
                                        path: StreamPath::new("volumes/project/commits")?,
                                        expected: 2,
                                    },
                                    crate::CommitCondition::Tail { path: path.clone(), expected: 0 },
                                ]
                            } else {
                                vec![crate::CommitCondition::Absent { path: path.clone() }]
                            },
                            vec![crate::CommitMutation::Append {
                                path,
                                records: vec![if prepared { Bytes::new() } else { Bytes::from_static(b"tool intent") }.into()],
                            }],
                        )],
                        idempotency_key: IdempotencyKey::unique(),
                    };
                    if index % 3 == 1 {
                        let outcome = provider.native_commit(request, None, meter.original()).await??;
                        assert!(matches!(outcome.value(), CommitOutcome::Committed(_)));
                    } else {
                        let prepared = provider.native_prepare_commit(request, crate::PreparedRecordCapacity {
                            payload_bytes: b"tool intent".len(), keys: 0, attributes: 0,
                        }, meter.original()).await??;
                        let (prepared, _original_funding) = prepared.into_parts();
                        let outcome = prepared.finish(Bytes::from_static(b"tool intent").into()).await??;
                        assert!(matches!(outcome.value(), CommitOutcome::Committed(_)));
                    }
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(started.elapsed())
            }
        })).await.into_iter().collect::<std::result::Result<Vec<_>, _>>()?;
        let mut writes = writes;
        writes.sort_unstable();
        latency.sort_unstable();
        assert!(!compacting.is_finished(), "snapshot device gate was not held");
        eprintln!("MUTATION_ADMISSION_LATENCY writers=8 p50_ns={} p99_ns={} committed_p50_us={} committed_p99_us={}",
            latency[3].as_nanos(), latency[7].as_nanos(), writes[3].as_micros(), writes[7].as_micros());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    // Closing the original gate also retires the worker on every error path.
    drop(release);
    compacting.await??;
    observed?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(
        reopened
            .tail(StreamPath::new("volumes/project/commits")?)
            .await?,
        2
    );
    for index in 0..8 {
        assert_eq!(
            reopened
                .tail(StreamPath::new(format!("agents/worker-{index}/execution"))?)
                .await?,
            1
        );
    }
    Ok(())
    })
}

/// Reopen actual flushed files at each transition, including an absent/torn
/// next header and a durable snapshot whose previous file was not retired.
#[tokio::test]
async fn rotated_prefix_and_suffix_survive_each_checkpoint_cut() -> Result {
    let source = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 4,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(source.path(), limits).await?;
    let prefix = StreamPath::new("volumes/project/commits")?;
    let suffix = StreamPath::new("agents/worker/execution")?;
    let key = IdempotencyKey::unique();
    let original = provider
        .append(AppendRequest {
            path: prefix.clone(),
            records: vec![Bytes::from_static(b"original prefix").into()],
            if_tail: None,
            idempotency_key: key.clone(),
        })
        .await?;
    assert!(matches!(original, AppendOutcome::Committed(_)));
    let original = provider.inspect_idempotency(key.clone()).await?;
    let visibility = provider.inner.visibility.write().await;
    let through = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal lock")?
        .appended;
    provider.await_flushed(through).await?;
    let state = provider.inner.provider.capture_state().await;
    let inner = Arc::clone(&provider.inner);
    let epoch = spawn_blocking(
        "streams",
        "rotate",
        PersistenceKey::new(Arc::as_ptr(&inner.journal) as usize, 0),
        move || {
            inner
                .journal
                .lock()
                .map_err(|_| LocalStreamError::Corrupt)?
                .rotate(None)
        },
    )
    .await??;
    drop(visibility);
    assert!(matches!(
        provider
            .append(AppendRequest {
                path: suffix.clone(),
                records: vec![Bytes::from_static(b"original suffix").into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?,
        AppendOutcome::Committed(_)
    ));
    let archived = std::fs::read(source.path().join(JOURNAL_PREVIOUS))?;
    let active = std::fs::read(source.path().join(JOURNAL_FILE))?;
    let snapshot = encode_snapshot(limits, epoch, &state.encode())?;
    let incarnation = std::fs::read(source.path().join(INCARNATION_FILE))?;
    drop(provider);
    // Each image is a physically possible prefix of the shared producer.
    for (name, active, snapshot, archive, expected_suffix) in [
        ("before header creation", None, None, true, 0),
        (
            "torn header",
            Some(&active[..HEADER_BYTES / 2]),
            None,
            true,
            0,
        ),
        ("whole header", Some(&active[..HEADER_BYTES]), None, true, 0),
        (
            "acknowledged suffix",
            Some(active.as_slice()),
            None,
            true,
            1,
        ),
        (
            "snapshot installed",
            Some(active.as_slice()),
            Some(snapshot.as_slice()),
            true,
            1,
        ),
        (
            "archive retired",
            Some(active.as_slice()),
            Some(snapshot.as_slice()),
            false,
            1,
        ),
    ] {
        let image = tempfile::tempdir()?;
        if let Some(active) = active {
            std::fs::write(image.path().join(JOURNAL_FILE), active)?;
        }
        if let Some(snapshot) = snapshot {
            std::fs::write(image.path().join(SNAPSHOT_FILE), snapshot)?;
        }
        if archive {
            std::fs::write(image.path().join(JOURNAL_PREVIOUS), &archived)?;
        }
        std::fs::write(image.path().join(INCARNATION_FILE), &incarnation)?;
        for _ in 0..2 {
            let restored = LocalStream::open(image.path(), limits).await?;
            assert_eq!(
                restored.tail(prefix.clone()).await?,
                1,
                "sealed prefix missing at {name}"
            );
            assert_eq!(
                restored.tail(suffix.clone()).await?,
                expected_suffix,
                "active suffix changed at {name}"
            );
            assert_eq!(
                restored.inspect_idempotency(key.clone()).await?,
                original,
                "canonical receipt changed at {name}"
            );
            assert!(
                !image.path().join(JOURNAL_PREVIOUS).exists(),
                "archive not retired at {name}"
            );
            drop(restored);
        }
    }
    // A torn sealed file was already acknowledged and cannot be repaired as
    // if it were an unacknowledged active-file suffix.
    for length in [HEADER_BYTES - 1, archived.len() - 1] {
        let image = tempfile::tempdir()?;
        std::fs::write(image.path().join(JOURNAL_PREVIOUS), &archived[..length])?;
        std::fs::write(image.path().join(JOURNAL_FILE), &active[..HEADER_BYTES])?;
        assert!(
            matches!(
                LocalStream::open(image.path(), limits).await,
                Err(LocalStreamError::Corrupt)
            ),
            "torn sealed prefix was accepted"
        );
    }
    Ok(())
}

#[test]
fn rotation_waits_for_the_original_flush_owner_after_an_explicit_flush() -> Result {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let (entered, held) = tokio::sync::oneshot::channel();
        let (release, device) = std::sync::mpsc::channel();
        *JOURNAL_FLUSHED_BLOCKER.lock().map_err(|_| "flush gate")? = Some((
            Arc::as_ptr(&provider.inner.journal) as usize,
            entered,
            device,
        ));
        let mut writing = tokio::spawn({
            let provider = provider.clone();
            async move {
                provider
                    .append(AppendRequest {
                        path: StreamPath::new("pending/flush")?,
                        records: vec![Bytes::from_static(b"accepted").into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::unique(),
                    })
                    .await
            }
        });
        journal_started(held, &mut writing).await?;
        let visibility = provider.inner.visibility.write().await;
        provider.flush()?;
        let through = provider
            .inner
            .journal
            .lock()
            .map_err(|_| "journal lock")?
            .appended;
        let mut rotation = Box::pin(provider.await_rotation_ready(through));
        let parked = futures::poll!(rotation.as_mut()).is_pending();
        // Both success and the negative control release the physical owner first.
        drop(release);
        if parked {
            rotation.await?;
        }
        drop(visibility);
        writing.await??;
        assert!(parked, "rotation raced a live earlier-file flush owner");
        Ok(())
    })
}
