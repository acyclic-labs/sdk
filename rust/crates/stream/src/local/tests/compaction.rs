use super::*;
use crate::{CommitMutation, Record};
use std::time::{Duration, Instant};
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
            path: StreamPath::new("agents/a")?,
            from: 0,
            limit: 2,
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
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits { journal_operations: 4, ..LocalStreamLimits::default() };
        let provider = LocalStream::open(directory.path(), limits).await?;
        let path = StreamPath::new("agents/a")?;
        let AppendOutcome::Committed(seed) = provider.append(AppendRequest {
            path: path.clone(), records: vec![Bytes::from_static(b"seed")],
            if_tail: None, idempotency_key: None,
        }).await? else { return Err("seed conflict".into()) };
        assert_eq!(provider.watermarks(path.clone()).await?.durable, 1);
        let mut records = provider.follow(path.clone(), 1).await?;
        let mut children = provider.children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?), limit: 1,
        }).await?;
        children.next().await.ok_or("missing seed child")??;
        assert!(futures::poll!(records.next()).is_pending());
        assert!(futures::poll!(children.next()).is_pending());
        let (started, waiting) = std::sync::mpsc::sync_channel(0);
        let (release, released) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_COMPACT_BLOCKER.lock().map_err(|_| "compaction hook")? = Some((
            Arc::as_ptr(&provider.inner.journal) as usize, started, released,
        ));
        let started = Instant::now();
        let writing = tokio::spawn({
            let provider = provider.clone();
            async move {
                provider.commit(CommitRequest {
                    conditions: vec![
                        crate::CommitCondition::Tail { path: StreamPath::new("agents/a")?, expected: 1 },
                        crate::CommitCondition::Absent { path: StreamPath::new("agents/b")? },
                    ],
                    mutations: ["agents/a", "agents/b"].into_iter().map(|path| Ok(CommitMutation::Append {
                        path: StreamPath::new(path)?, records: vec![Bytes::from_static(b"accepted")],
                    })).collect::<std::result::Result<Vec<_>, StreamError>>()?,
                    idempotency_key: IdempotencyKey::new(Bytes::from_static(b"compacting"))?,
                }).await
            }
        });
        if let Err(error) = tokio::task::spawn_blocking(move || waiting.recv_timeout(Duration::from_secs(1))).await? {
            if writing.is_finished() { return Err(format!("mutation ended before compaction: {:?}", writing.await?).into()); }
            return Err(error.into());
        }
        // One poll at a known held device cut establishes progress without a
        // timer or guessed number of idle polls. Drain/release before failing.
        let record = futures::poll!(records.next());
        let opened = futures::poll!(provider.follow_checked(path.clone(), 1, seed.commit_id));
        let child = futures::poll!(children.next());
        let ready = [record.is_ready(), opened.is_ready(), child.is_ready()].into_iter().filter(|ready| *ready).count();
        eprintln!("accepted_compaction_probes ready_before_device_release={ready}/3");
        if ready != 3 {
            release.send(())?;
            writing.await??;
            return Err("compaction delayed accepted publication".into());
        }
        let std::task::Poll::Ready(Some(record)) = record else { return Err("follow ended".into()) };
        let record: Record = record?;
        let std::task::Poll::Ready(opened) = opened else { unreachable!() };
        let mut resumed = opened?;
        let std::task::Poll::Ready(Some(child)) = child else { return Err("children ended".into()) };
        assert_eq!(child?.path.as_str(), "agents/b");
        drop(records);
        drop(children);
        let mut observing = tokio::spawn({
            let provider = provider.clone();
            let path = path.clone();
            async move {
                assert_eq!(record.sequence, 1);
                assert_eq!(resumed.next().await.ok_or("resume ended")??, record);
                let marks = provider.watermarks(path.clone()).await?;
                assert_eq!(marks, crate::StreamWatermarks { accepted: 2, durable: 1 });
                let mut head = provider.read(ReadRequest { path: path.clone(), from: 1, limit: 1 }).await?;
                assert_eq!(head.next().await.ok_or("head ended")??, record);
                #[cfg(feature = "grpc")]
                {
                    let relay = crate::grpc::tests::client(provider.clone())?;
                    assert_eq!(relay.watermarks(path.clone()).await?, marks);
                    let mut followed = relay.follow_checked(path, 1, seed.commit_id).await?;
                    assert_eq!(followed.next().await.ok_or("relay ended")??, record);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(started.elapsed())
            }
        });
        // Timeout bounds liveness only; the device remains gated until all
        // accepted observations finish. Release even if a regression blocks I/O.
        let observed = tokio::time::timeout(Duration::from_secs(1), &mut observing).await;
        let was_writing = !writing.is_finished();
        release.send(())?;
        writing.await??;
        match observed {
            Ok(value) => eprintln!("accepted_compaction_observations before_device_release=6 append_to_all_observers={:?}", value??),
            Err(error) => { observing.await??; return Err(error.into()); }
        }
        assert!(was_writing, "compaction completed before the device gate was released");
        assert_eq!(provider.watermarks(path).await?, crate::StreamWatermarks { accepted: 2, durable: 2 });
        assert_snapshot_restores(directory.path(), limits).await
    })
}
#[cfg(feature = "grpc")]
#[test]
fn append_to_follow_latency_with_frequent_compaction() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for transport in ["local", "grpc"] {
            let directory = tempfile::tempdir()?;
            let provider = LocalStream::open(directory.path(), LocalStreamLimits {
                journal_operations: 4, ..LocalStreamLimits::default()
            }).await?;
            let path = StreamPath::new("latency")?;
            provider.append(AppendRequest { path: path.clone(), records: vec![Bytes::from_static(b"seed")],
                if_tail: None, idempotency_key: None }).await?;
            let reader: Box<dyn StreamProvider> = if transport == "grpc" {
                Box::new(crate::grpc::tests::client(provider.clone())?)
            } else { Box::new(provider.clone()) };
            let mut records = reader.follow(path.clone(), 1).await?;
            let mut samples = Vec::new();
            for sequence in 1..=64 {
                let started = Instant::now();
                let writing = tokio::spawn({
                    let provider = provider.clone(); let path = path.clone();
                    async move { provider.append(AppendRequest {
                        path, records: vec![Bytes::from_static(b"delta")],
                        if_tail: Some(sequence), idempotency_key: None,
                    }).await }
                });
                let next = tokio::time::timeout(Duration::from_secs(1), records.next()).await?
                    .ok_or("latency follow ended")??;
                samples.push(started.elapsed().as_nanos());
                assert_eq!(next.sequence, sequence);
                writing.await??;
            }
            samples.sort_unstable();
            let (Some(p50), Some(p99)) = (samples.get(32), samples.get(63)) else {
                return Err("fewer than 64 samples".into());
            };
            eprintln!("compacting_append_to_follower transport={transport} samples=64 p50_ns={p50} p99_ns={p99}");
        }
        Ok(())
    })
}
