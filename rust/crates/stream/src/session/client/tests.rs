use super::*;
use crate::grpc;
use crate::{AppendRequest, MemoryStream, OwnedBytes};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tonic::Request;
use tonic::{Response as Reply, Status, Streaming, transport::Endpoint};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};
mod admissions;
mod allocations;
mod checked;
mod checked_recovery;
mod connections;
mod context;
mod credentials;
mod data;
mod isolation;
mod latest;
mod lifecycle;
mod local;
mod outbound;
mod recovery;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
mod fixture {
    use crate as acyclic_stream;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/session.rs"
    ));
}
use fixture::{Counts, Fixture};
impl Fixture {
    pub(super) async fn open<P: StreamProvider>(
        provider: Arc<P>,
        node: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::configured(provider, node, false, true).await
    }
    pub(super) async fn configured<P: StreamProvider>(
        provider: Arc<P>,
        node: &str,
        withhold_welcome: bool,
        confirm_checked: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::with_options(
            provider,
            node,
            fixture::Options {
                withhold_welcome,
                confirm_checked,
                tls: false,
            },
        )
        .await
    }
    fn client(&self) -> Result<grpc::Client, Box<dyn std::error::Error + Send + Sync>> {
        match &self.tls_client {
            Some(client) => Ok(client.with_credential("fixture")?),
            None => Ok(grpc::Client::from_channel(
                self._channel.clone(),
                "Bearer fixture".parse()?,
            )),
        }
    }
}

async fn append(provider: &MemoryStream, path: &str, count: usize) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![OwnedBytes::from_static(b"delta"); count],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}
async fn next(
    records: &mut RecordStream,
) -> Result<crate::Record, Box<dyn std::error::Error + Send + Sync>> {
    Ok(tokio::time::timeout(Duration::from_secs(1), records.next())
        .await?
        .ok_or("follow ended")??)
}
#[tokio::test]
async fn public_follow_shares_one_session_and_slow_channels_do_not_block_others() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "slow", 1).await?;
    append(&provider, "fast", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let mut slow = client.follow(StreamPath::new("slow")?, 1).await?;
    let mut fast = client.clone().follow(StreamPath::new("fast")?, 1).await?;
    append(&provider, "slow", 20).await?;
    append(&provider, "fast", 20).await?;
    for sequence in 1..=20 {
        assert_eq!(next(&mut fast).await?.sequence, sequence);
    }
    for sequence in 1..=20 {
        assert_eq!(next(&mut slow).await?.sequence, sequence);
    }
    drop(slow);
    append(&provider, "fast", 1).await?;
    assert_eq!(next(&mut fast).await?.sequence, 21);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.follows.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    Ok(())
}
#[tokio::test]
async fn refused_channel_does_not_close_the_shared_session() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "fast", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let mut fast = client.follow(StreamPath::new("fast")?, 1).await?;
    let mut refused = client.follow(StreamPath::new("missing")?, 0).await?;
    assert_eq!(refused.next().await, Some(Err(StreamError::NotFound)));
    assert!(refused.next().await.is_none());
    append(&provider, "fast", 1).await?;
    assert_eq!(next(&mut fast).await?.sequence, 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}
#[tokio::test]
async fn node_subscription_fan_in_reuses_the_same_client_session_protocol() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    for index in 0..16 {
        append(&provider, &format!("agents/{index}"), 1).await?;
    }
    let owner = Fixture::open(Arc::clone(&provider), "owner").await?;
    let relay = Fixture::open(Arc::new(owner.client()?), "relay").await?;
    let client = relay.client()?;
    let mut follows = Vec::new();
    for index in 0..16 {
        follows.push(
            client
                .follow(StreamPath::new(format!("agents/{index}"))?, 1)
                .await?,
        );
    }
    for (index, follow) in follows.iter_mut().enumerate() {
        append(&provider, &format!("agents/{index}"), 1).await?;
        assert_eq!(next(follow).await?.sequence, 1);
    }
    for fixture in [&owner, &relay] {
        assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.follows.load(Ordering::Relaxed), 16);
        assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    }
    Ok(())
}

#[tokio::test]
async fn moved_resumes_applied_cursor_and_dialing_does_not_block_other_channels() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    for path in ["move", "stay"] {
        append(&provider, path, 1).await?;
    }
    let old = Fixture::open(Arc::clone(&provider), "old").await?;
    let new = Fixture::open(Arc::clone(&provider), "new").await?;
    let (release, blocked) = oneshot::channel();
    let blocked = Arc::new(Mutex::new(Some(blocked)));
    let channel = new._channel.clone();
    let dials = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&dials);
    let started = Arc::new(tokio::sync::Notify::new());
    let dialing = Arc::clone(&started);
    let connect: Connect = Arc::new(move |node| {
        assert_eq!(node.node_id, "new");
        observed.fetch_add(1, Ordering::Relaxed);
        dialing.notify_one();
        let gate = blocked.lock().ok().and_then(|mut gate| gate.take());
        let channel = channel.clone();
        Box::pin(async move {
            gate.ok_or(StreamError::Unavailable)?
                .await
                .map_err(|_| StreamError::Unavailable)?;
            Ok(channel)
        })
    });
    let client = Client::with_connector(
        old._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        connect,
    );
    let mut moving = client.follow(StreamPath::new("move")?, 1).await?;
    let mut staying = client.follow(StreamPath::new("stay")?, 1).await?;
    append(&provider, "move", 4).await?;
    assert_eq!(next(&mut moving).await?.sequence, 1);
    old.controls.send(Ok(frame(
        1,
        1,
        Body::Moved(wire::SessionMoved {
            node: Some(wire::SessionNode {
                node_id: "new".into(),
                endpoint: "https://new.invalid".into(),
                placement_epoch: 1,
            }),
            resume: Some(cursor(999)),
        }),
    )))?;
    tokio::time::timeout(Duration::from_secs(1), started.notified()).await?;
    // The destination dial is blocked. Unrelated follows must still be live.
    append(&provider, "stay", 1).await?;
    assert_eq!(next(&mut staying).await?.sequence, 1);
    release.send(()).map_err(|_| "dial gate dropped")?;
    tokio::time::timeout(
        Duration::from_secs(1),
        new.until(|counts| {
            counts
                .resumes
                .lock()
                .is_ok_and(|resumes| resumes.contains(&(1, 2, 2)))
        }),
    )
    .await?;
    assert_eq!(
        new.counts
            .resumes
            .lock()
            .map_err(|_| "resumes poisoned")?
            .as_slice(),
        &[(1, 2, 2)]
    );
    append(&provider, "move", 1).await?;
    for sequence in 2..=5 {
        assert_eq!(next(&mut moving).await?.sequence, sequence);
    }
    assert_eq!(dials.load(Ordering::Relaxed), 1);
    for fixture in [&old, &new] {
        assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    }
    Ok(())
}

#[tokio::test]
async fn append_to_follow_latency() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    measure(memory.as_ref(), "memory").await?;
    let fixture = Fixture::open(Arc::new(MemoryStream::default()), "node").await?;
    measure(&fixture.client()?, "grpc_tcp").await?;
    println!(
        "sessions={} connections={}",
        fixture.counts.sessions.load(Ordering::Relaxed),
        fixture.counts.connections.load(Ordering::Relaxed)
    );
    Ok(())
}
async fn measure<P: StreamProvider>(provider: &P, label: &str) -> TestResult {
    let path = StreamPath::new("latency")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"seed")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let mut follow = provider.follow(path.clone(), 1).await?;
    let mut samples = Vec::new();
    for sequence in 1..=144 {
        let started = std::time::Instant::now();
        let (_, record, elapsed) = crate::test_latency::sample(
            async {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    provider
                        .append(AppendRequest {
                            path: path.clone(),
                            records: vec![OwnedBytes::from_static(b"delta")],
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await?,
                )
            },
            next(&mut follow),
            || started.elapsed().as_nanos(),
        )
        .await?;
        assert_eq!(record.sequence, sequence);
        assert_eq!(record.value.as_slice(), b"delta");
        if sequence > 16 {
            samples.push(elapsed);
        }
    }
    samples.sort_unstable();
    println!(
        "append_to_follower {label} n={} p50_ns={} p99_ns={}",
        samples.len(),
        samples.get(63).ok_or("p50 missing")?,
        samples.get(126).ok_or("p99 missing")?
    );
    Ok(())
}

#[tokio::test]
async fn transport_reconnect_reopens_all_channels_from_consumed_cursors() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    for path in ["a", "b"] {
        append(&provider, path, 1).await?;
    }
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let mut a = client.follow(StreamPath::new("a")?, 1).await?;
    let mut b = client.follow(StreamPath::new("b")?, 1).await?;
    for path in ["a", "b"] {
        append(&provider, path, 4).await?;
    }
    assert_eq!(next(&mut a).await?.sequence, 1);
    assert_eq!(next(&mut b).await?.sequence, 1);
    fixture.reset()?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let changed = fixture.counts.changed.notified();
            if fixture
                .counts
                .resumes
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .len()
                >= 4
            {
                return Ok::<(), StreamError>(());
            }
            changed.await;
        }
    })
    .await??;
    let resumed = fixture
        .counts
        .resumes
        .lock()
        .map_err(|_| "resumes poisoned")?
        .iter()
        .copied()
        .filter(|(_, generation, _)| *generation == 2)
        .collect::<Vec<_>>();
    assert_eq!(resumed.len(), 2);
    assert!(resumed.iter().all(|(_, _, next)| *next == 2));
    for path in ["a", "b"] {
        append(&provider, path, 1).await?;
    }
    for sequence in 2..=5 {
        assert_eq!(next(&mut a).await?.sequence, sequence);
        assert_eq!(next(&mut b).await?.sequence, sequence);
    }
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn cancelling_a_queued_open_does_not_leave_an_idle_subscription() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "a", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = Client::open(
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        None,
        None,
    );
    let mut cancelled = Box::pin(client.follow(StreamPath::new("a")?, 1));
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    let mut follow = client.follow(StreamPath::new("a")?, 1).await?;
    append(&provider, "a", 1).await?;
    assert_eq!(next(&mut follow).await?.sequence, 1);
    assert_eq!(fixture.counts.follows.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn aggregate_credit_is_replenished_beyond_the_initial_window() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "a", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let mut records = client.follow(StreamPath::new("a")?, 1).await?;
    append(&provider, "a", crate::MAX_ITEMS).await?;
    append(&provider, "a", 17).await?;
    for sequence in 1..=u64::try_from(crate::MAX_ITEMS + 17)? {
        assert_eq!(next(&mut records).await?.sequence, sequence);
    }
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn translating_follow_releases_credit_without_application_acknowledgements() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "a", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?.without_delivery_acknowledgements();
    let mut records = client.follow(StreamPath::new("a")?, 1).await?;
    append(&provider, "a", 17).await?;
    for sequence in 1..=17 {
        assert_eq!(next(&mut records).await?.sequence, sequence);
    }
    assert_eq!(fixture.counts.acknowledgements.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn dropping_buffered_follows_returns_aggregate_capacity() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    append(&memory, "unread", 1).await?;
    append(&memory, "healthy", 1).await?;
    let fixture = Fixture::open(Arc::clone(&memory), "node").await?;
    let client = fixture.client()?;
    let mut healthy = client.follow(StreamPath::new("healthy")?, 1).await?;
    for sequence in 1..=crate::MAX_ITEMS + 8 {
        let before = fixture.counts.sent.load(Ordering::Relaxed);
        let unread = client.follow(StreamPath::new("unread")?, 0).await?;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let changed = fixture.counts.changed.notified();
                if fixture.counts.sent.load(Ordering::Relaxed) > before {
                    break;
                }
                changed.await;
            }
        })
        .await?;
        // This marker follows the unread data on the same physical response.
        // Consuming it proves the earlier delivery is buffered in the client.
        append(&memory, "healthy", 1).await?;
        assert_eq!(next(&mut healthy).await?.sequence, sequence as u64);
        drop(unread);
    }
    append(&memory, "healthy", 1).await?;
    assert_eq!(
        next(&mut healthy).await?.sequence,
        (crate::MAX_ITEMS + 9) as u64
    );
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_first_follows_have_a_bounded_welcome_deadline() -> TestResult {
    let fixture =
        Fixture::configured(Arc::new(MemoryStream::default()), "silent", true, true).await?;
    let client = fixture.client()?;
    let one = StreamPath::new("one")?;
    let two = StreamPath::new("two")?;
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        let first = client.follow(one, 0);
        let second = client.follow(two, 0);
        futures::pin_mut!(first, second);
        for attempt in 1..=3 {
            // Neither logical follow may finish before all physical handshake
            // attempts have begun. Ordering is independent of elapsed wall time.
            tokio::select! {
                biased;
                _ = fixture.until(|counts| counts.sessions.load(Ordering::Relaxed) >= attempt) => {}
                _ = &mut first => return Err("first follow ended before the final handshake attempt"),
                _ = &mut second => return Err("second follow ended before the final handshake attempt"),
            }
        }
        Ok(tokio::join!(first, second))
    })
    .await??;
    assert!(matches!(first, Err(StreamError::Unavailable)));
    assert!(matches!(second, Err(StreamError::Unavailable)));
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 3);
    Ok(())
}

#[tokio::test]
async fn child_replay_and_records_share_the_native_node_session() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "agents/m", 1).await?;
    append(&provider, "agents/a", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "children").await?;
    let client = fixture.client()?;
    let mut records = client.follow(StreamPath::new("agents/m")?, 1).await?;
    let mut children = client
        .children(crate::ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 8,
        })
        .await?;
    let mut paths = Vec::new();
    for _ in 0..2 {
        paths.push(
            tokio::time::timeout(Duration::from_secs(1), children.next())
                .await?
                .ok_or("discovery ended")??
                .path,
        );
    }
    paths.sort();
    assert_eq!(
        paths,
        vec![StreamPath::new("agents/a")?, StreamPath::new("agents/m")?]
    );
    append(&provider, "agents/m", 1).await?;
    assert_eq!(next(&mut records).await?.sequence, 1);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn largest_child_path_fits_its_channel_credit() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let path = "a".repeat(crate::MAX_PATH_BYTES);
    append(&provider, &path, 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "large-child").await?;
    let client = fixture.client()?;
    let mut children = client
        .children(crate::ChildrenRequest {
            parent: None,
            limit: 1,
        })
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), children.next())
            .await?
            .ok_or("maximum child stalled")??
            .path
            .as_str(),
        path
    );
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn children_move_and_reconnect_from_the_yielded_opaque_cursor() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    for path in ["agents/m", "agents/a", "healthy"] {
        append(&provider, path, 1).await?;
    }
    let old = Fixture::open(Arc::clone(&provider), "old").await?;
    let new = Fixture::open(Arc::clone(&provider), "new").await?;
    let channel = new._channel.clone();
    let connect: Connect = Arc::new(move |_| {
        let channel = channel.clone();
        Box::pin(async move { Ok(channel) })
    });
    let client = Client::with_connector(
        old._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        connect,
    );
    let mut children = client
        .children(crate::ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    let mut healthy = client.follow(StreamPath::new("healthy")?, 1).await?;
    assert_eq!(
        children.next().await.ok_or("missing child")??.path.as_str(),
        "agents/m"
    );
    old.controls.send(Ok(frame(
        1,
        1,
        Body::Moved(wire::SessionMoved {
            node: Some(wire::SessionNode {
                node_id: "new".into(),
                endpoint: "https://new.invalid".into(),
                placement_epoch: 1,
            }),
            resume: Some(cursor(999)),
        }),
    )))?;
    tokio::time::timeout(
        Duration::from_secs(1),
        new.until(|counts| {
            counts
                .resumes
                .lock()
                .is_ok_and(|resumes| resumes.contains(&(1, 2, 1)))
        }),
    )
    .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), children.next())
            .await?
            .ok_or("missing resumed child")??
            .path
            .as_str(),
        "agents/a"
    );
    append(&provider, "agents/0", 1).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), children.next())
            .await?
            .ok_or("discovery ended")??
            .path
            .as_str(),
        "agents/0"
    );
    new.controls
        .send(Err(Status::unavailable("disconnect child session")))?;
    loop {
        let changed = new.counts.changed.notified();
        if new
            .counts
            .resumes
            .lock()
            .map_err(|_| "resumes poisoned")?
            .contains(&(1, 3, 3))
        {
            break;
        }
        tokio::time::timeout(Duration::from_secs(1), changed).await?;
    }
    append(&provider, "agents/b", 1).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), children.next())
            .await?
            .ok_or("reconnect ended")??
            .path
            .as_str(),
        "agents/b"
    );
    assert!(
        new.counts
            .resumes
            .lock()
            .map_err(|_| "resumes poisoned")?
            .contains(&(1, 2, 1))
    );
    assert!(
        new.counts
            .resumes
            .lock()
            .map_err(|_| "resumes poisoned")?
            .contains(&(1, 3, 3))
    );
    append(&provider, "healthy", 1).await?;
    assert_eq!(next(&mut healthy).await?.sequence, 1);
    for fixture in [&old, &new] {
        assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    }
    Ok(())
}

mod tenk;
