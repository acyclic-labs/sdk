use super::*;
use crate::grpc;
use crate::{AppendRequest, MemoryStream, OwnedBytes, RecordStream};
use futures::TryFutureExt as _;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tonic::Request;
use tonic::{Response as Reply, Status, Streaming, transport::Endpoint};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};
mod admissions;
mod allocations;
mod application;
mod checked;
mod checked_recovery;
mod connections;
mod context;
mod credential_progress;
mod credentials;
mod data;
mod isolation;
mod latest;
mod lifecycle;
mod local;
mod outbound;
mod reads;
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
        let client = match &self.tls_client {
            Some(client) => client.with_credential("fixture")?,
            None => grpc::Client::from_channel(self._channel.clone(), "Bearer fixture".parse()?),
        };
        client
            .session_client()?
            .register_credential(0, crate::session::testing::credential())?;
        Ok(client)
    }
}

async fn append(provider: &MemoryStream, path: &str, count: usize) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![(OwnedBytes::from_static(b"delta")).into(); count],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
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
    let mut slow = client
        .follow(crate::FollowRequest::new(StreamPath::new("slow")?, 1))
        .map_ok(crate::records_only)
        .await?;
    let mut fast = client
        .clone()
        .follow(crate::FollowRequest::new(StreamPath::new("fast")?, 1))
        .map_ok(crate::records_only)
        .await?;
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
    let mut fast = client
        .follow(crate::FollowRequest::new(StreamPath::new("fast")?, 1))
        .map_ok(crate::records_only)
        .await?;
    let mut refused = client
        .follow(crate::FollowRequest::new(StreamPath::new("missing")?, 0))
        .map_ok(crate::records_only)
        .await?;
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
                .follow(crate::FollowRequest::new(
                    StreamPath::new(format!("agents/{index}"))?,
                    1,
                ))
                .map_ok(crate::records_only)
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
        if node.node_id != "new" || node.endpoint != "https://new.invalid" {
            return Box::pin(future::ready(Err(StreamError::AccessDenied)));
        }
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
    let owner = grpc::Client::from_authenticated_channel(
        old._channel.clone(),
        grpc::CredentialProvider::new(|| future::ready(Ok("fixture".into()))),
        connect,
    );
    let client = owner.session_client()?;
    client.register_credential(0, crate::session::testing::credential())?;
    let mut moving = client
        .follow(crate::FollowRequest::new(StreamPath::new("move")?, 1))
        .map_ok(crate::records_only)
        .await?;
    let mut staying = client
        .follow(crate::FollowRequest::new(StreamPath::new("stay")?, 1))
        .map_ok(crate::records_only)
        .await?;
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
    // A route is an untrusted suggestion. Refusal must not fall back to a
    // different TLS policy or dial the suggested endpoint.
    new.controls.send(Ok(frame(
        1,
        2,
        Body::Moved(wire::SessionMoved {
            node: Some(wire::SessionNode {
                node_id: "foreign".into(),
                endpoint: "http://untrusted.invalid".into(),
                placement_epoch: 2,
            }),
            resume: Some(cursor(999)),
        }),
    )))?;
    assert!(
        matches!(next(&mut moving).await, Err(error) if error.downcast_ref::<StreamError>() == Some(&StreamError::AccessDenied))
    );
    assert_eq!(dials.load(Ordering::Relaxed), 1);
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
            records: vec![(OwnedBytes::from_static(b"seed")).into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let mut follow = provider
        .follow(crate::FollowRequest::new(path.clone(), 1))
        .map_ok(crate::records_only)
        .await?;
    let mut samples = Vec::new();
    for sequence in 1..=144 {
        let started = std::time::Instant::now();
        let (_, record, elapsed) = crate::test_latency::sample(
            async {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    provider
                        .append(AppendRequest {
                            path: path.clone(),
                            records: vec![(OwnedBytes::from_static(b"delta")).into()],
                            if_tail: None,
                            idempotency_key: crate::IdempotencyKey::unique(),
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
    let mut a = client
        .follow(crate::FollowRequest::new(StreamPath::new("a")?, 1))
        .map_ok(crate::records_only)
        .await?;
    let mut b = client
        .follow(crate::FollowRequest::new(StreamPath::new("b")?, 1))
        .map_ok(crate::records_only)
        .await?;
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
        Default::default(),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut cancelled = Box::pin(
        client
            .follow(crate::FollowRequest::new(StreamPath::new("a")?, 1))
            .map_ok(crate::records_only),
    );
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    let mut follow = client
        .follow(crate::FollowRequest::new(StreamPath::new("a")?, 1))
        .map_ok(crate::records_only)
        .await?;
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
    let mut records = client
        .follow(crate::FollowRequest::new(StreamPath::new("a")?, 1))
        .map_ok(crate::records_only)
        .await?;
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
    let mut records = client
        .follow(crate::FollowRequest::new(StreamPath::new("a")?, 1))
        .map_ok(crate::records_only)
        .await?;
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
    let mut healthy = client
        .follow(crate::FollowRequest::new(StreamPath::new("healthy")?, 1))
        .map_ok(crate::records_only)
        .await?;
    for sequence in 1..=crate::MAX_ITEMS + 8 {
        let before = fixture.counts.sent.load(Ordering::Relaxed);
        let unread = client
            .follow(crate::FollowRequest::new(StreamPath::new("unread")?, 0))
            .map_ok(crate::records_only)
            .await?;
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
        let first = client.follow(crate::FollowRequest::new(one, 0)).map_ok(crate::records_only);
        let second = client.follow(crate::FollowRequest::new(two, 0)).map_ok(crate::records_only);
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

mod tenk;
