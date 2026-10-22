use super::*;
use crate::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitCondition, CommitId,
    CommitMutation, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest,
    IdempotencyKey, IdempotencyObservation, ReadRequest, StreamBounds,
};
use futures::{
    stream::{FuturesUnordered, SelectAll},
    task::{ArcWake, AtomicWaker, waker_ref},
};
use std::time::Instant;

const AGENTS: usize = 10_000;
const NODES: usize = 8;

struct Owners {
    memory: Arc<MemoryStream>,
    nodes: Vec<Option<grpc::Client>>,
}
#[async_trait::async_trait]
impl StreamProvider for Owners {
    fn operation_claims(&self) -> &crate::OperationClaims {
        self.memory.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.memory.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.memory.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.memory.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.memory.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.memory.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.memory.read(request).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.memory.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.memory.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.memory.read_commit(id).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        let index = path
            .as_str()
            .strip_prefix("agents/")
            .and_then(|name| name.parse::<usize>().ok())
            .ok_or(StreamError::InvalidPath)?;
        match &self.nodes[index % NODES] {
            Some(node) => {
                node.follow(path, from, crate::FollowFilter::default())
                    .await
            }
            None => {
                self.memory
                    .follow(path, from, crate::FollowFilter::default())
                    .await
            }
        }
    }
}

async fn agent(
    memory: Arc<MemoryStream>,
    paths: Arc<Vec<StreamPath>>,
    id: usize,
    epoch: Instant,
) -> TestResult {
    let own = paths.get(id).ok_or("agent has no path")?.clone();
    let peer = paths.get(id ^ 1).ok_or("agent has no peer")?.clone();
    let first = id.is_multiple_of(2);
    if !first {
        let mut inbox = memory
            .follow(own.clone(), 1, crate::FollowFilter::default())
            .await?;
        assert_eq!(next(&mut inbox).await?.sequence, 1);
    }
    let stamp = u64::try_from(epoch.elapsed().as_nanos())?;
    let mut value = stamp.to_le_bytes().to_vec();
    value.extend_from_slice(&u32::try_from(id)?.to_le_bytes());
    let expected = if first { 1 } else { 2 };
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: own.clone(),
                expected,
            },
            CommitCondition::Tail {
                path: peer.clone(),
                expected,
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: own.clone(),
                records: vec![(OwnedBytes::from(value.clone())).into()],
            },
            CommitMutation::Append {
                path: peer,
                records: vec![(OwnedBytes::from(value)).into()],
            },
        ],
        idempotency_key: IdempotencyKey::new(format!("agent-{id}").into_bytes())?,
    };
    assert!(matches!(
        memory.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    if first {
        let mut reply = memory
            .follow(own, 2, crate::FollowFilter::default())
            .await?;
        assert_eq!(next(&mut reply).await?.sequence, 2);
    }
    Ok(())
}

async fn run_swarm<P: StreamProvider>(
    provider: &P,
    memory: Arc<MemoryStream>,
    label: &str,
) -> TestResult {
    let paths = Arc::new(
        (0..AGENTS)
            .map(|id| StreamPath::new(format!("agents/{id}")))
            .collect::<Result<Vec<_>, _>>()?,
    );
    for path in paths.iter() {
        append(&memory, path.as_str(), 1).await?;
    }
    let opens = stream::iter(paths.iter().cloned().enumerate())
        .map(|(id, path)| async move {
            Ok::<_, StreamError>(
                provider
                    .follow(path, 1, crate::FollowFilter::default())
                    .await?
                    .map(move |record| (id, record))
                    .boxed(),
            )
        })
        .buffer_unordered(64);
    futures::pin_mut!(opens);
    let mut followers = SelectAll::new();
    while let Some(records) = opens.next().await {
        followers.push(records?);
    }
    assert_eq!(followers.len(), AGENTS);
    let epoch = Instant::now();
    let mut agents = (0..AGENTS)
        .map(|id| agent(Arc::clone(&memory), Arc::clone(&paths), id, epoch))
        .collect::<FuturesUnordered<_>>();
    let produce = async move {
        let mut completed = 0;
        while let Some(result) = agents.next().await {
            result?;
            completed += 1;
            tokio::task::yield_now().await;
        }
        assert_eq!(completed, AGENTS);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let consume = async {
        let mut offsets = vec![1; AGENTS];
        let mut senders = vec![[false; 2]; AGENTS];
        let mut latencies = Vec::with_capacity(2 * AGENTS);
        for _ in 0..2 * AGENTS {
            let (id, record) = followers
                .next()
                .await
                .ok_or("follow ended before the expected prefix")?;
            let record = record?;
            let offset = offsets.get_mut(id).ok_or("follower has no offset")?;
            assert_eq!(record.sequence, *offset);
            *offset += 1;
            assert_eq!(record.value.len(), 12);
            let Some((stamp, sender)) = record.value.split_first_chunk::<8>() else {
                return Err("record shorter than its stamp".into());
            };
            let stamp = u64::from_le_bytes(*stamp);
            let sender = usize::try_from(u32::from_le_bytes(sender.try_into()?))?;
            assert!(sender == id || sender == id ^ 1);
            let seen = senders.get_mut(id).ok_or("follower has no senders")?;
            let seen = if sender == id {
                &mut seen[0]
            } else {
                &mut seen[1]
            };
            assert!(!*seen);
            *seen = true;
            latencies.push(u64::try_from(epoch.elapsed().as_nanos())?.saturating_sub(stamp));
        }
        assert!(offsets.iter().all(|next| *next == 3));
        assert!(senders.iter().all(|seen| *seen == [true, true]));
        assert_eq!(latencies.len(), 2 * AGENTS);
        latencies.sort_unstable();
        eprintln!(
            "swarm_delta {label} agents={AGENTS} records={} p50_ns={} p99_ns={}",
            latencies.len(),
            latencies.get(latencies.len() / 2).ok_or("no latencies")?,
            latencies
                .get(latencies.len() * 99 / 100)
                .ok_or("no latencies")?
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    // Independent tasks give the agent driver and follower their own runtime
    // cooperative budgets, as separate agent/client runtimes have in service.
    let (produced, consumed) = tokio::join!(tokio::spawn(produce), consume);
    produced??;
    consumed?;
    assert_eq!(followers.len(), AGENTS);
    // Consume completes before the producer's last checkpoint. Keep every
    // stream open until both tasks finish, then drain registered wakes directly
    // so extra data cannot hide behind SelectAll's cooperative polling budget.
    let mut followers = followers.into_iter().collect::<Vec<_>>();
    assert!(
        quiescent(&mut followers).await,
        "follow yielded extra data, an error or EOF after the expected prefix"
    );
    drop(followers);
    Ok(())
}

#[tokio::test]
async fn ten_thousand_agents_use_stream_transactions_and_one_session_per_node() -> TestResult {
    // Work and byte counts are the property; latencies are only reported. A
    // wall-clock bound would fail a correct run on a loaded host.
    async {
        let local = Arc::new(MemoryStream::default());
        run_swarm(local.as_ref(), Arc::clone(&local), "memory").await?;
        let memory = Arc::new(MemoryStream::default());
        let mut owners = Vec::new();
        let mut nodes = vec![None];
        for id in 1..NODES {
            let fixture = Fixture::open(Arc::clone(&memory), &format!("node-{id}")).await?;
            nodes.push(Some(fixture.client()?));
            owners.push(fixture);
        }
        let routed = Arc::new(Owners {
            memory: Arc::clone(&memory),
            nodes,
        });
        let edge = Fixture::open(routed, "node-0").await?;
        let client = edge.client()?;
        run_swarm(&client, Arc::clone(&memory), "grpc-eight-nodes").await?;
        for (node, follows) in
            std::iter::once((&edge, AGENTS)).chain(owners.iter().map(|node| (node, AGENTS / NODES)))
        {
            // Include cancellation/credit frames after the last application yield.
            loop {
                let changed = node.counts.changed.notified();
                if node.counts.closed_sessions.load(Ordering::Relaxed) == 1 {
                    break;
                }
                changed.await;
            }
            assert_eq!(node_work(&node.counts, follows), Ok(()));
            eprintln!(
                "swarm_bytes follows={follows} received={} transmitted={} budget={}",
                node.counts.received_bytes.load(Ordering::Relaxed),
                node.counts.transmitted_bytes.load(Ordering::Relaxed),
                byte_budget(follows)
            );
        }
        eprintln!(
            "swarm_connections client=1 node_pair={} total={} actor_channels=0",
            NODES - 1,
            NODES
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await?;
    Ok(())
}

// Paths are at most "agents/9999"; each follow carries two 12-byte records.
// One KiB covers the bidirectional open/data/ack/credit/remove exchange;
// 256 bytes cover Hello/Welcome and aggregate-window overhead.
const SESSION_BYTES: usize = 256;
const BYTES_PER_FOLLOW: usize = 1024;
fn byte_budget(follows: usize) -> usize {
    SESSION_BYTES + BYTES_PER_FOLLOW * follows
}

fn node_work(counts: &Counts, follows: usize) -> Result<(), &'static str> {
    if counts.sessions.load(Ordering::Relaxed) != 1 {
        return Err("sessions per node");
    }
    if counts.connections.load(Ordering::Relaxed) != 1 {
        return Err("connections per node");
    }
    if counts.follows.load(Ordering::Relaxed) != follows {
        return Err("follow count");
    }
    if counts.actors.load(Ordering::Relaxed) != 0 {
        return Err("actor channels");
    }
    let received = counts.received_bytes.load(Ordering::Relaxed);
    let transmitted = counts.transmitted_bytes.load(Ordering::Relaxed);
    if received == 0 || transmitted == 0 {
        return Err("missing byte accounting");
    }
    if received + transmitted > byte_budget(follows) {
        return Err("bytes per follow");
    }
    Ok(())
}

#[tokio::test]
async fn unshared_session_control_is_rejected_by_the_same_node_work_counters() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    append(&memory, "agents/0", 1).await?;
    let fixture = Fixture::open(memory, "unshared").await?;
    // Distinct SDK clients prevent Session cache sharing. The fixture's transport
    // still shares its TCP connection, and four interests suffice for rejection.
    let clients = (0..4)
        .map(|_| fixture.client())
        .collect::<Result<Vec<_>, _>>()?;
    let mut follows = Vec::new();
    for client in &clients {
        follows.push(
            client
                .follow(
                    StreamPath::new("agents/0")?,
                    1,
                    crate::FollowFilter::default(),
                )
                .await?,
        );
    }
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 4);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.follows.load(Ordering::Relaxed), 4);
    assert_eq!(node_work(&fixture.counts, 4), Err("sessions per node"));
    drop(follows);
    Ok(())
}

#[tokio::test]
async fn redundant_control_traffic_is_rejected_by_the_same_linear_byte_budget() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    append(&memory, "agents/0", 1).await?;
    let fixture = Fixture::open(Arc::clone(&memory), "redundant").await?;
    let client = fixture.client()?;
    let mut follow = client
        .follow(
            StreamPath::new("agents/0")?,
            1,
            crate::FollowFilter::default(),
        )
        .await?;
    assert_eq!(node_work(&fixture.counts, 1), Ok(()));
    let redundant = frame(0, 0, Body::Window(wire::SessionWindow::default()));
    // Valid zero-credit controls do not change delivery or exhaust a queue.
    // Wait on each publication so the broadcast fixture cannot overflow.
    for _ in 0..=byte_budget(1) / (5 + redundant.encoded_len()) {
        let before = fixture.counts.transmitted_bytes.load(Ordering::Relaxed);
        fixture.controls.send(Ok(redundant.clone()))?;
        loop {
            let changed = fixture.counts.changed.notified();
            if fixture.counts.transmitted_bytes.load(Ordering::Relaxed) > before {
                break;
            }
            changed.await;
        }
    }
    append(&memory, "agents/0", 1).await?;
    assert_eq!(next(&mut follow).await?.sequence, 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(node_work(&fixture.counts, 1), Err("bytes per follow"));
    Ok(())
}

#[derive(Default)]
struct WakeCount {
    wakes: AtomicUsize,
    caller: AtomicWaker,
}
impl ArcWake for WakeCount {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.wakes.fetch_add(1, Ordering::SeqCst);
        wake.caller.wake();
    }
}

pub(super) async fn quiescent<S: futures::Stream + Unpin>(followers: &mut [S]) -> bool {
    let wake = Arc::new(WakeCount::default());
    futures::future::poll_fn(|caller| {
        wake.caller.register(caller.waker());
        let before = wake.wakes.load(Ordering::SeqCst);
        let waker = waker_ref(&wake);
        let mut context = std::task::Context::from_waker(&waker);
        for follower in followers.iter_mut() {
            if std::pin::Pin::new(follower)
                .poll_next(&mut context)
                .is_ready()
            {
                return std::task::Poll::Ready(false);
            }
        }
        // Every previously pending wake was driven by this pass. A new wake
        // requires another pass; only Pending without new work is quiescent.
        if wake.wakes.load(Ordering::SeqCst) == before {
            std::task::Poll::Ready(true)
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

#[tokio::test]
async fn quiescence_rejects_extra_deliveries_and_truncated_follow_controls() -> TestResult {
    for (extra, truncate) in [(false, false), (true, false), (false, true), (true, true)] {
        let memory = MemoryStream::default();
        append(&memory, "probe", if extra { 3 } else { 2 }).await?;
        let follow = memory
            .follow(StreamPath::new("probe")?, 0, crate::FollowFilter::default())
            .await?;
        let mut follow = if truncate {
            follow.take(2).boxed()
        } else {
            follow
        };
        for sequence in 0..2 {
            assert_eq!(next(&mut follow).await?.sequence, sequence);
        }
        assert_eq!(
            quiescent(&mut [follow]).await,
            !extra && !truncate,
            "extra delivery or a take(2) EOF must fail the same quiescence check"
        );
    }
    Ok(())
}

#[tokio::test]
async fn quiescence_drives_self_wakes_and_rejects_a_third_poll_extra() {
    for extra in [false, true] {
        let polls = std::cell::Cell::new(0);
        let delayed = stream::poll_fn(|context| {
            polls.set(polls.get() + 1);
            if polls.get() < 3 {
                context.waker().wake_by_ref();
                std::task::Poll::Pending
            } else if extra {
                std::task::Poll::Ready(Some(()))
            } else {
                std::task::Poll::Pending
            }
        });
        assert_eq!(quiescent(&mut [delayed]).await, !extra);
        assert_eq!(
            polls.get(),
            3,
            "all scheduled work must be driven before deciding"
        );
    }
}
