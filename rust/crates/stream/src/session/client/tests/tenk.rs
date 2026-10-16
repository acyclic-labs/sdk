use super::*;
use crate::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitCondition, CommitId,
    CommitMutation, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest,
    IdempotencyKey, IdempotencyObservation, ReadRequest, StreamBounds,
};
use futures::stream::{FuturesUnordered, SelectAll};
use std::time::Instant;

const AGENTS: usize = 10_000;
const NODES: usize = 8;

struct Owners {
    memory: Arc<MemoryStream>,
    nodes: Vec<Option<grpc::Client>>,
}
#[async_trait::async_trait]
impl StreamProvider for Owners {
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
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let index = path
            .as_str()
            .strip_prefix("agents/")
            .and_then(|name| name.parse::<usize>().ok())
            .ok_or(StreamError::InvalidPath)?;
        match &self.nodes[index % NODES] {
            Some(node) => node.follow(path, from).await,
            None => self.memory.follow(path, from).await,
        }
    }
}

async fn agent(
    memory: Arc<MemoryStream>,
    paths: Arc<Vec<StreamPath>>,
    id: usize,
    epoch: Instant,
) -> TestResult {
    let own = paths[id].clone();
    let peer = paths[id ^ 1].clone();
    let first = id % 2 == 0;
    if !first {
        let mut inbox = memory.follow(own.clone(), 1).await?;
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
                records: vec![OwnedBytes::from(value.clone())],
            },
            CommitMutation::Append {
                path: peer,
                records: vec![OwnedBytes::from(value)],
            },
        ],
        idempotency_key: IdempotencyKey::new(format!("agent-{id}").into_bytes())?,
    };
    assert!(matches!(
        memory.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    if first {
        let mut reply = memory.follow(own, 2).await?;
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
                    .follow(path, 1)
                    .await?
                    .take(2)
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
        while let Some((id, record)) = followers.next().await {
            let record = record?;
            assert_eq!(record.sequence, offsets[id]);
            offsets[id] += 1;
            assert_eq!(record.value.len(), 12);
            let stamp = u64::from_le_bytes(record.value[..8].try_into()?);
            let sender = u32::from_le_bytes(record.value[8..].try_into()?) as usize;
            assert!(sender == id || sender == id ^ 1);
            let slot = usize::from(sender != id);
            assert!(!senders[id][slot]);
            senders[id][slot] = true;
            latencies.push(u64::try_from(epoch.elapsed().as_nanos())?.saturating_sub(stamp));
        }
        assert!(offsets.iter().all(|next| *next == 3));
        assert!(senders.iter().all(|seen| *seen == [true, true]));
        assert_eq!(latencies.len(), 2 * AGENTS);
        latencies.sort_unstable();
        eprintln!(
            "swarm_delta {label} agents={AGENTS} records={} p50_ns={} p99_ns={}",
            latencies.len(),
            latencies[latencies.len() / 2],
            latencies[latencies.len() * 99 / 100]
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    // Independent tasks give the agent driver and follower their own runtime
    // cooperative budgets, as separate agent/client runtimes have in service.
    let (produced, consumed) = tokio::join!(tokio::spawn(produce), consume);
    produced??;
    consumed?;
    Ok(())
}

#[tokio::test]
async fn ten_thousand_agents_use_stream_transactions_and_one_session_per_node() -> TestResult {
    tokio::time::timeout(Duration::from_secs(8), async {
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
        assert_eq!(edge.counts.follows.load(Ordering::Relaxed), AGENTS);
        for node in std::iter::once(&edge).chain(owners.iter()) {
            assert_eq!(node.counts.connections.load(Ordering::Relaxed), 1);
            assert_eq!(node.counts.sessions.load(Ordering::Relaxed), 1);
            assert_eq!(node.counts.actors.load(Ordering::Relaxed), 0);
        }
        for node in &owners {
            assert_eq!(node.counts.follows.load(Ordering::Relaxed), AGENTS / NODES);
        }
        eprintln!(
            "swarm_connections client=1 node_pair={} total={} actor_channels=0",
            NODES - 1,
            NODES
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
