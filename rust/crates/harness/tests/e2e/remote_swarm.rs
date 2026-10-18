//! A native SwarmClient followed through a Streams provider's Session. Two
//! agents publish a status and a streamed reply under one swarm root. A layout
//! selects the first agent. The client must discover both, follow both
//! statuses, and assemble the selected agent's reply. Every payload names the
//! agent that published it, and the final views must be exactly the expected
//! ones; they are also the trace every provider must reproduce.
use super::TestResult;
use acyclic_harness::{
    TaskId,
    client::{ClientViewLimits, QueryViewLimits, SwarmClient, SwarmClientLimits},
    task_volume::{Channel, ChildState, ChildStatus, FileKey, Query, TaskFile, TaskVolume, Write},
};
use acyclic_stream::{AppendRequest, StreamPath, StreamProvider, session, wire};
use futures::{FutureExt as _, channel::oneshot};
use prost::Message as _;
use serde_json::{Value, json};
use std::sync::Arc;

const AGENTS: usize = 2;

fn path(name: &str) -> TestResult<StreamPath> {
    Ok(StreamPath::new(name)?)
}

fn agent(root: &str, id: usize) -> TestResult<StreamPath> {
    path(&format!("{root}/a{id:05}"))
}

/// Agent `id`'s streamed reply: `reply`, tagged with the agent's name.
fn reply_of(id: usize, reply: &str) -> String {
    format!("a{id:05}:{reply}")
}

/// Agent `id`'s exact LATEST status value.
fn status_of(id: usize) -> TestResult<Vec<u8>> {
    Ok(serde_json::to_vec(&json!({"state": "idle", "agent": id}))?)
}

/// The exact views of a swarm seeded with `reply`, with the root stripped:
/// both agents, each one's own status (the second needing attention), and
/// the first agent's reply.
fn expected(reply: &str) -> TestResult<Value> {
    let mut status = serde_json::Map::new();
    for id in 0..AGENTS {
        let value = json!([{
            "key": "status",
            "value": String::from_utf8(status_of(id)?)?,
            "attention": id == 1,
        }]);
        status.insert(format!("/a{id:05}"), value);
    }
    Ok(json!({
        "agents": (0..AGENTS).map(|id| format!("/a{id:05}")).collect::<Vec<_>>(),
        "status": status,
        "selected": [{
            "key": head()?,
            "segments": [reply_of(0, reply), reply_of(0, LIVE)],
        }],
    }))
}

const CHUNKS: &str = ".tasks/t/turns/1/chunks";
const CHUNK: &str = ".tasks/t/turns/1/chunks/1";
const LIVE: &str = " delivered after snapshot";

/// The Text head a client views `CHUNK` under.
fn head() -> TestResult<FileKey> {
    let FileKey::Task {
        task,
        file: TaskFile::Chunk { turn, .. },
    } = FileKey::parse(CHUNK)?
    else {
        return Err(format!("{CHUNK} is not a chunk").into());
    };
    Ok(FileKey::Task {
        task,
        file: TaskFile::TextHead {
            turn,
            channel: Channel::Text,
        },
    })
}

fn client(root: &str) -> TestResult<SwarmClient> {
    Ok(SwarmClient::new(
        path(root)?,
        SwarmClientLimits {
            agents: AGENTS,
            selected: 1,
            layout_bytes: 1024,
            config_files: 4,
            snapshot_files: 16,
        },
        ClientViewLimits {
            agents: AGENTS,
            selected: 1,
            overview_values: AGENTS,
            overview_bytes: 4096,
            query: QueryViewLimits {
                files: 16,
                segments: 16,
                bytes: 4096,
            },
        },
    )?)
}

/// The volumes a SwarmClient follows: the root's membership, the client's
/// layout, and each agent's files.
struct Volumes<P> {
    parent: TaskVolume<P>,
    config: TaskVolume<P>,
    agents: Vec<TaskVolume<P>>,
}

async fn volumes<P: StreamProvider + 'static>(
    stream: &Arc<P>,
    root: &str,
) -> TestResult<Volumes<P>> {
    let mut agents = Vec::with_capacity(AGENTS);
    for id in 0..AGENTS {
        let files = path(&format!("{}/files", agent(root, id)?.as_str()))?;
        agents.push(TaskVolume::open(stream.clone(), files).await?);
    }
    Ok(Volumes {
        parent: TaskVolume::open(stream.clone(), path(&format!("{root}/files"))?).await?,
        config: TaskVolume::open(stream.clone(), path(&format!("{root}-config/files"))?).await?,
        agents,
    })
}

async fn put<P: StreamProvider + ?Sized>(volume: &TaskVolume<P>, write: &Write) -> TestResult {
    volume.append(volume.head().await?, write).await?;
    Ok(())
}

/// Publishes every agent's membership, status and reply, and a layout
/// selecting the first agent. Agent `id` publishes the payloads of agent
/// `author(id)`, so a negative control can cross them, and publishes its
/// tagged reply as one chunk, or split at `split` bytes into two.
async fn seed<P: StreamProvider + 'static>(
    stream: &Arc<P>,
    volumes: &Volumes<P>,
    root: &str,
    reply: &str,
    (author, split): (impl Fn(usize) -> usize, Option<usize>),
) -> TestResult {
    for (id, files) in volumes.agents.iter().enumerate() {
        let tagged = reply_of(author(id), reply);
        let chunks = match split.and_then(|at| tagged.split_at_checked(at)) {
            Some((first, second)) => vec![first, second],
            None => vec![tagged.as_str()],
        };
        for (index, text) in chunks.into_iter().enumerate() {
            let key = FileKey::parse(&format!("{CHUNKS}/{}", index + 1))?;
            let mut chunk = Write::new(key, text.as_bytes().to_vec());
            chunk.channel = Some(Channel::Text);
            put(files, &chunk).await?;
        }
        let status = wire::SessionLatestValue {
            key: b"status".to_vec().into(),
            value: status_of(author(id))?.into(),
            attention: author(id) == 1,
        };
        stream
            .append(AppendRequest {
                path: path(&format!("{}/status", agent(root, id)?.as_str()))?,
                records: vec![status.encode_to_vec().into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let member = ChildStatus {
            task: TaskId::from_bytes([1; 16]),
            state: ChildState::Running,
            settled: false,
            inputs: 1,
        };
        put(
            &volumes.parent,
            &Write::new(
                FileKey::parse(&format!("agents/a{id:05}/status.json"))?,
                serde_json::to_vec(&member)?,
            ),
        )
        .await?;
    }
    let selected = agent(root, 0)?;
    put(
        &volumes.config,
        &Write::new(
            FileKey::parse("workspace/layout.json")?,
            serde_json::to_vec(&json!({"in_view": [selected.as_str()]}))?,
        ),
    )
    .await
}

/// Follows a swarm seeded with `reply` under `root` on `stream`, through
/// `session`, until the views hold both agents' statuses and the selected
/// agent's reply. Returns those views with the root stripped, once they are
/// exactly the expected views.
pub(super) async fn follow<P: StreamProvider + 'static>(
    stream: Arc<P>,
    session: &session::Client,
    root: &str,
    reply: &str,
) -> TestResult<Value> {
    follow_publishing(stream, session, root, reply, (|id| id, None)).await
}

/// `follow`, with agent `id` publishing the payloads of agent `author(id)`,
/// its reply split at `split` bytes when given.
pub(super) async fn follow_publishing<P: StreamProvider + 'static>(
    stream: Arc<P>,
    session: &session::Client,
    root: &str,
    reply: &str,
    publish: (impl Fn(usize) -> usize, Option<usize>),
) -> TestResult<Value> {
    let volumes = volumes(&stream, root).await?;
    let index = if publish.1.is_some() { 3 } else { 2 };
    let live = reply_of(0, LIVE);
    let mut live_chunk = Write::new(
        FileKey::parse(&format!("{CHUNKS}/{index}"))?,
        live.as_bytes().to_vec(),
    );
    live_chunk.channel = Some(Channel::Text);
    seed(&stream, &volumes, root, reply, publish).await?;
    let client = client(root)?;
    let views = client.views();
    let (done, completed) = oneshot::channel();
    let mut done = Some(done);
    let mut published = false;
    let agents = (0..AGENTS)
        .map(|id| agent(root, id))
        .collect::<TestResult<Vec<_>>>()?;
    let ready = |views: &acyclic_harness::client::ClientViews, length: usize| {
        views.agents().eq(agents.iter())
            && agents.iter().all(|agent| {
                views
                    .status(agent)
                    .is_some_and(|status| status.values().next().is_some())
            })
            && views
                .files(&agents[0])
                .any(|file| file.length == length as u64)
    };
    let followed = client
        .follow(
            &volumes.config,
            &volumes.parent,
            session,
            |agent, maximum| {
                let volume = agents
                    .iter()
                    .position(|known| *known == agent)
                    .and_then(|id| volumes.agents.get(id));
                async move {
                    let volume = volume.ok_or(acyclic_harness::task_volume::Error::InvalidPath)?;
                    volume.heads(&Query::Root, None, maximum).await
                }
            },
            async |_| {
                // Publish only after the initial snapshot is installed. This
                // chunk must arrive through the selected content subscription.
                if !published && views.read(|views| ready(views, reply_of(0, reply).len()))? {
                    published = true;
                    put(&volumes.agents[0], &live_chunk)
                        .await
                        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
                }
                if published
                    && views.read(|views| ready(views, reply_of(0, reply).len() + live.len()))?
                    && let Some(done) = done.take()
                {
                    let _ = done.send(());
                }
                Ok(())
            },
        )
        .fuse();
    let completed = completed.fuse();
    futures::pin_mut!(followed, completed);
    let outcome = super::stall::guarded(
        std::path::Path::new("remote swarm"),
        super::stall::waiting("swarm views", async {
            futures::select! {
                result = followed => Err(format!("the swarm follow ended: {result:?}")),
                _ = completed => Ok(()),
            }
        }),
    )
    .await;
    outcome??;
    let strip = |agent: &StreamPath| agent.as_str().trim_start_matches(root).to_owned();
    let observed = views.read(|views| {
        json!({
            "agents": views.agents().map(strip).collect::<Vec<_>>(),
            "status": agents.iter().map(|agent| {
                let values: Vec<Value> = views.status(agent).into_iter().flat_map(|status| status.values()).map(|value| json!({
                    "key": String::from_utf8_lossy(&value.key),
                    "value": String::from_utf8_lossy(&value.value),
                    "attention": value.attention,
                })).collect();
                (strip(agent), Value::Array(values))
            }).collect::<serde_json::Map<_, _>>(),
            "selected": views.files(&agents[0]).map(|file| json!({
                "key": file.key,
                "segments": file.segments.iter().map(|segment| String::from_utf8_lossy(segment.as_ref()).into_owned()).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    })?;
    if observed != expected(reply)? {
        return Err(format!("the swarm views are {observed}").into());
    }
    Ok(observed)
}

/// How a SwarmClient over `session` is refused before it observes anything.
/// The volumes are opened, never published to, so a provider that refuses
/// head-fenced publication still reaches the client's own discovery.
pub(super) async fn refused<P: StreamProvider + 'static>(
    stream: Arc<P>,
    session: &session::Client,
    root: &str,
) -> TestResult<acyclic_harness::Error> {
    let volumes = volumes(&stream, root).await?;
    let followed = client(root)?.follow(
        &volumes.config,
        &volumes.parent,
        session,
        |_, _| async { Err(acyclic_harness::task_volume::Error::InvalidPath) },
        async |_| {
            Err(acyclic_harness::Error::Invalid(
                "a refused swarm observed a change".into(),
            ))
        },
    );
    match super::stall::guarded(
        std::path::Path::new("remote swarm refusal"),
        super::stall::waiting("swarm refusal", followed),
    )
    .await?
    {
        Err(error) => Ok(error),
        Ok(()) => Err("a refused swarm follow ended cleanly".into()),
    }
}
