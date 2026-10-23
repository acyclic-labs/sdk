//! Subagent fan-out followed over a Streams provider's Session. One root holds
//! an agent volume per subagent. A live children subscription, opened before
//! any agent exists, discovers each one. A live checked follow of each agent,
//! opened at its first cut, receives that agent's streamed reply and final
//! status. A reopened volume resumes after the first chunk, and a forged
//! cursor resets.
use super::TestResult;
use acyclic_harness::task_volume::{
    Change, ChangeStream, Channel, Error as VolumeError, FileKey, Generation, Query, TaskVolume,
    Write,
};
use acyclic_stream::{
    AppendRequest, ChildrenRequest, ForkRequest, MemoryStream, StreamError, StreamPath,
    StreamProvider, grpc::Client, wire,
};
use futures::{Stream, StreamExt as _};
use prost::Message as _;
use serde_json::{Value, json};
use std::sync::Arc;

/// Each subagent's task: its streamed reply and its final status file.
const CHUNKS: &str = ".tasks/t/turns/1/chunks";
const STATUS: &str = ".tasks/t/status.json";

/// What a provider must serve for harness subagents, as `refused` names it.
pub(super) const KINDS: [&str; 3] = ["head-fenced publication", "checked follow", "children"];
/// What a Session must serve for swarm status follows, as `latest` probes it.
pub(super) const LATEST: &str = "latest delivery";

/// A refusal: the kind refused and the provider's typed cause.
pub(super) type Refusal = (&'static str, StreamError);

include!("../support/schema/subagents.rs");

/// Names the step that failed, so a provider divergence names its operation.
fn at<E: std::fmt::Display>(step: impl std::fmt::Display) -> impl FnOnce(E) -> String {
    move |error| format!("{step}: {error}")
}

/// Next live item of `stream`, or an error naming `what` never arrived.
async fn next<T>(stream: &mut (impl Stream<Item = T> + Unpin), what: &str) -> TestResult<T> {
    super::stall::guarded(
        std::path::Path::new(what),
        super::stall::waiting("subagent delivery", stream.next()),
    )
    .await?
    .ok_or_else(|| format!("{what}: the live stream ended").into())
}

/// `count` changes of one follow.
async fn changes(follow: &mut ChangeStream, count: usize, what: &str) -> TestResult<Vec<Change>> {
    let mut changes = Vec::with_capacity(count);
    for _ in 0..count {
        changes.push(next(follow, what).await?.map_err(at(what))?);
    }
    Ok(changes)
}

/// The provider-independent observation of one change. Cursor identities
/// (volume, epoch, commit) differ between providers; positions do not.
fn observed(change: &Change) -> TestResult<Value> {
    let (Some(file), true, true) = (
        &change.file,
        change.files.is_empty(),
        change.removed.is_empty(),
    ) else {
        return Err("a publication is not exactly one file".into());
    };
    Ok(json!({
        "key": file.key,
        "channel": file.channel,
        "text": std::str::from_utf8(file.bytes.as_ref())?,
        "position": change.cursor.position,
    }))
}

fn chunk(sequence: usize, text: &str) -> TestResult<Write> {
    let mut write = Write::new(
        FileKey::parse(&format!("{CHUNKS}/{sequence}"))?,
        text.as_bytes().to_vec(),
    );
    write.channel = Some(Channel::Text);
    Ok(write)
}

/// The name of `child` under `root`.
fn name(root: &StreamPath, child: &StreamPath) -> TestResult<String> {
    child
        .as_str()
        .strip_prefix(root.as_str())
        .and_then(|name| name.strip_prefix('/'))
        .map(str::to_owned)
        .ok_or_else(|| format!("{} is outside {}", child.as_str(), root.as_str()).into())
}

/// The name of agent `id`, which tags every payload it publishes.
fn agent(id: usize) -> String {
    format!("a{id:05}")
}

/// The exact `status.json` agent `name` publishes in `state`.
fn status_of(name: &str, state: &str) -> Value {
    json!({ "state": state, "agent": name })
}

/// Checks that `trace`, the follow of agent `name`, delivered exactly that
/// agent's tagged chunks, each under its own key and in order, and then
/// exactly its expected status.
fn check(name: &str, trace: &[Value], expect: &Expect) -> TestResult {
    let (status, chunks) = trace.split_last().ok_or("an empty follow")?;
    let delivered = chunks
        .iter()
        .map(|seen| (&seen["key"], &seen["channel"], &seen["text"]))
        .collect::<Vec<_>>();
    let expected = expect
        .chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| {
            let key = FileKey::parse(&format!("{CHUNKS}/{}", index + 1))?;
            Ok((
                json!(key),
                json!(Channel::Text),
                json!(format!("{name}:{chunk}")),
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let decoded: Value = serde_json::from_str(status["text"].as_str().ok_or("no status")?)?;
    if !delivered.iter().copied().eq(expected
        .iter()
        .map(|(key, channel, text)| (key, channel, text)))
        || status["key"] != json!(FileKey::parse(STATUS)?)
        || decoded != status_of(name, &expect.status)
    {
        return Err(format!("{name} followed {trace:?}").into());
    }
    Ok(())
}

/// Runs `case` under `root` on `stream`, checks its oracle, and returns the
/// trace every provider must reproduce.
pub(super) async fn run_on<P: StreamProvider>(
    case: &SubagentCase,
    stream: Arc<P>,
    root: &str,
) -> TestResult<Value> {
    run_publishing(case, stream, root, |id| id, true).await
}

async fn create_volumes<P: StreamProvider>(
    count: usize,
    stream: Arc<P>,
    root: &StreamPath,
    fork_empty: bool,
) -> TestResult<Vec<TaskVolume<P>>> {
    let volumes = if fork_empty {
        let first = StreamPath::new(format!("{}/{}", root.as_str(), agent(0)))?;
        let initial = TaskVolume::open(stream.clone(), first.clone()).await?;
        let mut paths = vec![first.clone()];
        // Only catalog creation is ordered. Clone the immutable empty prefix, then
        // reconstruct independent volume indexes concurrently instead of replaying
        // each header before the next child can be created.
        for id in 1..count {
            let path = StreamPath::new(format!("{}/{}", root.as_str(), agent(id)))?;
            stream
                .fork(ForkRequest {
                    source: first.clone(),
                    destination: path.clone(),
                    at_tail: Some(1),
                    idempotency_key: None,
                })
                .await
                .map_err(at(format!("create a{id:05}")))?;
            paths.push(path);
        }
        let mut volumes = vec![initial];
        volumes.extend(
            futures::future::join_all(
                paths
                    .into_iter()
                    .skip(1)
                    .map(|path| TaskVolume::open_existing(stream.clone(), path)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?,
        );
        volumes
    } else {
        let mut volumes = Vec::with_capacity(count);
        for id in 0..count {
            let path = StreamPath::new(format!("{}/{}", root.as_str(), agent(id)))?;
            volumes.push(TaskVolume::open(stream.clone(), path).await?);
        }
        volumes
    };
    Ok(volumes)
}

async fn prepare_follows<P: StreamProvider>(
    volumes: &[TaskVolume<P>],
) -> TestResult<(Vec<Generation>, Vec<ChangeStream>)> {
    // Independent volumes have independent fences. Prepare all follows before
    // publishing, then retain each successful append's fence instead of reading
    // it back before the next append.
    let prepared =
        futures::future::join_all(volumes.iter().enumerate().map(|(id, volume)| async move {
            let cut = volume
                .cut(&Query::Root)
                .await
                .map_err(at(format!("cut a{id:05}")))?;
            let head = Generation {
                volume: cut.volume,
                epoch: cut.epoch,
                position: cut.position,
                commit: cut.commit,
            };
            TestResult::Ok((cut, head))
        }))
        .await
        .into_iter()
        .collect::<TestResult<Vec<_>>>()?;
    let (cuts, heads): (Vec<_>, Vec<_>) = prepared.into_iter().unzip();
    // Pipeline validation reads and Session admission without bursting Cloud's
    // eight queued preparations. Two per scenario leave admission room for the
    // other fan-out cases and the concurrent SwarmClient/capability probes.
    let follows = futures::stream::iter(volumes.iter().zip(cuts).enumerate())
        .map(|(id, (volume, cut))| async move {
            TestResult::Ok(
                volume
                    .follow(Query::Root, cut)
                    .await
                    .map_err(at(format!("follow a{id:05}")))?,
            )
        })
        .buffered(2)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<TestResult<Vec<_>>>()?;
    Ok((heads, follows))
}

async fn publish_rounds<P: StreamProvider>(
    case: &SubagentCase,
    volumes: &[TaskVolume<P>],
    mut heads: Vec<Generation>,
    author: impl Fn(usize) -> usize,
) -> TestResult {
    // Keep chunk order within each agent and interleave complete rounds across
    // agents, without serializing unrelated remote transactions.
    for (sequence, text) in case.chunks.iter().enumerate() {
        let writes = (0..case.agents)
            .map(|id| chunk(sequence + 1, &format!("{}:{text}", agent(author(id)))))
            .collect::<TestResult<Vec<_>>>()?;
        heads = futures::future::join_all(
            volumes
                .iter()
                .zip(&heads)
                .zip(&writes)
                .map(|((volume, head), write)| volume.append(*head, write)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    }
    let statuses = (0..case.agents)
        .map(|id| {
            TestResult::Ok(Write::new(
                FileKey::parse(STATUS)?,
                serde_json::to_vec(&status_of(&agent(author(id)), &case.status))?,
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    for result in futures::future::join_all(
        volumes
            .iter()
            .zip(&heads)
            .zip(&statuses)
            .map(|((volume, head), write)| volume.append(*head, write)),
    )
    .await
    {
        result.map_err(at("append the status"))?;
    }
    Ok(())
}

/// `run_on`, with agent `id` publishing the payloads of agent `author(id)`,
/// so a negative control can cross deliveries.
async fn run_publishing<P: StreamProvider>(
    case: &SubagentCase,
    stream: Arc<P>,
    root: &str,
    author: impl Fn(usize) -> usize,
    fork_empty: bool,
) -> TestResult<Value> {
    case.validate()?;
    let started = std::time::Instant::now();
    let phase = |label| {
        if case.agents >= 16 {
            eprintln!(
                "subagents {root} {label}: {:.3}s",
                started.elapsed().as_secs_f64()
            );
        }
    };
    let root = StreamPath::new(root)?;
    let mut children = stream
        .children(ChildrenRequest {
            parent: Some(root.clone()),
            limit: 1,
        })
        .await
        .map_err(at("open the children subscription"))?;
    let volumes = create_volumes(case.agents, stream.clone(), &root, fork_empty).await?;
    phase("initialized");
    let (heads, mut follows) = prepare_follows(&volumes).await?;
    phase("admitted");
    publish_rounds(case, &volumes, heads, author).await?;
    phase("published");
    let mut discovered = Vec::with_capacity(case.agents);
    for _ in 0..case.agents {
        let what = "children of the subagent root";
        let child = next(&mut children, what).await?.map_err(at(what))?;
        discovered.push(name(&root, &child.path)?);
    }
    if discovered != case.expect.children {
        return Err(format!("children discovered: {discovered:?}").into());
    }
    let published = case.chunks.len() + 1;
    let mut agents = Vec::with_capacity(case.agents);
    let mut resume_after = None;
    for (id, follow) in follows.iter_mut().enumerate() {
        let delivered = changes(follow, published, &format!("follow of a{id:05}")).await?;
        let trace = delivered
            .iter()
            .map(observed)
            .collect::<TestResult<Vec<_>>>()?;
        check(&agent(id), &trace, &case.expect)?;
        if resume_after.is_none() {
            resume_after = delivered.into_iter().next().map(|change| change.cursor);
        }
        agents.push(trace);
    }
    phase("observed");
    // Resume the first agent, reopened from its history, after its first chunk.
    let cursor = resume_after.ok_or("no first publication")?;
    let volume = volumes.first().ok_or("no agents")?;
    let reopened = TaskVolume::open_existing(stream.clone(), volume.path().clone())
        .await
        .map_err(at("reopen a00000"))?;
    let mut resumed = reopened
        .follow(Query::Root, cursor.clone())
        .await
        .map_err(at("resume a00000"))?;
    let resumed = changes(&mut resumed, published - 1, "resumed follow of a00000")
        .await?
        .iter()
        .map(observed)
        .collect::<TestResult<Vec<_>>>()?;
    if Some(resumed.as_slice()) != agents.first().and_then(|trace| trace.get(1..)) {
        return Err(format!("the resumed follow differs: {resumed:?}").into());
    }
    // A cursor naming a commit the history never had resets instead of following.
    let mut forged = cursor;
    forged.commit = forged.commit.map(|byte| !byte);
    match reopened.follow(Query::Root, forged).await {
        Err(VolumeError::ResetRequired) => {}
        Err(error) => return Err(format!("a forged cursor failed with {error}").into()),
        Ok(_) => return Err("a forged cursor was followed".into()),
    }
    phase("resumed and forged cursor rejected");
    Ok(json!({
        "children": discovered,
        "agents": agents,
        "resumed": resumed.len(),
    }))
}

/// The `KINDS` that `stream` refuses under `root`, each with its typed cause.
/// Any other failure, including one that is not a Streams error, fails the probe.
pub(super) async fn refused<P: StreamProvider>(
    stream: Arc<P>,
    root: &str,
) -> TestResult<Vec<Refusal>> {
    let [publication, checked, live_children] = KINDS;
    let cause = |kind: &'static str| {
        move |error: VolumeError| match error {
            VolumeError::Storage(cause) => Ok((kind, cause)),
            error => Err(format!("{kind} failed outside Streams: {error}")),
        }
    };
    let root = StreamPath::new(root)?;
    let mut refused = Vec::new();
    let mut children = stream
        .children(ChildrenRequest {
            parent: Some(root.clone()),
            limit: 1,
        })
        .await;
    let path = StreamPath::new(format!("{}/probe", root.as_str()))?;
    let volume = TaskVolume::open(stream.clone(), path)
        .await
        .map_err(at("open the probe volume"))?;
    let head = volume.head().await.map_err(at("probe head"))?;
    if let Err(error) = volume.append(head, &chunk(1, "probe")?).await {
        refused.push(cause(publication)(error)?);
    }
    let cut = volume
        .cut(&Query::Root)
        .await
        .map_err(at("cut the probe volume"))?;
    if let Err(error) = volume.follow(Query::Root, cut).await {
        refused.push(cause(checked)(error)?);
    }
    let discovered = match &mut children {
        Ok(children) => next(children, "children of the probe root").await?,
        Err(error) => Err(error.clone()),
    };
    match discovered {
        Ok(child) if name(&root, &child.path)? == "probe" => {}
        Ok(child) => return Err(format!("the probe discovered {}", child.path.as_str()).into()),
        Err(error) => refused.push((live_children, error)),
    }
    Ok(refused)
}

/// Follows one record of `root/latest` with LATEST delivery over `client`'s
/// Session: `None` when it is delivered, else the refusal's typed cause.
pub(super) async fn latest(client: &Client, root: &str) -> TestResult<Option<Refusal>> {
    let path = StreamPath::new(format!("{root}/latest"))?;
    client
        .append(AppendRequest {
            path: path.clone(),
            // LATEST delivers keyed values, not raw records.
            records: vec![
                wire::SessionLatestValue {
                    key: b"probe".to_vec().into(),
                    value: b"latest".to_vec().into(),
                    attention: false,
                }
                .encode_to_vec()
                .into(),
            ],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .map_err(at("append the LATEST probe record"))?;
    let follow = wire::SessionFollow {
        resource: acyclic_stream::wire_codec::session::stream_resource(path.as_str()),
        delivery: wire::SessionDelivery::Latest as i32,
        preceding_commit_id: Default::default(),
    };
    let delivered = match client.follow_data(follow, 0).await {
        Ok(mut data) => next(&mut data, "the LATEST follow").await?,
        Err(error) => Err(error),
    };
    Ok(delivered.err().map(|cause| (LATEST, cause)))
}

/// The scenario suite runs each case on in-process Streams.
pub(super) async fn run(case: SubagentCase) -> TestResult {
    run_on(&case, Arc::new(MemoryStream::default()), "subagents")
        .await
        .map(drop)
}

/// The oracle fails a follow that decodes another final state, one that
/// receives another agent's deliveries, and one whose reply is split differently.
#[tokio::test]
async fn the_oracle_rejects_a_wrong_status_crossed_deliveries_and_a_resplit_reply() -> TestResult {
    let case = |status: &str, expected: &str| SubagentCase {
        agents: 3,
        chunks: vec!["Plan ".into(), "the work.".into()],
        status: status.into(),
        expect: Expect {
            children: (0..3).map(agent).collect(),
            chunks: vec!["Plan ".into(), "the work.".into()],
            status: expected.into(),
        },
    };
    let stream = || Arc::new(MemoryStream::default());
    run_on(&case("failed", "failed"), stream(), "control").await?;
    if run_on(&case("done", "failed"), stream(), "control")
        .await
        .is_ok()
    {
        return Err("a failed agent that published done passed".into());
    }
    let swapped = |id| match id {
        1 => 2,
        2 => 1,
        id => id,
    };
    let crossed = run_publishing(&case("done", "done"), stream(), "control", swapped, true).await;
    if crossed.is_ok() {
        return Err("a00001 and a00002 swapped deliveries and passed".into());
    }
    // The same reply, split differently, is a different delivery.
    let mut resplit = case("done", "done");
    resplit.chunks = vec!["Plan t".into(), "he work.".into()];
    if run_on(&resplit, stream(), "control").await.is_ok() {
        return Err("a re-split reply passed".into());
    }
    Ok(())
}

#[tokio::test]
async fn forked_empty_prefix_preserves_the_independent_initialization_trace() -> TestResult {
    for agents in [1, 3, 16, 64] {
        let case = SubagentCase {
            agents,
            chunks: vec!["one ".into(), "two".into()],
            status: "done".into(),
            expect: Expect {
                children: (0..agents).map(agent).collect(),
                chunks: vec!["one ".into(), "two".into()],
                status: "done".into(),
            },
        };
        let baseline = run_publishing(
            &case,
            Arc::new(MemoryStream::default()),
            "agents",
            |id| id,
            false,
        )
        .await?;
        let candidate = run_on(&case, Arc::new(MemoryStream::default()), "agents").await?;
        assert_eq!(baseline, candidate);
        let mut changed = case.clone();
        changed.expect.children.reverse();
        if agents > 1 {
            assert!(
                run_on(&changed, Arc::new(MemoryStream::default()), "agents")
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}
