//! Subagent fan-out followed over a Streams provider's Session. One root holds
//! an agent volume per subagent. A live children subscription, opened before
//! any agent exists, discovers each one. A live checked follow of each agent,
//! opened at its first cut, receives that agent's streamed reply and final
//! status. A reopened volume resumes after the first chunk, and a forged
//! cursor resets.
use super::TestResult;
use acyclic_harness::task_volume::{
    Change, ChangeStream, Channel, Error as VolumeError, FileKey, Query, TaskVolume, Write,
};
use acyclic_stream::{
    AppendRequest, ChildrenRequest, MemoryStream, StreamError, StreamPath, StreamProvider,
    grpc::Client, wire,
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

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubagentCase {
    /// Subagents fanned out under the root, at most 64.
    pub agents: usize,
    /// Each subagent's streamed reply, one Text chunk per publication.
    pub chunks: Vec<String>,
    /// Each subagent's final `status.json` state, as it publishes it.
    pub status: String,
    pub expect: Expect,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expect {
    /// Children discovered under the root, in creation order.
    pub children: Vec<String>,
    /// Each subagent's reply chunks, in order, as its follow delivered them.
    pub chunks: Vec<String>,
    /// Each subagent's final state, as its follow decoded it.
    pub status: String,
}

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
    run_publishing(case, stream, root, |id| id).await
}

/// `run_on`, with agent `id` publishing the payloads of agent `author(id)`,
/// so a negative control can cross deliveries.
async fn run_publishing<P: StreamProvider>(
    case: &SubagentCase,
    stream: Arc<P>,
    root: &str,
    author: impl Fn(usize) -> usize,
) -> TestResult<Value> {
    if !(1..=64).contains(&case.agents) || case.chunks.is_empty() {
        return Err("a subagent case needs 1..=64 agents and a nonempty reply".into());
    }
    let root = StreamPath::new(root)?;
    let mut children = stream
        .children(ChildrenRequest {
            parent: Some(root.clone()),
            limit: 1,
        })
        .await
        .map_err(at("open the children subscription"))?;
    let mut volumes = Vec::with_capacity(case.agents);
    let mut follows = Vec::with_capacity(case.agents);
    for id in 0..case.agents {
        let path = StreamPath::new(&format!("{}/{}", root.as_str(), agent(id)))?;
        let volume = TaskVolume::open(stream.clone(), path)
            .await
            .map_err(at(format!("open a{id:05}")))?;
        let cut = volume
            .cut(&Query::Root)
            .await
            .map_err(at(format!("cut a{id:05}")))?;
        let follow = volume
            .follow(Query::Root, cut)
            .await
            .map_err(at(format!("follow a{id:05}")))?;
        follows.push(follow);
        volumes.push(volume);
    }
    // Interleave the agents' publications, as concurrent subagents would.
    for (sequence, text) in case.chunks.iter().enumerate() {
        for (id, volume) in volumes.iter().enumerate() {
            let tagged = format!("{}:{text}", agent(author(id)));
            let head = volume.head().await.map_err(at("head"))?;
            volume
                .append(head, &chunk(sequence + 1, &tagged)?)
                .await
                .map_err(at(format!("append chunk {}", sequence + 1)))?;
        }
    }
    for (id, volume) in volumes.iter().enumerate() {
        let status = serde_json::to_vec(&status_of(&agent(author(id)), &case.status))?;
        let head = volume.head().await.map_err(at("head"))?;
        volume
            .append(head, &Write::new(FileKey::parse(STATUS)?, status))
            .await
            .map_err(at("append the status"))?;
    }
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
    let path = StreamPath::new(&format!("{}/probe", root.as_str()))?;
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
    let path = StreamPath::new(&format!("{root}/latest"))?;
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
        path: path.as_str().to_owned(),
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
    let crossed = run_publishing(&case("done", "done"), stream(), "control", swapped).await;
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
