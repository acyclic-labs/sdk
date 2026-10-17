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
use acyclic_stream::{ChildrenRequest, MemoryStream, StreamPath, StreamProvider};
use futures::{Stream, StreamExt as _};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

/// How long one live delivery may take before the scenario names it as
/// missing. This bounds a hang. It is not a latency assertion.
const DELIVERY: Duration = Duration::from_secs(30);

/// Each subagent's task: its streamed reply and its final status file.
const CHUNKS: &str = ".tasks/t/turns/1/chunks";
const STATUS: &str = ".tasks/t/status.json";

/// What a provider must serve for harness subagents, as `refused` names it.
pub(super) const KINDS: [&str; 3] = ["head-fenced publication", "checked follow", "children"];

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubagentCase {
    /// Subagents fanned out under the root, at most 64.
    pub agents: usize,
    /// Each subagent's streamed reply, one Text chunk per publication.
    pub chunks: Vec<String>,
    /// Each subagent's final `status.json` state.
    pub status: String,
    pub expect: Expect,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expect {
    /// Children discovered under the root, in creation order.
    pub children: Vec<String>,
    /// Each subagent's reply, as its follow assembled it.
    pub text: String,
}

/// Names the step that failed, so a provider divergence names its operation.
fn at<E: std::fmt::Display>(step: impl std::fmt::Display) -> impl FnOnce(E) -> String {
    move |error| format!("{step}: {error}")
}

/// Next live item of `stream`, or an error naming `what` never arrived.
async fn next<T>(stream: &mut (impl Stream<Item = T> + Unpin), what: &str) -> TestResult<T> {
    tokio::time::timeout(DELIVERY, stream.next())
        .await
        .map_err(|_| format!("{what}: nothing delivered within {DELIVERY:?}"))?
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

/// Runs `case` under `root` on `stream`, checks its oracle, and returns the
/// trace every provider must reproduce.
pub(super) async fn run_on<P: StreamProvider>(
    case: &SubagentCase,
    stream: Arc<P>,
    root: &str,
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
        let path = StreamPath::new(&format!("{}/a{id:05}", root.as_str()))?;
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
        for volume in &volumes {
            let head = volume.head().await.map_err(at("head"))?;
            volume
                .append(head, &chunk(sequence + 1, text)?)
                .await
                .map_err(at(format!("append chunk {}", sequence + 1)))?;
        }
    }
    let status = serde_json::to_vec(&json!({ "state": case.status }))?;
    for volume in &volumes {
        let head = volume.head().await.map_err(at("head"))?;
        volume
            .append(head, &Write::new(FileKey::parse(STATUS)?, status.clone()))
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
    let status_key = json!(FileKey::parse(STATUS)?);
    let mut agents = Vec::with_capacity(case.agents);
    let mut resume_after = None;
    for (id, follow) in follows.iter_mut().enumerate() {
        let delivered = changes(follow, published, &format!("follow of a{id:05}")).await?;
        let trace = delivered
            .iter()
            .map(observed)
            .collect::<TestResult<Vec<_>>>()?;
        let text: String = trace
            .iter()
            .filter(|seen| seen["channel"] == json!(Channel::Text))
            .filter_map(|seen| seen["text"].as_str())
            .collect();
        if text != case.expect.text || trace.last().map(|last| &last["key"]) != Some(&status_key) {
            return Err(format!("a{id:05} followed {trace:?}").into());
        }
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

/// The `KINDS` that `stream` refuses under `root`, each with its error.
pub(super) async fn refused<P: StreamProvider>(
    stream: Arc<P>,
    root: &str,
) -> TestResult<Vec<(&'static str, String)>> {
    let [publication, checked, live_children] = KINDS;
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
        refused.push((publication, error.to_string()));
    }
    let cut = volume
        .cut(&Query::Root)
        .await
        .map_err(at("cut the probe volume"))?;
    if let Err(error) = volume.follow(Query::Root, cut).await {
        refused.push((checked, error.to_string()));
    }
    let discovered = match &mut children {
        Ok(children) => next(children, "children of the probe root").await?,
        Err(error) => Err(error.clone()),
    };
    match discovered {
        Ok(child) if name(&root, &child.path)? == "probe" => {}
        Ok(child) => return Err(format!("the probe discovered {}", child.path.as_str()).into()),
        Err(error) => refused.push((live_children, error.to_string())),
    }
    Ok(refused)
}

/// The scenario suite runs each case on in-process Streams.
pub(super) async fn run(case: SubagentCase) -> TestResult {
    run_on(&case, Arc::new(MemoryStream::default()), "subagents")
        .await
        .map(drop)
}
