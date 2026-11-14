//! Seeded GraphCoder swarms on the production local harness.
//!
//! A seed draws a spawn tree (wide and deep) and each agent's steps: spawns,
//! file writes and waits, then a final answer. A responding scripted model
//! plays every agent's steps; the seeded executor and entropy choose the
//! interleaving (children settling mid-parent-turn, concurrently, on any
//! activation shard). After the root's prompt, the whole tree must settle and
//! every turn must complete. A failure reports the seed; replaying that one
//! seed (`DST_SEED=<n>`) also shrinks its steps to a 1-minimal failing plan.
use super::*;
use acyclic_harness::{
    agent::{AgentPath, ConversationEntry, TurnOutcome, decode_record, inbox_tail},
    model::{ModelContent, ModelContentPart, ModelEvent, ModelRequest, ModelRole, ToolCall},
    test_support::ScriptedEvent,
};
use acyclic_native_runtime::testing::sweep;
use serde_json::json;
use sha2::Digest as _;
use std::collections::BTreeSet;

/// Seeds in the fast suite. `DST_SEEDS=<a>..<b>` runs a deeper sweep.
const SEEDS: std::ops::Range<u64> = 0..64;
const MARKER: &str = "swarm-node:";

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    /// Spawn these named workers in one model step.
    Spawn(Vec<String>),
    /// Write one file in the agent's fork.
    Write(String),
    /// Wait for these named children.
    Wait(Vec<String>),
}

/// One agent's step, by node id: `root`, `root/w0`, `root/w0/w1`.
type Step = (String, Action);

/// Seeded draws, independent of the runtime's own entropy stream.
struct Draws(blake3::OutputReader);

impl Draws {
    fn new(seed: u64) -> Self {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.dst.swarm.plan.v1");
        hash.update(&seed.to_le_bytes());
        Self(hash.finalize_xof())
    }

    fn below(&mut self, bound: usize) -> usize {
        let mut bytes = [0; 8];
        self.0.fill(&mut bytes);
        usize::try_from(u64::from_le_bytes(bytes) % bound as u64).unwrap_or(0)
    }
}

/// The seed's complete swarm: every agent's steps in plan order.
fn generate(seed: u64) -> Vec<Step> {
    let mut draws = Draws::new(seed);
    let mut steps = Vec::new();
    let mut pending = vec![("root".to_owned(), 0)];
    while let Some((node, depth)) = pending.pop() {
        let width = match depth {
            0 => 2 + draws.below(3),
            1 => draws.below(3),
            _ => 0,
        };
        let names: Vec<String> = (0..width).map(|index| format!("w{index}")).collect();
        let mut writes = 0;
        let mut write = |steps: &mut Vec<Step>| {
            writes += 1;
            let file = format!("{}-{writes}.txt", node.replace('/', "-"));
            steps.push((node.clone(), Action::Write(file)));
        };
        let mut rest = names.as_slice();
        while !rest.is_empty() {
            let (batch, tail) = rest.split_at(1 + draws.below(rest.len()));
            steps.push((node.clone(), Action::Spawn(batch.to_vec())));
            rest = tail;
            // Later steps keep the parent mid-turn while children settle.
            for _ in 0..draws.below(3) {
                write(&mut steps);
            }
        }
        if !names.is_empty() && draws.below(2) == 0 {
            let need = names[..1 + draws.below(names.len())].to_vec();
            steps.push((node.clone(), Action::Wait(need)));
        }
        for _ in 0..draws.below(2) {
            write(&mut steps);
        }
        pending.extend(
            names
                .iter()
                .map(|name| (format!("{node}/{name}"), depth + 1)),
        );
    }
    steps
}

/// The steps one agent plays, given the (possibly minimized) plan. A wait
/// names only children this plan still spawns.
fn agent_steps(plan: &[Step], node: &str) -> Vec<Action> {
    let mut spawned = BTreeSet::new();
    plan.iter()
        .filter(|(owner, _)| owner == node)
        .filter_map(|(_, action)| match action {
            Action::Spawn(names) => {
                spawned.extend(names.iter().cloned());
                Some(action.clone())
            }
            Action::Wait(names) => {
                let names: Vec<_> = names
                    .iter()
                    .filter(|name| spawned.contains(*name))
                    .cloned()
                    .collect();
                (!names.is_empty()).then_some(Action::Wait(names))
            }
            Action::Write(_) => Some(action.clone()),
        })
        .collect()
}

/// The task text that briefs `node`.
fn brief(node: &str) -> String {
    format!("{MARKER}{node} plays its seeded steps")
}

/// The node an agent's request belongs to: its newest brief (a forked
/// worker's history also holds its ancestors' briefs, earlier). A brief is
/// user text naming the marker, or a staged input file whose content digest
/// is a plan node's brief, as the harness projects user input. A notice part
/// is never a brief, whatever its body quotes.
fn node_of(plan: &[Step], request: &ModelRequest) -> Option<(String, usize)> {
    let mut nodes = vec!["root".to_owned()];
    for (owner, action) in plan {
        if let Action::Spawn(names) = action {
            nodes.extend(names.iter().map(|name| format!("{owner}/{name}")));
        }
    }
    let digests: std::collections::BTreeMap<[u8; 32], String> = nodes
        .into_iter()
        .map(|node| (sha2::Sha256::digest(brief(&node)).into(), node))
        .collect();
    let named = |text: &str| {
        let start = text.find(MARKER)? + MARKER.len();
        Some(
            text[start..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '/')
                .collect::<String>(),
        )
    };
    request
        .messages
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, message)| message.role == ModelRole::User)
        .find_map(|(index, message)| {
            let node = match &message.content {
                ModelContent::Text(text) => named(text),
                content => content.parts().iter().find_map(|part| match part {
                    ModelContentPart::Text { text, .. } => named(text),
                    ModelContentPart::File { file, .. } => {
                        digests.get(file.descriptor().sha256()).cloned()
                    }
                    _ => None,
                }),
            };
            node.map(|node| (node, index))
        })
}

/// Plays `plan` for whichever agent sent `request`: its next step, or its
/// final answer once its steps are done (including turns that settled
/// children's notices wake later).
fn respond(plan: &[Step], request: &ModelRequest) -> Result<ScriptedTurn> {
    let (node, briefed) = node_of(plan, request).ok_or_else(|| {
        Error::Invalid(format!(
            "swarm request names no node; user messages: {}",
            serde_json::to_string(
                &request
                    .messages
                    .iter()
                    .filter(|message| message.role == ModelRole::User)
                    .map(|message| &message.content)
                    .collect::<Vec<_>>()
            )
            .unwrap_or_default()
        ))
    })?;
    let step = request.messages[briefed..]
        .iter()
        .filter(|message| message.role == ModelRole::Assistant)
        .count();
    let call = |index: usize, name: &str, arguments| {
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            format!("s{step}c{index}"),
            name.into(),
            arguments,
        )))
    };
    let mut events = match agent_steps(plan, &node).get(step) {
        None => return Ok(ScriptedTurn::Text(format!("{node} settled"))),
        Some(Action::Spawn(names)) => names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let task = brief(&format!("{node}/{name}"));
                call(
                    index,
                    "spawn",
                    json!({"name": name, "task": task, "role": "worker"}),
                )
            })
            .collect(),
        Some(Action::Write(file)) => vec![call(
            0,
            "write",
            json!({"path": file, "content": format!("{node} wrote {file}")}),
        )],
        Some(Action::Wait(names)) => vec![call(0, "wait", json!({"agents": names}))],
    };
    events.push(ScriptedEvent::Event(ModelEvent::Completed {
        metadata: json!({}),
    }));
    Ok(ScriptedTurn::Events(events))
}

/// Each model request's node and the `(child, turn)` settlements its history
/// delivers, in history order.
type Seen = Arc<std::sync::Mutex<Vec<(String, Vec<(String, String)>)>>>;

fn settlements(request: &ModelRequest) -> Vec<(String, String)> {
    request
        .messages
        .iter()
        .filter_map(|message| message.content.notice_parts())
        .flat_map(|(notices, _)| notices)
        .filter(|notice| notice.kind == "child.settled")
        .map(|notice| (notice.source.clone(), notice.body["turn"].to_string()))
        .collect()
}

/// Every child result reaches its parent exactly once: no request's history
/// holds one settlement twice, and each parent's requests deliver exactly one
/// settlement per child turn that reports (every turn but cancelled ones).
async fn exactly_once(
    harness: &Arc<LocalHarness>,
    tree: &[(String, AgentPath)],
    seen: &Seen,
) -> Result<()> {
    let seen = seen.lock().map_err(storage)?.clone();
    for (node, delivered) in &seen {
        let mut unique = BTreeSet::new();
        if let Some(twice) = delivered.iter().find(|settled| !unique.insert(*settled)) {
            return Err(Error::Conflict(format!(
                "{node} received settlement {twice:?} twice in one request"
            )));
        }
    }
    for (node, agent) in tree {
        let Some((parent, _)) = node.rsplit_once('/') else {
            continue;
        };
        let reported = entry(turns(harness, agent))
            .await?
            .iter()
            .filter(|turn| {
                turn.input.context().is_none() && !matches!(turn.outcome, TurnOutcome::Cancelled)
            })
            .count();
        let source = agent.to_string();
        let delivered: BTreeSet<_> = seen
            .iter()
            .filter(|(owner, _)| owner == parent)
            .flat_map(|(_, settled)| settled)
            .filter(|(child, _)| *child == source)
            .collect();
        if delivered.len() != reported {
            return Err(Error::Conflict(format!(
                "{parent} received {} settlements of {node}, which reported {reported} turns: {delivered:?}",
                delivered.len()
            )));
        }
    }
    Ok(())
}

/// Every agent of the plan that exists, root first, by node id.
async fn agents_of(harness: &Arc<LocalHarness>, plan: &[Step]) -> Result<Vec<(String, AgentPath)>> {
    let (agents, root) = harness.agents().await?;
    let mut found = vec![("root".to_owned(), root.clone())];
    let mut index = 0;
    while let Some((node, path)) = found.get(index).cloned() {
        index += 1;
        for action in agent_steps(plan, &node) {
            let Action::Spawn(names) = action else {
                continue;
            };
            for name in names {
                if let Some(child) = agents
                    .resolve_spawn_name(&path, &name)
                    .await
                    .map_err(storage)?
                    .live()
                {
                    found.push((format!("{node}/{name}"), child));
                }
            }
        }
    }
    Ok(found)
}

/// Waits, on each agent's own settlement events, until a whole pass over the
/// tree observes no new input anywhere: every notice has been answered.
async fn settle(harness: &Arc<LocalHarness>, plan: &[Step]) -> Result<Vec<(String, AgentPath)>> {
    let (agents, _) = harness.agents().await?;
    let mut observed = BTreeMap::new();
    loop {
        let tree = agents_of(harness, plan).await?;
        let mut changed = false;
        for (node, path) in &tree {
            let tail = inbox_tail(&**agents.provider(), path)
                .await
                .map_err(storage)?;
            agents
                .wait_for_input(path, tail - 1)
                .await
                .map_err(|error| storage(format!("{node} did not settle: {error}")))?;
            changed |= observed.insert(node.clone(), tail) != Some(tail);
        }
        if !changed {
            return Ok(tree);
        }
    }
}

/// Every committed turn of `agent`, in order.
async fn turns(harness: &Arc<LocalHarness>, agent: &AgentPath) -> Result<Vec<ConversationEntry>> {
    use acyclic_stream::{ReadRequest, StreamProvider as _, StreamProviderExt as _};
    use futures::TryStreamExt as _;
    let (agents, _) = harness.agents().await?;
    let path = agent.conversation().map_err(storage)?;
    let tail = agents
        .provider()
        .tail(path.clone())
        .await
        .map_err(storage)?;
    let read = agents
        .provider()
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: u32::try_from(tail.max(1)).map_err(storage)?,
            at_least: Default::default(),
        })
        .await
        .map_err(storage)?;
    let read: Vec<_> = read.try_collect().await.map_err(storage)?;
    read.iter()
        .map(|record| decode_record::<ConversationEntry>(&record.value).map_err(storage))
        .collect()
}

/// One swarm world: the plan's agents run to quiescence, and every turn of
/// every agent completes. Returns the broken invariant.
fn swarm<'a>(directory: &'a Path, plan: &'a [Step], seen: &'a Seen) -> EntryFuture<'a, Result<()>> {
    entry(async move {
        let reference =
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
        let volume = VolumeRef::new(
            reference.clone(),
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("swarm".into()),
            &mut None,
        )?;
        let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
            .await
            .map_err(storage)?;
        FilesystemHost::new(fs, &reference, None)?
            .create_volume(&volume)
            .await?;
        let model = Model::new("scripted".into(), "swarm".into(), "1".into())?;
        let owned = plan.to_vec();
        let recorded = Arc::clone(seen);
        let provider = Arc::new(ScriptedModelProvider::responding(move |request| {
            // An unidentified request is recorded too, so a stall names it.
            let node = node_of(&owned, request).map_or_else(|| "?".to_owned(), |(node, _)| node);
            recorded
                .lock()
                .map_err(storage)?
                .push((node, settlements(request)));
            respond(&owned, request)
        }));
        let config = local_harness_config(directory.join("filesystem"), (model, provider))?
            .without_user_settings()
            .volume(volume)
            .state_directory(directory.join("state"));
        let harness = Arc::new(entry(LocalHarness::open(config)).await?);
        let prompt = brief("root");
        if let PromptOutcome::Failed(message) =
            entry(prompt_settled(&harness, "swarm", &prompt)).await?
        {
            return Err(Error::Conflict(format!("root prompt failed: {message}")));
        }
        let tree = entry(settle(&harness, plan)).await?;
        for (node, agent) in &tree {
            for (index, turn) in entry(turns(&harness, agent)).await?.iter().enumerate() {
                if let TurnOutcome::Failed { message } = &turn.outcome {
                    return Err(Error::Conflict(format!(
                        "{node} turn {index} failed: {message}"
                    )));
                }
            }
        }
        exactly_once(&harness, &tree, &seen).await
    })
}

/// Runs one seeded world over `plan`; the error is its broken invariant,
/// including a panic, so shrinking can probe any candidate plan.
fn run(seed: u64, plan: &[Step]) -> std::result::Result<(), String> {
    std::panic::catch_unwind(|| run_world(seed, plan)).unwrap_or_else(|panic| {
        Err(panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
            .unwrap_or_else(|| "non-string panic".into()))
    })
}

fn run_world(seed: u64, plan: &[Step]) -> std::result::Result<(), String> {
    let runtime =
        acyclic_native_runtime::testing::seeded_runtime(seed).map_err(|e| e.to_string())?;
    runtime.block_on(async {
        tokio::time::pause();
        let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
        let mut driver = driver::Driver::new(seed, None);
        let seen = Seen::default();
        // Simulated time: a swarm that stops making progress fails with the
        // requests each node made, instead of hanging.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(3600),
            driver.run(swarm(directory.path(), plan, &seen)),
        )
        .await;
        match outcome {
            Ok(driver::Outcome::Completed(result)) => result.map_err(|error| error.to_string()),
            Ok(driver::Outcome::Crashed(cause)) => Err(format!("unplanned storage fault: {cause}")),
            Err(_) => {
                let requests: Vec<String> = seen
                    .lock()
                    .map(|seen| seen.iter().map(|(node, _)| node.clone()).collect())
                    .unwrap_or_default();
                Err(format!(
                    "swarm did not settle within 1 h of simulated time; model requests by node, in order: {requests:?}"
                ))
            }
        }
    })
}

/// The seed's world, run once. The sweep stops at its first failing seed.
fn world(seed: u64) -> std::result::Result<(), String> {
    run(seed, &generate(seed))
}

/// Shrinks a failing seed's plan to the 1-minimal subset that still fails.
/// Replays many worlds, so it runs only when one seed is reproduced.
fn minimal_trace(seed: u64) -> String {
    let plan = generate(seed);
    let minimal = sweep::minimize(plan.clone(), |candidate| run(seed, candidate).is_err());
    let failure = run(seed, &minimal).err().unwrap_or_default();
    format!(
        "minimal plan ({} of {} steps), failing with: {failure}
{minimal:#?}",
        minimal.len(),
        plan.len()
    )
}

#[test]
fn seeded_swarms_settle_and_complete_every_turn() {
    let seeds = sweep::seeds(SEEDS);
    let single = seeds.end - seeds.start == 1;
    let swept = sweep::sweep("swarm", seeds, world).unwrap_or_else(|failure| {
        let trace = if single {
            minimal_trace(failure.seed)
        } else {
            format!("minimal trace: rerun with DST_SEED={}", failure.seed)
        };
        panic!(
            "{failure}
{trace}"
        )
    });
    eprintln!(
        "swarm: {} seeds in {:?} ({:?} per 1000 seeds)",
        swept.seeds,
        swept.elapsed,
        swept.per_thousand()
    );
}
