//! The agent tools of the native agent model: `spawn`, `send`, `archive` and
//! `wait`.
//!
//! `spawn`, `send` and `archive` each record exactly one effect through the
//! turn's [`TurnEffects`] and return at once. The effect commits in the step
//! that settles the turn, or earlier: before any later call of the same turn
//! dispatches, the turn commits what its earlier calls recorded (see
//! [`TurnEffects::holds`]), so a child spawned by one call runs, and is
//! reachable by name, from the next call on. `wait` records nothing and holds
//! nothing: until enough of the named children settle, preparation defers
//! its call and the turn commits
//! [`crate::agent::Wait::Until`], which their settlements or its timeout
//! resume. Preparation selects each child's status once (see
//! [`crate::agent::settled`]); the original claim publishes that selection
//! and each settled child's exact answer, without reading its status again.
//!
//! A child works on its own fork of its parent's project. Neither settling
//! nor `wait` joins that fork into the parent: the parent merges it
//! explicitly, as a reviewed step it can also skip, with the `fs` facade's
//! `merge agents/<name>` (inspect with `diff`). `wait`
//! names that branch for each settled child it was given by spawn name.

use crate::{
    Error, Result,
    agent::{
        AgentPath, AgentState, CreationConfig, IntentId, MAX_OPEN_CHILDREN, SpawnTemplates,
        TurnChildren, TurnEffect, TurnEffects,
    },
    conversation::{ContentAccess, ContentPublisher},
    runtime::ToolContext,
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolPreparation,
        ToolRegistry, ToolResult, json_projection_schema, schema,
    },
};
use acyclic_stream::BoxProviderFuture;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Revision of the agent tools, selected for the model over other revisions.
pub const REVISION: &str = "agent-2";
/// Names of the agent tools; every agent turn is granted them.
pub const AGENT_TOOLS: [&str; 4] = ["spawn", "send", "archive", "wait"];

/// Spawn one child agent.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnAgentInput {
    /// Child name, unique under this agent; it names the child's path and branch.
    pub name: String,
    /// Self-contained task brief, including the child's file ownership.
    pub task: String,
    /// Role file selecting the child's context and permissions: `worker` forks
    /// your settled conversation; `advisor` and `reviewer` see only the brief.
    pub role: String,
    /// Sibling names, spawned already or later, the child may also `send`
    /// to; `*` admits every agent under you. Edges may form cycles. None by
    /// default: the child reaches only you and its own children.
    #[serde(default)]
    pub peers: Vec<String>,
}

/// Queue one message in a parent, open child or explicitly authorized peer inbox.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendAgentInput {
    /// Recipient: a child's name or agent path, as spawn returned them, the
    /// parent's agent path, or a peer's sibling name or agent path.
    pub agent: String,
    /// Message text.
    pub message: String,
    /// Steer at the next model boundary (default), or reserve a subsequent turn.
    pub delivery: Option<crate::agent::MessageDelivery>,
}

/// Archive one open child and, through its cascade, its descendants.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveAgentInput {
    /// Child name or agent path, as spawn returned them.
    pub agent: String,
}

/// Wait for children to settle.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WaitAgentInput {
    /// Child names or agent paths, as spawn returned them.
    pub agents: Vec<String>,
    /// How many of them must settle before the wait returns: all of them by
    /// default, 1 for the first, or any quorum in between.
    pub need: Option<u32>,
    /// Return after this many milliseconds even if fewer settled.
    pub timeout_ms: Option<u64>,
}

/// What one turn's agent tools act for.
struct Turn {
    agent: AgentPath,
    parent: Option<AgentPath>,
    children: BTreeSet<AgentPath>,
    effects: Arc<dyn TurnEffects>,
    /// What `wait` observes; `None` when the host observes no children.
    observer: Option<Arc<dyn TurnChildren>>,
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: u64,
    /// This agent's creation snapshot and the host's template reader; spawn refuses
    /// when either is absent.
    templates: Option<(CreationConfig, Arc<dyn SpawnTemplates>)>,
    volumes: Vec<crate::agent::VolumeBinding>,
    /// Held from a spawn's limit check through its record, so concurrent
    /// spawns of one turn see each other.
    spawning: futures::lock::Mutex<()>,
    /// The first unfinished wait consumes the actor deadline release.
    released: AtomicBool,
}

struct AgentTool {
    name: &'static str,
    turn: Arc<Turn>,
}

fn rejected(reason: impl Into<String>) -> ToolResult {
    super::rejected_input(Error::Invalid(reason.into()))
}

fn decode<'de, T: Deserialize<'de>>(
    arguments: impl serde::Deserializer<'de, Error = serde_json::Error>,
) -> std::result::Result<T, ToolResult> {
    T::deserialize(arguments).map_err(|error| rejected(error.to_string()))
}

/// Path-safe key of one call, ordered by model step: effects of later steps
/// lower after earlier ones.
fn call_key(invocation: &ToolInvocation, context: Option<&ToolContext>) -> String {
    let step = context.map_or(0, |context| context.model_turn.1);
    crate::agent::call_key(step, invocation.operation_id)
}

/// Prefix of every tool spawn's intent; the rest is the child's name.
const SPAWN: &str = "spawn:";

/// The child `parent`'s `spawn` tool names `name`: the one derivation every
/// host resolving a child by its spawn name (`wait`, `agents/<name>`) shares.
///
/// # Errors
/// When `name` is not a valid spawn name or the child would exceed the depth.
pub fn spawned_child(parent: &AgentPath, name: &str) -> Result<AgentPath> {
    let intent = IntentId::try_from(format!("{SPAWN}{name}")).map_err(agent)?;
    TurnEffect::child(parent, &intent).map_err(agent)
}

impl AgentTool {
    async fn run(
        &self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> Result<ToolResult> {
        let turn = &*self.turn;
        if self.name == "wait" {
            return match self.prepare_wait(&invocation.arguments, true).await? {
                ToolPreparation::Ready(result) => Ok(result),
                ToolPreparation::Deferred(_) => {
                    Err(Error::Conflict("released wait deferred its result".into()))
                }
            };
        }
        let key = call_key(&invocation, context.as_ref());
        let operation = crate::agent::operation_key(invocation.operation_id);
        let _spawning = match self.name {
            "spawn" => Some(turn.spawning.lock().await),
            _ => None,
        };
        let effect = match self.name {
            "spawn" => {
                let input: SpawnAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                if input.task.is_empty() || input.task.len() as u64 > turn.maximum_bytes {
                    return Ok(rejected("task is empty or too large"));
                }
                let Ok(intent) = IntentId::try_from(format!("{SPAWN}{}", input.name)) else {
                    return Ok(rejected(
                        "name must be short printable ASCII without spaces",
                    ));
                };
                let Some((parent, templates)) = &turn.templates else {
                    return Ok(super::rejected_input(Error::Unsupported(
                        "this agent has no creation template to spawn from".into(),
                    )));
                };
                let peers = match peer_grants(&turn.agent, input.peers) {
                    Ok(peers) => peers,
                    Err(rejected) => return Ok(rejected),
                };
                let mut profile = match templates
                    .child(
                        parent,
                        &turn.agent,
                        &turn.volumes,
                        &input.role,
                        context
                            .as_ref()
                            .map(|context| context.effective_scope().grants()),
                    )
                    .await
                {
                    Ok(profile) => profile,
                    Err(error) => return Ok(super::rejected_input(error)),
                };
                profile.peers = peers;
                // The agent's hard limit: a spawn the step would refuse is
                // refused here, before the model is told it succeeded.
                if !self.within(&intent, MAX_OPEN_CHILDREN).await? {
                    return Ok(rejected(format!(
                        "at most {MAX_OPEN_CHILDREN} open children; archive one or wait for one to finish"
                    )));
                }
                let brief = turn
                    .writer
                    .stage_once(
                        invocation.operation_id,
                        &format!("briefs/{operation}.md"),
                        input.task.as_bytes(),
                        "text/markdown",
                        "brief",
                        ContentAccess::default(),
                    )?
                    .await?
                    .0;
                TurnEffect::Spawn {
                    intent,
                    brief: Box::new(brief),
                    profile,
                }
            }
            "send" => {
                let input: SendAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                // A peer by path, or by sibling name; the scope's grants or
                // the parent's spawn-time peers authorize it.
                let peer = || {
                    let path = AgentPath::try_from(input.agent.clone()).ok().or_else(|| {
                        turn.parent
                            .as_ref()
                            .and_then(|parent| spawned_child(parent, &input.agent).ok())
                    })?;
                    let granted = context.as_ref().is_some_and(|context| {
                        crate::agent::peer_send_allowed(context.effective_scope().grants(), &path)
                    }) || turn.templates.as_ref().is_some_and(|(creation, _)| {
                        crate::agent::peer_send_allowed(&creation.peers, &path)
                    });
                    granted.then_some(path)
                };
                let Some(to) = self.recipient(&input.agent, true).or_else(peer) else {
                    return Ok(rejected(
                        "send requires a parent, open child or authorized peer inbox",
                    ));
                };
                if input.message.len() as u64 > turn.maximum_bytes {
                    return Ok(rejected("message is too large"));
                }
                let body = turn
                    .writer
                    .stage_once(
                        invocation.operation_id,
                        &format!("messages/{operation}.md"),
                        input.message.as_bytes(),
                        "text/markdown",
                        "message",
                        ContentAccess::default(),
                    )?
                    .await?
                    .0;
                TurnEffect::Send {
                    delivery: input.delivery.unwrap_or_default(),
                    to,
                    id: IntentId::try_from(format!("send:{operation}")).map_err(agent)?,
                    body: Box::new(body),
                }
            }
            "archive" => {
                let input: ArchiveAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                let Some(target) = self.recipient(&input.agent, false) else {
                    return Ok(rejected("archive reaches only an open child"));
                };
                TurnEffect::Archive {
                    target,
                    id: IntentId::try_from(format!("archive:{operation}")).map_err(agent)?,
                }
            }
            _ => return Err(Error::Unsupported(format!("agent tool {}", self.name))),
        };
        let recorded = match turn.effects.record(&key, &effect).await {
            Ok(recorded) => recorded,
            Err(error @ Error::Conflict(_)) => return Ok(super::rejected_input(error)),
            Err(error) => return Err(error),
        };
        self.result(recorded)
    }

    fn result(&self, recorded: TurnEffect) -> Result<ToolResult> {
        let value = match recorded {
            TurnEffect::Spawn { intent, .. } => json!({
                "agent": TurnEffect::child(&self.turn.agent, &intent).map_err(agent)?.as_str(),
                "name": intent.as_str().strip_prefix(SPAWN).unwrap_or(intent.as_str()),
            }),
            TurnEffect::Send { to, .. } => json!({"sent": to.as_str(), "status": "queued"}),
            TurnEffect::Archive { target, .. } => json!({"archived": target.as_str()}),
        };
        Ok(ToolResult { value })
    }

    /// Whether spawning `intent` keeps at most `limit` open children: those
    /// open when the turn started plus this turn's recorded spawns. A spawn
    /// already recorded (a replayed call) is within its own limit.
    async fn within(&self, intent: &IntentId, limit: usize) -> Result<bool> {
        let turn = &*self.turn;
        let mut open = turn.children.clone();
        for effect in turn.effects.recorded().await? {
            if let TurnEffect::Spawn { intent, .. } = effect {
                open.insert(TurnEffect::child(&turn.agent, &intent).map_err(agent)?);
            }
        }
        let child = TurnEffect::child(&turn.agent, intent).map_err(agent)?;
        Ok(open.contains(&child) || open.len() < limit)
    }

    /// `agent` when this turn may reach it: an open child, or the parent too.
    fn recipient(&self, agent: &str, parent: bool) -> Option<AgentPath> {
        let turn = &*self.turn;
        if parent && let Some(to) = turn.parent.as_ref().filter(|path| path.as_str() == agent) {
            return Some(to.clone());
        }
        named_child(&turn.agent, agent).filter(|child| turn.children.contains(child))
    }

    /// Select every status and answer position once under this turn's
    /// authority. Readiness and the result consume the same observations.
    async fn prepare_wait(&self, arguments: &Value, released: bool) -> Result<ToolPreparation> {
        let Some(observer) = &self.turn.observer else {
            return Ok(ToolPreparation::Ready(rejected(
                "this host observes no children",
            )));
        };
        let (children, mut need, timeout_ms) = match waited(&self.turn.agent, arguments) {
            Ok(waited) => waited,
            Err(rejected) => return Ok(ToolPreparation::Ready(rejected)),
        };
        let observations = futures::future::try_join_all(children.iter().map(|(name, child)| {
            let observer = &**observer;
            async move { observer.settled(child).await.map_err(|error| (name, error)) }
        }))
        .await;
        let observations = match observations {
            Ok(observed) => observed,
            Err((name, Error::NotFound(_))) => {
                return Ok(ToolPreparation::Ready(rejected(format!(
                    "{name} names no child of this agent"
                ))));
            }
            Err((_, error)) => return Err(error),
        };
        let mut unsettled = BTreeSet::new();
        for ((_, child), observed) in children.iter().zip(&observations) {
            if observed.is_some() {
                need = need.saturating_sub(1);
            } else if !released {
                unsettled.insert(child.clone());
            }
        }
        if !released && need != 0 {
            return Ok(ToolPreparation::Deferred(
                crate::agent::Until::quorum(unsettled, need).within(timeout_ms),
            ));
        }
        let mut settled = Vec::with_capacity(children.len());
        let mut running = Vec::new();
        for ((name, child), observed) in children.iter().zip(observations) {
            let Some(observed) = observed else {
                running.push(name.as_str());
                continue;
            };
            let answer = match observed.answer {
                Some(sequence) => Some(observer.answer(child, sequence).await?),
                None => None,
            };
            let mut entry = json!({
                "name": name,
                "agent": child.as_str(),
                "phase": serde_json::to_value(&observed.phase)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                "answer": answer,
            });
            // The fork's branch: its changes reach this agent's project
            // only through `fs merge` of it. A spawn name names it; an agent
            // path does not.
            if name != child.as_str() {
                entry["branch"] = json!(format!("agents/{name}"));
            }
            settled.push(entry);
        }
        Ok(ToolPreparation::Ready(ToolResult {
            value: json!({"settled": settled, "running": running}),
        }))
    }
}

/// The send labels `peers` confers on a child of `parent`: each sibling
/// name's exact inbox, or `*` for `parent`'s whole subtree. A spawning agent
/// confers only inboxes under itself, which it may already reach.
fn peer_grants(
    parent: &AgentPath,
    peers: Vec<String>,
) -> std::result::Result<crate::Capabilities, ToolResult> {
    let mut grants = Vec::with_capacity(peers.len());
    for peer in peers {
        grants.push(if peer == "*" {
            format!("agent.send-subtree:{parent}")
        } else {
            let sibling = spawned_child(parent, &peer)
                .map_err(|_| rejected(format!("peer {peer} is not a valid sibling name")))?;
            format!("agent.send:{sibling}")
        });
    }
    Ok(crate::Capabilities::new(grants))
}

/// The child of `agent` that `reference` names: a spawn name, or the agent
/// path of one of its children, open or settled.
fn named_child(agent: &AgentPath, reference: &str) -> Option<AgentPath> {
    if let Ok(path) = AgentPath::try_from(reference.to_owned()) {
        return path.is_child_of(agent).then_some(path);
    }
    spawned_child(agent, reference).ok()
}

/// The distinct children a `wait` of `agent` names, each with the reference
/// naming it, how many must settle and its timeout; the model's error when
/// the input names none, names a non-child, or needs none or more than it
/// names.
fn waited<'de>(
    agent: &AgentPath,
    arguments: impl serde::Deserializer<'de, Error = serde_json::Error>,
) -> std::result::Result<(Vec<(String, AgentPath)>, u32, Option<u64>), ToolResult> {
    let input: WaitAgentInput = decode(arguments)?;
    let mut children: Vec<(String, AgentPath)> = Vec::with_capacity(input.agents.len());
    let mut seen = HashSet::with_capacity(input.agents.len());
    for reference in input.agents {
        let Some(child) = named_child(agent, &reference) else {
            return Err(rejected(format!(
                "{reference} names no child of this agent"
            )));
        };
        // Resolution already checked the complete parent. Within that one
        // parent the fixed-size child identity suffices; retain its first name.
        if seen.insert(wait_identity(&child)) {
            children.push((reference, child));
        }
    }
    if children.is_empty() {
        return Err(rejected("wait names no child"));
    }
    let named = u32::try_from(children.len()).unwrap_or(u32::MAX);
    let need = input.need.unwrap_or(named);
    if need == 0 || need > named {
        return Err(rejected(format!(
            "need must be between 1 and the {named} distinct children named"
        )));
    }
    Ok((children, need, input.timeout_ms))
}

#[inline]
fn wait_identity(child: &AgentPath) -> [u8; 16] {
    #[cfg(any(test, feature = "test-support"))]
    selection_checks::IDENTITIES.with(|count| count.set(count.get() + 1));
    child.id_bytes()
}

#[cfg(any(test, feature = "test-support"))]
mod selection_checks;
#[cfg(feature = "test-support")]
pub use selection_checks::check as check_agent_wait_selection;

fn agent(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}

impl ToolExecutor for AgentTool {
    fn prepare<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _context: Option<&'a ToolContext>,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<Option<crate::tool::ToolPreparationFuture<'a>>> {
        if self.name != "wait" {
            return Ok(None);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.is_some_and(|io| io.original_native_work().is_some()) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "agent wait preparation has no native observation producer",
            });
        }
        Ok(Some(crate::conversation::BoundedContentFuture::new(
            io,
            || async move {
                let released = self.turn.released.swap(false, Ordering::AcqRel);
                self.prepare_wait(&invocation.arguments, released)
                    .await
                    .map(|prepared| (prepared, None))
            },
        )?))
    }

    /// The effect is what a replay reproduces: a replayed spawn runs its child.
    fn replays_recorded(&self) -> bool {
        false
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.run(invocation, context))
    }

    /// Recover the original effect before consulting any mutable spawn template
    /// or recipient state. An interruption before recording still admits normally.
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            if self.name != "wait" {
                let key = call_key(&invocation, context.as_ref());
                if let Some(effect) = self.turn.effects.recorded_call(&key).await? {
                    return self.result(effect).map(Some);
                }
            }
            self.run(invocation, context).await.map(Some)
        })
    }
}

fn definition<T: JsonSchema>(name: &str, description: &str) -> Result<ToolDefinition> {
    let output = json!({"type": "object"});
    let definition = ToolDefinition {
        name: name.into(),
        revision: REVISION.into(),
        description: description.into(),
        input_schema: model_schema(schema::input::<T>()?),
        projection_schema: json_projection_schema(output.clone()),
        output_schema: output,
    };
    definition.validate(&mut None)?;
    Ok(definition)
}

/// `schema` as the model reads it, with nothing every request would repeat
/// for no meaning: the root's draft URI (2020-12 is the compiler's default),
/// its type title, and its description (the tool's own description says it),
/// and each all-constant `oneOf` as the `enum` of its values, whose meaning
/// the field's description states. Every agent tool request carries this, so
/// it is the context a turn's mandatory content pays for each tool.
fn model_schema(mut schema: serde_json::Value) -> serde_json::Value {
    if let Some(root) = schema.as_object_mut() {
        for key in ["$schema", "title", "description"] {
            root.remove(key);
        }
    }
    enumerate(&mut schema);
    schema
}

fn enumerate(schema: &mut serde_json::Value) {
    match schema {
        serde_json::Value::Object(object) => {
            let values = object.get("oneOf").and_then(|variants| {
                variants
                    .as_array()?
                    .iter()
                    .map(|variant| {
                        variant
                            .get("const")
                            .filter(|value| value.is_string())
                            .cloned()
                    })
                    .collect::<Option<Vec<_>>>()
            });
            if let Some(values) = values {
                object.remove("oneOf");
                object.insert("type".into(), json!("string"));
                object.insert("enum".into(), serde_json::Value::Array(values));
            }
            object.values_mut().for_each(enumerate);
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(enumerate),
        _ => {}
    }
}

/// The model definitions of the agent tools among `names`, exactly as
/// [`bind_agent_tools`] registers them: a turn's context manifest lists them
/// with its other tools, so the model sees the tools the turn binds.
///
/// # Errors
/// When a definition's schema is invalid.
pub fn definitions(names: &[String]) -> Result<Vec<ToolDefinition>> {
    Ok(selected(names)?
        .into_iter()
        .map(|(_, definition)| definition)
        .collect())
}

/// The agent tools among `names`, by name, with their definitions.
fn selected(names: &[String]) -> Result<Vec<(&'static str, ToolDefinition)>> {
    let mut selected = Vec::with_capacity(AGENT_TOOLS.len());
    for (name, definition) in [
        (
            "spawn",
            definition::<SpawnAgentInput>(
                "spawn",
                "Delegate a task to a subagent: spawn a named child agent from templates/<role>/AGENT.md in a fork of your volumes. Spawn every independent subtask before waiting, then call wait. The selected template chooses fresh or forked conversation; your creation template decides which templates you may spawn. Give it a self-contained task and file ownership; peers names the siblings it may message (`*` for all). Returns its name and path. Its changes stay on branch agents/<name> until you merge them; use send for messages, wait for its report, and fs to inspect and merge its branch",
            )?,
        ),
        (
            "send",
            definition::<SendAgentInput>(
                "send",
                "Queue a message for your parent, an open child (by name or path), or a peer your spawn granted (by sibling name or path); steering arrives at the next model boundary after in-flight tools settle, and follow-up starts a later turn",
            )?,
        ),
        (
            "archive",
            definition::<ArchiveAgentInput>(
                "archive",
                "Archive an open child (by name or path) and its descendants; wait on it to see it archived",
            )?,
        ),
        (
            "wait",
            definition::<WaitAgentInput>(
                "wait",
                "Wait for subagents (children) to finish their current work and messages. Returns each settled child's phase and last answer, plus its branch when addressed by spawn name, and lists the rest as running. need (default all) sets how many must settle, 1 for the first; timeout_ms returns after that long regardless. Inspect changes with fs diff <branch>, then bring accepted changes into your project with fs merge <branch>",
            )?,
        ),
    ] {
        if names.iter().any(|selected| selected == name) {
            selected.push((name, definition));
        }
    }
    Ok(selected)
}

/// Registers `agent`'s tools for one turn into `registry`, bound to the turn's
/// `effects`, the `observer` its `wait` reads, and its private `writer`, and selects them for the model over any
/// other revision of the same names. Only the agent tools `names` selects are
/// bound, so a host omits or swaps each one like any standard tool. `spawn`
/// admits a child only through `templates`, under `agent`'s creation snapshot.
pub fn bind_agent_tools(
    registry: &mut ToolRegistry,
    agent: &AgentState,
    (effects, observer): (Arc<dyn TurnEffects>, Option<Arc<dyn TurnChildren>>),
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: u64,
    (names, templates): (&[String], Option<Arc<dyn SpawnTemplates>>),
) -> Result<()> {
    let turn = Arc::new(Turn {
        agent: agent.path.clone(),
        parent: agent.parent.clone(),
        children: agent.open_children.clone(),
        effects,
        observer,
        writer,
        maximum_bytes,
        templates: agent.profile.clone().zip(templates),
        volumes: agent.volumes.clone(),
        spawning: futures::lock::Mutex::new(()),
        released: AtomicBool::new(matches!(agent.phase, crate::agent::Phase::Waiting { .. })),
    });
    for (name, definition) in selected(names)? {
        let tool = Tool {
            hook_names: &[],
            definition,
            executor: Arc::new(AgentTool {
                name,
                turn: Arc::clone(&turn),
            }),
            projection: Arc::new(JsonToolProjection),
        };
        registry.register((tool, None), &mut None, None)?;
        registry.select_model_version(name, REVISION)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_schemas_carry_only_meaning() -> Result<()> {
        let names = AGENT_TOOLS.map(String::from);
        for definition in definitions(&names)? {
            let root = definition.input_schema.as_object();
            for key in ["$schema", "title", "description"] {
                assert!(root.is_some_and(|root| !root.contains_key(key)), "{key}");
            }
        }
        let wait = &definitions(&["wait".into()])?[0].input_schema;
        let check = |value| {
            crate::executor::check_json_schema_value(wait, &value, "tool", &mut None)
                .map(|checked| checked.is_ok())
        };
        assert!(check(json!({"agents": ["a"], "need": 1, "timeout_ms": 5}))?);
        assert!(check(json!({"agents": ["a"]}))?);
        // Negative control: the schema still rejects what the type rejects.
        assert!(!check(json!({"agents": ["a"], "need": -1}))?);
        assert!(
            serde_json::from_value::<WaitAgentInput>(json!({"agents": [], "need": -1})).is_err()
        );
        Ok(())
    }
}
