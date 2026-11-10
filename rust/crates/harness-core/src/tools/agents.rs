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

use crate::agent::spawn_names::{spawn_intent, spawn_name};
use crate::{
    Error, Result,
    agent::{
        AgentPath, AgentState, CreationConfig, IntentId, MAX_OPEN_CHILDREN, SpawnTemplates,
        TurnChildren, TurnEffect, TurnEffects, TurnOutcome, named_child,
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
    collections::BTreeSet,
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
    /// Child name, unique under this agent; reusable after a setup failure
    /// before any reply or agent action committed. It names the child's path and branch.
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

impl AgentTool {
    async fn run(
        &self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> Result<ToolResult> {
        let turn = &*self.turn;
        if self.name == "wait" {
            return match self
                .prepare_wait(invocation.operation_id, &invocation.arguments, true)
                .await?
            {
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
                let Ok(intent) = spawn_intent(&input.name, 0) else {
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
                let recipient = match self.recipient(&input.agent, true).await? {
                    Some(path) => Some(path),
                    None => self.peer(&input.agent, context.as_ref()).await?,
                };
                let Some(to) = recipient else {
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
                let Some(target) = self.recipient(&input.agent, false).await? else {
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
                "name": spawn_name(&intent).map_err(agent)?,
            }),
            TurnEffect::Send { to, .. } => json!({"sent": to.as_str(), "status": "queued"}),
            TurnEffect::Archive { target, .. } => json!({"archived": target.as_str()}),
        };
        Ok(ToolResult { value })
    }

    /// The open children: those open when the turn started plus this turn's
    /// recorded spawns.
    async fn open(&self) -> Result<BTreeSet<AgentPath>> {
        let turn = &*self.turn;
        let mut open = turn.children.clone();
        for effect in turn.effects.recorded().await? {
            if let TurnEffect::Spawn { intent, .. } = effect {
                open.insert(TurnEffect::child(&turn.agent, &intent).map_err(agent)?);
            }
        }
        Ok(open)
    }

    /// Whether spawning `intent` keeps at most `limit` open children. A spawn
    /// already recorded (a replayed call) is within its own limit.
    async fn within(&self, intent: &IntentId, limit: usize) -> Result<bool> {
        let open = self.open().await?;
        let child = TurnEffect::child(&self.turn.agent, intent).map_err(agent)?;
        Ok(open.contains(&child) || open.len() < limit)
    }

    /// `agent` when this turn may reach it: an open child, or the parent too.
    async fn recipient(&self, agent: &str, parent: bool) -> Result<Option<AgentPath>> {
        let turn = &*self.turn;
        if parent && let Some(to) = turn.parent.as_ref().filter(|path| path.as_str() == agent) {
            return Ok(Some(to.clone()));
        }
        Ok(
            named_child(turn.observer.as_deref(), &turn.agent, agent, false)
                .await?
                .filter(|child| turn.children.contains(child)),
        )
    }

    async fn peer(
        &self,
        reference: &str,
        context: Option<&ToolContext>,
    ) -> Result<Option<AgentPath>> {
        let turn = &*self.turn;
        let path = match AgentPath::try_from(reference.to_owned()) {
            Ok(path) => Some(path),
            Err(_) => match &turn.parent {
                Some(parent) => {
                    named_child(turn.observer.as_deref(), parent, reference, false).await?
                }
                None => None,
            },
        };
        let Some(path) = path else {
            return Ok(None);
        };
        for grants in context
            .map(|context| context.effective_scope().grants())
            .into_iter()
            .chain(turn.templates.as_ref().map(|(creation, _)| &creation.peers))
        {
            if crate::agent::peer_send_allowed(grants, &path) {
                return Ok(Some(path));
            }
            if let Some(observer) = &turn.observer
                && let Some(parent) = &turn.parent
            {
                if crate::agent::sibling_send_allowed(&**observer, grants, parent, &path).await? {
                    return Ok(Some(path));
                }
            }
        }
        Ok(None)
    }

    /// Select every status and answer position once under this turn's
    /// authority. Readiness and the result consume the same observations.
    async fn prepare_wait(
        &self,
        operation: crate::OperationId,
        arguments: &Value,
        released: bool,
    ) -> Result<ToolPreparation> {
        let Some(observer) = &self.turn.observer else {
            return Ok(ToolPreparation::Ready(rejected(
                "this host observes no children",
            )));
        };
        let (children, mut need, timeout_ms) = match waited(&**observer, operation, arguments).await
        {
            Ok(waited) => waited,
            Err(error @ Error::Invalid(_)) => {
                return Ok(ToolPreparation::Ready(super::rejected_input(error)));
            }
            Err(error) => return Err(error),
        };
        let open = self.open().await?;
        let observations = futures::future::try_join_all(children.iter().map(|(name, child)| {
            let observer = &**observer;
            let open = &open;
            async move {
                match observer.settled(child).await {
                    Ok(observed) => Ok(Some(observed)),
                    // Spawned this turn: its creation commits with the step.
                    Err(Error::NotFound(_)) if open.contains(child) => Ok(None),
                    Err(error) => Err((name, error)),
                }
            }
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
        let mut pending = BTreeSet::new();
        let mut unsettled = Vec::new();
        let mut stranded = Vec::new();
        for ((name, child), observed) in children.iter().zip(&observations) {
            if observed.as_ref().is_some_and(|observed| observed.settled) {
                need = need.saturating_sub(1);
                continue;
            }
            let phase = observed
                .as_ref()
                .map_or(Ok(json!("spawning")), |observed| {
                    serde_json::to_value(&observed.phase)
                        .map_err(|error| Error::Invalid(error.to_string()))
                })?;
            if open.contains(child) {
                pending.insert(child.clone());
                unsettled.push(json!({"name": name, "phase": phase}));
            } else {
                stranded.push(format!("{name} ({phase}, not an open child)"));
            }
        }
        if pending.len() < need as usize {
            return Ok(ToolPreparation::Ready(rejected(format!(
                "wait cannot be satisfied: {}",
                stranded.join(", ")
            ))));
        }
        if !released && need != 0 {
            return Ok(ToolPreparation::Deferred(
                crate::agent::Until::quorum(pending, need).within(timeout_ms),
            ));
        }
        let settled = futures::future::try_join_all(children.iter().zip(observations).filter_map(
            |((name, child), observed)| {
                let observed = observed.filter(|observed| observed.settled)?;
                Some(async move {
                    let (answer, outcome) = match observed.answer {
                        Some(sequence) => {
                            let (text, outcome) = observer.answer(child, sequence).await?;
                            (Some(text), Some(outcome))
                        }
                        None => (None, None),
                    };
                    let mut entry = json!({
                        "name": name,
                        "agent": child.as_str(),
                        "phase": serde_json::to_value(&observed.phase)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                        "answer": answer,
                    });
                    if let Some(TurnOutcome::Failed { message }) = outcome {
                        entry["failed"] = json!(message);
                    }
                    // The first reference selects the branch's display name.
                    if name != child.as_str() {
                        entry["branch"] = json!(format!("agents/{name}"));
                    }
                    Ok::<_, Error>(entry)
                })
            },
        ))
        .await?;
        Ok(ToolPreparation::Ready(ToolResult {
            value: json!({"settled": settled, "unsettled": unsettled}),
        }))
    }
}

/// The send labels `peers` confers on a child of `parent`: each sibling
/// name's current inbox, or `*` for `parent`'s whole subtree. A spawning agent
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
            spawn_intent(&peer, 0)
                .map_err(|_| rejected(format!("peer {peer} is not a valid sibling name")))?;
            format!("agent.send-sibling:{peer}")
        });
    }
    Ok(crate::Capabilities::new(grants))
}

/// The distinct children a `wait` of `agent` names, each with the reference
/// naming it, how many must settle and its timeout; the model's error when
/// the input names none, names a non-child, or needs none or more than it
/// names.
async fn waited<'de>(
    observer: &dyn TurnChildren,
    operation: crate::OperationId,
    arguments: impl serde::Deserializer<'de, Error = serde_json::Error>,
) -> Result<(Vec<(String, AgentPath)>, u32, Option<u64>)> {
    let input: WaitAgentInput = WaitAgentInput::deserialize(arguments)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let children = observer.select(operation, &input.agents).await?;
    if children.is_empty() {
        return Err(Error::Invalid("wait names no child".into()));
    }
    let named = u32::try_from(children.len()).unwrap_or(u32::MAX);
    let need = input.need.unwrap_or(named);
    if need == 0 || need > named {
        return Err(Error::Invalid(format!(
            "need must be between 1 and the {named} distinct children named"
        )));
    }
    Ok((children, need, input.timeout_ms))
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
                self.prepare_wait(invocation.operation_id, &invocation.arguments, released)
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
                "Wait for subagents (children) to finish their current work and messages. Returns each settled child's phase and last answer, its error as failed when that turn failed, plus its branch when addressed by spawn name, and lists the rest as unsettled with their phase; a wait its open children cannot meet fails naming each child that cannot settle. need (default all) sets how many must settle, 1 for the first; timeout_ms returns after that long regardless. Inspect changes with fs diff <branch>, then bring accepted changes into your project with fs merge <branch>",
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

    /// Children of one agent, `settled` of them settled.
    struct Children {
        agent: AgentPath,
        settled: BTreeSet<AgentPath>,
        stream: Arc<acyclic_stream::MemoryStream>,
    }

    impl TurnChildren for Children {
        fn agent(&self) -> &AgentPath {
            &self.agent
        }

        fn select<'a>(
            &'a self,
            _: crate::OperationId,
            references: &'a [String],
        ) -> BoxProviderFuture<'a, Result<Vec<(String, AgentPath)>>> {
            Box::pin(crate::agent::select_children(self, references))
        }

        fn resolve<'a>(
            &'a self,
            parent: &'a AgentPath,
            name: &'a str,
        ) -> BoxProviderFuture<'a, Result<crate::agent::SpawnResolution>> {
            Box::pin(async move {
                crate::agent::resolve_spawn_name(
                    &self.stream,
                    &acyclic_actors::addressing::ActorReference::new(b"a", b"agents", b"owner")
                        .expect("actor namespace"),
                    parent,
                    name,
                )
                .await
                .map_err(agent)
            })
        }

        fn settled<'a>(
            &'a self,
            child: &'a AgentPath,
        ) -> BoxProviderFuture<'a, Result<crate::agent::Settlement>> {
            let settled = self.settled.contains(child);
            let phase = crate::agent::Settlement {
                settled,
                phase: if settled {
                    crate::agent::Phase::Done {
                        outcome: crate::agent::AgentOutcome::Failed {
                            message: String::new(),
                        },
                    }
                } else {
                    crate::agent::Phase::Running
                },
                answer: None,
            };
            Box::pin(async move { Ok(phase) })
        }

        fn answer<'a>(
            &'a self,
            _: &'a AgentPath,
            _: u64,
        ) -> BoxProviderFuture<'a, Result<(String, crate::agent::TurnOutcome)>> {
            Box::pin(async { Err(Error::Invalid("no answer selected".into())) })
        }
    }

    #[tokio::test]
    async fn deepest_agent_lookup_still_reaches_its_authorized_sibling() -> Result<()> {
        use crate::agent::{AgentId, MAX_DEPTH};
        let mut parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
        for _ in 1..MAX_DEPTH {
            parent = parent.child(&AgentId::from_bytes([2; 16])).map_err(agent)?;
        }
        let deepest = parent.child(&AgentId::from_bytes([3; 16])).map_err(agent)?;
        let sibling = TurnEffect::child(&parent, &spawn_intent("sibling", 0).map_err(agent)?)
            .map_err(agent)?;
        let children = Children {
            agent: deepest.clone(),
            settled: BTreeSet::new(),
            stream: Arc::new(acyclic_stream::MemoryStream::default()),
        };
        let actor = acyclic_actors::addressing::ActorReference::new(
            b"a",
            b"agents",
            sibling.id().as_str().as_bytes(),
        )
        .expect("sibling actor");
        let store = crate::agent::actor_store(&children.stream).map_err(agent)?;
        let effects = [
            store
                .create(None, &actor, acyclic_actors::local::Creation::default())
                .map_err(|error| Error::Storage(error.to_string()))?,
            store.activation(&actor),
        ];
        let request = acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: acyclic_stream::IdempotencyKey::new(b"sibling".to_vec())
                .map_err(|error| Error::Storage(error.to_string()))?,
            conditions: effects
                .iter()
                .flat_map(|effect| effect.conditions.clone())
                .collect(),
            mutations: effects.into_iter().map(|effect| effect.mutation).collect(),
        };
        assert!(matches!(
            crate::agent::submit(&*children.stream, request)
                .await
                .map_err(agent)?,
            crate::agent::Submitted::Committed(_)
        ));
        assert!(
            named_child(Some(&children), &deepest, "sibling", false)
                .await?
                .is_none()
        );
        assert_eq!(
            named_child(Some(&children), &parent, "sibling", false).await?,
            Some(sibling.clone())
        );
        let grants = crate::Capabilities::new(["agent.send-sibling:sibling"]);
        assert!(crate::agent::sibling_send_allowed(&children, &grants, &parent, &sibling).await?);
        Ok(())
    }
}
