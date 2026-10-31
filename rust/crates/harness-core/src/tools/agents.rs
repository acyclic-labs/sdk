//! The agent tools of the native agent model: `spawn`, `send`, `archive` and
//! `wait`.
//!
//! `spawn`, `send` and `archive` each record exactly one effect through the
//! turn's [`TurnEffects`] and return at once. The effect commits in the step
//! that settles the turn, or earlier: before any later call of the same turn
//! dispatches, the turn commits what its earlier calls recorded (see
//! [`TurnEffects::holds`]), so a child spawned by one call runs, and is
//! reachable by name, from the next call on. `wait` records nothing and holds
//! nothing: until the named children settle, its call does not dispatch (see
//! [`awaited`]) and the turn commits [`crate::agent::Wait::Children`], which
//! each child's settlement wakes; dispatched, it reads each child's status
//! once (see [`crate::agent::settled`]) and returns each one's last answer.
//!
//! A child works on its own fork of its parent's project. Neither settling
//! nor `wait` joins that fork into the parent: the parent merges it
//! explicitly, as a reviewed step it can also skip, with the `fs` facade's
//! `merge agents/<name>` (inspect with `diff`). `wait`
//! names that branch for each settled child it was given by spawn name.

use crate::{
    Error, Result,
    agent::{
        AgentPath, AgentState, IntentId, MAX_OPEN_CHILDREN, TurnChildren, TurnEffect, TurnEffects,
    },
    conversation::{ContentAccess, ContentPublisher},
    runtime::ToolContext,
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry,
        ToolResult, json_projection_schema, schema,
    },
};
use acyclic_stream::BoxProviderFuture;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

/// Revision of the agent tools, selected for the model over other revisions.
pub const REVISION: &str = "agent-1";
/// Names of the agent tools; every agent turn is granted them.
pub const AGENT_TOOLS: [&str; 4] = ["spawn", "send", "archive", "wait"];

/// Spawn one child agent.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnAgentInput {
    /// Self-contained task brief, including the child's file ownership.
    pub task: String,
    /// Optional child name, unique under this agent; it names the child's path.
    pub name: Option<String>,
}

/// Send one message to the parent or an open child.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendAgentInput {
    /// Recipient: a child's name or agent path, as spawn returned them, or
    /// the parent's agent path.
    pub agent: String,
    /// Message text.
    pub message: String,
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
    /// `all` (the default) waits for every child, `any` for the first.
    pub mode: Option<WaitMode>,
}

/// Which children a wait returns on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    /// Every named child settled.
    #[default]
    All,
    /// The first named child settled.
    Any,
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
    /// Open children allowed at once, this turn's spawns included.
    max_children: Option<u32>,
    /// Held from a spawn's limit check through its record, so concurrent
    /// spawns of one turn see each other.
    spawning: futures::lock::Mutex<()>,
}

struct AgentTool {
    name: &'static str,
    turn: Arc<Turn>,
}

fn rejected(reason: impl Into<String>) -> ToolResult {
    super::rejected_input(Error::Invalid(reason.into()))
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: Value) -> std::result::Result<T, ToolResult> {
    serde_json::from_value(arguments).map_err(|error| rejected(error.to_string()))
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
            return self.wait(invocation.arguments).await;
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
                let Ok(intent) = IntentId::try_from(format!(
                    "{SPAWN}{}",
                    input.name.as_deref().unwrap_or(&operation)
                )) else {
                    return Ok(rejected(
                        "name must be short printable ASCII without spaces",
                    ));
                };
                // The configured bound, never past the agent's hard limit:
                // a spawn the step would refuse is refused here, before the
                // model is told it succeeded.
                let limit = turn.max_children.map_or(MAX_OPEN_CHILDREN, |limit| {
                    usize::try_from(limit)
                        .map_or(MAX_OPEN_CHILDREN, |limit| limit.min(MAX_OPEN_CHILDREN))
                });
                if limit == 0 {
                    return Ok(super::rejected_input(Error::Unsupported(
                        "this host runs no children".into(),
                    )));
                }
                if !self.within(&intent, limit).await? {
                    return Ok(rejected(format!(
                        "at most {limit} open children; archive one or wait for one to finish"
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
                }
            }
            "send" => {
                let input: SendAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                let Some(to) = self.recipient(&input.agent, true) else {
                    return Ok(rejected("send reaches only the parent or an open child"));
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
        let value = match recorded {
            TurnEffect::Spawn { intent, .. } => json!({
                "agent": TurnEffect::child(&turn.agent, &intent).map_err(agent)?.as_str(),
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

    /// Reads the named children once; see [`WaitAgentInput`]. Its call
    /// dispatches only once they settled (see [`awaited`]), so a child still
    /// running here started again since, and is reported running.
    async fn wait(&self, arguments: Value) -> Result<ToolResult> {
        let Some(observer) = &self.turn.observer else {
            return Ok(rejected("this host observes no children"));
        };
        let (children, _) = match waited(&self.turn.agent, arguments) {
            Ok(waited) => waited,
            Err(rejected) => return Ok(rejected),
        };
        let mut settled = Vec::with_capacity(children.len());
        let mut running = Vec::new();
        for (name, child) in &children {
            let phase = match observer.settled(child).await {
                Ok(Some(phase)) => phase,
                Ok(None) => {
                    running.push(name.as_str());
                    continue;
                }
                Err(Error::NotFound(_)) => {
                    return Ok(rejected(format!("{name} names no child of this agent")));
                }
                Err(error) => return Err(error),
            };
            let mut entry = json!({
                "name": name,
                "agent": child.as_str(),
                "phase": serde_json::to_value(&phase)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                "answer": observer.answer(child).await?,
            });
            // The fork's branch: its changes reach this agent's project
            // only through `fs merge` of it. A spawn name names it; an agent
            // path does not.
            if AgentPath::try_from(name.clone()).is_err() {
                entry["branch"] = json!(format!("agents/{name}"));
            }
            settled.push(entry);
        }
        Ok(ToolResult {
            value: json!({"settled": settled, "running": running}),
        })
    }
}

/// The child of `agent` that `reference` names: a spawn name, or the agent
/// path of one of its children, open or settled.
fn named_child(agent: &AgentPath, reference: &str) -> Option<AgentPath> {
    if let Ok(path) = AgentPath::try_from(reference.to_owned()) {
        return (path.parent().as_ref() == Some(agent)).then_some(path);
    }
    spawned_child(agent, reference).ok()
}

/// The distinct children a `wait` of `agent` names, each with the reference
/// naming it, and whether it waits for any rather than all; the model's
/// error when the input names none or names a non-child.
fn waited(
    agent: &AgentPath,
    arguments: Value,
) -> std::result::Result<(Vec<(String, AgentPath)>, bool), ToolResult> {
    let input: WaitAgentInput = decode(arguments)?;
    let mut children: Vec<(String, AgentPath)> = Vec::with_capacity(input.agents.len());
    for reference in input.agents {
        let Some(child) = named_child(agent, &reference) else {
            return Err(rejected(format!(
                "{reference} names no child of this agent"
            )));
        };
        if !children.iter().any(|(_, known)| *known == child) {
            children.push((reference, child));
        }
    }
    if children.is_empty() {
        return Err(rejected("wait names no child"));
    }
    Ok((children, input.mode.unwrap_or_default() == WaitMode::Any))
}

/// What a call of `tool` with `arguments` waits for before it dispatches: for
/// a `wait` whose children have not settled (every one, or with `any` the
/// first), those still unsettled and whether any one suffices. Each child's
/// status is read once; nothing waits here. The turn then commits
/// [`crate::agent::Wait::Children`], holding no delivery, and its call
/// dispatches when the children's settlement resumes it.
///
/// # Errors
/// Stream failures reading a child's status.
pub(crate) async fn awaited(
    children: &dyn TurnChildren,
    tool: &str,
    arguments: &Value,
) -> Result<Option<(BTreeSet<AgentPath>, bool)>> {
    if tool != "wait" {
        return Ok(None);
    }
    // A malformed wait dispatches to receive its error.
    let Ok((named, any)) = waited(children.agent(), arguments.clone()) else {
        return Ok(None);
    };
    let mut unsettled = BTreeSet::new();
    for (_, child) in named {
        match children.settled(&child).await {
            Ok(Some(_)) if any => return Ok(None),
            Ok(Some(_)) => {}
            // A wait naming no child dispatches to receive its error.
            Err(Error::NotFound(_)) => return Ok(None),
            Ok(None) => {
                unsettled.insert(child);
            }
            Err(error) => return Err(error),
        }
    }
    Ok((!unsettled.is_empty()).then_some((unsettled, any)))
}

fn agent(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}

impl ToolExecutor for AgentTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.run(invocation, context))
    }

    /// Recording is create-once and the body is staged once, so observing an
    /// interrupted call re-derives exactly the effect and result it recorded.
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move { self.run(invocation, context).await.map(Some) })
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
                "Spawn a named child in a fork of your volumes and settled conversation. Give it a self-contained task and file ownership. Returns its name and path. Its changes stay on branch agents/<name> until you merge them; use send for messages, wait for its report, and fs to inspect and merge its branch",
            )?,
        ),
        (
            "send",
            definition::<SendAgentInput>(
                "send",
                "Send a message to your parent or an open child (by name or path); it is queued for the recipient before your next tool call runs",
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
                "Wait for children to finish their current work and messages. Returns each child's phase and last answer, plus its branch when addressed by spawn name. mode all (default) waits for every child; any waits for the first. Inspect changes with fs diff <branch>, then bring accepted changes into your project with fs merge <branch>",
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
/// bound, so a host omits or swaps each one like any standard tool.
/// `max_children`, capped at [`MAX_OPEN_CHILDREN`], bounds the agent's open
/// children; `spawn` refuses a child beyond it.
pub fn bind_agent_tools(
    registry: &mut ToolRegistry,
    agent: &AgentState,
    (effects, observer): (Arc<dyn TurnEffects>, Option<Arc<dyn TurnChildren>>),
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: u64,
    (names, max_children): (&[String], Option<u32>),
) -> Result<()> {
    let turn = Arc::new(Turn {
        agent: agent.path.clone(),
        parent: agent.parent.clone(),
        children: agent.open_children.clone(),
        effects,
        observer,
        writer,
        maximum_bytes,
        max_children,
        spawning: futures::lock::Mutex::new(()),
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
        assert_eq!(wait["$defs"]["WaitMode"]["enum"], json!(["all", "any"]));
        let check = |value| {
            crate::executor::check_json_schema_value(wait, &value, "tool", &mut None)
                .map(|checked| checked.is_ok())
        };
        assert!(check(json!({"agents": ["a"], "mode": "any"}))?);
        assert!(check(json!({"agents": ["a"]}))?);
        // Negative control: the enum still rejects what the type rejects.
        assert!(!check(json!({"agents": ["a"], "mode": "some"}))?);
        assert!(
            serde_json::from_value::<WaitAgentInput>(json!({"agents": [], "mode": "some"}))
                .is_err()
        );
        Ok(())
    }
}
