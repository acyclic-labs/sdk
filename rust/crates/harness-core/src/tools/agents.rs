//! The agent tools of the native agent model: `spawn`, `send` and `archive`.
//!
//! A call records exactly one effect through the turn's [`TurnEffects`] and
//! returns at once; the effect reaches the stream only in the step commit that
//! settles the turn, together with its conversation record. Spawning names a
//! child the parent may message or archive from its next turn on.

use crate::{
    Error, Result,
    agent::{AgentPath, AgentState, IntentId, MAX_OPEN_CHILDREN, TurnEffect, TurnEffects},
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
pub const AGENT_TOOLS: [&str; 3] = ["spawn", "send", "archive"];

/// Spawn one child agent.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnAgentInput {
    /// The child's task; its private volume is created holding it as `brief.md`.
    pub task: String,
    /// Optional child name, unique under this agent; it names the child's path.
    pub name: Option<String>,
}

/// Send one message to the parent or an open child.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendAgentInput {
    /// Recipient agent path, as returned by spawn.
    pub agent: String,
    /// Message text.
    pub message: String,
}

/// Archive one open child and, through its cascade, its descendants.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveAgentInput {
    /// Child agent path, as returned by spawn.
    pub agent: String,
}

/// What one turn's agent tools act for.
struct Turn {
    agent: AgentPath,
    parent: Option<AgentPath>,
    children: BTreeSet<AgentPath>,
    effects: Arc<dyn TurnEffects>,
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
    format!("{step:010}-{}", hex(&invocation.operation_id.into_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl AgentTool {
    async fn run(
        &self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> Result<ToolResult> {
        let turn = &*self.turn;
        let key = call_key(&invocation, context.as_ref());
        let operation = hex(&invocation.operation_id.into_bytes());
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
                    "spawn:{}",
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
                if !self.within(&intent, limit).await? {
                    return Ok(rejected(format!(
                        "at most {limit} open children; archive one or wait for one to finish"
                    )));
                }
                TurnEffect::Spawn {
                    intent,
                    brief: input.task,
                }
            }
            "send" => {
                let input: SendAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                let Some(to) = self.recipient(input.agent, true) else {
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
                let Some(target) = self.recipient(input.agent, false) else {
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
            TurnEffect::Spawn { intent, .. } => {
                json!({"agent": TurnEffect::child(&turn.agent, &intent).map_err(agent)?.as_str()})
            }
            TurnEffect::Send { to, .. } => json!({"sent": to.as_str()}),
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
    fn recipient(&self, agent: String, parent: bool) -> Option<AgentPath> {
        let agent = AgentPath::try_from(agent).ok()?;
        let turn = &*self.turn;
        (turn.children.contains(&agent) || (parent && turn.parent.as_ref() == Some(&agent)))
            .then_some(agent)
    }
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
        input_schema: schema::input::<T>()?,
        projection_schema: json_projection_schema(output.clone()),
        output_schema: output,
    };
    definition.validate(&mut None)?;
    Ok(definition)
}

/// Registers `agent`'s tools for one turn into `registry`, bound to the turn's
/// `effects` and its private `writer`, and selects them for the model over any
/// other revision of the same names. Only the agent tools `names` selects are
/// bound, so a host omits or swaps each one like any standard tool.
/// `max_children`, capped at [`MAX_OPEN_CHILDREN`], bounds the agent's open
/// children; `spawn` refuses a child beyond it.
pub fn bind_agent_tools(
    registry: &mut ToolRegistry,
    agent: &AgentState,
    effects: Arc<dyn TurnEffects>,
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: u64,
    (names, max_children): (&[String], Option<u32>),
) -> Result<()> {
    let turn = Arc::new(Turn {
        agent: agent.path.clone(),
        parent: agent.parent.clone(),
        children: agent.open_children.clone(),
        effects,
        writer,
        maximum_bytes,
        max_children,
        spawning: futures::lock::Mutex::new(()),
    });
    for (name, definition) in [
        (
            "spawn",
            definition::<SpawnAgentInput>(
                "spawn",
                "Spawn a child agent on a fork of your volumes and conversation; returns its agent path",
            )?,
        ),
        (
            "send",
            definition::<SendAgentInput>(
                "send",
                "Send a message to your parent or an open child, delivered when this turn ends",
            )?,
        ),
        (
            "archive",
            definition::<ArchiveAgentInput>(
                "archive",
                "Archive an open child and its descendants when this turn ends",
            )?,
        ),
    ] {
        if !names.iter().any(|selected| selected == name) {
            continue;
        }
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
