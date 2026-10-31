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
//! [`awaited_with_io`]) and the turn commits [`crate::agent::Wait::Children`], which
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
        AgentPath, AgentProfilePin, AgentState, IntentId, MAX_OPEN_CHILDREN, SpawnProfiles,
        TurnChildren, TurnEffect, TurnEffects,
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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

/// Revision of the agent tools, selected for the model over other revisions.
pub const REVISION: &str = "agent-1";
/// Names of the agent tools; every agent turn is granted them.
pub const AGENT_TOOLS: [&str; 4] = ["spawn", "send", "archive", "wait"];

/// Spawn one child agent.
#[derive(Debug, Deserialize, JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct SpawnAgentInput {
    /// Self-contained task brief, including the child's file ownership.
    pub task: String,
    /// Child name, unique under this agent; it names the child's branch.
    pub name: String,
    /// Role file selecting the child's model, context and permissions.
    pub role: String,
}

/// Send one message to the parent or an open child.
#[derive(Debug, Deserialize, JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct SendAgentInput {
    /// Recipient: a child's name or agent path, as spawn returned them, or
    /// the parent's agent path.
    pub agent: String,
    /// Message text.
    pub message: String,
    /// Optional plan file, copied at this exact version before delivery.
    pub file: Option<SendAgentFile>,
}

/// A source snapshot selected through the same read path and version as read/edit.
#[derive(Debug, Deserialize, JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct SendAgentFile {
    /// Project path or `.volumes/<name>/path` in this executing agent's volumes.
    pub path: String,
    /// Opaque immutable version returned by read; omission pins the current file.
    pub version: Option<String>,
}

/// Archive one open child and, through its cascade, its descendants.
#[derive(Debug, Deserialize, JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct ArchiveAgentInput {
    /// Child name or agent path, as spawn returned them.
    pub agent: String,
}

/// Wait for children to settle.
#[derive(Debug, Deserialize, JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct WaitAgentInput {
    /// Child names or agent paths, as spawn returned them.
    pub agents: Vec<String>,
    /// `all` (the default) waits for every child, `any` for the first.
    pub mode: Option<WaitMode>,
}

/// Which children a wait returns on.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, JsonSchema, crate::HeapLayout,
)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    /// Every named child settled.
    #[default]
    All,
    /// The first named child settled.
    Any,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for SpawnAgentInput {
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.task.capacity() as u64,
            self.name.capacity() as u64,
            self.role.capacity() as u64,
        ])
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for SendAgentInput {
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.agent.capacity() as u64,
            self.message.capacity() as u64,
            self.file.as_ref().map_or(0, |file| {
                (file.path.capacity() + file.version.as_ref().map_or(0, String::capacity)) as u64
            }),
        ])
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for ArchiveAgentInput {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(self.agent.capacity() as u64)
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for WaitAgentInput {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.agents.iter().try_fold(
            crate::core::owned_buffer::<String>(self.agents.capacity())?,
            |bytes, name| crate::core::owned_sum([bytes, name.capacity() as u64]),
        )
    }
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
    profile: Option<AgentProfilePin>,
    profiles: Option<Arc<dyn SpawnProfiles>>,
    /// Held from a spawn's limit check through its record, so concurrent
    /// spawns of one turn see each other.
    spawning: futures::lock::Mutex<()>,
}

struct AgentTool {
    name: &'static str,
    turn: Arc<Turn>,
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

fn format_work(
    arguments: std::fmt::Arguments<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<String> {
    crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(arguments, work)
    })
}

fn vector<T>(capacity: usize, io: Option<&crate::executor::PreparationIo>) -> Result<Vec<T>> {
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let values = crate::core::reserve_copy_vector(capacity, &mut quota)?;
        quota.transfer();
        Ok(values)
    })
}

fn invalid(reason: &'static str) -> Error {
    Error::tool_rejected(reason)
}

fn intent(
    parts: std::fmt::Arguments<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<IntentId> {
    IntentId::try_from(format_work(parts, io)?)
        .map_err(|_| invalid("agent intent is not printable ASCII or exceeds its record frame"))
}

fn named_child_with_io(
    parent: &AgentPath,
    reference: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<AgentPath> {
    if reference.starts_with("harness/agents/") {
        let path = AgentPath::try_from(format_work(format_args!("{reference}"), io)?)
            .map_err(|_| invalid("invalid child path"))?;
        let direct = path.depth() == parent.depth() + 1 && parent.contains(&path);
        return if direct {
            Ok(path)
        } else {
            Err(invalid("path names no direct child"))
        };
    }
    let identity = intent(format_args!("{SPAWN}{reference}"), io)?;
    crate::executor::with_history_io(io, |work| parent.child_for_intent(identity.as_str(), work))
}

fn boxed_file(
    file: crate::conversation::FileRef,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Box<crate::conversation::FileRef>> {
    crate::executor::with_history_io(io, |work| {
        if let Some(work) = work {
            let bytes = std::mem::size_of::<crate::conversation::FileRef>() as u64;
            work.retain_owned(bytes)?;
            work.allocation_attempt(bytes)?;
        }
        Ok(Box::new(file))
    })
}

fn retained_publication<'a>(
    output: (
        crate::conversation::FileRef,
        Option<crate::conversation::ContentHold<'a>>,
    ),
) -> Result<crate::conversation::FileRef> {
    #[cfg(feature = "filesystem")]
    if let Some(hold) = output.1 {
        hold.into_retained()?;
    }
    Ok(output.0)
}

#[derive(Serialize)]
#[serde(untagged)]
enum AgentResult<'a> {
    Spawn { agent: &'a str, name: &'a str },
    Send { sent: &'a str, status: &'static str },
    Archive { archived: &'a str },
}

#[derive(Serialize)]
struct SettledChild<'a> {
    name: &'a str,
    agent: &'a AgentPath,
    phase: crate::agent::Phase,
    answer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    branch: Option<String>,
}

#[derive(Serialize)]
struct WaitResult<'a> {
    settled: Vec<SettledChild<'a>>,
    running: Vec<&'a str>,
}

impl AgentTool {
    async fn run(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let original_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = original_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<&crate::executor::PreparationIo> = None;
        self.authorize(
            context.map(ToolContext::effective_scope),
            context,
            invocation,
            work,
        )?;
        let turn = &*self.turn;
        if self.name == "wait" {
            return self.wait(&invocation.arguments, work, io).await;
        }
        let operation = invocation.operation_id.into_bytes();
        let mut hexadecimal = [0u8; 32];
        let alphabet = b"0123456789abcdef";
        for (index, byte) in operation.iter().enumerate() {
            hexadecimal[2 * index] = alphabet[(byte >> 4) as usize];
            hexadecimal[2 * index + 1] = alphabet[(byte & 15) as usize];
        }
        let operation = std::str::from_utf8(&hexadecimal)
            .map_err(|_| invalid("operation identity is not ASCII"))?;
        let step = context.map_or(0, |context| context.model_turn.1);
        let key = format_work(format_args!("{step:010}-{operation}"), io)?;
        let _spawning = if self.name == "spawn" {
            Some(turn.spawning.lock().await)
        } else {
            None
        };
        if let Some(recorded) = turn.effects.recorded_call_with_io(&key, io)?.await? {
            return self.recorded_result(recorded, work, io);
        }
        let access = || ContentAccess {
            ceiling: context.map(|context| context.effective_scope().grants()),
            io,
        };
        let effect = match self.name {
            "spawn" => {
                let (input, _input) =
                    crate::tool::decode::<SpawnAgentInput>(&invocation.arguments, work)?;
                if input.task.is_empty() || input.task.len() as u64 > turn.maximum_bytes {
                    return Err(invalid("task is empty or exceeds original admission"));
                }
                if input.name.is_empty()
                    || input.name.contains('/')
                    || input.name == "."
                    || input.name == ".."
                {
                    return Err(invalid("name must be a nonempty branch component"));
                }
                let intent = intent(format_args!("{SPAWN}{}", input.name), io)?;
                let (Some(parent), Some(profiles)) = (&turn.profile, &turn.profiles) else {
                    return Err(invalid("host has no admitted child profiles"));
                };
                let profile = if io.is_some() {
                    let pin = profiles.child_ref(parent, &input.role)?;
                    crate::executor::with_history_io(io, |work| {
                        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                        let pin = crate::core::copy(pin, &mut quota, |pin, _| {
                            Ok((pin.role.capacity() + pin.model.capacity()) as u64)
                        })?;
                        quota.transfer();
                        Ok::<_, Error>(pin)
                    })?
                } else {
                    profiles.child(parent, &input.role)?
                };
                if !self.within(&intent, MAX_OPEN_CHILDREN, io).await? {
                    return Err(invalid("original open-child state has no free slot"));
                }
                let path = format_work(format_args!("briefs/{operation}.md"), io)?;
                let brief = retained_publication(
                    turn.writer
                        .stage_once(
                            invocation.operation_id,
                            &path,
                            input.task.as_bytes(),
                            "text/markdown",
                            "brief",
                            access(),
                        )?
                        .await?,
                )?;
                TurnEffect::Spawn {
                    intent,
                    brief: boxed_file(brief, io)?,
                    profile,
                }
            }
            "send" => {
                let (input, _input) =
                    crate::tool::decode::<SendAgentInput>(&invocation.arguments, work)?;
                let to = self.recipient(&input.agent, true, io)?;
                if input.message.len() as u64 > turn.maximum_bytes {
                    return Err(invalid("message exceeds original admission"));
                }
                let receipt = turn
                    .writer
                    .observe_staged_once(invocation.operation_id, turn.maximum_bytes, access())?
                    .await?;
                let body = if let Some(file) = receipt.0 {
                    #[cfg(feature = "filesystem")]
                    if let Some(hold) = receipt.1 {
                        hold.into_retained()?;
                    }
                    file
                } else {
                    let message = if let Some(source) = input.file.as_ref() {
                        let context = context.ok_or_else(|| {
                            invalid("sending a file requires its admitted tool context")
                        })?;
                        let (file, bytes, _source) = context
                            .read_tool_source(&source.path, source.version.as_deref(), work, io)
                            .await?;
                        let text = std::str::from_utf8(&bytes)
                            .map_err(|_| invalid("plan snapshot must be UTF-8 text"))?;
                        let (version, _version) =
                            crate::tool::files::encode_file_version(&file, work)?;
                        format_work(
                            format_args!(
                                "{}\n\nSnapshot version: {version}\nPlan snapshot: {}\n\n{text}",
                                input.message, source.path
                            ),
                            io,
                        )?
                    } else {
                        input.message
                    };
                    if message.len() as u64 > turn.maximum_bytes {
                        return Err(invalid("plan snapshot exceeds original message admission"));
                    }
                    let path = format_work(format_args!("messages/{operation}.md"), io)?;
                    retained_publication(
                        turn.writer
                            .stage_once(
                                invocation.operation_id,
                                &path,
                                message.as_bytes(),
                                "text/markdown",
                                "message",
                                access(),
                            )?
                            .await?,
                    )?
                };
                TurnEffect::Send {
                    to,
                    id: intent(format_args!("send:{operation}"), io)?,
                    body: boxed_file(body, io)?,
                }
            }
            "archive" => {
                let (input, _input) =
                    crate::tool::decode::<ArchiveAgentInput>(&invocation.arguments, work)?;
                TurnEffect::Archive {
                    target: self.recipient(&input.agent, false, io)?,
                    id: intent(format_args!("archive:{operation}"), io)?,
                }
            }
            _ => return Err(invalid("unknown agent tool")),
        };
        let recorded = turn.effects.record_with_io(&key, &effect, io)?.await?;
        self.recorded_result(recorded, work, io)
    }

    fn recorded_result(
        &self,
        recorded: TurnEffect,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let child;
        let result = match &recorded {
            TurnEffect::Spawn { intent, .. } => {
                child = crate::executor::with_history_io(io, |work| {
                    self.turn.agent.child_for_intent(intent.as_str(), work)
                })?;
                AgentResult::Spawn {
                    agent: child.as_str(),
                    name: intent
                        .as_str()
                        .strip_prefix(SPAWN)
                        .unwrap_or(intent.as_str()),
                }
            }
            TurnEffect::Send { to, .. } => AgentResult::Send {
                sent: to.as_str(),
                status: "queued",
            },
            TurnEffect::Archive { target, .. } => AgentResult::Archive {
                archived: target.as_str(),
            },
        };
        let (value, custody) = crate::tool::value(&result, self.turn.maximum_bytes, work)?;
        Ok((ToolResult { value }, custody))
    }

    async fn within(
        &self,
        intent: &IntentId,
        limit: usize,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        let turn = &*self.turn;
        let child = crate::executor::with_history_io(io, |work| {
            turn.agent.child_for_intent(intent.as_str(), work)
        })?;
        if turn.children.contains(&child) {
            return Err(invalid("an agent with this name exists"));
        }
        let recorded = turn.effects.recorded_with_io(io)?.await?;
        let mut children = vector(recorded.len(), io)?;
        for effect in &recorded {
            if let TurnEffect::Spawn {
                intent: recorded, ..
            } = effect
            {
                if recorded == intent {
                    return Err(invalid("an agent with this name exists"));
                }
                let child = crate::executor::with_history_io(io, |work| {
                    turn.agent.child_for_intent(recorded.as_str(), work)
                })?;
                if !turn.children.contains(&child) && !children.contains(&child) {
                    children.push(child);
                }
            }
        }
        Ok(turn.children.len() + children.len() < limit)
    }

    fn recipient(
        &self,
        reference: &str,
        parent: bool,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<AgentPath> {
        if parent
            && self
                .turn
                .parent
                .as_ref()
                .is_some_and(|path| path.as_str() == reference)
        {
            return AgentPath::try_from(format_work(format_args!("{reference}"), io)?)
                .map_err(|_| invalid("parent path is malformed"));
        }
        let child = named_child_with_io(&self.turn.agent, reference, io)?;
        if !self.turn.children.contains(&child) {
            return Err(invalid("recipient is not an open child"));
        }
        Ok(child)
    }

    async fn wait(
        &self,
        arguments: &Value,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let observer = self
            .turn
            .observer
            .as_ref()
            .ok_or_else(|| invalid("host observes no children"))?;
        let (input, _input) = crate::tool::decode::<WaitAgentInput>(arguments, work)?;
        if input.agents.is_empty() {
            return Err(invalid("wait names no child"));
        }
        let mut selected = vector(input.agents.len(), io)?;
        for name in &input.agents {
            let child = named_child_with_io(&self.turn.agent, name, io)?;
            if !selected.iter().any(|(_, known)| known == &child) {
                selected.push((name.as_str(), child));
            }
        }
        let mut result = WaitResult {
            settled: vector(selected.len(), io)?,
            running: vector(selected.len(), io)?,
        };
        for (name, child) in &selected {
            let Some(phase) = observer.settled_with_io(child, io)?.await? else {
                result.running.push(*name);
                continue;
            };
            let answer = observer.answer_with_io(child, io)?.await?;
            let branch = if name.starts_with("harness/agents/") {
                None
            } else {
                Some(format_work(format_args!("agents/{name}"), io)?)
            };
            result.settled.push(SettledChild {
                name,
                agent: child,
                phase,
                answer,
                branch,
            });
        }
        let (value, custody) = crate::tool::value(&result, self.turn.maximum_bytes, work)?;
        Ok((ToolResult { value }, custody))
    }
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
pub(crate) async fn awaited_with_io(
    children: &dyn TurnChildren,
    tool: &str,
    arguments: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Option<(BTreeSet<AgentPath>, bool)>> {
    if tool != "wait" {
        return Ok(None);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let work = if let Some(io) = io {
        Some(io.original_native_work().ok_or_else(|| {
            crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "child wait requires original native work",
            )
        })?)
    } else {
        None
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let work: crate::NativeWork<'_> = None;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    if io.is_some() {
        return Err(crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "native child wait observation is unavailable",
        ));
    }
    // Invalid model arguments dispatch to the tool's normal refusal. Resource,
    // cancellation and provider faults retain their original terminal causes.
    let input =
        match crate::tool::decode::<WaitAgentInput>(arguments, work).map_err(Error::tool_input) {
            Ok(input) => input,
            Err(Error::Invalid(_) | Error::ToolRejected { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
    if input.0.agents.is_empty() {
        return Ok(None);
    }
    let any = input.0.mode.unwrap_or_default() == WaitMode::Any;
    let mut unsettled = BTreeSet::new();
    // Fund the original result tree before std creates its nodes. Its size is
    // bounded by the explicitly supplied references, never by the agent tree.
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        crate::core::admit_tree_storage::<AgentPath>(input.0.agents.len(), io)?;
    }
    for reference in &input.0.agents {
        let child = match named_child_with_io(children.agent(), reference, io) {
            Ok(child) => child,
            Err(Error::ToolRejected { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        if unsettled.contains(&child) {
            continue;
        }
        let settled = children.settled_with_io(&child, io)?.await;
        match settled {
            Ok(Some(_)) if any => return Ok(None),
            Ok(Some(_)) => {}
            Err(error) if error.code() == crate::wire::ErrorCode::NotFound => return Ok(None),
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
    fn authorize(
        &self,
        scope: Option<&crate::runtime::RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<crate::executor::PreparationIo> = None;
        let _ = work;
        if self.name != invocation.name
            || context.is_some_and(|context| {
                context.operation_id() != invocation.operation_id
                    || context.call_id() != invocation.call_id
            })
        {
            return Err(invalid("agent tool requires its exact admitted invocation"));
        }
        crate::executor::with_history_io(io.as_ref(), |history| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), history)
        })?;
        let granted = match self.name {
            "spawn" => "tool:call:spawn",
            "send" => "tool:call:send",
            "wait" => "tool:call:wait",
            "archive" => "tool:call:archive",
            _ => return Err(invalid("unknown agent tool")),
        };
        if let Some(scope) = context.map(ToolContext::effective_scope).or(scope) {
            if !scope.grants().contains(granted) {
                return Err(invalid("agent tool is not granted"));
            }
        } else {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(invalid("native agent tool requires its original scope"));
            }
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let (result, custody) = self.run(invocation, Some(context), Some(work)).await?;
            Ok((
                result,
                custody.ok_or_else(|| invalid("native agent result lacks original custody"))?,
            ))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let (result, custody) = self.run(invocation, Some(context), Some(work)).await?;
            Ok((
                Some(result),
                custody.ok_or_else(|| invalid("native agent replay lacks original custody"))?,
            ))
        })
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.run(&invocation, context.as_ref(), None)
                .await
                .map(|(result, _)| result)
        })
    }

    /// Recording is create-once and the body is staged once, so observing an
    /// interrupted call re-derives exactly the effect and result it recorded.
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.run(&invocation, context.as_ref(), None)
                .await
                .map(|(result, _)| Some(result))
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
                "Spawn a named child with its role's model and tools, in a fork of your volumes. Workers inherit your settled conversation; advisors and reviewers see only the brief and explicitly sent files, so give them a self-contained task and file ownership. Cross-model workers project the history and pay a cold-cache cost. Returns name and path. Changes stay on agents/<name> until fs merge; send updates and wait for the child's final report",
            )?,
        ),
        (
            "send",
            definition::<SendAgentInput>(
                "send",
                "Send a message to your parent or an open child (by name or path). Optional file copies one immutable plan snapshot and names its version. A revised plan must name the received snapshot version as its base in the message or plan; the recipient never reads your live file. It is queued before your next tool call runs",
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
/// `profiles` is the host's original immutable profile admission; without it,
/// spawning refuses before staging a brief or recording an effect.
pub fn bind_agent_tools(
    registry: &mut ToolRegistry,
    agent: &AgentState,
    (effects, observer): (Arc<dyn TurnEffects>, Option<Arc<dyn TurnChildren>>),
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: u64,
    (names, profiles): (&[String], Option<Arc<dyn SpawnProfiles>>),
) -> Result<()> {
    let turn = Arc::new(Turn {
        agent: agent.path.clone(),
        parent: agent.parent.clone(),
        children: agent.open_children.clone(),
        effects,
        observer,
        writer,
        maximum_bytes,
        profile: agent.profile.clone(),
        profiles,
        spawning: futures::lock::Mutex::new(()),
    });
    for (name, definition) in selected(names)? {
        let tool = Tool {
            skill_source: None,
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
