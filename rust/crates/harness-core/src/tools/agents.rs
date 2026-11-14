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
        AgentPath, AgentState, CreationConfig, IntentId, SpawnTemplates, TurnChildren, TurnEffect,
        TurnEffects, TurnOutcome, named_child,
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
use std::{collections::BTreeSet, sync::Arc};

/// Revision of the agent tools, selected for the model over other revisions.
pub const REVISION: &str = "agent-5";
/// Names of the agent tools; every agent turn is granted them.
pub const AGENT_TOOLS: [&str; 4] = ["spawn", "send", "archive", "wait"];

/// Spawn one child agent.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnAgentInput {
    /// Optional fallback provider/model; a configured role model always wins.
    #[serde(default)]
    pub model: Option<String>,
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
    /// Message text. Relative paths in prose refer to the recipient's fork;
    /// select sender files with `files` to share their immutable contents.
    pub message: String,
    /// Sender workspace paths to share read-only, pinned at this send.
    /// Files written after the recipient forked must be selected here.
    #[serde(default)]
    pub files: Vec<String>,
    /// Optional identity of the message being answered.
    pub re: Option<String>,
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

/// Wait for children or the replies to this agent's original sends.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WaitAgentInput {
    /// Child names or agent paths, as spawn returned them.
    #[serde(default)]
    pub agents: Vec<String>,
    /// Original send intent IDs returned by this agent's send calls. Replies
    /// report only that request's settlement, not the recipient's history.
    #[serde(default)]
    pub sent: Vec<String>,
    /// How many of them must settle before the wait returns: all of them by
    /// default, 1 for the first, or any quorum in between.
    pub need: Option<u32>,
    /// Return after this many milliseconds even if fewer settled.
    pub timeout_ms: Option<u64>,
    /// Also resume on inbox input. With no agents, wait for input or the alarm.
    #[serde(default)]
    pub input: bool,
}

/// What one turn's agent tools act for.
struct Turn {
    spawned_children: u64,
    spawning: futures::lock::Mutex<()>,
    agent: AgentPath,
    parent: Option<AgentPath>,
    children: crate::agent::Children,
    effects: Arc<dyn TurnEffects>,
    /// What `wait` observes; `None` when the host observes no children.
    observer: Option<Arc<dyn TurnChildren>>,
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: Option<u64>,
    /// This agent's creation snapshot and the host's template reader; spawn refuses
    /// when either is absent.
    templates: Option<(CreationConfig, Arc<dyn SpawnTemplates>)>,
    volumes: Vec<crate::agent::WorkspaceEntry>,
    /// Only the original deferred invocation consumes its named release event.
    released: Option<crate::agent::WaitResume>,
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

/// The child `parent`'s `spawn` tool names `name`: the one derivation every
/// host resolving a child by its spawn name (`wait`, `agents/<name>`) shares.
///
/// # Errors
/// When `name` is not a valid spawn name or the child would exceed the depth.
pub fn spawned_child(parent: &AgentPath, name: &str) -> Result<AgentPath> {
    let intent = spawn_intent(name, 0).map_err(Error::from)?;
    TurnEffect::child(parent, &intent).map_err(Error::from)
}

/// Resolves the child operand of diff, merge, discard or show exactly once. File
/// operands (including `add agents/file`) retain their ordinary path meaning.
/// The driver supplies its existing child derivation; the repository key is
/// always the complete canonical address, including the owning ancestry.
/// Both local names and canonical addresses must name a direct child.
pub fn resolve_child_branch(
    parent: &AgentPath,
    arguments: &mut [String],
    resolve: impl FnOnce(&AgentPath, &str) -> Result<AgentPath>,
) -> Result<Option<AgentPath>> {
    let Some((index, branch, path)) = super::fs_command::child_branch_operand(arguments) else {
        return Ok(None);
    };
    let child = if let Some(name) = branch.strip_prefix("agents/") {
        resolve(parent, name)?
    } else if branch.starts_with("harness/agents/") {
        AgentPath::try_from(branch.to_owned()).map_err(Error::from)?
    } else {
        resolve(parent, branch)?
    };
    if !child.is_child_of(parent) {
        return Err(Error::Invalid(
            "only direct children can be coordinated; ask the owning parent".into(),
        ));
    }
    arguments[index] = path.map_or_else(|| child.to_string(), |path| format!("{child}:{path}"));
    Ok(Some(child))
}

impl AgentTool {
    async fn send_files(
        &self,
        operation: crate::OperationId,
        paths: &[String],
        context: Option<&ToolContext>,
    ) -> Result<Option<Box<crate::conversation::FileRef>>> {
        use crate::conversation::{Attachment, ContentChange};

        if paths.is_empty() {
            return Ok(None);
        }
        let context = context.ok_or_else(|| {
            Error::tool_rejected("send files requires the sender's workspace context")
        })?;
        let writer = &self.turn.writer;
        // The destination is host-owned input storage. Source reads below
        // still use the sender's original tool context and effective scope.
        let access = ContentAccess::default();
        let identity = |part: &[u8]| {
            let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.send-files/v1");
            hash.update(&operation.into_bytes());
            hash.update(part);
            let mut id = [0; 16];
            id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
            crate::OperationId::from_bytes(id)
        };
        let manifest_operation = identity(b"manifest");
        if let Some(manifest) = writer
            .observe_staged_once(manifest_operation, None, self.turn.maximum_bytes, access)?
            .await?
            .0
        {
            return Ok(Some(Box::new(manifest)));
        }
        let copies_operation = identity(b"copies");
        let copies = match writer
            .observe_changes(
                writer.volume(),
                copies_operation,
                self.turn.maximum_bytes,
                access,
            )?
            .await?
            .0
        {
            Some(copies) => copies,
            None => {
                // Resolve with the same path and authority checks as read. Stage
                // the complete selected set in one publication, never one commit
                // per file. No file is delivered if a member cannot be read.
                let mut sources = Vec::new();
                let mut remaining = self.turn.maximum_bytes;
                for (index, path) in paths.iter().enumerate() {
                    let destination = format!("inputs/messages/{operation}/files/{index}");
                    let exhausted =
                        || Error::tool_rejected("selected files exceed the send byte budget");
                    if let Some(budget) = remaining {
                        remaining = Some(
                            budget
                                .checked_sub(
                                    destination.len() as u64
                                        + std::mem::size_of::<(
                                            String,
                                            crate::conversation::FileRef,
                                            acyclic_stream::OwnedBytes,
                                        )>() as u64,
                                )
                                .ok_or_else(exhausted)?,
                        );
                    }
                    let (file, bytes, _) = context
                        .read_tool_path_bounded(path, None, remaining, None)
                        .await?;
                    if let Some(budget) = remaining {
                        let metadata_bytes = file.logical_owned_bytes()?;
                        remaining = Some(
                            budget
                                .checked_sub(bytes.len() as u64)
                                .and_then(|remaining| remaining.checked_sub(metadata_bytes))
                                .ok_or_else(exhausted)?,
                        );
                    }
                    sources.push((destination, file, bytes));
                }
                let changes = sources
                    .iter()
                    .map(|(path, file, bytes)| ContentChange::CreateAbsent {
                        path,
                        bytes: bytes.as_ref(),
                        media_type: file.descriptor().media_type(),
                        display_name: file.display_name(),
                    })
                    .collect::<Vec<_>>();
                writer
                    .stage_changes(writer.volume(), copies_operation, &changes, access)?
                    .await?
                    .0
            }
        };
        let items = copies
            .files()
            .cloned()
            .map(|file| Attachment { file, label: None })
            .collect::<Vec<_>>();
        let bytes = crate::conversation::encode_attachment_manifest(&items)?;
        let manifest = writer
            .stage_once(
                manifest_operation,
                &format!("inputs/messages/{operation}/files/manifest.json"),
                &bytes,
                crate::conversation::ATTACHMENT_MANIFEST_MEDIA_TYPE,
                "attachments.json",
                access,
            )?
            .await?
            .0;
        Ok(Some(Box::new(manifest)))
    }

    fn release(&self, operation: crate::OperationId) -> Option<crate::agent::WaitReason> {
        self.turn
            .released
            .as_ref()
            .filter(|release| release.operation == operation)
            .and_then(|release| release.reason.clone())
    }
    async fn run(
        &self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> Result<ToolResult> {
        let turn = &*self.turn;
        if self.name == "wait" {
            return match self
                .prepare_wait(
                    invocation.operation_id,
                    &invocation.arguments,
                    self.release(invocation.operation_id),
                )
                .await?
            {
                ToolPreparation::Ready(result) => Ok(result),
                ToolPreparation::Deferred(_) => Err(Error::Unsupported(
                    "wait requires its deferred actor condition before execution".into(),
                )),
            };
        }
        let key = call_key(&invocation, context.as_ref());
        let operation = crate::agent::operation_key(invocation.operation_id);
        if self.name == "send" {
            let id = IntentId::try_from(format!("send:{operation}")).map_err(Error::from)?;
            if let Some(sent) = turn.effects.sent(&id).await? {
                return Ok(sent_result(sent));
            }
        }
        let _spawning = if self.name == "spawn" {
            Some(turn.spawning.lock().await)
        } else {
            None
        };
        let effect = match self.name {
            "spawn" => {
                let input: SpawnAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                if input.task.is_empty()
                    || turn
                        .maximum_bytes
                        .is_some_and(|maximum| input.task.len() as u64 > maximum)
                {
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
                if let Some(limit) = parent
                    .topology
                    .as_ref()
                    .and_then(|topology| topology.spawn_limit(turn.parent.is_none()))
                {
                    let mut pending = BTreeSet::new();
                    for effect in turn.effects.recorded().await? {
                        if let TurnEffect::Spawn { intent, .. } = effect {
                            pending.insert(intent);
                        }
                    }
                    if !pending.contains(&intent)
                        && turn.spawned_children.saturating_add(pending.len() as u64) >= limit
                    {
                        return Ok(ToolResult {
                            value: json!({"error": format!("tool execution failed: forced topology permits at most {limit} total spawns")}),
                        });
                    }
                }
                let peers = match peer_grants(&turn.agent, input.peers) {
                    Ok(peers) => peers,
                    Err(rejected) => return Ok(rejected),
                };
                let mut fallback = std::borrow::Cow::Borrowed(parent);
                if let Some(model) = input.model {
                    if let Err(error) = crate::model::selection::validate(&model) {
                        return Ok(super::rejected_input(error));
                    }
                    fallback.to_mut().model = Some(model);
                }
                let mut profile = match templates
                    .child(
                        &fallback,
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
                let brief = turn
                    .writer
                    .stage_once(
                        invocation.operation_id,
                        &format!("inputs/briefs/{operation}.md"),
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
                let re = match input.re.map(IntentId::try_from).transpose() {
                    Ok(re) => re,
                    Err(error) => return Ok(rejected(error.to_string())),
                };
                // A peer by path, or by sibling name; the scope's grants or
                // the parent's spawn-time peers authorize it.
                let recipient = match self.recipient(&input.agent, true).await? {
                    Some(path) => Some((path.into(), None)),
                    None => self.peer(&input.agent, context.as_ref()).await?,
                };
                let Some((to, peer)) = recipient else {
                    return Ok(rejected(
                        "send requires a parent, open child or authorized peer inbox",
                    ));
                };
                if turn
                    .maximum_bytes
                    .is_some_and(|maximum| input.message.len() as u64 > maximum)
                {
                    return Ok(rejected("message is too large"));
                }
                let attachments = self
                    .send_files(invocation.operation_id, &input.files, context.as_ref())
                    .await?;
                let body = turn
                    .writer
                    .stage_once(
                        invocation.operation_id,
                        &format!("inputs/messages/{operation}.md"),
                        input.message.as_bytes(),
                        "text/markdown",
                        "message",
                        ContentAccess::default(),
                    )?
                    .await?
                    .0;
                TurnEffect::Send {
                    peer,
                    delivery: input.delivery.unwrap_or_default(),
                    to,
                    id: IntentId::try_from(format!("send:{operation}")).map_err(Error::from)?,
                    re,
                    body: Box::new(body),
                    attachments,
                }
            }
            "archive" => {
                let input: ArchiveAgentInput = match decode(invocation.arguments) {
                    Ok(input) => input,
                    Err(rejected) => return Ok(rejected),
                };
                let Some(target) = self.recipient(&input.agent, false).await? else {
                    return Ok(rejected(self.unarchivable(&input.agent).await?));
                };
                TurnEffect::Archive {
                    target,
                    id: IntentId::try_from(format!("archive:{operation}")).map_err(Error::from)?,
                }
            }
            _ => return Err(Error::Unsupported(format!("agent tool {}", self.name))),
        };
        let recorded = match turn.effects.record(&key, &effect).await {
            Ok(recorded) => recorded,
            Err(error @ (Error::Conflict(_) | Error::MergeConflict { .. })) => {
                return Ok(super::rejected_input(error));
            }
            Err(error) => return Err(error),
        };
        self.result(recorded)
    }

    fn result(&self, recorded: TurnEffect) -> Result<ToolResult> {
        let value = match recorded {
            TurnEffect::Spawn { intent, .. } => json!({
                "agent": TurnEffect::child(&self.turn.agent, &intent).map_err(Error::from)?.as_str(),
                "name": spawn_name(&intent).map_err(Error::from)?,
            }),
            TurnEffect::Send { to, id, .. } => {
                json!({"sent": to, "id": id, "status": "queued"})
            }
            TurnEffect::Archive { target, .. } => {
                json!({"archived": target.as_str(), "branch": target.as_str()})
            }
        };
        Ok(ToolResult { value })
    }

    /// The open children: those open when the turn started plus this turn's
    /// recorded spawns.
    async fn open(&self) -> Result<crate::agent::Children> {
        let turn = &*self.turn;
        let mut open = turn.children.clone();
        for effect in turn.effects.recorded().await? {
            if let TurnEffect::Spawn { intent, .. } = effect {
                open.insert(&TurnEffect::child(&turn.agent, &intent).map_err(Error::from)?)
                    .await
                    .map_err(Error::from)?;
            }
        }
        Ok(open)
    }

    /// `agent` when this turn may reach it: an open child, or the parent too.
    async fn recipient(&self, reference: &str, parent: bool) -> Result<Option<AgentPath>> {
        let turn = &*self.turn;
        if parent
            && let Some(to) = turn
                .parent
                .as_ref()
                .filter(|path| path.as_str() == reference)
        {
            return Ok(Some(to.clone()));
        }
        let Some(child) =
            named_child(turn.observer.as_deref(), &turn.agent, reference, false).await?
        else {
            return Ok(None);
        };
        Ok(turn
            .children
            .contains(&child)
            .await
            .map_err(Error::from)?
            .then_some(child))
    }

    async fn peer(
        &self,
        reference: &str,
        context: Option<&ToolContext>,
    ) -> Result<
        Option<(
            crate::agent::AgentAddress,
            Option<crate::agent::PeerAdmission>,
        )>,
    > {
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
            if let Some(admission) = peer_grant_allowed(
                turn.observer.as_deref(),
                grants,
                turn.parent.as_ref(),
                &path,
            )
            .await?
            {
                return Ok(Some((path.into(), admission)));
            }
        }
        Ok(None)
    }

    /// Why `archive` of `agent` has nothing to archive, in the terms `wait`
    /// reports: a settled child is named with the phase `wait` returns for
    /// it, so the two tools never disagree about one child.
    async fn unarchivable(&self, agent: &str) -> Result<String> {
        let child =
            named_child(self.turn.observer.as_deref(), &self.turn.agent, agent, true).await?;
        let (Some(child), Some(observer)) = (child, &self.turn.observer) else {
            return Ok(format!("{agent} names no child of this agent"));
        };
        Ok(match observer.settled(&child).await {
            Ok(observed) if observed.settled => {
                let phase = serde_json::to_string(&observed.phase)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                format!("{agent} already settled ({phase}); wait returns its outcome")
            }
            Ok(_) => format!("{agent} is not an open child of this agent"),
            Err(Error::NotFound(_)) => format!("{agent} names no child of this agent"),
            Err(error) => return Err(error),
        })
    }

    /// Select every status and answer position once under this turn's
    /// authority. Readiness and the result consume the same observations.
    async fn prepare_wait(
        &self,
        operation: crate::OperationId,
        arguments: &Value,
        released: Option<crate::agent::WaitReason>,
    ) -> Result<ToolPreparation> {
        let input: WaitAgentInput = match WaitAgentInput::deserialize(arguments) {
            Ok(input) => input,
            Err(error) => return Ok(ToolPreparation::Ready(rejected(error.to_string()))),
        };
        if input.agents.is_empty() && input.sent.is_empty() {
            if input.need.is_some() {
                return Ok(ToolPreparation::Ready(rejected(
                    "need requires named children or sent messages",
                )));
            }
            let mut until = if input.input {
                crate::agent::Until::input(input.timeout_ms)
            } else if let Some(timeout) = input.timeout_ms {
                crate::agent::Until::sleep(timeout)
            } else {
                return Ok(ToolPreparation::Ready(rejected(
                    "wait needs children, sent messages, input or an alarm",
                )));
            };
            until.operation = Some(operation);
            return Ok(if let Some(reason) = released {
                ToolPreparation::Ready(ToolResult {
                    value: json!({"settled": [], "unsettled": [], "reason": reason}),
                })
            } else {
                ToolPreparation::Deferred(until)
            });
        }
        let on_input = input.input;
        let Some(observer) = &self.turn.observer else {
            return Ok(ToolPreparation::Ready(rejected(
                "this host observes no agent settlements",
            )));
        };
        let WaitSelection {
            children,
            sent,
            mut need,
            timeout_ms,
        } = match waited(&**observer, operation, input).await {
            Ok(waited) => waited,
            Err(error @ Error::Invalid(_)) => {
                return Ok(ToolPreparation::Ready(super::rejected_input(error)));
            }
            Err(error) => return Err(error),
        };
        let replies = futures::future::try_join_all(sent.iter().map(|id| async move {
            observer.sent_reply(id).await?.ok_or_else(|| {
                Error::Invalid(format!(
                    "{} names no published send by this agent",
                    id.as_str()
                ))
            })
        }))
        .await;
        let replies = match replies {
            Ok(replies) => replies,
            Err(error @ (Error::Invalid(_) | Error::Unsupported(_))) => {
                return Ok(ToolPreparation::Ready(super::rejected_input(error)));
            }
            Err(error) => return Err(error),
        };
        let mut pending_replies = Vec::new();
        let mut sent_results = Vec::with_capacity(replies.len());
        for reply in replies {
            match &reply.status {
                crate::agent::SentReplyStatus::Pending => {
                    pending_replies.push(reply.watch.ok_or_else(|| {
                        Error::Invalid("pending reply has no event position".into())
                    })?)
                }
                _ => need = need.saturating_sub(1),
            }
            sent_results.push(json!({
                "id": reply.message.id.as_str(),
                "agent": reply.message.to,
                "delivery": reply.message.delivery,
                "reply": reply.status,
            }));
        }
        let open = self.open().await?;
        let observations = futures::future::try_join_all(children.iter().map(|(name, child)| {
            let observer = &**observer;
            let open = &open;
            async move {
                match observer.settled(child).await {
                    Ok(observed) => Ok(Some(observed)),
                    // Spawned this turn: its creation commits with the step.
                    Err(Error::NotFound(_)) => {
                        if open
                            .contains(child)
                            .await
                            .map_err(|error| (name, Error::from(error)))?
                        {
                            Ok(None)
                        } else {
                            Err((name, Error::NotFound(child.to_string())))
                        }
                    }
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
            if open.contains(child).await.map_err(Error::from)? {
                pending.insert(child.clone());
                unsettled.push(json!({
                    "name": name,
                    "phase": phase,
                    "progress": observed.as_ref().and_then(|observed| observed.activity.as_ref()),
                }));
            } else {
                stranded.push(format!("{name} ({phase}, not an open child)"));
            }
        }
        if pending.len().saturating_add(pending_replies.len()) < need as usize {
            return Ok(ToolPreparation::Ready(rejected(format!(
                "wait cannot be satisfied: {}",
                stranded.join(", ")
            ))));
        }
        // A settlement delivery may precede newer accepted child work. Only
        // input and the original deadline can release an unmet condition.
        let interrupted = matches!(
            released.as_ref(),
            Some(crate::agent::WaitReason::Deadline | crate::agent::WaitReason::Input { .. })
        );
        if !interrupted && need != 0 {
            return Ok(ToolPreparation::Deferred(crate::agent::Until {
                operation: Some(operation),
                input: on_input,
                watches: pending_replies.into_iter().collect(),
                ..crate::agent::Until::quorum(pending, need).within(timeout_ms)
            }));
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
                        "progress": observed.activity,
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
            value: json!({"settled": settled, "unsettled": unsettled, "sent": sent_results, "reason": released.unwrap_or(crate::agent::WaitReason::NeedMet)}),
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
#[derive(Debug, PartialEq, Eq)]
struct WaitSelection {
    children: Vec<(String, AgentPath)>,
    sent: Vec<IntentId>,
    need: u32,
    timeout_ms: Option<u64>,
}

async fn waited(
    observer: &dyn TurnChildren,
    operation: crate::OperationId,
    input: WaitAgentInput,
) -> Result<WaitSelection> {
    let children = if input.agents.is_empty() {
        Vec::new()
    } else {
        observer.select(operation, &input.agents).await?
    };
    let mut seen = BTreeSet::new();
    let mut sent = Vec::new();
    for id in input.sent {
        if seen.insert(id.clone()) {
            sent.push(IntentId::try_from(id).map_err(Error::from)?);
        }
    }
    if children.is_empty() && sent.is_empty() {
        return Err(Error::Invalid("wait names no child or sent message".into()));
    }
    let named = u32::try_from(children.len().saturating_add(sent.len())).unwrap_or(u32::MAX);
    let need = input.need.unwrap_or(named);
    if need == 0 || need > named {
        return Err(Error::Invalid(format!(
            "need must be between 1 and the {named} distinct children and sent messages named"
        )));
    }
    Ok(WaitSelection {
        children,
        sent,
        need,
        timeout_ms: input.timeout_ms,
    })
}

#[cfg(any(test, feature = "test-support"))]
mod selection_checks;
#[cfg(feature = "test-support")]
pub use selection_checks::check as check_agent_wait_selection;

/// Explicit capabilities are independent of automatic session membership.
/// Custom hosts can admit a peer without hosting the account's peer directory.
async fn peer_grant_allowed(
    observer: Option<&dyn TurnChildren>,
    grants: &crate::Capabilities,
    parent: Option<&AgentPath>,
    recipient: &AgentPath,
) -> Result<Option<Option<crate::agent::PeerAdmission>>> {
    if crate::agent::peer_send_allowed(grants, recipient) {
        return Ok(Some(None));
    }
    let Some(observer) = observer else {
        return Ok(None);
    };
    match parent {
        None if recipient.depth() == 0 && grants.contains(crate::agent::SEND_PEER) => {
            Ok(observer.peer_admission(grants, recipient).await?.map(Some))
        }
        Some(parent) => Ok(
            crate::agent::sibling_send_allowed(observer, grants, parent, recipient)
                .await?
                .then_some(None),
        ),
        None => Ok(None),
    }
}

fn sent_result(sent: crate::agent::SentMessage) -> ToolResult {
    let value = match sent.delivery {
        crate::agent::SentDelivery::Complete(acyclic_actors::route::Routed::Delivered(
            delivered,
        )) => json!({
            "kind": "accepted", "sent": sent.to, "id": sent.id, "inbox": delivered.inbox,
        }),
        crate::agent::SentDelivery::Complete(acyclic_actors::route::Routed::Rejected(reason)) => {
            json!({
                "kind": "rejected", "sent": sent.to, "id": sent.id, "reason": match reason {
                    acyclic_actors::route::Rejection::Closed => "closed",
                    acyclic_actors::route::Rejection::NoSuchActor => "no_such_actor",
                },
            })
        }
        crate::agent::SentDelivery::Complete(acyclic_actors::route::Routed::Relayed {
            relay_seq,
            ..
        }) => json!({
            "kind": "relayed", "sent": sent.to, "id": sent.id, "relay_seq": relay_seq,
        }),
        crate::agent::SentDelivery::Pending { source, from } => json!({
            "status": "pending_admission", "sent": sent.to, "id": sent.id,
            "source": source, "from": from,
        }),
    };
    ToolResult { value }
}

impl ToolExecutor for AgentTool {
    fn preparation_timeout(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<Option<crate::tool::PreparationTimeout>> {
        if self.name != "wait" {
            return Ok(None);
        }
        let Ok(input) = WaitAgentInput::deserialize(&invocation.arguments) else {
            return Ok(None);
        };
        Ok(input.timeout_ms.map(|timeout| {
            // The actor has already ordered an accepted input before its alarm.
            // Preparation may expire while reopening that selected invocation;
            // retain the input identity even when no observations can be read.
            let reason = match self.release(invocation.operation_id) {
                Some(reason @ crate::agent::WaitReason::Input { .. }) => reason,
                _ => crate::agent::WaitReason::Deadline,
            };
            let unsettled: Vec<_> = input
                .agents
                .iter()
                .map(|name| json!({"name": name, "phase": "unknown", "progress": null}))
                .collect();
            crate::tool::PreparationTimeout {
                timeout_ms: timeout,
                result: ToolResult {
                    value: json!({
                        "settled": [], "unsettled": unsettled, "sent": [],
                        "unobserved_sent": input.sent,
                        "reason": reason
                    }),
                },
                observe_on_expiry: matches!(
                    self.release(invocation.operation_id),
                    Some(
                        crate::agent::WaitReason::Deadline | crate::agent::WaitReason::Input { .. }
                    )
                ),
            }
        }))
    }

    fn deferral(&self, result: &ToolResult) -> Result<Option<crate::tool::ToolDeferral>> {
        if self.name != "send" {
            return Ok(None);
        }
        match result.value.get("status").and_then(Value::as_str) {
            Some("queued") => Ok(Some(crate::tool::ToolDeferral::Effects)),
            Some("pending_admission") => {
                let path = result
                    .value
                    .get("source")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Invalid("pending send has no source".into()))?;
                let from = result
                    .value
                    .get("from")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| Error::Invalid("pending send has no source position".into()))?;
                Ok(Some(crate::tool::ToolDeferral::Observe(
                    crate::agent::Until::record(acyclic_stream::StreamPath::new(path)?, from),
                )))
            }
            _ => Ok(None),
        }
    }

    fn prepare<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _context: Option<&'a ToolContext>,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<Option<crate::tool::ToolPreparationFuture<'a>>> {
        if self.name != "wait" {
            return Ok(None);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if io.is_some_and(|io| io.original_native_work().is_some()) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "agent wait preparation has no native observation producer",
            });
        }
        Ok(Some(crate::conversation::BoundedContentFuture::new(
            io,
            || async move {
                let released = self.release(invocation.operation_id);
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
                "Return an accepted inbox receipt after durably queuing a message for your parent, an open child (by name or path), a peer session (by path), or a sibling your spawn granted (by name or path). Select sender workspace paths in files to attach immutable read-only copies; relative paths in message text alone refer to the recipient's fork. Steering arrives at the next model boundary after in-flight tools settle, and follow-up starts a later turn",
            )?,
        ),
        (
            "archive",
            definition::<ArchiveAgentInput>(
                "archive",
                "Archive an open child (by name or path) and its descendants; wait on it to see it archived. The returned branch retains its unmerged files: read fs show <branch>:PATH without merging after archive",
            )?,
        ),
        (
            "wait",
            definition::<WaitAgentInput>(
                "wait",
                "Wait for children to settle or for replies to original send IDs in sent. Child results include phase and last answer, failed when a turn failed, and branch when addressed by spawn name. Running children include progress from their current input: the zero-based step, original journal publication time and age_ms; null means no activity observation is available. Sent results report only the original delivery and reply settlement, never the peer's transcript or files; exchange reply content with send and re. Every result identifies its reason: need_met, deadline, input (with its inbox sequence), or settlement (with its child). need (default all) counts distinct children and send IDs together, 1 for the first; timeout_ms returns regardless. With neither agents nor sent, use timeout_ms for one actor alarm, input=true for inbox input, or both for whichever arrives first. input=true also interrupts a settlement wait. No timer runs per waiting agent. Inspect child changes with fs diff <branch>, then accept them with fs merge <branch>",
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
/// `effects`, the `observer` its `wait` reads, and its host-owned input `writer`.
/// Selects them for the model over any other revision of the same names. Only the agent tools `names` selects are
/// bound, so a host omits or swaps each one like any standard tool. `spawn`
/// admits a child only through `templates`, under `agent`'s creation snapshot.
pub fn bind_agent_tools(
    registry: &mut ToolRegistry,
    agent: &AgentState,
    (effects, observer): (Arc<dyn TurnEffects>, Option<Arc<dyn TurnChildren>>),
    writer: Arc<dyn ContentPublisher>,
    maximum_bytes: Option<u64>,
    (names, templates): (&[String], Option<Arc<dyn SpawnTemplates>>),
) -> Result<()> {
    let turn = Arc::new(Turn {
        spawned_children: agent.spawned_children.len() as u64,
        spawning: futures::lock::Mutex::new(()),
        agent: agent.path.clone(),
        parent: agent.parent.clone(),
        children: agent.open_children.clone(),
        effects,
        observer,
        writer,
        maximum_bytes,
        templates: agent.profile.clone().zip(templates),
        volumes: agent.volumes.clone(),
        released: agent.wait_resume.clone(),
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
    use std::sync::atomic::Ordering;

    struct Effects(std::sync::atomic::AtomicUsize, bool);

    impl TurnEffects for Effects {
        fn sent<'a>(
            &'a self,
            _: &'a IntentId,
        ) -> BoxProviderFuture<'a, Result<Option<crate::agent::SentMessage>>> {
            Box::pin(async { Ok(None) })
        }

        fn recorded_call<'a>(
            &'a self,
            _: &'a str,
        ) -> BoxProviderFuture<'a, Result<Option<TurnEffect>>> {
            Box::pin(async { Ok(None) })
        }
        fn record<'a>(
            &'a self,
            _: &'a str,
            effect: &'a TurnEffect,
        ) -> BoxProviderFuture<'a, Result<TurnEffect>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                if self.1 {
                    return Err(Error::MergeConflict {
                        paths: vec!["work/output.txt".into()],
                    });
                }
                Ok(effect.clone())
            })
        }
        fn recorded(&self) -> BoxProviderFuture<'_, Result<Vec<TurnEffect>>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn holds(&self, _: crate::OperationId) -> BoxProviderFuture<'_, Result<bool>> {
            Box::pin(async { Ok(false) })
        }
    }

    #[tokio::test]
    async fn archive_preserves_index_faults_and_repairable_effect_refusals() -> Result<()> {
        use crate::agent::AgentId;
        let parent = AgentPath::root(&AgentId::from_bytes([71; 16]));
        let child = parent.child(&AgentId::from_bytes([72; 16]))?;
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let mut children = crate::agent::Children::new();
        children.bind(stream.clone());
        children.insert(&child).await?;
        let checkpoint = serde_json::to_vec(&children).unwrap();

        for (missing_source, merge_conflict) in [(false, false), (true, false), (false, true)] {
            let mut children: crate::agent::Children = serde_json::from_slice(&checkpoint).unwrap();
            children.bind(if missing_source {
                Arc::new(acyclic_stream::MemoryStream::default())
            } else {
                stream.clone()
            });
            let effects = Arc::new(Effects(
                std::sync::atomic::AtomicUsize::new(0),
                merge_conflict,
            ));
            let tool = AgentTool {
                name: "archive",
                turn: Arc::new(Turn {
                    spawned_children: 1,
                    spawning: futures::lock::Mutex::new(()),
                    agent: parent.clone(),
                    parent: None,
                    children,
                    effects: effects.clone(),
                    observer: None,
                    writer: Arc::new(crate::runtime::test_context::PrivateVolume(
                        crate::runtime::test_context::private_volume(71)?,
                    )),
                    maximum_bytes: None,
                    templates: None,
                    volumes: Vec::new(),
                    released: None,
                }),
            };
            let invocation = ToolInvocation {
                operation_id: crate::OperationId::from_bytes([73; 16]),
                call_id: "archive-child".into(),
                name: "archive".into(),
                arguments: json!({"agent":child.as_str()}),
            };
            let result = tool.execute(invocation.clone(), None).await;
            if missing_source {
                let error = result.expect_err("the checkpoint's immutable node is missing");
                assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
                assert!(!error.is_tool_failure());
                assert!(!error.uncertain(), "a missing index is a definitive fault");
                assert!(matches!(error, Error::Agent(_)));
                assert_eq!(effects.0.load(Ordering::SeqCst), 0);
            } else {
                let result = result.expect("repairable effect refusal must be model feedback");
                if merge_conflict {
                    assert_eq!(result.value["kind"], "merge conflict");
                    assert_eq!(result.value["paths"], json!(["work/output.txt"]));
                } else {
                    assert_eq!(result.value["archived"], child.as_str());
                    assert_eq!(result.value["branch"], child.as_str());
                }
                assert_eq!(effects.0.load(Ordering::SeqCst), 1);
            }
            // A genuine bad reference is still ordinary feedback, even when
            // the unrelated persistent child index cannot be read.
            let rejected = tool
                .execute(
                    ToolInvocation {
                        arguments: json!({"agent":"missing-child"}),
                        ..invocation
                    },
                    None,
                )
                .await?;
            assert!(rejected.value["error"].is_string());
        }
        Ok(())
    }

    #[tokio::test]
    async fn explicit_root_peer_grants_do_not_require_a_membership_observer() -> Result<()> {
        let root = AgentPath::root(&crate::agent::AgentId::from_bytes([42; 16]));
        for prefix in ["agent.send:", "agent.send-subtree:"] {
            let grants = crate::Capabilities::new([format!("{prefix}{root}")]);
            assert!(
                peer_grant_allowed(None, &grants, None, &root)
                    .await?
                    .is_some()
            );
        }
        for grants in [
            crate::Capabilities::default(),
            crate::Capabilities::new([crate::agent::SEND_PEER]),
        ] {
            assert!(
                peer_grant_allowed(None, &grants, None, &root)
                    .await?
                    .is_none()
            );
        }
        Ok(())
    }

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
                .map_err(Error::from)
            })
        }

        fn settled<'a>(
            &'a self,
            child: &'a AgentPath,
        ) -> BoxProviderFuture<'a, Result<crate::agent::Settlement>> {
            let settled = self.settled.contains(child);
            let phase = crate::agent::Settlement {
                input: None,
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
                activity: None,
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
        use crate::agent::AgentId;
        let mut parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
        for _ in 1..128 {
            parent = parent
                .child(&AgentId::from_bytes([2; 16]))
                .map_err(Error::from)?;
        }
        let deepest = parent
            .child(&AgentId::from_bytes([3; 16]))
            .map_err(Error::from)?;
        let sibling = TurnEffect::child(&parent, &spawn_intent("sibling", 0).map_err(Error::from)?)
            .map_err(Error::from)?;
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
        let store = crate::agent::actor_store(&children.stream).map_err(Error::from)?;
        let effects = [
            store
                .create(None, &actor, acyclic_actors::local::Creation::default())
                .map_err(|error| Error::Storage(error.to_string()))?,
            store
                .activation(&actor)
                .map_err(|error| Error::Storage(error.to_string()))?,
        ];
        let request = acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: acyclic_stream::IdempotencyKey::new(b"sibling".to_vec())
                .map_err(|error| Error::Storage(error.to_string()))?,
            effects: effects.into(),
        };
        assert!(matches!(
            crate::agent::submit(&*children.stream, request)
                .await
                .map_err(Error::from)?,
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
