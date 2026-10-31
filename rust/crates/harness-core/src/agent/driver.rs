//! The turn driver over the replaceable [`Executor`]: one agent turn is one
//! executor operation, journaled by the host and settled by the agent step.
//!
//! The executor's journal holds every intent before its effect, so a crash before
//! the step commit re-runs the same operation and the journal replays it. The
//! conversation is written only by the step commit (all-or-none with the inbox
//! cursor): one [`ConversationEntry`] naming the turn's input and its staged output.

use super::{
    AgentOutcome, AgentRecord, AgentState, ApprovalRequested, ContextAction, Error, InboxRecord,
    IntentId, Out, Progress, Queued, TurnDriver, TurnFuture, TurnInput, TurnStep, Wait,
};
use crate::{
    OperationId,
    conversation::FileRef,
    executor::{
        ExecutionJournal, ExecutionProgress, Executor, TurnInput as ExecutorInput, TurnOutput,
    },
    model::{FileProjectionPolicy, ModelContent, ModelContentPart},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use std::{ops::ControlFlow, sync::Arc};

/// One opened turn: the executor and journal bound to the agent.
pub struct OpenTurn {
    /// Executor bound to the agent's tools, volumes and policy.
    pub executor: Arc<dyn Executor>,
    /// Journal of this agent's turns, on its private volume.
    pub journal: Arc<dyn ExecutionJournal>,
    /// Model and tool steps allowed in one turn.
    pub max_steps: u32,
    /// Signing custody of the journal's authority; the journal holds only a
    /// weak admission port, so the custody lives exactly as long as the turn.
    pub custody: Option<crate::core::RuntimeAuthority>,
    /// The agent's prior conversation and this input, as the one selection the
    /// journal admits; `None` for a turn without history.
    pub selected: Option<crate::projection::SelectedModelContext>,
    /// The staged output of the latest checkpoint in the agent's context,
    /// whose summary `selected` begins with; `None` without one.
    pub checkpoint: Option<FileRef>,
    /// Owner of the turn's approval files; `None` when the host runs no approvals.
    pub approvals: Option<Arc<crate::approvals::AgentApprovals>>,
    /// Effects recorded by the turn's agent tools, lowered when it settles;
    /// `None` when the host binds no agent tools.
    pub effects: Option<Arc<dyn super::TurnEffects>>,
    /// The agent's goal file, checked once when the turn settles; `None` when
    /// the host runs no goals.
    pub goal: Option<Arc<dyn super::TurnGoal>>,
    /// Where the turn's settled output is staged.
    pub outputs: Arc<dyn TurnOutputs>,
    /// The input the executor runs, with its attachment manifests expanded
    /// ([`expand_attachments`]); `None` runs the input as given.
    pub input: Option<ModelContent>,
}

fn is_manifest(part: &ModelContentPart) -> bool {
    matches!(
        part,
        ModelContentPart::File {
            file,
            policy: FileProjectionPolicy::Reference,
        } if file.descriptor().media_type() == crate::conversation::ATTACHMENT_MANIFEST_MEDIA_TYPE
    )
}

/// `content` with each attachment manifest [`model_input`] names replaced by
/// the files it lists, in order, each as it projects
/// ([`FileProjectionPolicy::for_attachment`]), read through `reader`, whose
/// grant holds the manifest and its directory.
pub async fn expand_attachments(
    content: ModelContent,
    reader: &dyn crate::conversation::ContentResidencyVerifier,
) -> crate::Result<ModelContent> {
    let ModelContent::Parts(parts) = content else {
        return Ok(content);
    };
    if !parts.iter().any(is_manifest) {
        return Ok(ModelContent::Parts(parts));
    }
    let mut expanded = Vec::with_capacity(parts.len());
    for part in parts {
        if !is_manifest(&part) {
            expanded.push(part);
            continue;
        }
        let ModelContentPart::File { file, .. } = part else {
            continue;
        };
        let bytes = reader
            .read(&file, file.descriptor().byte_length(), None, None)?
            .await?;
        let items =
            crate::conversation::decode_attachment_manifest(&file, bytes.as_ref(), None, None)?;
        expanded.reserve(items.len());
        expanded.extend(items.into_iter().map(|item| ModelContentPart::File {
            policy: FileProjectionPolicy::for_attachment(&item.file),
            file: item.file,
        }));
    }
    Ok(ModelContent::Parts(expanded))
}

/// Settled turn outputs: conversation content, which every fork of the agent
/// inherits with its conversation and reads by exact version.
pub trait TurnOutputs: acyclic_stream::ProviderPlatform {
    /// Stages `turn`'s output `bytes` once and names the staged file.
    fn stage<'a>(
        &'a self,
        turn: OperationId,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, crate::Result<FileRef>>;

    /// The answer `turn` streamed before it was cancelled, which its
    /// cancelled entry keeps; empty where the host streams none.
    fn partial(&self, turn: OperationId) -> BoxFuture<'_, crate::Result<String>> {
        let _ = turn;
        Box::pin(async { Ok(String::new()) })
    }
}

/// Host binding of agent turns to an executor and journal.
pub trait TurnHost: acyclic_stream::ProviderPlatform {
    /// Opens `turn` for `agent`, whose model-visible input is `input`.
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, crate::Result<OpenTurn>>;
}

impl<H: TurnHost + ?Sized> TurnHost for Arc<H> {
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, crate::Result<OpenTurn>> {
        (**self).open(agent, turn, input, stop)
    }
}

/// The model request a compaction checkpoint sends over the current context;
/// its answer replaces that context in every later turn.
pub const COMPACT_INSTRUCTION: &str = "Summarize this conversation for continued work. Preserve facts, decisions, constraints, unresolved work and references; the summary replaces the conversation in later context. Answer with the summary alone.";

/// How a turn settled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum TurnOutcome {
    /// The executor finished the turn; its output is complete.
    #[default]
    Completed,
    /// A cancel or archive stopped the turn; its output holds the answer
    /// streamed so far. A cancelled checkpoint selects nothing.
    Cancelled,
}

impl TurnOutcome {
    /// Whether the turn finished.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

/// One conversation record: a settled turn, or a checkpoint (its input is
/// [`TurnInput::Context`] and its output holds the summary later turns start
/// from).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct ConversationEntry {
    /// Inbox sequence of the record that started the turn (see [`Queued`]).
    /// An agent appends its own entries in strictly increasing inbox order.
    pub inbox: u64,
    /// Input that started the turn.
    pub input: TurnInput,
    /// The executor's output, staged on the agent's private volume:
    /// complete, or as far as a cancelled turn streamed it.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
    pub output: FileRef,
    /// How the turn settled; absent when it completed.
    #[serde(default, skip_serializing_if = "TurnOutcome::is_completed")]
    pub outcome: TurnOutcome,
    /// The original goal run whose continuation this entry commits or settles.
    #[cfg_attr(feature = "wasm", tsify(type = "string", optional))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<OperationId>,
}

impl ConversationEntry {
    /// Whether this entry belongs to the authenticated self-continuation of
    /// this exact agent and original goal run.
    #[must_use]
    pub fn continues_goal(&self, agent: &super::AgentPath, run: OperationId) -> bool {
        self.goal == Some(run) && super::goal::continuing(agent, &self.input) == Some(run)
    }

    /// The selection this entry makes for later turns: a completed checkpoint's.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&ContextAction> {
        match self.outcome {
            TurnOutcome::Completed => self.input.context(),
            TurnOutcome::Cancelled => None,
        }
    }
}

impl AgentRecord for ConversationEntry {}

/// [`TurnDriver`] running each turn as one executor operation.
pub struct ExecutorDriver<H> {
    host: H,
}

impl<H: TurnHost> ExecutorDriver<H> {
    /// Driver over `host`.
    pub const fn new(host: H) -> Self {
        Self { host }
    }
}

/// Operation identity of the turn `input` starts. Every input is unique within an
/// agent's inbox (its sender and intent form the Streams idempotency key, and a
/// child reports once), so this is stable across replays and distinct per turn,
/// including turns started by an alarm at an unchanged inbox cursor.
pub fn turn_id(agent: &AgentState, input: &TurnInput) -> Result<OperationId, Error> {
    let encoded = encode(input)?;
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.turn.v1");
    hasher.update(agent.path.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&encoded);
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    Ok(OperationId::from_bytes(bytes))
}

fn encode(input: &impl Serialize) -> Result<Vec<u8>, Error> {
    crate::contract::canonical_json_bytes(input)
        .map_err(|_| Error::Record("turn input does not encode"))
}

/// The driver's checkpoint of a turn in progress: the input that started it
/// and how many of its effect segments already committed. A turn commits its
/// recorded effects whenever a later call waits on them (see
/// [`super::TurnEffects::holds`]) and continues in the next segment.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    inbox: u64,
    input: TurnInput,
    #[serde(default)]
    segment: u32,
}

/// The input of the turn a step continues (kept in the checkpoint) and its
/// effect segment, or new input in its first segment.
fn starting(agent: &AgentState, input: Option<Queued>) -> Result<(Queued, u32), Error> {
    match (&agent.turn, input) {
        (
            Some(_),
            None
            | Some(Queued {
                input: TurnInput::Approval { .. },
                ..
            }),
        ) => {
            let Checkpoint {
                inbox,
                input,
                segment,
            } = checkpoint(agent)?.ok_or(Error::Record("resume without a turn in progress"))?;
            Ok((Queued { inbox, input }, segment))
        }
        (None, Some(input)) => Ok((input, 0)),
        (Some(_), Some(_)) => Err(Error::Record("new input while a turn is in progress")),
        (None, None) => Err(Error::Record("resume without a turn in progress")),
    }
}

fn checkpoint(agent: &AgentState) -> Result<Option<Checkpoint>, Error> {
    agent
        .turn
        .as_ref()
        .map(|Progress { checkpoint, .. }| {
            serde_json::from_slice(checkpoint)
                .map_err(|_| Error::Record("turn checkpoint is not a turn input"))
        })
        .transpose()
}

/// The effect segment `agent`'s next turn step records into: its turn in
/// progress keeps its segment, a new turn starts at zero.
///
/// # Errors
/// When the turn's checkpoint is malformed.
pub fn turn_segment(agent: &AgentState) -> Result<u32, Error> {
    Ok(checkpoint(agent)?.map_or(0, |checkpoint| checkpoint.segment))
}

/// Model-visible content of a turn input: a message or event file as a file
/// part, followed by a message's attachment manifest by its exact version
/// (which [`expand_attachments`] replaces with the files it lists), every
/// other input as its canonical record text.
pub fn model_input(input: &TurnInput) -> Result<ModelContent, Error> {
    let body = |file: &FileRef| ModelContentPart::File {
        file: file.clone(),
        policy: FileProjectionPolicy::BoundedFull,
    };
    Ok(match input {
        TurnInput::Message {
            body: file,
            attachments: Some(manifest),
            ..
        } => ModelContent::Parts(vec![
            body(file),
            ModelContentPart::File {
                file: FileRef::clone(manifest),
                policy: FileProjectionPolicy::Reference,
            },
        ]),
        TurnInput::Brief { brief: file }
        | TurnInput::Message { body: file, .. }
        | TurnInput::Hook { event: file, .. } => ModelContent::Part(body(file)),
        TurnInput::Context {
            action: ContextAction::Compact { instructions },
            ..
        } => match instructions {
            Some(file) => ModelContent::Parts(vec![
                ModelContentPart::Text {
                    text: format!("{COMPACT_INSTRUCTION} Follow this guidance:"),
                },
                body(file),
            ]),
            None => ModelContent::Text(COMPACT_INSTRUCTION.to_owned()),
        },
        other => {
            let text = String::from_utf8(encode(other)?)
                .map_err(|_| Error::Record("turn input is not text"))?;
            // A child's result file follows its report, so the parent's turn
            // is granted and reads exactly that file.
            match other {
                TurnInput::ChildDone {
                    done:
                        super::ChildDone {
                            outcome: AgentOutcome::Succeeded { result: Some(file) },
                            ..
                        },
                } => ModelContent::Parts(vec![
                    ModelContentPart::Text { text },
                    ModelContentPart::File {
                        file: FileRef::clone(file),
                        policy: FileProjectionPolicy::BoundedFull,
                    },
                ]),
                _ => ModelContent::Text(text),
            }
        }
    })
}

fn host_error(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}

/// The children `effects` spawn that `agent` does not already have open.
fn reserved(agent: &AgentState, effects: Vec<super::TurnEffect>) -> usize {
    effects
        .into_iter()
        .filter_map(|effect| match effect {
            super::TurnEffect::Spawn { intent, .. } => {
                super::TurnEffect::child(&agent.path, &intent).ok()
            }
            _ => None,
        })
        .filter(|child| !agent.open_children.contains(child))
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// Stages `text` as `turn`'s whole output.
async fn stage_text(open: &OpenTurn, turn: OperationId, text: String) -> Result<FileRef, Error> {
    let output = TurnOutput {
        text,
        attachments: Vec::new(),
        metadata: serde_json::Value::Null,
        steps: 0,
    };
    let bytes =
        crate::contract::canonical_json_bytes(&output).map_err(|error| host_error(error.into()))?;
    open.outputs.stage(turn, &bytes).await.map_err(host_error)
}

/// Settles a rewind or clear, which runs no model request: its checkpoint
/// carries the latest checkpoint's summary forward (a rewind) or an empty
/// one (a clear, or a rewind with no checkpoint to restore).
async fn settle_selection(
    open: &OpenTurn,
    turn: OperationId,
    inbox: u64,
    input: TurnInput,
) -> Result<TurnStep, Error> {
    let restored = match input.context() {
        Some(ContextAction::Rewind) => open.checkpoint.clone(),
        _ => None,
    };
    let output = match restored {
        Some(output) => output,
        None => stage_text(open, turn, String::new()).await?,
    };
    let entry = ConversationEntry {
        inbox,
        input,
        output,
        outcome: TurnOutcome::Completed,
        goal: None,
    };
    Ok(TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: settled(&entry)?,
        wait: Wait::Input,
        drops: None,
    })
}

/// A settled entry's appends: the entry, indexed as the agent's latest
/// checkpoint when it is one.
fn settled(entry: &ConversationEntry) -> Result<Vec<Out>, Error> {
    let mut outs = vec![Out::Conversation(entry.encode()?)];
    if entry.checkpoint().is_some() {
        outs.push(Out::Checkpoint);
    }
    if let Some(run) = entry.goal {
        outs.push(Out::GoalCompleted { run });
    }
    Ok(outs)
}

/// Every settled turn commits its already-recorded actions, including a turn
/// whose later model call fails. Pending or uncertain turns retain their journal.
async fn settle_effects(
    agent: &AgentState,
    open: &OpenTurn,
    mut step: TurnStep,
) -> Result<TurnStep, Error> {
    if let Some(effects) = &open.effects {
        let mut outs = super::effect_outs(agent, effects.recorded().await.map_err(host_error)?);
        outs.append(&mut step.outs);
        step.outs = outs;
    }
    Ok(step)
}

impl<H: TurnHost> TurnDriver for ExecutorDriver<H> {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        input: Option<Queued>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let (Queued { inbox, input }, segment) = starting(agent, input)?;
            let turn = turn_id(agent, &input)?;
            let content = model_input(&input)?;
            let stop = cancellation;
            let open = self
                .host
                .open(agent, turn, &content, stop.clone())
                .await
                .map_err(host_error)?;
            if matches!(
                input.context(),
                Some(ContextAction::Rewind | ContextAction::Clear)
            ) {
                return settle_selection(&open, turn, inbox, input).await;
            }
            let progress = open
                .executor
                .execute_progress(
                    ExecutorInput {
                        operation_id: turn,
                        input: open.input.clone().unwrap_or(content),
                        selected_context: open.selected.clone(),
                        max_steps: open.max_steps,
                    },
                    &*open.journal,
                )
                .await;
            if stop.is_cancelled() && !matches!(progress, Ok(ExecutionProgress::Ready(_))) {
                // A cancel or archive stops this turn: it settles durably as
                // a cancelled entry holding its prompt and the answer it
                // streamed so far, with no further model request.
                let partial = open.outputs.partial(turn).await.map_err(host_error)?;
                let goal = super::goal::continuing(&agent.path, &input);
                let entry = ConversationEntry {
                    inbox,
                    input,
                    output: stage_text(&open, turn, partial).await?,
                    outcome: TurnOutcome::Cancelled,
                    goal,
                };
                return Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: None,
                    outs: settled(&entry)?,
                    wait: Wait::Input,
                    drops: None,
                });
            }
            let failed = |message: &str| TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Done(AgentOutcome::Failed {
                    message: message.to_owned(),
                }),
                drops: None,
            };
            let progress = match progress {
                Ok(progress) => progress,
                // A provider owns bounded retry/recovery and returns these typed
                // outcomes only when it is finished. Settle through the normal
                // agent commit so status followers and parents observe failure.
                Err(error) if error.is_model_failure() => {
                    return settle_effects(agent, &open, failed(&error.to_string())).await;
                }
                Err(error) => return Err(host_error(error)),
            };
            let step = match progress {
                ExecutionProgress::Ready(output) => {
                    // An active goal continues the agent through its own inbox;
                    // a checkpoint is no step of it.
                    let checkpoint = input.context().is_some();
                    let goal = match &open.goal {
                        Some(goal) if !checkpoint => {
                            match super::goal::settle(agent, &input, turn, &**goal).await? {
                                ControlFlow::Break(goal) => goal,
                                ControlFlow::Continue(()) => {
                                    // Keep the completed model segment and yield the
                                    // conflicting rewrite through its original actor.
                                    return Ok(TurnStep {
                                        checkpoint: Some(encode(&Checkpoint {
                                            inbox,
                                            input,
                                            segment,
                                        })?),
                                        reserved_children: 0,
                                        outs: Vec::new(),
                                        wait: Wait::Continue,
                                        drops: None,
                                    });
                                }
                            }
                        }
                        _ => None,
                    };
                    let bytes = crate::contract::canonical_json_bytes(&output)
                        .map_err(|error| host_error(error.into()))?;
                    let output = crate::obs::waiting(
                        "output-commit",
                        turn,
                        open.outputs.stage(turn, &bytes),
                    )
                    .await
                    .map_err(host_error)?;
                    let run = super::goal::continuing(&agent.path, &input)
                        .or_else(|| goal.as_ref().map(|_| turn));
                    let entry = ConversationEntry {
                        inbox,
                        input,
                        output,
                        outcome: TurnOutcome::Completed,
                        goal: run,
                    };
                    // The tools' recorded effects commit with the turn's settlement.
                    let mut outs = if goal.is_some() {
                        vec![Out::Conversation(entry.encode()?)]
                    } else {
                        settled(&entry)?
                    };
                    outs.extend(goal);
                    // Every agent waits for input once its turn settles: a child
                    // stays open to its parent's messages until archived, and a
                    // parent observes its answer through `wait`.
                    TurnStep {
                        reserved_children: 0,
                        checkpoint: None,
                        outs,
                        wait: Wait::Input,
                        drops: None,
                    }
                }
                ExecutionProgress::Pending(_)
                    if open
                        .approvals
                        .as_ref()
                        .is_some_and(|approvals| approvals.yielded()) =>
                {
                    // The next call waits on this turn's recorded effects: they
                    // commit now, and the turn continues at once in its next
                    // segment, where the call reaches what they created.
                    let outs = match &open.effects {
                        Some(effects) => {
                            super::effect_outs(agent, effects.recorded().await.map_err(host_error)?)
                        }
                        None => Vec::new(),
                    };
                    let segment = segment
                        .checked_add(1)
                        .ok_or(Error::Record("turn effect segments exhausted"))?;
                    return Ok(TurnStep {
                        checkpoint: Some(encode(&Checkpoint {
                            inbox,
                            input,
                            segment,
                        })?),
                        reserved_children: 0,
                        outs,
                        wait: Wait::Continue,
                        drops: None,
                    });
                }
                ExecutionProgress::Pending(_)
                    if let Some((children, any)) = open
                        .approvals
                        .as_ref()
                        .and_then(|approvals| approvals.awaited()) =>
                {
                    // A `wait` call dispatches once its children settle: the
                    // turn commits that wait, holding no delivery, and their
                    // settlement resumes it in this same segment.
                    let reserved_children = match &open.effects {
                        Some(effects) => {
                            reserved(agent, effects.recorded().await.map_err(host_error)?)
                        }
                        None => 0,
                    };
                    return Ok(TurnStep {
                        checkpoint: Some(encode(&Checkpoint {
                            inbox,
                            input,
                            segment,
                        })?),
                        reserved_children,
                        outs: Vec::new(),
                        wait: Wait::Children { children, any },
                        drops: None,
                    });
                }
                ExecutionProgress::Pending(approval) => {
                    // The approval owner names the request the turn waits on;
                    // a host without one waits on the pending operation.
                    let (id, parent) = match open.approvals.as_ref().and_then(|a| a.pending()) {
                        Some((request, parent)) => (request, parent),
                        None => (approval_intent(approval), false),
                    };
                    let id = IntentId::try_from(id)?;
                    // A parent stage asks the parent in the same commit as the pause.
                    let outs = match &agent.parent {
                        Some(to) if parent => vec![Out::Send {
                            to: to.clone(),
                            record: InboxRecord::ApprovalRequested(ApprovalRequested {
                                child: agent.path.clone(),
                                id: id.clone(),
                            }),
                        }],
                        _ => Vec::new(),
                    };
                    // The paused turn's recorded spawns keep their slots.
                    let reserved_children = match &open.effects {
                        Some(effects) => {
                            reserved(agent, effects.recorded().await.map_err(host_error)?)
                        }
                        None => 0,
                    };
                    return Ok(TurnStep {
                        checkpoint: Some(encode(&Checkpoint {
                            inbox,
                            input,
                            segment,
                        })?),
                        reserved_children,
                        outs,
                        wait: Wait::Approval(id),
                        drops: None,
                    });
                }
                ExecutionProgress::Failed(kind) => failed(kind.message()),
                ExecutionProgress::PreDispatchFailed(failure) => failed(failure.message()),
            };
            settle_effects(agent, &open, step).await
        })
    }
}

/// Approval intent naming the executor's pending approval operation.
#[must_use]
pub fn approval_intent(operation: OperationId) -> String {
    let mut text = String::with_capacity(37);
    text.push_str("tool:");
    for byte in operation.into_bytes() {
        for nibble in [byte >> 4, byte & 0x0f] {
            text.extend(char::from_digit(u32::from(nibble), 16));
        }
    }
    text
}
