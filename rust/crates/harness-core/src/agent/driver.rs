//! The turn driver over the replaceable [`Executor`]: one agent turn is one
//! executor operation, journaled by the host and settled by the agent step.
//!
//! The executor's journal holds every intent before its effect, so a crash before
//! the step commit re-runs the same operation and the journal replays it. The
//! conversation is written only by the step commit (all-or-none with the inbox
//! cursor): one [`ConversationEntry`] naming the turn's input and its staged output.

use super::{
    AgentOutcome, AgentRecord, AgentState, ApprovalRequested, Error, InboxRecord, IntentId, Out,
    Progress, Queued, TurnDriver, TurnFuture, TurnInput, TurnStep, Wait,
};
use crate::{
    OperationId,
    conversation::FileRef,
    executor::{ExecutionJournal, ExecutionProgress, Executor, TurnInput as ExecutorInput},
    model::{FileProjectionPolicy, ModelContent, ModelContentPart},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    pub custody: Option<crate::core::AuthorityRoot>,
    /// The agent's prior conversation and this input, as the one selection the
    /// journal admits; `None` for a turn without history.
    pub selected: Option<crate::projection::SelectedModelContext>,
    /// Owner of the turn's approval files; `None` when the host runs no approvals.
    pub approvals: Option<Arc<crate::approvals::AgentApprovals>>,
    /// Effects recorded by the turn's agent tools, lowered when it settles;
    /// `None` when the host binds no agent tools.
    pub effects: Option<Arc<dyn super::TurnEffects>>,
    /// The agent's goal file, checked once when the turn settles; `None` when
    /// the host runs no goals.
    pub goal: Option<Arc<dyn super::TurnGoal>>,
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

/// One conversation record: a settled turn.
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
    /// The executor's complete output, staged on the agent's private volume.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
    pub output: FileRef,
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

/// The input of the turn a step continues (kept in the checkpoint), or new input.
fn starting(agent: &AgentState, input: Option<Queued>) -> Result<Queued, Error> {
    match (&agent.turn, input) {
        (
            Some(Progress {
                checkpoint: bytes, ..
            }),
            None
            | Some(Queued {
                input: TurnInput::Approval { .. },
                ..
            }),
        ) => serde_json::from_slice(bytes)
            .map_err(|_| Error::Record("turn checkpoint is not a turn input")),
        (None, Some(input)) => Ok(input),
        (Some(_), Some(_)) => Err(Error::Record("new input while a turn is in progress")),
        (None, None) => Err(Error::Record("resume without a turn in progress")),
    }
}

/// Model-visible content of a turn input: a message or event file as a file
/// part, every other input as its canonical record text.
pub fn model_input(input: &TurnInput) -> Result<ModelContent, Error> {
    Ok(match input {
        TurnInput::Brief { brief: file }
        | TurnInput::Message { body: file, .. }
        | TurnInput::Hook { event: file, .. } => ModelContent::Part(ModelContentPart::File {
            file: FileRef::clone(file),
            policy: FileProjectionPolicy::BoundedFull,
        }),
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
            let Queued { inbox, input } = starting(agent, input)?;
            let turn = turn_id(agent, &input)?;
            let content = model_input(&input)?;
            let stop = cancellation;
            let open = self
                .host
                .open(agent, turn, &content, stop.clone())
                .await
                .map_err(host_error)?;
            let progress = open
                .executor
                .execute_progress(
                    ExecutorInput {
                        operation_id: turn,
                        input: content,
                        selected_context: open.selected.clone(),
                        max_steps: open.max_steps,
                    },
                    &*open.journal,
                )
                .await;
            if stop.is_cancelled() && !matches!(progress, Ok(ExecutionProgress::Ready(_))) {
                // A pending cancel or archive drops this turn when admitted;
                // until then the agent takes no further step of it.
                return Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: None,
                    outs: Vec::new(),
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
                    let bytes = crate::contract::canonical_json_bytes(&output)
                        .map_err(|error| host_error(error.into()))?;
                    let output = open
                        .journal
                        .stage(turn, "agent-turn-output", &bytes, "application/json", None)
                        .map_err(host_error)?
                        .await
                        .map_err(host_error)?;
                    // An active goal continues the agent through its own inbox.
                    let goal = match &open.goal {
                        Some(goal) => super::goal::settle(agent, &input, turn, &**goal).await?,
                        None => None,
                    };
                    let entry = ConversationEntry {
                        inbox,
                        input,
                        output: output.clone(),
                    };
                    // A child reports when its work is done and no goal continues it;
                    // a root waits for input.
                    let wait =
                        if goal.is_none() && agent.parent.is_some() && agent.queued.is_empty() {
                            Wait::Done(AgentOutcome::Succeeded {
                                result: Some(Box::new(output)),
                            })
                        } else {
                            Wait::Input
                        };
                    // The tools' recorded effects commit with the turn's settlement.
                    let mut outs = Vec::new();
                    outs.push(Out::Conversation(entry.encode()?));
                    outs.extend(goal);
                    TurnStep {
                        reserved_children: 0,
                        checkpoint: None,
                        outs,
                        wait,
                        drops: None,
                    }
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
                        checkpoint: Some(encode(&Queued { inbox, input })?),
                        reserved_children,
                        outs,
                        wait: Wait::Approval(id),
                        drops: None,
                    });
                }
                ExecutionProgress::Rejected(reason) => failed(&format!("approval {reason}")),
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
