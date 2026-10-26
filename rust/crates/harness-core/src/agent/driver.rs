//! The turn driver over the replaceable [`Executor`]: one agent turn is one
//! executor operation, journaled by the host and settled by the agent step.
//!
//! The executor's journal holds every intent before its effect, so a crash before
//! the step commit re-runs the same operation and the journal replays it. The
//! conversation is written only by the step commit (all-or-none with the inbox
//! cursor): one [`ConversationEntry`] naming the turn's input and its staged output.

use super::{
    AgentOutcome, AgentPath, AgentRecord, AgentState, ApprovalRequested, Error, InboxRecord,
    IntentId, Out,
    TurnDriver, TurnFuture, TurnInput, TurnStep, Wait,
};
use crate::{
    OperationId,
    conversation::FileRef,
    executor::{ExecutionJournal, ExecutionProgress, Executor, TurnInput as ExecutorInput},
    model::{FileProjectionPolicy, ModelContent, ModelContentPart},
};
use acyclic_stream::{BoxProviderFuture as BoxFuture, FollowFilter, StreamProvider};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

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
}

/// Host binding of agent turns to an executor and journal.
pub trait TurnHost: acyclic_stream::ProviderPlatform {
    /// Opens `turn` for `agent`, whose model-visible input is `input`.
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        stop: Arc<TurnStop>,
    ) -> BoxFuture<'a, crate::Result<OpenTurn>>;
}

impl<H: TurnHost + ?Sized> TurnHost for Arc<H> {
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        stop: Arc<TurnStop>,
    ) -> BoxFuture<'a, crate::Result<OpenTurn>> {
        (**self).open(agent, turn, input, stop)
    }
}

/// The stop signal of one running turn: a `Cancel` or `Archive` in the agent's
/// inbox at or after the turn's cursor. The turn's journal follows it as the
/// executor's cancellation, so a control interrupts the turn's model calls and
/// tools instead of waiting for the turn to end; the journal keeps every intent.
pub struct TurnStop {
    agent: AgentPath,
    from: u64,
    fired: AtomicBool,
}

impl TurnStop {
    /// The stop of `agent`'s turn that consumed its inbox before `from`.
    #[must_use]
    pub fn new(agent: AgentPath, from: u64) -> Arc<Self> {
        Arc::new(Self {
            agent,
            from,
            fired: AtomicBool::new(false),
        })
    }

    /// Resolves once the inbox holds a `Cancel` or `Archive` at or after `from`.
    pub async fn follow<P: StreamProvider + ?Sized>(&self, provider: &P) -> Result<(), Error> {
        let mut records = match provider
            .follow(self.agent.inbox()?, self.from, FollowFilter::default())
            .await
        {
            Ok(records) => records,
            // A turn driven outside an agent subtree has no inbox to stop it.
            Err(acyclic_stream::StreamError::NotFound) => return std::future::pending().await,
            Err(error) => return Err(Error::Stream(error)),
        };
        while let Some(record) = records.try_next().await.map_err(Error::Stream)? {
            if matches!(
                InboxRecord::decode(&record.value)?,
                InboxRecord::Cancel(_) | InboxRecord::Archive(_)
            ) {
                self.fired.store(true, Ordering::Release);
                return Ok(());
            }
        }
        Err(Error::Record("agent inbox follow ended"))
    }

    /// Whether a control has stopped the turn.
    #[must_use]
    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

/// One conversation record: a settled turn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationEntry {
    /// Input that started the turn.
    pub input: TurnInput,
    /// The executor's complete output, staged on the agent's private volume.
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

fn encode(input: &TurnInput) -> Result<Vec<u8>, Error> {
    crate::contract::canonical_json_bytes(input)
        .map_err(|_| Error::Record("turn input does not encode"))
}

/// The input of the turn a step continues (kept in the checkpoint), or new input.
fn starting(agent: &AgentState, input: Option<TurnInput>) -> Result<TurnInput, Error> {
    match (&agent.turn, input) {
        (Some(bytes), None | Some(TurnInput::Approval { .. })) => serde_json::from_slice(bytes)
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
        other => ModelContent::Text(
            String::from_utf8(encode(other)?)
                .map_err(|_| Error::Record("turn input is not text"))?,
        ),
    })
}

fn host_error(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}

impl<H: TurnHost> TurnDriver for ExecutorDriver<H> {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        cursor: u64,
        input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let input = starting(agent, input)?;
            let turn = turn_id(agent, &input)?;
            let content = model_input(&input)?;
            let stop = TurnStop::new(agent.path.clone(), cursor);
            let open = self
                .host
                .open(agent, turn, &content, Arc::clone(&stop))
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
            if stop.fired() && !matches!(progress, Ok(ExecutionProgress::Ready(_))) {
                // A pending cancel or archive drops this turn when admitted;
                // until then the agent takes no further step of it.
                return Ok(TurnStep {
                    checkpoint: None,
                    outs: Vec::new(),
                    wait: Wait::Input,
                });
            }
            let progress = progress.map_err(host_error)?;
            let failed = |message: &str| TurnStep {
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Done(AgentOutcome::Failed {
                    message: message.to_owned(),
                }),
            };
            Ok(match progress {
                ExecutionProgress::Ready(output) => {
                    let bytes = crate::contract::canonical_json_bytes(&output)
                        .map_err(|error| host_error(error.into()))?;
                    let output = open
                        .journal
                        .stage(turn, "agent-turn-output", &bytes, "application/json", None)
                        .map_err(host_error)?
                        .await
                        .map_err(host_error)?;
                    let entry = ConversationEntry {
                        input,
                        output: output.clone(),
                    };
                    // A child reports when its work is done; a root waits for input.
                    let wait = if agent.parent.is_some() && agent.queued.is_empty() {
                        Wait::Done(AgentOutcome::Succeeded {
                            result: Some(Box::new(output)),
                        })
                    } else {
                        Wait::Input
                    };
                    TurnStep {
                        checkpoint: None,
                        outs: vec![Out::Conversation(entry.encode()?)],
                        wait,
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
                    TurnStep {
                        checkpoint: Some(encode(&input)?),
                        outs,
                        wait: Wait::Approval(id),
                    }
                }
                ExecutionProgress::Rejected(reason) => failed(&format!("approval {reason}")),
                ExecutionProgress::Failed(kind) => failed(kind.message()),
                ExecutionProgress::PreDispatchFailed(failure) => failed(failure.message()),
            })
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
