//! One agent step: the pure transition from (state, input) to (state, effects, alarm).
//!
//! The step is deterministic given the turn driver's result, so a crash before the
//! actor checkpoint commits simply recomputes it. Its effects are lowered into one
//! composed commit by [`super::lower::lower`]; nothing is acknowledged before that
//! commit.

use super::checkpoint::{self, MAX_CHECKPOINT_BYTES, TAIL_SLACK};
use super::{
    AgentOutcome, AgentPath, ChildDone, Error, ForkAdopted, InboxRecord, IntentId, Phase, Sender,
    StatusRecord, VolumeBinding,
    kernel::{self, Admit, Control, Mode, Pick},
};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

/// Most children an agent may have open at once.
pub const MAX_OPEN_CHILDREN: usize = 256;
/// Most inputs held in the checkpoint. Later input stays in the inbox, which is
/// the agent's unbounded durable queue, and is read back as the window drains.
pub const MAX_QUEUED: usize = 32;
/// Most encoded bytes of the inputs held in the checkpoint behind the first:
/// the window always holds its front input, and holds more only within this.
pub const MAX_QUEUED_BYTES: usize = 8 * 1024;
/// Largest encoded outcome a turn may settle with. It travels in the parent's
/// `ChildDone` next to the child's path, so it is well within that record.
pub const MAX_OUTCOME_BYTES: usize = 8 * 1024;
/// The failure an agent settles with when its own state cannot be checkpointed.
pub const OVERSIZED_STATE: &str = "agent state exceeds MAX_CHECKPOINT_BYTES";
/// The failure a turn settles with when its outcome exceeds [`MAX_OUTCOME_BYTES`].
pub const OVERSIZED_OUTCOME: &str = "agent outcome exceeds MAX_OUTCOME_BYTES";

/// Why a step did not create a requested child.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnRefusal {
    /// The agent is archived.
    Archived,
    /// The child would exceed [`super::MAX_DEPTH`].
    Depth,
    /// The agent already has [`MAX_OPEN_CHILDREN`] open children.
    Limit,
}

/// What the step that applied a spawn did with it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SpawnOutcome {
    /// The child was created, or was already open, in that step's commit.
    Spawned {
        /// The child.
        child: AgentPath,
    },
    /// No child was created.
    Refused {
        /// Why.
        reason: SpawnRefusal,
    },
}

/// Input queued for the turn, in inbox order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnInput {
    /// The adoption brief.
    Brief {
        /// Brief file.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        brief: Box<FileRef>,
    },
    /// A message.
    Message {
        /// Sender.
        from: Sender,
        /// Message identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Body.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        body: Box<FileRef>,
        /// Inbox sequence of the message record, as returned
        /// by `Delivered::Committed`; it correlates a settled turn with
        /// its prompt, including a replayed send.
        inbox: u64,
    },
    /// A child finished.
    ChildDone {
        /// Child and outcome.
        done: ChildDone,
    },
    /// A child delegated an approval request.
    ApprovalRequested {
        /// The child.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        child: AgentPath,
        /// Request identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// A response file exists for this agent's approval request.
    Approval {
        /// Request identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// A host hook event.
    Hook {
        /// Host.
        host: String,
        /// Hook identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Event file.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        event: Box<FileRef>,
    },
}

/// Durable agent state, carried in the actor checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentState {
    /// This agent.
    pub path: AgentPath,
    /// Spawning agent.
    pub parent: Option<AgentPath>,
    /// Owned volumes.
    pub volumes: Vec<VolumeBinding>,
    /// Approval policy file.
    pub policy: Option<FileRef>,
    /// Exact conversation tail; the agent is its only writer.
    pub conversation_tail: u64,
    /// Exact status tail; the agent is its only writer.
    pub status_tail: u64,
    /// Last published phase.
    pub phase: Phase,
    /// Children that have not reported `ChildDone`.
    pub open_children: BTreeSet<AgentPath>,
    /// Open children that still have to receive `Archive`.
    pub archive_pending: BTreeSet<AgentPath>,
    /// Inputs waiting for the turn, in inbox order.
    pub queued: VecDeque<TurnInput>,
    /// Inbox sequence of the first input not held in `queued`. It and every
    /// later input are read back from the inbox, in order, as `queued` drains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overflow: Option<u64>,
    /// Sequences after `overflow` already taken out of order: the awaited
    /// approval responses. Read-back skips them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub taken: BTreeSet<u64>,
    /// Archived: no further turns, no further input accepted.
    pub closed: bool,
    /// `ChildDone` has been sent to the parent.
    pub reported: bool,
    /// Turn driver checkpoint while a turn is in progress.
    pub turn: Option<Vec<u8>>,
}

impl AgentState {
    /// State at adoption, before the first step.
    #[must_use]
    pub fn adopted(path: AgentPath, adopted: &ForkAdopted) -> Self {
        Self {
            path,
            parent: adopted.parent.clone(),
            volumes: adopted.volumes.clone(),
            policy: adopted.policy.clone(),
            conversation_tail: adopted.conversation_tail,
            // The creating commit wrote the first status (`Idle`).
            status_tail: 1,
            phase: Phase::Idle,
            open_children: BTreeSet::new(),
            archive_pending: BTreeSet::new(),
            queued: VecDeque::new(),
            overflow: None,
            taken: BTreeSet::new(),
            closed: false,
            reported: false,
            turn: None,
        }
    }

    /// The outcome of the spawn named `intent`, read from the state committed
    /// by the step that applied it. Its child is derived from the intent, and
    /// that step either opened it, found it open, or refused it for exactly
    /// one reason; no later input of the same step closes a child. So the
    /// outcome is a function of that state, and no state records outcomes.
    #[must_use]
    pub fn spawn_outcome(&self, intent: &IntentId) -> SpawnOutcome {
        let Ok(child) = self.spawned(intent) else {
            return SpawnOutcome::Refused {
                reason: SpawnRefusal::Depth,
            };
        };
        if self.open_children.contains(&child) {
            SpawnOutcome::Spawned { child }
        } else if self.closed {
            SpawnOutcome::Refused {
                reason: SpawnRefusal::Archived,
            }
        } else {
            SpawnOutcome::Refused {
                reason: SpawnRefusal::Limit,
            }
        }
    }

    /// The child a spawn named `intent` creates.
    fn spawned(&self, intent: &IntentId) -> Result<AgentPath, Error> {
        self.path
            .child(&super::AgentId::derive(&self.path.id(), intent.as_str()))
    }

    /// Whether the input window holds `input` next: always as its front, then
    /// within [`MAX_QUEUED`] inputs and [`MAX_QUEUED_BYTES`] behind the front.
    fn holds(&self, input: &TurnInput) -> bool {
        if self.queued.is_empty() {
            return true;
        }
        let bytes = |input: &TurnInput| serde_json::to_vec(input).map_or(usize::MAX, |b| b.len());
        let held: usize = self.queued.iter().skip(1).map(bytes).sum();
        self.queued.len() < MAX_QUEUED && held.saturating_add(bytes(input)) <= MAX_QUEUED_BYTES
    }

    /// Drops every pending input, held or still in the inbox.
    fn drop_queue(&mut self) {
        self.queued.clear();
        self.overflow = None;
        self.taken.clear();
    }

    /// The overflowed inbox records to read back before inbox sequence `end`:
    /// the first sequence and a record bound that refills the window past
    /// every taken sequence.
    #[must_use]
    pub fn overflowed(&self, end: u64) -> Option<(u64, u32)> {
        let from = self.overflow.filter(|from| *from < end)?;
        let room = MAX_QUEUED.saturating_sub(self.queued.len()) + self.taken.len();
        let limit = u32::try_from(room).ok().filter(|room| *room > 0)?;
        Some((from, limit))
    }

    /// Refills the window from overflowed inbox `records`, read in sequence
    /// order from [`Self::overflowed`] and all before `end`.
    pub fn refill(&mut self, records: Vec<(u64, InboxRecord)>, end: u64) {
        let mut next = self.overflow.unwrap_or(end);
        for (sequence, record) in records {
            next = sequence + 1;
            if self.taken.remove(&sequence) {
                continue;
            }
            let Some(input) = input(sequence, record) else {
                continue;
            };
            if !self.holds(&input) {
                self.overflow = Some(sequence);
                return;
            }
            self.queued.push_back(input);
        }
        if next >= end {
            self.overflow = None;
            self.taken.clear();
        } else {
            self.overflow = Some(next);
        }
    }
}

/// An effect of one step, lowered into the step's single commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    /// Append to this agent's conversation.
    Conversation(Vec<u8>),
    /// Send an inbox record to another agent.
    Send {
        /// Recipient.
        to: AgentPath,
        /// Record (never `ForkAdopted` or `Archive`).
        record: InboxRecord,
    },
    /// Archive another agent (a child).
    Archive {
        /// Target.
        target: AgentPath,
        /// Archive intent.
        id: IntentId,
    },
    /// Spawn a child.
    Spawn {
        /// Spawning intent; names the child.
        intent: IntentId,
        /// Task brief bytes; [`super::SpawnVolumes`] creates the child's private
        /// volume holding them, and the child names that file at adoption.
        brief: Option<Vec<u8>>,
        /// Volumes bound to the child (filled by the filesystem contribution).
        volumes: Vec<VolumeBinding>,
    },
}

/// What the turn waits for after a turn step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wait {
    /// Run the next turn step at once.
    Continue,
    /// Wait for the response to this approval request.
    Approval(IntentId),
    /// Wait until this Unix millisecond deadline.
    Until(u64),
    /// The turn finished; wait for more input.
    Input,
    /// The agent's task is complete.
    Done(AgentOutcome),
}

/// The result of one turn step.
pub struct TurnStep {
    /// Driver checkpoint for the next step; ignored when the turn finished.
    pub checkpoint: Option<Vec<u8>>,
    /// Effects, in order.
    pub outs: Vec<Out>,
    /// What to wait for next.
    pub wait: Wait,
}

/// Future returned by a turn driver: `Send` on native hosts, event-loop
/// owned in the browser, like every provider and executor future.
pub type TurnFuture<'a> = acyclic_stream::BoxProviderFuture<'a, Result<TurnStep, Error>>;

/// Executes turn steps (model calls, tool calls) for an agent.
///
/// A step may repeat after a crash; anything durable it does outside the returned
/// effects must be keyed by `(agent, cursor)` so the repeat replays exactly.
pub trait TurnDriver: acyclic_stream::ProviderPlatform {
    /// Runs one step. `input` is `None` when continuing or after an alarm.
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        cursor: u64,
        input: Option<TurnInput>,
    ) -> TurnFuture<'a>;
}

/// What woke the agent.
pub enum Wake {
    /// The inbox record at `sequence`.
    Inbox {
        /// Inbox sequence.
        sequence: u64,
        /// Decoded record.
        record: InboxRecord,
    },
    /// The actor alarm fired.
    Alarm,
}

/// Desired alarm after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alarm {
    /// No alarm.
    Clear,
    /// Fire at this Unix millisecond deadline ([`super::CONTINUE_NOW`] fires at once).
    At(u64),
}

/// One committed step: new state, ordered effects, desired alarm.
pub struct Step {
    /// Next state.
    pub state: AgentState,
    /// Effects, lowered into the same commit as the checkpoint.
    pub outs: Vec<Out>,
    /// Status appended by this step, if the phase changed.
    pub status: Option<StatusRecord>,
    /// Desired alarm.
    pub alarm: Alarm,
}

/// Runs one step for `wake`. Every control decision is made by [`kernel`].
///
/// `state` is `None` only for the first delivery, which must be `ForkAdopted`.
pub async fn step<T: TurnDriver + ?Sized>(
    driver: &T,
    path: &AgentPath,
    state: Option<AgentState>,
    cursor: u64,
    wake: Wake,
) -> Result<Step, Error> {
    let mut outs = Vec::new();
    let alarm_wake = matches!(wake, Wake::Alarm);
    // The last published phase: the creating commit published `Idle`, and
    // admitting a record (a cancel) can itself change the phase.
    let previous = state
        .as_ref()
        .map_or(Phase::Idle, |state| state.phase.clone());
    let mut state = match (state, wake) {
        (
            None,
            Wake::Inbox {
                record: InboxRecord::ForkAdopted(adopted),
                ..
            },
        ) => {
            if adopted.agent != path.id() {
                return Err(Error::Record("ForkAdopted names another agent"));
            }
            let mut state = AgentState::adopted(path.clone(), &adopted);
            state
                .queued
                .extend(adopted.brief.map(|brief| TurnInput::Brief {
                    brief: Box::new(brief),
                }));
            state
        }
        (None, _) => return Err(Error::Record("first agent input is not ForkAdopted")),
        (Some(state), Wake::Alarm) => state,
        // A requested spawn is this step's own spawn effect; it is never a
        // turn input, so it neither wakes nor reorders the turn.
        (
            Some(mut state),
            Wake::Inbox {
                sequence,
                record: InboxRecord::Spawn(spawn),
            },
        ) => {
            outs.push(Out::Spawn {
                intent: requested_intent(sequence)?,
                brief: Some(spawn.brief.into_bytes()),
                volumes: Vec::new(),
            });
            // Applied before the turn runs, so its tools see this child.
            apply_spawns(&mut state, &mut outs, 0);
            state
        }
        (Some(state), Wake::Inbox { sequence, record }) => admit(state, sequence, record)?,
    };
    let applied = outs.len();
    if state.closed {
        cascade(&mut state, &mut outs)?;
    } else {
        run(driver, &mut state, cursor, alarm_wake, &mut outs).await?;
    }
    apply_spawns(&mut state, &mut outs, applied);
    // The one point every state passes before it is checkpointed: one that
    // would not fit fails the agent visibly, shedding everything a step can
    // grow, so no step ever produces a record its stream refuses.
    if checkpoint::encode(&state, cursor)?.len() > MAX_CHECKPOINT_BYTES - TAIL_SLACK {
        shed(&mut state, &mut outs);
    }
    let alarm = match kernel::alarm(
        control(&state),
        !state.queued.is_empty() || state.overflow.is_some(),
        approval_position(&state).is_some(),
        state.archive_pending.len(),
    ) {
        Some(deadline) => Alarm::At(deadline),
        None => Alarm::Clear,
    };
    let appended = outs
        .iter()
        .filter(|out| matches!(out, Out::Conversation(_)))
        .count() as u64;
    // A status records each phase change, and also the step that creates the
    // conversation stream: before it exists, a follower can only wait on the
    // status stream, and a settled Idle -> Idle turn changes no phase.
    let creates_conversation = state.conversation_tail == 0 && appended > 0;
    let status = (state.phase != previous || creates_conversation).then(|| StatusRecord {
        phase: state.phase.clone(),
        cursor,
    });
    state.conversation_tail += appended;
    state.status_tail += u64::from(status.is_some());
    Ok(Step {
        state,
        outs,
        status,
        alarm,
    })
}

fn control(state: &AgentState) -> Control {
    Control {
        mode: match &state.phase {
            Phase::Idle => Mode::Idle,
            Phase::Running => Mode::Running,
            Phase::WaitingApproval { .. } => Mode::Approval,
            Phase::WaitingAlarm { deadline_unix_ms } => Mode::Alarm(*deadline_unix_ms),
            Phase::Done { .. } => Mode::Done,
        },
        closed: state.closed,
        reported: state.reported,
        turn: state.turn.is_some(),
    }
}

/// Folds one inbox record into the state.
fn admit(mut state: AgentState, sequence: u64, record: InboxRecord) -> Result<AgentState, Error> {
    let kind = match &record {
        InboxRecord::ForkAdopted(_) => return Err(Error::Record("agent adopted twice")),
        InboxRecord::Cancel(_) => Admit::Cancel,
        InboxRecord::Archive(_) => Admit::Archive,
        _ => Admit::Input,
    };
    let admitted = kernel::admit(control(&state), kind);
    if admitted.clear_queue {
        state.drop_queue();
    }
    if !admitted.control.turn {
        state.turn = None;
    }
    if admitted.control.mode == Mode::Idle {
        state.phase = Phase::Idle;
    }
    if kind == Admit::Archive {
        state.archive_pending.clone_from(&state.open_children);
    }
    state.closed = admitted.control.closed;
    if let InboxRecord::ChildDone(done) = &record {
        state.open_children.remove(&done.child);
        state.archive_pending.remove(&done.child);
    }
    if let Some(input) = input(sequence, record).filter(|_| admitted.enqueue) {
        // The response the waiting turn needs is taken now, out of order;
        // this step consumes it. Any other input joins the window, or, once
        // the window is full or already overflowed, stays in the inbox to be
        // read back in order. No admitted input is ever dropped.
        let awaited = matches!(
            (&state.phase, &input),
            (Phase::WaitingApproval { id }, TurnInput::Approval { id: response }) if id == response
        );
        if awaited {
            if state.overflow.is_some() {
                state.taken.insert(sequence);
            }
            state.queued.push_back(input);
        } else if state.overflow.is_none() && state.holds(&input) {
            state.queued.push_back(input);
        } else {
            state.overflow.get_or_insert(sequence);
        }
    }
    Ok(state)
}

/// The turn input an inbox record at `sequence` carries.
fn input(sequence: u64, record: InboxRecord) -> Option<TurnInput> {
    match record {
        InboxRecord::Message(message) => Some(TurnInput::Message {
            from: message.from,
            id: message.id,
            body: message.body,
            inbox: sequence,
        }),
        InboxRecord::ChildDone(done) => Some(TurnInput::ChildDone { done }),
        InboxRecord::Approval(approval) => Some(TurnInput::Approval { id: approval.id }),
        InboxRecord::ApprovalRequested(request) => Some(TurnInput::ApprovalRequested {
            child: request.child,
            id: request.id,
        }),
        InboxRecord::Hook(hook) => Some(TurnInput::Hook {
            host: hook.host,
            id: hook.id,
            event: hook.event,
        }),
        InboxRecord::ForkAdopted(_)
        | InboxRecord::Cancel(_)
        | InboxRecord::Archive(_)
        | InboxRecord::Spawn(_) => None,
    }
}

/// Position of the queued response to the approval the turn waits for.
fn approval_position(state: &AgentState) -> Option<usize> {
    let Phase::WaitingApproval { id } = &state.phase else {
        return None;
    };
    state
        .queued
        .iter()
        .position(|input| matches!(input, TurnInput::Approval { id: queued } if queued == id))
}

/// Runs at most one turn step, as chosen by [`kernel::pick`].
async fn run<T: TurnDriver + ?Sized>(
    driver: &T,
    state: &mut AgentState,
    cursor: u64,
    alarm_wake: bool,
    outs: &mut Vec<Out>,
) -> Result<(), Error> {
    let approval = approval_position(state);
    let input = match kernel::pick(
        control(state),
        alarm_wake,
        !state.queued.is_empty(),
        approval.is_some(),
    ) {
        Pick::Nothing => return Ok(()),
        Pick::Resume => None,
        Pick::Front => state.queued.pop_front(),
        Pick::Approval => approval.and_then(|position| state.queued.remove(position)),
    };
    let turn = driver.step(state, cursor, input).await?;
    outs.extend(turn.outs);
    let wait = match turn.wait {
        Wait::Done(outcome) => Wait::Done(bounded(outcome)),
        wait => wait,
    };
    let (wait, approval, outcome) = match wait {
        Wait::Continue => (kernel::Wait::Continue, None, None),
        Wait::Approval(id) => (kernel::Wait::Approval, Some(id), None),
        Wait::Until(deadline) => (kernel::Wait::Until(deadline), None, None),
        Wait::Input => (kernel::Wait::Input, None, None),
        Wait::Done(outcome) => (kernel::Wait::Done, None, Some(outcome)),
    };
    let (next, send) = kernel::settle(control(state), wait);
    state.turn = next.turn.then(|| turn.checkpoint.unwrap_or_default());
    state.reported = next.reported;
    state.phase = match (next.mode, approval, outcome) {
        (Mode::Running, ..) => Phase::Running,
        (Mode::Approval, Some(id), _) => Phase::WaitingApproval { id },
        (Mode::Alarm(deadline_unix_ms), ..) => Phase::WaitingAlarm { deadline_unix_ms },
        (Mode::Done, _, Some(outcome)) => {
            state.drop_queue();
            if send {
                report(state, outcome.clone(), outs);
            }
            Phase::Done { outcome }
        }
        _ => Phase::Idle,
    };
    Ok(())
}

/// `outcome`, or a failure naming it oversized: the parent's `ChildDone`
/// and every status carrying it must stay within their records.
fn bounded(outcome: AgentOutcome) -> AgentOutcome {
    let fits = match &outcome {
        AgentOutcome::Failed { message } => message.len() <= super::MAX_MESSAGE_BYTES,
        _ => true,
    } && serde_json::to_vec(&outcome)
        .is_ok_and(|bytes| bytes.len() <= MAX_OUTCOME_BYTES);
    if fits {
        outcome
    } else {
        AgentOutcome::Failed {
            message: OVERSIZED_OUTCOME.into(),
        }
    }
}

/// Fails an agent whose state would exceed its checkpoint: drops the input
/// window and the turn, the only parts a step grows without bound, and
/// settles `Done` as a failure, reported once. What remains is bounded by
/// construction (see [`checkpoint`]). An archived agent settles by its
/// cascade, and a settled one keeps its bounded outcome.
fn shed(state: &mut AgentState, outs: &mut Vec<Out>) {
    state.drop_queue();
    state.turn = None;
    if state.closed || matches!(state.phase, Phase::Done { .. }) {
        return;
    }
    let outcome = AgentOutcome::Failed {
        message: OVERSIZED_STATE.into(),
    };
    let (next, send) = kernel::report(control(state));
    state.reported = next.reported;
    if send {
        report(state, outcome.clone(), outs);
    }
    state.phase = Phase::Done { outcome };
}

/// Archive cascade: bounded fan-out to open children, then one report upward.
fn cascade(state: &mut AgentState, outs: &mut Vec<Out>) -> Result<(), Error> {
    let step = kernel::cascade(control(state), state.archive_pending.len());
    let id = IntentId::try_from(format!("cascade:{}", state.path.id()))?;
    for _ in 0..step.archive {
        let Some(child) = state.archive_pending.pop_first() else {
            break;
        };
        outs.push(Out::Archive {
            id: id.clone(),
            target: child,
        });
    }
    state.reported = step.control.reported;
    if step.report {
        state.phase = Phase::Done {
            outcome: AgentOutcome::Archived,
        };
        report(state, AgentOutcome::Archived, outs);
    }
    Ok(())
}

/// Sends `ChildDone` to the parent; called only when the kernel reports.
fn report(state: &AgentState, outcome: AgentOutcome, outs: &mut Vec<Out>) {
    if let Some(parent) = &state.parent {
        outs.push(Out::Send {
            to: parent.clone(),
            record: InboxRecord::ChildDone(ChildDone {
                child: state.path.clone(),
                outcome,
            }),
        });
    }
}

/// The child that the `Spawn` request at `parent`'s inbox `sequence` names,
/// and its spawning intent. Tool spawns name theirs `spawn:*`, so the two
/// never collide.
pub fn requested_child(parent: &AgentPath, sequence: u64) -> Result<(IntentId, AgentPath), Error> {
    let intent = requested_intent(sequence)?;
    let child = parent.child(&super::AgentId::derive(&parent.id(), intent.as_str()))?;
    Ok((intent, child))
}

fn requested_intent(sequence: u64) -> Result<IntentId, Error> {
    IntentId::try_from(format!("request:{sequence}"))
}

/// Applies each spawn in `outs[from..]`, in order, to `state`: each sees the
/// children every earlier spawn of the step opened. A refused spawn creates
/// nothing and is dropped from `outs`; a requester reads every outcome from
/// the committed state with [`AgentState::spawn_outcome`].
fn apply_spawns(state: &mut AgentState, outs: &mut Vec<Out>, from: usize) {
    let mut index = from;
    while index < outs.len() {
        let Out::Spawn { intent, .. } = &outs[index] else {
            index += 1;
            continue;
        };
        let opened = match state.spawned(intent) {
            Ok(child) if state.open_children.contains(&child) => true,
            Ok(child) if !state.closed && state.open_children.len() < MAX_OPEN_CHILDREN => {
                state.open_children.insert(child);
                true
            }
            _ => false,
        };
        if opened {
            index += 1;
        } else {
            outs.remove(index);
        }
    }
}
