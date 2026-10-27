//! One agent step: the pure transition from (state, input) to (state, effects, alarm).
//!
//! The step is deterministic given the turn driver's result, so a crash before the
//! actor checkpoint commits simply recomputes it. Its effects are lowered into one
//! composed commit by [`super::lower::lower`]; nothing is acknowledged before that
//! commit.

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

/// One input held for the turn, at the inbox sequence of the record that
/// carried it (a brief: its `ForkAdopted`). The turn it starts settles one
/// conversation entry at the same sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Queued {
    /// Inbox sequence of the carrying record, as `Delivered::Committed` returned it.
    pub inbox: u64,
    /// The input.
    pub input: TurnInput,
}

/// A turn in progress: the inbox sequence of the input that started it and
/// the turn driver's checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    /// Inbox sequence of the turn's starting input.
    pub inbox: u64,
    /// Turn driver checkpoint for the next step.
    pub checkpoint: Vec<u8>,
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
    /// Inherited conversation tail at adoption: this agent's own entries are
    /// `[conversation_base, conversation_tail)`, in increasing inbox order.
    pub conversation_base: u64,
    /// Exact status tail; the agent is its only writer.
    pub status_tail: u64,
    /// Last published phase.
    pub phase: Phase,
    /// Children that have not reported `ChildDone`.
    pub open_children: BTreeSet<AgentPath>,
    /// Open children that still have to receive `Archive`.
    pub archive_pending: BTreeSet<AgentPath>,
    /// Inputs waiting for the turn, in inbox order.
    pub queued: VecDeque<Queued>,
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
    /// The turn in progress.
    pub turn: Option<Progress>,
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
            conversation_base: adopted.conversation_tail,
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
            if self.queued.len() >= MAX_QUEUED {
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
        input: Option<Queued>,
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
            state.queued.extend(adopted.brief.map(|brief| Queued {
                inbox: adopted.forked_at,
                input: TurnInput::Brief {
                    brief: Box::new(brief),
                },
            }));
            state
        }
        (None, _) => return Err(Error::Record("first agent input is not ForkAdopted")),
        (Some(state), Wake::Alarm) => state,
        (Some(state), Wake::Inbox { sequence, record }) => admit(state, sequence, record)?,
    };
    if state.closed {
        cascade(&mut state, &mut outs)?;
    } else {
        run(driver, &mut state, cursor, alarm_wake, &mut outs).await?;
    }
    track_children(&mut state, &outs)?;
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
            (&state.phase, &input.input),
            (Phase::WaitingApproval { id }, TurnInput::Approval { id: response }) if id == response
        );
        if awaited {
            if state.overflow.is_some() {
                state.taken.insert(sequence);
            }
            state.queued.push_back(input);
        } else if state.overflow.is_none() && state.queued.len() < MAX_QUEUED {
            state.queued.push_back(input);
        } else {
            state.overflow.get_or_insert(sequence);
        }
    }
    Ok(state)
}

/// The turn input an inbox record at `sequence` carries.
fn input(sequence: u64, record: InboxRecord) -> Option<Queued> {
    let input = match record {
        InboxRecord::Message(message) => Some(TurnInput::Message {
            from: message.from,
            id: message.id,
            body: message.body,
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
        InboxRecord::ForkAdopted(_) | InboxRecord::Cancel(_) | InboxRecord::Archive(_) => None,
    };
    input.map(|input| Queued {
        inbox: sequence,
        input,
    })
}

/// Position of the queued response to the approval the turn waits for.
fn approval_position(state: &AgentState) -> Option<usize> {
    let Phase::WaitingApproval { id } = &state.phase else {
        return None;
    };
    state.queued.iter().position(
        |queued| matches!(&queued.input, TurnInput::Approval { id: queued } if queued == id),
    )
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
    // A resumed turn keeps the input that started it.
    let started = match (&state.turn, &input) {
        (Some(turn), _) => Some(turn.inbox),
        (None, input) => input.as_ref().map(|input| input.inbox),
    };
    let turn = driver.step(state, cursor, input).await?;
    outs.extend(turn.outs);
    let (wait, approval, outcome) = match turn.wait {
        Wait::Continue => (kernel::Wait::Continue, None, None),
        Wait::Approval(id) => (kernel::Wait::Approval, Some(id), None),
        Wait::Until(deadline) => (kernel::Wait::Until(deadline), None, None),
        Wait::Input => (kernel::Wait::Input, None, None),
        Wait::Done(outcome) => (kernel::Wait::Done, None, Some(outcome)),
    };
    let (next, send) = kernel::settle(control(state), wait);
    state.turn = started.filter(|_| next.turn).map(|inbox| Progress {
        inbox,
        checkpoint: turn.checkpoint.unwrap_or_default(),
    });
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

/// Records spawned children as open.
fn track_children(state: &mut AgentState, outs: &[Out]) -> Result<(), Error> {
    for out in outs {
        if let Out::Spawn { intent, .. } = out {
            let child = state
                .path
                .child(&super::AgentId::derive(&state.path.id(), intent.as_str()))?;
            if state.open_children.len() >= MAX_OPEN_CHILDREN
                && !state.open_children.contains(&child)
            {
                return Err(Error::Record("agent has too many open children"));
            }
            state.open_children.insert(child);
        }
    }
    Ok(())
}
