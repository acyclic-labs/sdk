//! One agent step: the pure transition from (state, input) to (state, effects, alarm).
//!
//! The step is deterministic given the turn driver's result, so a crash before the
//! actor checkpoint commits simply recomputes it. Its effects are lowered into one
//! composed commit by [`super::lower::lower`]; nothing is acknowledged before that
//! commit.

use super::checkpoint::{self, MAX_CHECKPOINT_BYTES, TAIL_SLACK};
pub use super::input::{Progress, Queued, TurnInput};
use super::input::{awaits, input};
use super::{
    AgentOutcome, AgentPath, ChildDone, Error, ForkAdopted, InboxRecord, IntentId, Phase,
    StatusRecord, VolumeBinding,
    kernel::{self, Admit, Control, Mode, Pick},
};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

pub use super::slots::MAX_OPEN_CHILDREN;
/// Most inputs held in the checkpoint. Later input stays in the inbox, which is
/// the agent's unbounded durable queue, and is read back as the window drains.
pub const MAX_QUEUED: usize = 32;
/// Most encoded bytes of the inputs held in the checkpoint behind the first:
/// the window always holds its front input, and holds more only within this.
pub const MAX_QUEUED_BYTES: usize = 8 * 1024;
/// Largest encoded outcome a turn may settle with: above every success, whose
/// one file reference is bounded, so only an overlong failure message is
/// replaced. It travels in the parent's `ChildDone` next to the child's path.
pub const MAX_OUTCOME_BYTES: usize = 16 * 1024;
/// The failure a turn settles with when its driver returns an effect beyond
/// its record's limits; none of the turn's effects are applied.
pub const INADMISSIBLE_EFFECT: &str = "agent turn returns an effect beyond its record limits";
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
    /// While a turn is in progress, the children its recorded spawns open
    /// when it settles, beyond those already open, plus slots vacated while
    /// it pauses. A replayed recorded spawn may name one of those children;
    /// clients cannot take its slot before the turn settles.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub reserved_children: u16,
}

fn is_zero(count: &u16) -> bool {
    *count == 0
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
            reserved_children: 0,
        }
    }

    /// Open-child slots a spawn requested now may take.
    fn free_children(&self) -> usize {
        super::slots::client_ceiling(self.turn.is_some(), usize::from(self.reserved_children))
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
        let held: usize = self
            .queued
            .iter()
            .skip(1)
            .map(|queued| bytes(&queued.input))
            .sum();
        self.queued.len() < MAX_QUEUED && held.saturating_add(bytes(input)) <= MAX_QUEUED_BYTES
    }

    /// Drops every pending input, held or still in the inbox.
    fn drop_queue(&mut self) {
        self.queued.clear();
        self.overflow = None;
        self.taken.clear();
    }

    /// Drops every pending input before inbox sequence `end`; the inbox
    /// read-back resumes at `end`.
    fn drop_before(&mut self, end: u64) {
        self.queued.retain(|queued| queued.inbox >= end);
        self.taken.retain(|sequence| *sequence >= end);
        if self.overflow.is_some_and(|from| from < end) {
            self.overflow = Some(end);
        }
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
            if !self.holds(&input.input) {
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
    /// Wait for an event hook from this host, such as a native process
    /// claiming or settling the turn's input. Like an approval response, the
    /// hook is taken ahead of the input queued behind the turn.
    Host(String),
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
    /// While the turn stays in progress, how many children its recorded
    /// spawns open when it settles, beyond those already open.
    pub reserved_children: usize,
    /// Effects, in order.
    pub outs: Vec<Out>,
    /// What to wait for next.
    pub wait: Wait,
    /// Drops every input pending before this inbox sequence, held or still
    /// in the inbox: the host's own cancel or archive, taken out of order.
    pub drops: Option<u64>,
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
    // A requested spawn's step carries only that spawn: its child is created
    // by this step's commit, so no turn may reach it until the commit lands.
    // Pending turn work continues at once on the next step (the kernel's
    // alarm), where the child is open and reachable.
    let spawn_wake = matches!(
        wake,
        Wake::Inbox {
            record: InboxRecord::Spawn(_),
            ..
        }
    );
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
            // A turn in progress keeps the slots its recorded spawns take.
            let free = state.free_children();
            apply_spawns(&mut state, &mut outs, 0, free);
            state
        }
        (Some(state), Wake::Inbox { sequence, record }) => admit(state, sequence, record)?,
    };
    let applied = outs.len();
    if state.closed {
        cascade(&mut state, &mut outs)?;
    } else if !spawn_wake {
        run(driver, &mut state, cursor, alarm_wake, &mut outs).await?;
    }
    apply_spawns(&mut state, &mut outs, applied, MAX_OPEN_CHILDREN);
    // The one point every state passes before it is checkpointed: one that
    // would not fit fails the agent visibly, shedding everything a step can
    // grow, so no step ever produces a record its stream refuses.
    if checkpoint::encode(&state, cursor)?.len() > MAX_CHECKPOINT_BYTES - TAIL_SLACK {
        shed(&mut state, &mut outs, OVERSIZED_STATE);
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
    // A settled Idle -> Idle turn still advances observation: publish its
    // consumed inbox cursor atomically with every conversation append, so a
    // client can associate that output with its delivered input.
    let status = (state.phase != previous || appended > 0).then(|| StatusRecord {
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
            Phase::WaitingApproval { .. } | Phase::WaitingHost { .. } => Mode::Approval,
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
        if state.open_children.remove(&done.child) && state.turn.is_some() {
            // The paused journal may already contain a replayed spawn of this
            // child. Its slot remains owned by the turn until the driver next
            // reconciles recorded effects. Duplicate ChildDone consumes none.
            state.reserved_children = super::slots::reserve_vacated(state.reserved_children);
        }
        state.archive_pending.remove(&done.child);
    }
    if let Some(input) = input(sequence, record).filter(|_| admitted.enqueue) {
        // The response the waiting turn needs is taken now, out of order;
        // this step consumes it. Any other input joins the window, or, once
        // the window is full or already overflowed, stays in the inbox to be
        // read back in order. No admitted input is ever dropped.
        if awaits(&state.phase, &input.input) {
            if state.overflow.is_some() {
                state.taken.insert(sequence);
            }
            state.queued.push_back(input);
        } else if state.overflow.is_none() && state.holds(&input.input) {
            state.queued.push_back(input);
        } else {
            state.overflow.get_or_insert(sequence);
        }
    }
    Ok(state)
}

/// Whether a driver's effect fits the record that carries it.
fn admissible(out: &Out) -> bool {
    match out {
        Out::Conversation(record) => record.len() <= super::MAX_RECORD_BYTES,
        Out::Send { record, .. } => {
            !matches!(
                record,
                InboxRecord::ForkAdopted(_) | InboxRecord::Archive(_)
            ) && super::AgentRecord::encode(record).is_ok()
        }
        Out::Archive { .. } => true,
        Out::Spawn { volumes, .. } => super::records::check_volumes(volumes).is_ok(),
    }
}

/// Position of the queued input the waiting turn takes next (see [`awaits`]).
fn approval_position(state: &AgentState) -> Option<usize> {
    state
        .queued
        .iter()
        .position(|queued| awaits(&state.phase, &queued.input))
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
    let mut turn = driver.step(state, cursor, input).await?;
    // The driver's effects are the step's one unchecked input: a turn
    // returning one no record can carry fails, applying none of them, so
    // the step's commit is always buildable.
    if !turn.outs.iter().all(admissible)
        || matches!(&turn.wait, Wait::Host(host) if super::records::check_text(host).is_err())
    {
        turn = TurnStep {
            checkpoint: None,
            reserved_children: 0,
            outs: Vec::new(),
            wait: Wait::Done(AgentOutcome::Failed {
                message: INADMISSIBLE_EFFECT.into(),
            }),
            drops: None,
        };
    }
    if let Some(end) = turn.drops {
        state.drop_before(end);
    }
    outs.extend(turn.outs);
    let wait = match turn.wait {
        Wait::Done(outcome) => Wait::Done(bounded(outcome)),
        wait => wait,
    };
    let (wait, approval, outcome) = match wait {
        Wait::Continue => (kernel::Wait::Continue, None, None),
        Wait::Approval(id) => (
            kernel::Wait::Approval,
            Some(Phase::WaitingApproval { id }),
            None,
        ),
        Wait::Host(host) => (
            kernel::Wait::Approval,
            Some(Phase::WaitingHost { host }),
            None,
        ),
        Wait::Until(deadline) => (kernel::Wait::Until(deadline), None, None),
        Wait::Input => (kernel::Wait::Input, None, None),
        Wait::Done(outcome) => (kernel::Wait::Done, None, Some(outcome)),
    };
    let (next, send) = kernel::settle(control(state), wait);
    state.turn = started.filter(|_| next.turn).map(|inbox| Progress {
        inbox,
        checkpoint: turn.checkpoint.unwrap_or_default(),
    });
    state.reserved_children = match state.turn {
        Some(_) => u16::try_from(turn.reserved_children.min(MAX_OPEN_CHILDREN)).unwrap_or(u16::MAX),
        None => 0,
    };
    state.reported = next.reported;
    state.phase = match (next.mode, approval, outcome) {
        (Mode::Running, ..) => Phase::Running,
        (Mode::Approval, Some(waiting), _) => waiting,
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

/// Fails an agent with `failure`, as when its state would exceed its
/// checkpoint: drops the input
/// window and the turn, the only parts a step grows without bound, and
/// settles `Done` as a failure, reported once. What remains is bounded by
/// construction (see [`checkpoint`]). An archived agent settles by its
/// cascade, and a settled one keeps its bounded outcome.
fn shed(state: &mut AgentState, outs: &mut Vec<Out>, failure: &str) {
    state.drop_queue();
    state.turn = None;
    if state.closed || matches!(state.phase, Phase::Done { .. }) {
        return;
    }
    let outcome = AgentOutcome::Failed {
        message: failure.into(),
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
fn apply_spawns(state: &mut AgentState, outs: &mut Vec<Out>, from: usize, limit: usize) {
    let mut index = from;
    while index < outs.len() {
        let Out::Spawn { intent, .. } = &outs[index] else {
            index += 1;
            continue;
        };
        let opened = match state.spawned(intent) {
            Ok(child) if state.open_children.contains(&child) => true,
            Ok(child) if !state.closed && super::slots::admits(state.open_children.len(), limit, false) => {
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
