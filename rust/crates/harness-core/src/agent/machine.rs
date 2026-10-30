//! One agent step: the pure transition from (state, input) to (state, effects, alarm).
//!
//! The step is deterministic given the turn driver's result, so a crash before the
//! actor checkpoint commits simply recomputes it. Its effects are lowered into one
//! composed commit by [`super::lower::lower`]; nothing is acknowledged before that
//! commit.

use super::checkpoint;
pub use super::input::{Progress, Queued, TurnInput};
use super::input::{awaits, input};
use super::{
    AgentOutcome, AgentPath, ChildDone, ChildSettled, Error, ForkAdopted, InboxRecord, IntentId,
    Phase, StatusRecord, VolumeBinding,
    kernel::{self, Admit, Control, Mode, Pick},
};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

/// Most inputs held in the checkpoint. Later input stays in the inbox, which is
/// the agent's unbounded durable queue, and is read back as the window drains.
pub const MAX_QUEUED: usize = 32;
/// Most encoded bytes of the inputs held in the checkpoint behind the first:
/// the window always holds its front input, and holds more only within this.
pub const MAX_QUEUED_BYTES: usize = 8 * 1024;
/// The failure a turn settles with when its driver returns an effect beyond
/// its record's limits; none of the turn's effects are applied.
pub const INADMISSIBLE_EFFECT: &str = "agent turn returns an invalid effect";
/// Why a step did not create a requested child.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnRefusal {
    /// The agent is archived.
    Archived,
    /// The host runs no children, so it creates none to wait forever.
    Unhosted,
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
    /// Identity digest of the brief its adoption named: on a spawned agent,
    /// the one file of its parent's it may read, granted to exactly that
    /// reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brief: Option<[u8; 32]>,
    /// Exact conversation tail; the agent is its only writer.
    pub conversation_tail: u64,
    /// Cancelled inputs before this sequence, including goal continuations.
    #[serde(default)]
    pub dropped_before: u64,
    /// Inherited conversation tail at adoption: this agent's own entries are
    /// `[conversation_base, conversation_tail)`, in increasing inbox order.
    pub conversation_base: u64,
    /// Conversation sequence of the latest context checkpoint (compaction,
    /// rewind or clear), committed with that entry: the one entry a turn's
    /// context starts from, however far behind the tail it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<u64>,
    /// Digest of the execution-target roster this agent's context last
    /// announced ([`crate::agent::Roster`]); a context checkpoint clears it,
    /// so the next turn announces the roster again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roster: Option<[u8; 32]>,
    /// Exact status tail; the agent is its only writer.
    pub status_tail: u64,
    /// Last published phase.
    pub phase: Phase,
    /// Children that have not reported `ChildDone`.
    pub open_children: super::Children,
    /// Open children the turn in progress waits for in
    /// [`Phase::WaitingChildren`]; empty in every other phase.
    #[serde(default, skip_serializing_if = "super::Children::is_empty")]
    pub awaiting: super::Children,
    /// The parent's turn waits for this agent to settle: the next step that
    /// publishes it idle or done answers with one `ChildSettled`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub awaited: bool,
    /// Open children that still have to receive `Archive`.
    pub archive_pending: super::Children,
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

/// The identity a brief's read grant names: the digest of its exact reference.
fn brief_identity(file: &FileRef) -> crate::Result<[u8; 32]> {
    file.identity_digest(None, &mut None)
}

impl AgentState {
    /// State at adoption, before the first step.
    ///
    /// # Errors
    /// When the adoption's brief has no identity digest.
    pub fn adopted(path: AgentPath, adopted: &ForkAdopted) -> Result<Self, Error> {
        let brief = adopted
            .brief
            .as_ref()
            .map(brief_identity)
            .transpose()
            .map_err(|error| Error::Turn(Box::new(error)))?;
        Ok(Self {
            path,
            parent: adopted.parent.clone(),
            volumes: adopted.volumes.clone(),
            policy: adopted.policy.clone(),
            brief,
            conversation_tail: adopted.conversation_tail,
            dropped_before: 0,
            conversation_base: adopted.conversation_tail,
            checkpoint: adopted.checkpoint,
            // A spawned agent announces its tree's roster in its first turn.
            roster: None,
            // The creating commit wrote the first status (`Idle`).
            status_tail: 1,
            phase: Phase::Idle,
            open_children: super::Children::new(),
            awaiting: super::Children::new(),
            awaited: false,
            archive_pending: super::Children::new(),
            queued: VecDeque::new(),
            overflow: None,
            taken: BTreeSet::new(),
            closed: false,
            reported: false,
            turn: None,
        })
    }

    /// `file` when it is exactly the brief this agent's adoption named, the
    /// one file outside its own volumes it may read; otherwise nothing.
    #[must_use]
    pub fn brief_grant<'f>(&self, file: &'f FileRef) -> &'f [FileRef] {
        let named = self
            .brief
            .is_some_and(|brief| brief_identity(file).is_ok_and(|identity| identity == brief));
        if named {
            std::slice::from_ref(file)
        } else {
            &[]
        }
    }

    /// The outcome of the spawn named `intent`, read from the state committed
    /// by the step that applied it. Its child is derived from the intent, and
    /// that step either opened it, found it open, or refused it for exactly
    /// one reason; no later input of the same step closes a child. So the
    /// outcome is a function of that state, and no state records outcomes.
    #[must_use]
    pub async fn spawn_outcome(&self, intent: &IntentId) -> Result<SpawnOutcome, Error> {
        let child = self.spawned(intent)?;
        Ok(
            if self.open_children.contains(&child).await? || !self.closed {
                SpawnOutcome::Spawned { child }
            } else {
                SpawnOutcome::Refused {
                    reason: SpawnRefusal::Archived,
                }
            },
        )
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
        self.dropped_before = self.dropped_before.max(end);
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
    /// The preceding conversation entry announced the roster of this digest.
    Roster([u8; 32]),
    /// Complete this goal run with the preceding conversation entry.
    GoalCompleted {
        /// Original turn identity, preserved across self-continuations.
        run: crate::OperationId,
    },
    /// The conversation entry just before this is the agent's latest context
    /// checkpoint, indexed in its state with the append.
    Checkpoint,
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
        /// Task brief, staged on the spawning agent's private volume; the
        /// child's adoption names it.
        brief: Option<FileRef>,
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
    /// Wait, holding no delivery, until these children settle: every one of
    /// them, or with `any` the first. Each open child is asked once, in this
    /// step's commit, and answers when it settles; a child already done
    /// settled, so the turn continues at once.
    Children {
        /// The awaited children.
        children: BTreeSet<AgentPath>,
        /// Whether the first to settle resumes the turn.
        any: bool,
    },
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
    /// `cancellation` is signalled by the actor runtime when a later control
    /// matches the handler's interrupt filter; settle cleanup through this step.
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        cursor: u64,
        input: Option<Queued>,
        cancellation: acyclic_actors::guest::CancellationToken,
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

pub(super) struct PreparedStep {
    step: Step,
    checkpoint: acyclic_actors::guest::Checkpoint,
}

impl PreparedStep {
    pub(super) fn new(step: Step) -> Result<Self, Error> {
        let bytes = checkpoint::encode(&step.state)?;
        Ok(Self {
            step,
            checkpoint: checkpoint::from_encoded(bytes)?,
        })
    }

    pub(super) fn step(&self) -> &Step {
        &self.step
    }

    pub(super) fn state_and_outs(&mut self) -> (&AgentState, &mut Vec<Out>) {
        (&self.step.state, &mut self.step.outs)
    }

    pub(super) fn into_checkpoint(self) -> acyclic_actors::guest::Checkpoint {
        self.checkpoint
    }
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
    cancellation: acyclic_actors::guest::CancellationToken,
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
    let previous = state.as_ref().map_or(Phase::Idle, published);
    let inbox_wake = !alarm_wake;
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
            let mut state = AgentState::adopted(path.clone(), &adopted)?;
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
                brief: Some(*spawn.brief),
                volumes: Vec::new(),
            });
            apply_spawns(&mut state, &mut outs, 0).await?;
            state
        }
        (Some(state), Wake::Inbox { sequence, record }) => admit(state, sequence, record).await?,
    };
    let applied = outs.len();
    if state.closed {
        cascade(&mut state, &mut outs).await?;
    } else if !spawn_wake {
        run(
            driver,
            &mut state,
            cursor,
            alarm_wake,
            &mut outs,
            cancellation,
        )
        .await?;
    }
    apply_spawns(&mut state, &mut outs, applied).await?;
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
    // client can associate that output with its delivered input. Every inbox
    // record an idle agent consumes publishes its cursor too, so `Idle` at a
    // cursor proves every record before it settled (see [`super::settled`]).
    let phase = published(&state);
    // An awaited child answers its parent once it settles, in the commit
    // that publishes it so: the parent's wait wakes on exactly that record.
    if state.awaited && matches!(phase, Phase::Idle | Phase::Done { .. }) {
        state.awaited = false;
        if let Some(parent) = &state.parent {
            outs.push(Out::Send {
                to: parent.clone(),
                record: InboxRecord::ChildSettled(ChildSettled {
                    child: state.path.clone(),
                }),
            });
        }
    }
    let mut sequence = state.conversation_tail;
    for out in &outs {
        match out {
            Out::Conversation(_) => {
                sequence = sequence
                    .checked_add(1)
                    .ok_or(Error::Record("agent conversation tail overflow"))?;
            }
            Out::Checkpoint => {
                state.checkpoint = sequence.checked_sub(1);
                state.roster = None;
            }
            Out::Roster(digest) => state.roster = Some(*digest),
            Out::Send { .. }
            | Out::Archive { .. }
            | Out::Spawn { .. }
            | Out::GoalCompleted { .. } => {}
        }
    }
    state.conversation_tail = state
        .conversation_tail
        .checked_add(appended)
        .ok_or(Error::Record("agent conversation tail overflow"))?;
    let status = (phase != previous || appended > 0 || (inbox_wake && phase == Phase::Idle))
        .then_some(StatusRecord {
            phase,
            cursor,
            conversation: state.conversation_tail,
        });
    state.status_tail = state
        .status_tail
        .checked_add(u64::from(status.is_some()))
        .ok_or(Error::Record("agent status tail overflow"))?;
    Ok(Step {
        state,
        outs,
        status,
        alarm,
    })
}

/// The phase a status publishes: an idle agent still holding input runs it
/// next, so it publishes `Running`; `Idle` always means nothing is pending.
fn published(state: &AgentState) -> Phase {
    if state.phase == Phase::Idle && (!state.queued.is_empty() || state.overflow.is_some()) {
        Phase::Running
    } else {
        state.phase.clone()
    }
}

fn control(state: &AgentState) -> Control {
    Control {
        mode: match &state.phase {
            Phase::Idle => Mode::Idle,
            Phase::Running => Mode::Running,
            Phase::WaitingApproval { .. }
            | Phase::WaitingHost { .. }
            | Phase::WaitingChildren { .. } => Mode::Approval,
            Phase::WaitingAlarm { deadline_unix_ms } => Mode::Alarm(*deadline_unix_ms),
            Phase::Done { .. } => Mode::Done,
        },
        closed: state.closed,
        reported: state.reported,
        turn: state.turn.is_some(),
    }
}

/// Folds one inbox record into the state.
async fn admit(
    mut state: AgentState,
    sequence: u64,
    record: InboxRecord,
) -> Result<AgentState, Error> {
    let kind = match &record {
        InboxRecord::ForkAdopted(_) => return Err(Error::Record("agent adopted twice")),
        InboxRecord::Cancel(_) => Admit::Cancel,
        InboxRecord::Archive(_) => Admit::Archive,
        _ => Admit::Input,
    };
    let admitted = kernel::admit(control(&state), kind);
    if admitted.clear_queue {
        state.dropped_before = state.dropped_before.max(sequence);
        state.drop_queue();
    }
    if !admitted.control.turn {
        state.turn = None;
        state.awaiting.clear();
    }
    if admitted.control.mode == Mode::Idle {
        state.phase = Phase::Idle;
    }
    if kind == Admit::Archive {
        state.archive_pending.clone_from(&state.open_children);
    }
    state.closed = admitted.control.closed;
    if let InboxRecord::ChildDone(done) = &record
        && done.child.parent().as_ref() == Some(&state.path)
    {
        state.open_children.remove(&done.child).await?;
        state.archive_pending.remove(&done.child).await?;
    }
    match &record {
        InboxRecord::Awaited => state.awaited = true,
        InboxRecord::ChildDone(ChildDone { child, .. })
        | InboxRecord::ChildSettled(ChildSettled { child }) => {
            settle_awaited(&mut state, child).await?
        }
        _ => {}
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

/// An awaited `child` settled: the turn waiting in
/// [`Phase::WaitingChildren`] resumes once its last awaited child (with `any`,
/// its first) has. Each child is awaited once, so this is one set removal.
async fn settle_awaited(state: &mut AgentState, child: &AgentPath) -> Result<(), Error> {
    if child.parent().as_ref() != Some(&state.path) || !state.awaiting.remove(child).await? {
        return Ok(());
    }
    if let Phase::WaitingChildren { any } = state.phase
        && (any || state.awaiting.is_empty())
    {
        state.awaiting.clear();
        state.phase = Phase::Running;
    }
    Ok(())
}

/// Reject driver effects that violate the agent protocol.
fn valid_effect(out: &Out) -> bool {
    match out {
        Out::Conversation(_) => true,
        Out::Send { record, .. } => {
            !matches!(
                record,
                InboxRecord::ForkAdopted(_) | InboxRecord::Archive(_)
            ) && super::AgentRecord::check(record).is_ok()
        }
        Out::Archive { .. } | Out::Checkpoint | Out::Roster(_) | Out::GoalCompleted { .. } => true,
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
    cancellation: acyclic_actors::guest::CancellationToken,
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
    // A turn that fails for good settles as a committed failure, which
    // every observer reads like any other outcome; only an uncertain
    // failure retries the step.
    let mut turn = match driver.step(state, cursor, input, cancellation).await {
        Ok(turn) => turn,
        Err(error) if error.uncertain() => return Err(error),
        Err(error) => TurnStep {
            checkpoint: None,
            outs: Vec::new(),
            wait: Wait::Done(AgentOutcome::Failed {
                message: error.to_string(),
            }),
            drops: None,
        },
    };
    // The driver's effects are the step's one unchecked input: a turn
    // returning an invalid effect fails without publishing those effects.
    if !turn.outs.iter().all(valid_effect)
        || matches!(&turn.wait, Wait::Host(host) if super::records::check_text(host).is_err())
    {
        turn = TurnStep {
            checkpoint: None,
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
    let wait = turn.wait;
    let mut awaiting = state.awaiting.clone();
    awaiting.clear();
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
        Wait::Children { children, any } => {
            // Only an open child can still settle; one already done has, so
            // a wait it satisfies continues at once and asks no child.
            let mut open = Vec::new();
            for child in &children {
                if child.parent().as_ref() == Some(&state.path)
                    && state.open_children.contains(child).await?
                {
                    open.push(child.clone());
                }
            }
            let waits = !open.is_empty() && (!any || open.len() == children.len());
            if waits {
                outs.extend(open.iter().map(|child| Out::Send {
                    to: child.clone(),
                    record: InboxRecord::Awaited,
                }));
                for child in open {
                    awaiting.insert(child).await?;
                }
                (
                    kernel::Wait::Approval,
                    Some(Phase::WaitingChildren { any }),
                    None,
                )
            } else {
                (kernel::Wait::Continue, None, None)
            }
        }
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
    if next.mode != Mode::Approval {
        awaiting.clear();
    }
    state.awaiting = awaiting;
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

async fn cascade(state: &mut AgentState, outs: &mut Vec<Out>) -> Result<(), Error> {
    let step = kernel::cascade(control(state), state.archive_pending.len());
    let archive_id = IntentId::try_from(format!("cascade:{}", state.path.id()))?;
    for _ in 0..step.archive {
        let Some(id) = state.archive_pending.first().await? else {
            break;
        };
        let child = state.path.child(&id)?;
        state.archive_pending.remove(&child).await?;
        outs.push(Out::Archive {
            id: archive_id.clone(),
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

pub(super) fn requested_intent(sequence: u64) -> Result<IntentId, Error> {
    IntentId::try_from(format!("request:{sequence}"))
}

/// Applies each spawn in `outs[from..]`, in order, to `state`: each sees the
/// children every earlier spawn of the step opened. A refused spawn creates
/// nothing and is dropped from `outs`; a requester reads every outcome from
/// the committed state with [`AgentState::spawn_outcome`].
async fn apply_spawns(
    state: &mut AgentState,
    outs: &mut Vec<Out>,
    from: usize,
) -> Result<(), Error> {
    let mut index = from;
    while index < outs.len() {
        let Out::Spawn { intent, .. } = &outs[index] else {
            index += 1;
            continue;
        };
        let opened = match state.spawned(intent) {
            Ok(child) if state.open_children.contains(&child).await? => true,
            Ok(child) if !state.closed => {
                state.open_children.insert(child).await?;
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
    Ok(())
}
