//! One agent step: the pure transition from (state, input) to (state, effects, alarm).
//!
//! The step is deterministic given the turn driver's result, so a crash before the
//! actor checkpoint commits simply recomputes it. Its effects are lowered into one
//! composed commit by [`super::lower::lower`]; nothing is acknowledged before that
//! commit.

use super::checkpoint::{self, MAX_CHECKPOINT_BYTES};
#[cfg(feature = "test-support")]
use super::control_trace::{self, Decision};
pub use super::input::{Progress, Queued, TurnInput};
use super::input::{awaits, input};
use super::{
    AgentOutcome, AgentPath, ChildDone, ChildSettled, ChildSettlement, ChildWakes, Error,
    ForkAdopted, InboxRecord, IntentId, Phase, StatusRecord, WorkspaceEntry,
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
    /// Refused by the forced run topology.
    Topology,
    /// The agent is archived.
    Archived,
    /// The host runs no children, so it creates none to wait forever.
    Unhosted,
    /// The parent's role does not admit the requested role.
    Role,
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
    /// Every admitted worker, including settled children; replays count once.
    pub spawned_children: super::Children,
    /// Successful tool spawns retained by a paused turn, not yet published.
    pub reserved_spawns: u64,
    /// Original topology refusals, retained after later input and archival.
    pub topology_refusals: super::Children,
    /// First original completion fact in the status stream for this wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_settlements: Option<u64>,
    /// This agent.
    pub path: AgentPath,
    /// Spawning agent.
    pub parent: Option<AgentPath>,
    /// Role profile pinned at adoption.
    pub profile: Option<super::CreationConfig>,
    /// Owned volumes.
    pub volumes: Vec<WorkspaceEntry>,
    /// Approval policy file.
    pub policy: Option<FileRef>,
    /// Identity digest of the brief its adoption named: on a spawned agent,
    /// the one file of its parent's it may read, granted to exactly that
    /// reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brief: Option<[u8; 32]>,
    /// Exact conversation tail; the agent is its only writer.
    pub conversation_tail: u64,
    /// Exclusive inbox range containing notices not yet frozen into a request.
    /// Payloads remain in the original inbox; checkpoint size is independent
    /// of the number of pending events. This is not a conversation-item cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_notices: Option<std::ops::Range<u64>>,
    /// Exclusive inbox cursor included at an admitted model boundary. Payloads
    /// remain in the original inbox; checkpoint cost is constant per agent.
    pub steered_through: u64,
    /// Latest committed steerable input, including records outside the queue window.
    pub steering_tail: u64,
    /// Cancelled inputs before this sequence, including goal continuations.
    #[serde(default)]
    pub dropped_before: u64,
    /// Inherited conversation tail at adoption: this agent's own entries are
    /// `[conversation_base, conversation_tail)`, in increasing inbox order.
    pub conversation_base: u64,
    /// An agent action has committed, even if its model turn never answered.
    pub committed_actions: bool,
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
    /// Open children whose settlement the turn in progress still counts in
    /// [`Phase::Waiting`]; empty in every other phase.
    #[serde(default, skip_serializing_if = "super::Children::is_empty")]
    pub awaiting: super::Children,
    /// How many more of `awaiting` must settle before the waiting turn
    /// resumes; zero in every other phase.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub need: u32,
    /// The parent's turn waits for this agent to settle: the next step that
    /// publishes it idle or done answers with one `ChildSettled`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub awaited: bool,
    /// Open children that still have to receive `Archive`.
    pub archive_pending: super::Children,
    /// A terminal publication refusal stopped the cascade. Pending children
    /// remain durable, but neither arrivals nor alarms retry their writes.
    pub archive_refused: bool,
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

fn is_zero<T: Default + PartialEq>(count: &T) -> bool {
    *count == T::default()
}

/// The identity a brief's read grant names: the digest of its exact reference.
fn brief_identity(file: &FileRef) -> crate::Result<[u8; 32]> {
    file.identity_digest(
        crate::store::default_projection_read_limits().maximum_bytes,
        &mut None,
    )
}

impl AgentState {
    pub(super) fn validate_parent(&self) -> Result<(), Error> {
        if !self.path.parent_is(self.parent.as_ref()) {
            return Err(Error::Record("agent parent is not its path's parent"));
        }
        Ok(())
    }

    pub(super) fn already_steered(&self, sequence: u64, record: &InboxRecord) -> bool {
        sequence < self.steered_through
            && matches!(record, InboxRecord::Message(_) | InboxRecord::Notice { .. })
    }

    /// A terminal setup failure frees its name only before any reply or action
    /// committed. Model failure after a tool action must retain the identity.
    /// The brief is queued at adoption, not appended to the conversation: the
    /// first committed entry includes its response. Inherited entries do not
    /// count. Done is terminal, so a released generation remains released.
    #[must_use]
    pub fn released(&self) -> bool {
        super::release::released(
            matches!(
                self.phase,
                Phase::Done {
                    outcome: AgentOutcome::Failed { .. }
                }
            ),
            self.conversation_tail,
            self.conversation_base,
            self.committed_actions,
        )
    }

    /// Whether an inbox input remains unread, queued, or is the running turn.
    /// `cursor` is the original actor delivery's exclusive inbox cursor. The
    /// bounded input window and existing overflow exceptions are the authority;
    /// an out-of-order approval/hook already removed by the driver is consumed.
    #[must_use]
    pub fn input_pending(&self, cursor: u64, sequence: u64) -> bool {
        sequence >= cursor
            || self.queued.iter().any(|queued| queued.inbox == sequence)
            || self
                .overflow
                .is_some_and(|first| sequence >= first && !self.taken.contains(&sequence))
            || self.turn.as_ref().is_some_and(|turn| {
                turn.inbox == sequence
                    || (sequence >= self.dropped_before
                        && turn.inbox < sequence
                        && sequence < self.steered_through)
            })
    }

    /// Exclusive inbox prefix with no remaining turn or queued work, at the
    /// actor's original consumed cursor. Consumption alone may checkpoint a
    /// paused turn or move later inputs into the retained window. Fixed
    /// comparisons suffice; no queue, child or history scan is required.
    pub fn settled_before(&self, consumed: u64) -> u64 {
        consumed
            .min(self.turn.as_ref().map_or(consumed, |turn| turn.inbox))
            .min(self.queued.front().map_or(consumed, |queued| queued.inbox))
            .min(self.overflow.unwrap_or(consumed))
    }

    /// State at adoption, before the first step.
    ///
    /// # Errors
    /// When the adoption's brief has no identity digest.
    pub fn adopted(path: AgentPath, adopted: &ForkAdopted) -> Result<Self, Error> {
        super::records::check_volumes(adopted.volumes.iter())?;
        let brief = adopted
            .brief
            .as_ref()
            .map(brief_identity)
            .transpose()
            .map_err(|error| Error::Turn(Box::new(error)))?;
        Ok(Self {
            spawned_children: super::Children::new(),
            reserved_spawns: 0,
            topology_refusals: super::Children::new(),
            child_settlements: None,
            path,
            parent: adopted.parent.clone(),
            profile: adopted.profile.clone(),
            volumes: adopted.volumes.clone(),
            policy: adopted.policy.clone(),
            brief,
            conversation_tail: adopted.conversation_tail,
            pending_notices: None,
            steered_through: 0,
            steering_tail: 0,
            dropped_before: 0,
            conversation_base: adopted.conversation_tail,
            committed_actions: false,
            checkpoint: adopted.checkpoint,
            // A spawned agent announces its tree's roster in its first turn.
            roster: None,
            // The creating commit wrote the first status (`Idle`).
            status_tail: 1,
            phase: Phase::Idle,
            open_children: super::Children::new(),
            awaiting: super::Children::new(),
            need: 0,
            awaited: false,
            archive_pending: super::Children::new(),
            archive_refused: false,
            queued: adopted
                .brief
                .iter()
                .cloned()
                .map(|brief| Queued {
                    steering: false,
                    inbox: adopted.forked_at,
                    input: TurnInput::Brief {
                        brief: Box::new(brief),
                    },
                })
                .collect(),
            overflow: None,
            taken: BTreeSet::new(),
            closed: false,
            reported: false,
            turn: None,
        })
    }

    /// `file` when this agent's immutable adoption names it: its brief,
    /// template, or continuation request. No other external file is admitted.
    #[must_use]
    pub fn adoption_grant<'f>(&self, file: &'f FileRef) -> &'f [FileRef] {
        let named = self
            .brief
            .is_some_and(|brief| brief_identity(file).is_ok_and(|identity| identity == brief));
        let creation = self.profile.as_ref();
        let pinned = creation.and_then(|creation| creation.template.as_ref()) == Some(file)
            || creation
                .and_then(|creation| creation.continuation.as_ref())
                .and_then(|continuation| continuation.override_item.as_ref())
                == Some(file);
        if named || pinned {
            std::slice::from_ref(file)
        } else {
            &[]
        }
    }

    /// The outcome of the spawn named `intent`, read from the state committed
    /// by the step that applied it. Persistent admission and refusal membership
    /// preserve that decision after the child settles or the parent archives.
    #[must_use]
    pub async fn spawn_outcome(&self, intent: &IntentId) -> Result<SpawnOutcome, Error> {
        let child = self.spawned(intent)?;
        Ok(if self.spawned_children.contains(&child).await? {
            SpawnOutcome::Spawned { child }
        } else if self.topology_refusals.contains(&child).await? {
            SpawnOutcome::Refused {
                reason: SpawnRefusal::Topology,
            }
        } else if self.closed {
            SpawnOutcome::Refused {
                reason: SpawnRefusal::Archived,
            }
        } else {
            return Err(Error::Record("spawn outcome precedes its applying step"));
        })
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
            if (sequence < self.steered_through
                && matches!(record, InboxRecord::Message(_) | InboxRecord::Notice { .. }))
                || self.taken.remove(&sequence)
            {
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
    /// Retain the paused turn's successful, unpublished spawn admissions.
    PendingSpawns(u64),
    /// Inbox inputs before this exclusive cursor already reached the model.
    Steered(u64),
    /// Append to this agent's conversation.
    Conversation(Vec<u8>),
    /// The preceding conversation entry announced the roster of this digest.
    Roster([u8; 32]),
    /// Consume a frozen Notice prefix with its original conversation append.
    Notices(super::NoticeSelection),
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
        /// Role profile admitted before the spawn effect was recorded; `None`
        /// for a host-requested spawn.
        profile: Option<super::CreationConfig>,
        /// Volumes bound to the child (filled by the filesystem contribution).
        volumes: Vec<WorkspaceEntry>,
        /// Optional retained conversation cut selected by the caller.
        history: Option<super::HistorySelection>,
    },
}

/// What the turn waits for after a turn step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wait {
    /// Run the next turn step at once.
    Continue,
    /// Wait for the response to this approval request.
    Approval {
        /// Original request identity.
        id: IntentId,
        /// Exact action retained by its approval owner; absent for an external
        /// host that does not publish an approval request.
        binding: Option<crate::interaction::ApprovalBinding>,
    },
    /// Wait for an event hook from this host, such as a native process
    /// claiming or settling the turn's input. Like an approval response, the
    /// hook is taken ahead of the input queued behind the turn.
    Host(String),
    /// Wait, holding no delivery, until a monotone condition holds or its
    /// timeout passes (see [`Until`]). Input can also resume it when selected.
    Until(Until),
    /// The turn finished; wait for more input.
    Input,
    /// A settled turn's own committed UI write asks for a user response.
    User(String),
    /// The agent's task is complete.
    Done(AgentOutcome),
}

/// A monotone wait condition: `need` of the `awaited` agents settle, or the
/// timeout passes, whichever is first. Every agent wait is one of its presets
/// ([`Self::all`], [`Self::any`], [`Self::quorum`], [`Self::sleep`]).
///
/// Each awaited open agent is asked once, in the step's commit, and answers
/// once when it settles; one already done has settled and counts at once.
/// Each answer lowers the remaining need by one, so the condition only moves
/// toward holding and a turn resumes at most once per wait. The timeout is
/// one actor alarm, set from the delivery clock of the committing step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Until {
    /// Agents whose settlement counts, each at most once.
    pub awaited: BTreeSet<AgentPath>,
    /// How many of them must settle.
    pub need: u32,
    /// Milliseconds after the committing step at which the turn resumes
    /// regardless; `None` waits for the condition alone.
    pub timeout_ms: Option<u64>,
    /// Also resume on inbox input; used by reply-or-alarm waits.
    pub input: bool,
}

impl Until {
    /// Every one of `awaited` settled.
    #[must_use]
    pub fn all(awaited: BTreeSet<AgentPath>) -> Self {
        let need = u32::try_from(awaited.len()).unwrap_or(u32::MAX);
        Self::quorum(awaited, need)
    }

    /// The first of `awaited` settled.
    #[must_use]
    pub fn any(awaited: BTreeSet<AgentPath>) -> Self {
        Self::quorum(awaited, 1)
    }

    /// `need` of `awaited` settled.
    #[must_use]
    pub fn quorum(awaited: BTreeSet<AgentPath>, need: u32) -> Self {
        Self {
            awaited,
            need,
            timeout_ms: None,
            input: false,
        }
    }

    /// Nothing but the timeout.
    #[must_use]
    pub fn sleep(timeout_ms: u64) -> Self {
        Self {
            awaited: BTreeSet::new(),
            need: 1,
            timeout_ms: Some(timeout_ms),
            input: false,
        }
    }

    /// Wait for inbox input, with an optional actor alarm.
    #[must_use]
    pub fn input(timeout_ms: Option<u64>) -> Self {
        Self {
            awaited: BTreeSet::new(),
            need: 1,
            timeout_ms,
            input: true,
        }
    }

    /// The same condition, resuming after `timeout_ms` at the latest.
    #[must_use]
    pub fn within(self, timeout_ms: Option<u64>) -> Self {
        Self { timeout_ms, ..self }
    }
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
    pub(super) async fn persist_children<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        provider: std::sync::Arc<P>,
    ) -> Result<(), Error> {
        self.step.state.open_children.bind(provider.clone());
        self.step.state.archive_pending.bind(provider.clone());
        self.step.state.topology_refusals.bind(provider.clone());
        self.step.state.spawned_children.bind(provider.clone());
        self.step.state.awaiting.bind(provider);
        self.step.state.open_children.persist().await?;
        self.step.state.archive_pending.persist().await?;
        self.step.state.topology_refusals.persist().await?;
        self.step.state.spawned_children.persist().await?;
        self.step.state.awaiting.persist().await
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

/// Owns the logical transition before the host chooses its publication encoding.
struct StepDraft {
    state: AgentState,
    outs: Vec<Out>,
    previous: (Phase, u64),
    cursor: u64,
    child_wakes: Option<ChildWakes>,
    child_settlement: Option<ChildSettlement>,
}

impl StepDraft {
    fn finish(self) -> Result<Step, Error> {
        finalize_step(
            self.state,
            self.outs,
            &self.previous,
            self.cursor,
            self.child_wakes,
            self.child_settlement,
        )
    }
}

impl PreparedStep {
    fn new(draft: StepDraft) -> Result<Self, Error> {
        let StepDraft {
            state,
            outs,
            previous,
            cursor,
            child_wakes,
            child_settlement,
        } = draft;
        let tails = (state.conversation_tail, state.status_tail);
        let completion = (
            state.awaited,
            state.checkpoint,
            state.roster,
            outs.len(),
            state.pending_notices.clone(),
        );
        let mut step = finalize_step(
            state,
            outs,
            &previous,
            cursor,
            child_wakes,
            child_settlement,
        )?;
        let mut bytes = checkpoint::encode(&step.state)?;
        // Check the exact final tails, then retain these same bytes for commit.
        // Only a state that must shed its growing input is encoded again.
        if bytes.len() > MAX_CHECKPOINT_BYTES {
            let child_wakes = step
                .status
                .as_mut()
                .map(|status| std::mem::take(&mut status.child_wakes))
                .unwrap_or_default();
            let child_settlement = step
                .status
                .as_mut()
                .and_then(|status| status.child_settlement.take());
            let Step {
                mut state,
                mut outs,
                ..
            } = step;
            state.conversation_tail = tails.0;
            state.status_tail = tails.1;
            state.awaited = completion.0;
            state.checkpoint = completion.1;
            state.roster = completion.2;
            state.pending_notices = completion.4;
            outs.truncate(completion.3);
            shed(&mut state, &mut outs, OVERSIZED_STATE);
            step = finalize_step(
                state,
                outs,
                &previous,
                cursor,
                child_wakes,
                child_settlement,
            )?;
            bytes = checkpoint::encode(&step.state)?;
        }
        Ok(PreparedStep {
            step,
            checkpoint: checkpoint::from_encoded(bytes)?,
        })
    }
}

/// Runs one step for `wake`. Every control decision is made by [`kernel`].
///
/// `state` is `None` only for the first delivery, which must be `ForkAdopted`.
/// `now_unix_ms` is the delivery's clock: a wait's timeout counts from it.
/// Returns logical effects; checkpoint serialization belongs to the publishing host.
pub async fn step<T: TurnDriver + ?Sized>(
    driver: &T,
    path: &AgentPath,
    state: Option<AgentState>,
    (cursor, now_unix_ms): (u64, u64),
    wake: Wake,
    cancellation: acyclic_actors::guest::CancellationToken,
) -> Result<Step, Error> {
    draft_step(
        driver,
        path,
        state,
        (cursor, now_unix_ms, None),
        wake,
        cancellation,
    )
    .await?
    .finish()
}

pub(super) async fn prepare_step<T: TurnDriver + ?Sized>(
    driver: &T,
    path: &AgentPath,
    state: Option<AgentState>,
    (cursor, now_unix_ms, settled_at_micros): (u64, u64, Option<u64>),
    wake: Wake,
    cancellation: acyclic_actors::guest::CancellationToken,
) -> Result<PreparedStep, Error> {
    PreparedStep::new(
        draft_step(
            driver,
            path,
            state,
            (cursor, now_unix_ms, settled_at_micros),
            wake,
            cancellation,
        )
        .await?,
    )
}

async fn draft_step<T: TurnDriver + ?Sized>(
    driver: &T,
    path: &AgentPath,
    state: Option<AgentState>,
    (cursor, now_unix_ms, settled_at_micros): (u64, u64, Option<u64>),
    wake: Wake,
    cancellation: acyclic_actors::guest::CancellationToken,
) -> Result<StepDraft, Error> {
    let mut outs = Vec::new();
    let alarm_wake = matches!(wake, Wake::Alarm);
    // Consuming a non-waking stream event never advances the model driver,
    // including when another turn is currently paused or executing.
    let workspace_change = matches!(
        &wake,
        Wake::Inbox {
            record: InboxRecord::WorkspaceChanged(_),
            ..
        }
    );
    let quiet_notice = workspace_change
        || matches!(
            &wake,
            Wake::Inbox {
                record: InboxRecord::Notice { notice, .. },
                ..
            } if !notice.wakes()
        );
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
    let previous = state.as_ref().map_or((Phase::Idle, 0), |state| {
        (published(state), state.dropped_before)
    });
    let mut child_settlement = None;
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
            AgentState::adopted(path.clone(), &adopted)?
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
                brief: spawn.brief.map(|brief| *brief),
                profile: Some(spawn.profile),
                volumes: Vec::new(),
                history: spawn.history,
            });
            apply_spawns(&mut state, &mut outs, 0).await?;
            state
        }
        (Some(state), Wake::Inbox { sequence, record }) => {
            let (state, fact) = admit(state, sequence, record, settled_at_micros).await?;
            child_settlement = fact;
            state
        }
    };
    state.validate_parent()?;
    let mut child_wakes = None;
    let applied = outs.len();
    if state.closed {
        state.child_settlements = None;
        cascade(&mut state, &mut outs).await?;
    } else if !spawn_wake && !quiet_notice {
        run(
            driver,
            &mut state,
            (cursor, now_unix_ms, child_settlement.is_some()),
            alarm_wake,
            &mut outs,
            &mut child_wakes,
            cancellation,
        )
        .await?;
    }
    apply_spawns(&mut state, &mut outs, applied).await?;
    Ok(StepDraft {
        state,
        outs,
        previous,
        cursor,
        child_wakes,
        child_settlement,
    })
}

/// A refused publication discards its speculative effects and settles from the
/// original checkpoint. The normal shedding path reports the child exactly once.
pub(super) async fn failed_step(
    path: &AgentPath,
    state: Option<AgentState>,
    wake: Wake,
    cursor: u64,
    failure: &str,
) -> Result<PreparedStep, Error> {
    let mut state = match (state, wake) {
        // The archive input closes this agent even when its first cascade
        // publication was refused. Retain that control decision, not any of
        // the refused publication's speculative child writes.
        (
            Some(state),
            Wake::Inbox {
                sequence,
                record: record @ InboxRecord::Archive(_),
            },
        ) => admit(state, sequence, record, None).await?.0,
        (Some(state), _) => state,
        (
            None,
            Wake::Inbox {
                record: InboxRecord::ForkAdopted(adopted),
                ..
            },
        ) => AgentState::adopted(path.clone(), &adopted)?,
        _ => return Err(Error::Record("first agent input is not ForkAdopted")),
    };
    let previous = (published(&state), state.dropped_before);
    let mut outs = Vec::new();
    shed(&mut state, &mut outs, failure);
    let step = finalize_step(state, outs, &previous, cursor, None, None)?;
    let checkpoint = checkpoint::from_encoded(checkpoint::encode(&step.state)?)?;
    Ok(PreparedStep { step, checkpoint })
}

// A parked turn can ingest messages/notices at its next model boundary. Other
// queued inputs retain their own turn and cannot release an input wait.
fn wake_input(state: &AgentState) -> bool {
    if control(state).input {
        // Starting input belongs to the retained turn even when a custom
        // driver has no stock model boundary or steering journal.
        let through = state.steered_through.max(state.dropped_before).max(
            state
                .turn
                .as_ref()
                .map_or(0, |turn| turn.inbox.saturating_add(1)),
        );
        state.steering_tail > through
    } else {
        !state.queued.is_empty()
    }
}

fn finalize_step(
    mut state: AgentState,
    mut outs: Vec<Out>,
    previous: &(Phase, u64),
    cursor: u64,
    child_wakes: Option<ChildWakes>,
    child_settlement: Option<ChildSettlement>,
) -> Result<Step, Error> {
    // Retire admitted boundary input before choosing the next actor alarm.
    for out in &outs {
        if let Out::Steered(through) = out {
            state.steered_through = state.steered_through.max(*through);
            state
                .queued
                .retain(|input| input.inbox >= *through || !input.steering);
        }
    }
    let queued = wake_input(&state) || (!control(&state).input && state.overflow.is_some());
    let approval_ready = approval_position(&state).is_some();
    let pending = state.archive_pending.len();
    let before = control(&state);
    let alarm = kernel::alarm(before, queued, approval_ready, pending);
    #[cfg(feature = "test-support")]
    control_trace::record(
        &state,
        Decision::Alarm(before, queued, approval_ready, pending, alarm),
    );
    let alarm = match alarm {
        Some(deadline) => Alarm::At(deadline),
        None => Alarm::Clear,
    };
    let appended = outs
        .iter()
        .filter(|out| matches!(out, Out::Conversation(_)))
        .count() as u64;
    state.committed_actions |= outs.iter().any(|out| {
        matches!(
            out,
            Out::Spawn { .. }
                | Out::Archive { .. }
                | Out::Send {
                    record: InboxRecord::Message(_),
                    ..
                }
        )
    });
    let phase = published(&state);
    // An awaited child answers its parent once it settles, in the commit
    // that publishes it so: the parent's wait wakes on exactly that record.
    if state.awaited
        && matches!(
            phase,
            Phase::Idle | Phase::WaitingUser { .. } | Phase::Done { .. }
        )
    {
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
            Out::Notices(selection) => selection.consume(&mut state)?,
            Out::Send { .. }
            | Out::PendingSpawns(_)
            | Out::Steered(_)
            | Out::Archive { .. }
            | Out::Spawn { .. }
            | Out::GoalCompleted { .. } => {}
        }
    }
    // Frozen initial notices remain valid while a turn is parked. At its
    // settlement, consume that exact selection first, then retire any later
    // notices already admitted at model boundaries.
    if state.turn.is_none() {
        for out in &outs {
            if let Out::Steered(through) = out
                && let Some(range) = &mut state.pending_notices
            {
                range.start = range.start.max(*through);
                if range.start >= range.end {
                    state.pending_notices = None;
                }
            }
        }
    }
    state.conversation_tail = state
        .conversation_tail
        .checked_add(appended)
        .ok_or(Error::Record("agent conversation tail overflow"))?;
    // Settled entries and dropped inputs wake observers even when the actor's
    // phase stays unchanged. An opening snapshot may have reported Running
    // from accepted input while the committed checkpoint still said Idle.
    let status = (phase != previous.0
        || state.dropped_before != previous.1
        || child_wakes.is_some()
        || child_settlement.is_some()
        || appended != 0)
        .then_some(StatusRecord {
            child_settlement,
            child_wakes,
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

/// The phase a status publishes: retained input runs next, so `Idle` means
/// that no admitted input remains pending in this checkpoint.
pub(super) fn published(state: &AgentState) -> Phase {
    if matches!(state.phase, Phase::Idle | Phase::WaitingUser { .. })
        && (!state.queued.is_empty() || state.overflow.is_some())
    {
        Phase::Running
    } else {
        state.phase.clone()
    }
}

fn control(state: &AgentState) -> Control {
    Control {
        mode: match &state.phase {
            Phase::Idle | Phase::WaitingUser { .. } => Mode::Idle,
            Phase::Running => Mode::Running,
            Phase::WaitingApproval { .. } | Phase::WaitingHost { .. } => Mode::Approval,
            Phase::Waiting {
                deadline_unix_ms, ..
            } => Mode::Await(*deadline_unix_ms),
            Phase::Done { .. } => Mode::Done,
        },
        input: matches!(state.phase, Phase::Waiting { input: true, .. }),
        closed: state.closed,
        archive_refused: state.archive_refused,
        reported: state.reported,
        turn: state.turn.is_some(),
    }
}

/// Folds one inbox record into the state.
async fn admit(
    mut state: AgentState,
    sequence: u64,
    record: InboxRecord,
    settled_at_micros: Option<u64>,
) -> Result<(AgentState, Option<ChildSettlement>), Error> {
    let mut fact = None;
    if state.already_steered(sequence, &record) {
        return Ok((state, None));
    }
    let kind = match &record {
        InboxRecord::ForkAdopted(_) => return Err(Error::Record("agent adopted twice")),
        InboxRecord::Cancel(_) => Admit::Cancel,
        InboxRecord::Archive(_) => Admit::Archive,
        _ => Admit::Input,
    };
    let before = control(&state);
    let admitted = kernel::admit(before, kind);
    #[cfg(feature = "test-support")]
    control_trace::record(&state, Decision::Admit(before, kind, admitted));
    if admitted.clear_queue {
        state.dropped_before = state.dropped_before.max(sequence);
        state.drop_queue();
        state.pending_notices = None;
    }
    if !admitted.control.turn {
        state.child_settlements = None;
        state.turn = None;
        state.reserved_spawns = 0;
        state.awaiting.clear();
        state.need = 0;
    }
    if admitted.control.mode == Mode::Idle {
        state.phase = Phase::Idle;
    }
    if kind == Admit::Archive && !state.closed {
        state.archive_pending.clone_from(&state.open_children);
    }
    state.closed = admitted.control.closed;
    if admitted.enqueue && matches!(&record, InboxRecord::Notice { notice, .. } if !notice.wakes())
    {
        let end = sequence
            .checked_add(1)
            .ok_or(Error::Record("notice inbox cursor overflow"))?;
        match &mut state.pending_notices {
            Some(range) if range.end <= sequence => range.end = end,
            Some(_) => return Err(Error::Record("notice inbox cursor moved backwards")),
            None => state.pending_notices = Some(sequence..end),
        }
    }
    if let InboxRecord::ChildDone(done) = &record {
        if done.child.is_child_of(&state.path) {
            state.open_children.remove(&done.child).await?;
            state.archive_pending.remove(&done.child).await?;
        }
    }
    match &record {
        InboxRecord::WorkspaceChanged(change) if !state.closed => {
            super::workspaces::WorkspaceTable::apply_entries(&mut state.volumes, change)?;
        }
        InboxRecord::Awaited => state.awaited = true,
        InboxRecord::ChildDone(ChildDone { child, .. })
        | InboxRecord::ChildSettled(ChildSettled { child }) => {
            if state.awaiting.contains(child).await?
                && let Some(settled_at) = settled_at_micros
            {
                state.child_settlements.get_or_insert(state.status_tail);
                fact = Some(ChildSettlement {
                    child: child.id(),
                    settled_at_micros: settled_at,
                });
            }
            settle_awaited(&mut state, child).await?;
        }
        _ => {}
    }
    if let Some(input) = input(sequence, record).filter(|_| admitted.enqueue) {
        if input.steering {
            state.steering_tail = state.steering_tail.max(sequence.saturating_add(1));
        }
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
    Ok((state, fact))
}

/// An awaited `child` settled: it counts once toward the [`Until`] the turn
/// waits on in [`Phase::Waiting`], which resumes, through
/// [`kernel::satisfy`], when the last needed one has. One set removal.
async fn settle_awaited(state: &mut AgentState, child: &AgentPath) -> Result<(), Error> {
    if !child.is_child_of(&state.path) || !state.awaiting.remove(child).await? {
        return Ok(());
    }
    state.need = state.need.saturating_sub(1);
    if state.need == 0 {
        let before = control(state);
        let next = kernel::satisfy(before);
        #[cfg(feature = "test-support")]
        control_trace::record(state, Decision::Satisfy(before, next));
        if next.mode == Mode::Running {
            state.awaiting.clear();
            state.phase = Phase::Running;
        }
    }
    Ok(())
}

/// Whether a driver's effect fits the record that carries it.
fn admissible(out: &Out) -> bool {
    match out {
        Out::Steered(_) | Out::PendingSpawns(_) => true,
        Out::Conversation(record) => record.len() <= acyclic_stream::MAX_RECORD_BYTES,
        Out::Send { record, .. } => {
            !matches!(
                record,
                InboxRecord::ForkAdopted(_)
                    | InboxRecord::Archive(_)
                    | InboxRecord::WorkspaceChanged(_)
            ) && super::AgentRecord::encode(record).is_ok()
        }
        Out::Archive { .. }
        | Out::Checkpoint
        | Out::Roster(_)
        | Out::Notices(_)
        | Out::GoalCompleted { .. } => true,
        Out::Spawn { volumes, .. } => super::records::check_volumes(volumes.iter()).is_ok(),
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
    (cursor, now_unix_ms, returning_completion): (u64, u64, bool),
    alarm_wake: bool,
    outs: &mut Vec<Out>,
    child_wakes: &mut Option<ChildWakes>,
    cancellation: acyclic_actors::guest::CancellationToken,
) -> Result<(), Error> {
    let approval = approval_position(state);
    let queued = wake_input(state);
    let approval_ready = approval.is_some();
    let before = control(state);
    let picked = kernel::pick(before, alarm_wake, queued, approval_ready);
    #[cfg(feature = "test-support")]
    control_trace::record(
        state,
        Decision::Pick(before, alarm_wake, queued, approval_ready, picked),
    );
    let input = match picked {
        Pick::Nothing => return Ok(()),
        Pick::Resume => None,
        Pick::Front => state.queued.pop_front(),
        Pick::Approval => approval.and_then(|position| state.queued.remove(position)),
    };
    // This is the actual wait return, before the driver performs any work.
    // Completion times came with the original deliveries; this adds no read,
    // commit or telemetry channel. Partial facts have no return clock until here.
    if picked == Pick::Resume {
        let until = state
            .status_tail
            .checked_add(u64::from(returning_completion))
            .ok_or(Error::Record("status tail overflows"))?;
        *child_wakes = state.child_settlements.take().map(|from| ChildWakes {
            from,
            until,
            woke_at_micros: now_unix_ms.saturating_mul(1_000),
        });
    }
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
    // returning one no record can carry fails, applying none of them, so
    // the step's commit is always buildable.
    if !turn.outs.iter().all(admissible)
        || matches!(&turn.wait, Wait::Host(host) if super::records::check_text(host).is_err())
        || matches!(&turn.wait, Wait::User(path) if super::records::check_ui_path(path).is_err())
        || matches!(&turn.wait, Wait::Approval { binding: Some(binding), .. } if binding.validate().is_err())
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
    state.reserved_spawns = turn
        .outs
        .iter()
        .rev()
        .find_map(|out| match out {
            Out::PendingSpawns(count) => Some(*count),
            _ => None,
        })
        .unwrap_or(0);
    let spawned = outs.len();
    outs.extend(turn.outs);
    let wait = match turn.wait {
        Wait::Done(outcome) => Wait::Done(bounded(outcome)),
        wait => wait,
    };
    let mut awaiting = (super::Children::new(), 0);
    let (wait, approval, outcome) = match wait {
        Wait::Continue => (kernel::Wait::Continue, None, None),
        Wait::Approval { id, binding } => (
            kernel::Wait::Approval,
            Some(Phase::WaitingApproval { id, binding }),
            None,
        ),
        Wait::Host(host) => (
            kernel::Wait::Approval,
            Some(Phase::WaitingHost { host }),
            None,
        ),
        Wait::Until(until) => {
            // Only an open child can still settle; one already done has and
            // counts now. This step's own spawns are open children too, so
            // apply them first: a wait on a child spawned in the same step
            // asks it. Applying is idempotent for the step's later pass.
            apply_spawns(state, outs, spawned).await?;
            let mut open = Vec::new();
            for child in &until.awaited {
                if child.is_child_of(&state.path) && state.open_children.contains(child).await? {
                    open.push(child);
                }
            }
            let settled = u32::try_from(until.awaited.len() - open.len()).unwrap_or(u32::MAX);
            let need = until.need.saturating_sub(settled);
            let deadline = until
                .timeout_ms
                .map(|timeout| now_unix_ms.saturating_add(timeout));
            let sleep = until.awaited.is_empty();
            let reachable = open.len();
            let input = until.input;
            let wait = kernel::until(need, reachable, sleep, deadline, input);
            #[cfg(feature = "test-support")]
            control_trace::record(
                state,
                Decision::Until(need, reachable, sleep, deadline, input, wait),
            );
            if matches!(wait, kernel::Wait::Await { .. }) {
                outs.extend(open.iter().map(|child| Out::Send {
                    to: (**child).clone(),
                    record: InboxRecord::Awaited,
                }));
                for child in open {
                    awaiting.0.insert(child).await?;
                }
                awaiting.1 = need;
            }
            (wait, None, None)
        }
        Wait::Input => (kernel::Wait::Input, None, None),
        Wait::User(path) => (kernel::Wait::Input, Some(Phase::WaitingUser { path }), None),
        Wait::Done(outcome) => (kernel::Wait::Done, None, Some(outcome)),
    };
    let before = control(state);
    let (next, send) = kernel::settle(before, wait);
    #[cfg(feature = "test-support")]
    control_trace::record(state, Decision::Settle(before, wait, (next, send)));
    state.turn = started.filter(|_| next.turn).map(|inbox| Progress {
        inbox,
        checkpoint: turn.checkpoint.unwrap_or_default(),
    });
    state.reported = next.reported;
    (state.awaiting, state.need) = if matches!(next.mode, Mode::Await(_)) {
        awaiting
    } else {
        (super::Children::new(), 0)
    };
    state.phase = match (next.mode, approval, outcome) {
        (Mode::Running, ..) => Phase::Running,
        (Mode::Approval, Some(waiting), _) => waiting,
        (Mode::Await(deadline_unix_ms), ..) => Phase::Waiting {
            deadline_unix_ms,
            input: next.input,
        },
        (Mode::Idle, Some(waiting @ Phase::WaitingUser { .. }), _) => waiting,
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
/// construction (see [`checkpoint`]). A refused archive cascade settles as
/// a visible failure, retaining its pending children with a terminal refusal
/// so it cannot rearm itself or lose the unfinished archive request.
/// A settled agent with no pending archive work keeps its bounded outcome.
fn shed(state: &mut AgentState, outs: &mut Vec<Out>, failure: &str) {
    state.drop_queue();
    state.turn = None;
    state.reserved_spawns = 0;
    if matches!(state.phase, Phase::Done { .. }) && state.archive_pending.is_empty() {
        return;
    }
    state.archive_refused |= !state.archive_pending.is_empty();
    let outcome = bounded(AgentOutcome::Failed {
        message: failure.into(),
    });
    let before = control(state);
    let (next, send) = kernel::report(before);
    #[cfg(feature = "test-support")]
    control_trace::record(state, Decision::Report(before, (next, send)));
    state.reported = next.reported;
    if send {
        report(state, outcome.clone(), outs);
    }
    state.phase = Phase::Done { outcome };
}

/// Archive cascade: bounded fan-out to open children, then one report upward.
async fn cascade(state: &mut AgentState, outs: &mut Vec<Out>) -> Result<(), Error> {
    let pending = state.archive_pending.len();
    let before = control(state);
    let step = kernel::cascade(before, pending);
    #[cfg(feature = "test-support")]
    control_trace::record(state, Decision::Cascade(before, pending, step));
    let id = IntentId::try_from(format!("cascade:{}", state.path.id()))?;
    for _ in 0..step.archive {
        let Some(child_id) = state.archive_pending.first().await? else {
            break;
        };
        let child = state.path.child(&child_id)?;
        state.archive_pending.remove(&child).await?;
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
            Ok(child)
                if !state.closed
                    && (state.spawned_children.contains(&child).await?
                        || state
                            .profile
                            .as_ref()
                            .and_then(|profile| profile.topology.as_ref())
                            .and_then(|topology| topology.spawn_limit(state.parent.is_none()))
                            .is_none_or(|limit| {
                                (state.spawned_children.len() as u64)
                                    .saturating_add(state.reserved_spawns)
                                    < limit
                            })) =>
            {
                state.spawned_children.insert(&child).await?;
                state.open_children.insert(&child).await?;
                true
            }
            Ok(child) => {
                let opened = state.open_children.contains(&child).await?;
                if !opened && !state.closed {
                    state.topology_refusals.insert(&child).await?;
                }
                opened
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
