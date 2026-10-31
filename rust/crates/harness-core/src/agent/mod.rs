//! The native agent model: an agent is an actor, a stream subtree and its volumes.
//!
//! The subtree's root stream is the agent's inbox and its only input. Spawn is one
//! commit forking the parent's subtree; send and archive are one conditional commit
//! each on the recipient's inbox; an agent wakes on inbox append or its one alarm
//! and commits each step (checkpoint, conversation, status and every outgoing
//! effect) as one Streams commit. There is no coordinator, polling or timer.

mod checkpoint;
mod commit;
#[cfg(feature = "filesystem")]
pub(crate) use commit::sender_text;
mod driver;
mod effects;
pub use effects::{
    TurnEffect, TurnEffects, call_key, is_call_of, operation_key, outs as effect_outs,
};
mod goal;
pub use goal::{GOAL_INTENT, TurnGoal};
mod handler;
mod host;
mod input;
pub mod kernel;
mod layout;
mod lifecycle;
pub use lifecycle::{AgentComposer, AgentLifecycle, CLOSE};
mod lower;
mod machine;
pub(crate) mod records;
mod reply;
mod settled;
pub use settled::{TurnChildren, answer, settled};
mod slots;
mod targets;
pub use targets::{
    MAX_OWNER_BYTES, MAX_TARGET_NAME_BYTES, PathStyle, Refused, Roster, TargetClass, TargetRecord,
    TargetRefusal, TargetState, USER_PREFIX,
};
mod volumes;
pub use volumes::SpawnVolumes;

pub use checkpoint::MAX_CHECKPOINT_BYTES;
pub(crate) use checkpoint::checkpoint_state;
pub use commit::{
    Spawn, Submitted, archive, create_root, inbox_tail, intent_key, send, spawn, submit,
};
pub use driver::{
    COMPACT_INSTRUCTION, ConversationEntry, ExecutorDriver, OpenTurn, TurnHost, TurnOutcome,
    TurnOutputs, approval_intent, expand_attachments, model_input, roster_message, turn_id,
    turn_roster, turn_segment,
};
pub use handler::{AgentHandler, INBOX};
pub use host::{AgentHost, Delivered, actor_store};
pub use kernel::{CONTINUE_NOW, MAX_FANOUT};
pub use layout::{
    ACTORS, AGENTS, AgentId, AgentPath, MAX_DEPTH, NOT_INHERITED, STATUSES, TARGET_STATUS, VOLUMES,
};
pub use lower::{delivery_key, lower};
pub use machine::{
    AgentState, Alarm, INADMISSIBLE_EFFECT, MAX_OPEN_CHILDREN, MAX_OUTCOME_BYTES, MAX_QUEUED,
    MAX_QUEUED_BYTES, OVERSIZED_OUTCOME, OVERSIZED_STATE, Out, Progress, Queued, SpawnOutcome,
    SpawnRefusal, Step, TurnDriver, TurnFuture, TurnInput, TurnStep, Wait, Wake, requested_child,
    step,
};
pub use records::{
    AgentOutcome, AgentRecord, Approval, ApprovalRequested, ChildDone, ChildSettled, Closed,
    ContextAction, ContextRequest, Control, ForkAdopted, Hook, InboxRecord, IntentId,
    MAX_INTENT_BYTES, MAX_MESSAGE_BYTES, MAX_RECORD_BYTES, MAX_VOLUMES, Message, Phase,
    RECORD_VERSION, Sender, SpawnRequest, StatusRecord, UnsupportedRecordVersion, VolumeBinding,
    decode_record,
};
pub use reply::{Reply, reply};

/// Agent model failures.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A path is not a canonical agent path.
    #[error("invalid agent path: {0}")]
    Path(&'static str),
    /// A record violates its bounds or the step protocol.
    #[error("invalid agent record: {0}")]
    Record(&'static str),
    /// The recipient agent was never created.
    #[error("no such agent")]
    NoSuchAgent,
    /// The parent's step applied the spawn and created no child.
    #[error("spawn refused: {0:?}")]
    SpawnRefused(SpawnRefusal),
    /// A committed record contradicts the request's identity.
    #[error("agent request conflicts: {0}")]
    Conflict(&'static str),
    /// Completion is uncertain; retry this exact request until it resolves.
    #[error("agent commit outcome is unknown")]
    Unknown(Box<acyclic_stream::CommitRequest>),
    /// Streams failure.
    #[error(transparent)]
    Stream(acyclic_stream::StreamError),
    /// Actor runtime failure.
    #[error(transparent)]
    Actor(Box<acyclic_actors::local::Error>),
    /// Turn driver failure.
    #[error(transparent)]
    Turn(Box<crate::Error>),
    /// A persisted record of another format version, refused whole.
    #[error(transparent)]
    UnsupportedRecordVersion(UnsupportedRecordVersion),
}

impl Error {
    /// Whether the failed work may yet succeed unchanged: a commit whose
    /// outcome is unknown, or a store that left its outcome undecided however
    /// that failure is wrapped. Only these retry; every other turn failure
    /// settles the agent as failed.
    #[must_use]
    pub fn uncertain(&self) -> bool {
        match self {
            Self::Unknown(_) => true,
            Self::Stream(error) => error.uncertain(),
            Self::Turn(error) => error.uncertain(),
            Self::Actor(error) => match &**error {
                acyclic_actors::local::Error::Unknown { .. } => true,
                acyclic_actors::local::Error::Stream(error) => error.uncertain(),
                _ => false,
            },
            Self::Path(_)
            | Self::Record(_)
            | Self::UnsupportedRecordVersion(_)
            | Self::NoSuchAgent
            | Self::SpawnRefused(_)
            | Self::Conflict(_) => false,
        }
    }

    /// Stable public classification shared by native and WASM client adapters.
    /// The error value itself retains its typed cause and uncertain request.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Path(_) => "invalid_agent_path",
            Self::Record(_) => "invalid_agent_record",
            Self::UnsupportedRecordVersion(_) => "unsupported_record_version",
            Self::NoSuchAgent => "no_such_agent",
            Self::SpawnRefused(_) => "spawn_refused",
            Self::Conflict(_) => "agent_conflict",
            Self::Unknown(_) => "agent_unknown",
            Self::Stream(cause) => cause.code(),
            Self::Actor(_) => "agent_runtime_error",
            Self::Turn(cause) if cause.code() == crate::wire::ErrorCode::Conflict => {
                "agent_conflict"
            }
            Self::Turn(_) => "agent_turn_error",
        }
    }

    /// Borrow the original commit while retaining ownership of the error.
    /// Presentation may inspect it, but must not reconstruct it from prose.
    #[must_use]
    pub fn unknown_request(&self) -> Option<&acyclic_stream::CommitRequest> {
        match self {
            Self::Unknown(request) => Some(request),
            _ => None,
        }
    }
}

#[cfg(test)]
mod uncertain_tests {
    use super::Error;
    use acyclic_stream::StreamError;

    #[test]
    fn store_uncertainty_survives_every_wrapper() {
        for undecided in [StreamError::Unavailable, StreamError::DeadlineElapsed] {
            assert!(Error::Stream(undecided.clone()).uncertain());
            assert!(Error::Turn(Box::new(crate::Error::from(undecided.clone()))).uncertain());
            assert!(
                Error::Actor(Box::new(acyclic_actors::local::Error::Stream(
                    undecided.clone()
                )))
                .uncertain()
            );
        }
        for decided in [StreamError::NotFound, StreamError::InvalidArgument] {
            assert!(!Error::Stream(decided.clone()).uncertain());
            assert!(
                !Error::Actor(Box::new(acyclic_actors::local::Error::Stream(
                    decided.clone()
                )))
                .uncertain()
            );
        }
        // A harness failure is classified by its wire class, whichever
        // variant carries it: a storage failure's outcome is undecided.
        assert!(Error::Turn(Box::new(crate::Error::Storage("unavailable".into()))).uncertain());
        assert!(!Error::Turn(Box::new(crate::Error::Invalid("definitive".into()))).uncertain());
    }
}
