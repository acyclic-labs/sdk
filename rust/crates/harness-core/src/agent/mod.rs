//! The native agent model: an agent is an actor, a stream subtree and its volumes.
//!
//! The subtree's root stream is the agent's inbox and its only input. Spawn is one
//! commit forking the parent's subtree; send and archive are one conditional commit
//! each on the recipient's inbox; an agent wakes on inbox append or its one alarm
//! and commits each step (checkpoint, conversation, status and every outgoing
//! effect) as one Streams commit. There is no coordinator, polling or timer.

mod commit;
mod driver;
mod handler;
mod host;
pub mod kernel;
mod machine;
mod layout;
mod lower;
mod records;
mod volumes;
pub use volumes::SpawnVolumes;

pub use commit::{Spawn, Submitted, archive, create_root, inbox_tail, intent_key, send, spawn, submit};
pub use machine::{
    Alarm, AgentState, MAX_OPEN_CHILDREN, MAX_QUEUED, Out, Step,
    TurnDriver, TurnFuture, TurnInput, TurnStep, Wait, Wake, step,
};
pub use handler::{AgentHandler, INBOX};
pub use host::{AgentHost, Delivered};
pub use driver::{
    ConversationEntry, ExecutorDriver, OpenTurn, TurnHost, TurnStop, approval_intent, model_input,
    turn_id,
};
pub use kernel::{CONTINUE_NOW, MAX_FANOUT};
pub use layout::{ACTORS, AGENTS, AgentId, AgentPath, MAX_DEPTH, NOT_INHERITED, STATUSES, VOLUMES};
pub use lower::{delivery_key, lower};
pub use records::{
    AgentOutcome, AgentRecord, Approval, ApprovalRequested, ChildDone, Closed, Control,
    ForkAdopted, Hook, InboxRecord, IntentId, MAX_INTENT_BYTES, MAX_MESSAGE_BYTES,
    MAX_RECORD_BYTES, MAX_VOLUMES, Message, Phase, Sender, StatusRecord, VolumeBinding,
};

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
}
