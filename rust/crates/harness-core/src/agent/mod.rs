//! The native agent model: an agent is an actor, a stream subtree and its volumes.
//!
//! The subtree's root stream is the agent's inbox and its only input. Spawn is one
//! commit forking the parent's subtree; send and archive are one conditional commit
//! each on the recipient's inbox; an agent wakes on inbox append or its one alarm
//! and commits each step (checkpoint, conversation, status and every outgoing
//! effect) as one Streams commit. There is no coordinator, polling or timer.

mod announce;
pub(crate) use announce::InstructionManifest;
pub use announce::{
    AnnouncedDocument, AnnouncedInstructions, Announcement, AnnouncementDocument, AnnouncementKind,
    WorkspaceAnnouncement, WorkspaceExposure, WorkspaceInstructions, WorkspaceUpdate,
    announcement_messages, discover_workspace_instructions,
};
pub(crate) mod checkpoint;
mod children;
pub use children::Children;
mod continuation;
pub use continuation::{Continuation, ContinuationOverride, ContinuationRequest, ForcedCall};
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod codec_fuzz;
mod commit;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod control_trace;
pub(crate) use commit::sender_text;
mod diagnostic;
pub use diagnostic::{Diagnostic, DiagnosticRange, DiagnosticRead};
mod driver;
mod effects;
pub use effects::{
    TurnEffect, TurnEffects, call_key, is_call_of, operation_key, outs as effect_outs,
};
mod goal;
pub use goal::{GOAL_INTENT, TurnGoal};
mod handler;
mod history;
mod host;
pub use history::{
    ContextCut, HistoryCheckpoint, HistorySelection, HistorySummary, HistoryVolume, RestoreMode,
    SavedVolume,
};
mod input;
mod input_preview;
pub(crate) use input::input as turn_input;
pub use input_preview::{InputPreview, InputPreviewStep, InputSelection};
pub mod kernel;
mod layout;
mod lifecycle;
pub use lifecycle::{AgentComposer, AgentLifecycle, CLOSE};
mod lower;
mod machine;
mod names;
mod notices;
mod presence;
pub use names::NameIndex;
pub use notices::{ApprovalControl, ApprovalSelection, NoticeSelection};
pub use presence::{SessionPhase, SessionProfile, SessionPublication, SessionRegistry, SessionRow};
pub(crate) mod records;
mod release;
mod reply;
mod sent;
pub(crate) use reply::entry as conversation_entry;
pub use sent::{
    ReplyWatch, SentDelivery, SentMessage, SentReply, SentReplyStatus, sent, sent_reply,
};
mod settled;
#[cfg(test)]
mod ui_tests;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use settled::SELECTED_IDENTITIES;
pub(crate) use settled::snapshot as observed_settlement;
pub use settled::{Settlement, TurnChildren, answer, settled};
pub(crate) use settled::{named_child, select_children};
pub(crate) mod spawn_names;
pub use spawn_names::{SpawnResolution, resolve_spawn_name};
mod cancellation;
mod messaging;
mod watch;
pub use watch::Watch;
mod steering;
pub use messaging::{
    MessageDelivery, PeerAdmission, PeerMembership, PeerMembershipHead, SEND_PEER,
    initial_peer_membership, peer_send_allowed, session_peer_admission, session_peer_allowed,
    sibling_send_allowed,
};
pub use steering::{
    BoundaryCut, BoundaryEnd, BoundarySelection, ModelBoundary, SteeringSnapshot,
    SteeringTranscript, select_boundary,
};
mod targets;
pub use targets::{
    DeviceKind, MAX_OWNER_BYTES, MAX_TARGET_NAME_BYTES, PathStyle, Refused, Roster, TargetClass,
    TargetRecord, TargetRefusal, TargetState, USER_PREFIX,
};
mod word;
pub(crate) mod workspace_checkpoint;
mod workspaces;
pub use workspaces::{
    EntryId, WorkspaceChanged, WorkspaceEntry, WorkspaceFilter, WorkspaceInherit, WorkspaceKind,
    WorkspaceMode, WorkspaceOp, WorkspaceRole, WorkspaceSource, WorkspaceTable,
};
mod volumes;
pub use volumes::SpawnVolumes;

pub use checkpoint::MAX_CHECKPOINT_BYTES;
pub(crate) use checkpoint::{checkpoint_head, checkpoint_state, committed_head};
pub(crate) use commit::bind_root_policy;
pub use commit::{
    Spawn, Submitted, archive, create_root, create_root_with_peer, inbox_tail, intent_key, send,
    set_peer_membership, spawn, submit,
};
pub use driver::{
    COMPACT_INSTRUCTION, ConversationEntry, ExecutorDriver, NOTICE_MEDIA_TYPE, OpenControl,
    OpenTriage, OpenTurn, TurnHost, TurnIdentity, TurnOutcome, TurnOutputs, USER_INPUT_MEDIA_TYPE,
    approval_intent, encode_user_input, expand_attachments, model_input, model_input_at,
    turn_announcements, turn_id, turn_notices, turn_segment,
};
pub use handler::{AgentHandler, Companions, INBOX};
pub(crate) use handler::{Agents, observe_state};
pub use host::{AgentHost, Delivered, InputProgress, actor_store};
pub use kernel::{CONTINUE_NOW, MAX_FANOUT};
pub use layout::{
    ACTORS, AGENTS, AgentId, AgentPath, NOT_INHERITED, STATUSES, TARGET_STATUS, VOLUMES,
};
pub use lower::{delivery_key, lower};
mod routing;
pub use machine::{
    AgentState, Alarm, INADMISSIBLE_EFFECT, MAX_QUEUED, MAX_QUEUED_BYTES, OVERSIZED_STATE, Out,
    Progress, Queued, ReviewFuture, ReviewStep, SpawnOutcome, SpawnRefusal, Step, TurnDriver,
    TurnFuture, TurnInput, TurnStep, Until, Wait, WaitReason, WaitResume, Wake, requested_child,
    step,
};
pub use records::{
    AgentOutcome, AgentRecord, Approval, ApprovalRequested, Cancellation, ChildDone, ChildSettled,
    ChildSettlement, ChildWake, ChildWakes, Closed, ContextAction, ContextRequest, Control,
    CreationConfig, ForkAdopted, Hook, InboxRecord, IntentId, MAX_INTENT_BYTES, MAX_MESSAGE_BYTES,
    MAX_RECORD_BYTES, Membership, Message, NoticePayload, Phase, RECORD_VERSION, Sender,
    SpawnContext, SpawnRequest, SpawnTemplates, StatusRecord, UnsupportedRecordVersion,
    decode_record,
};
pub use reply::{Reply, SteeringReply, reply};
pub use routing::{PeerResultRoute, PeerRoute, Routing};

/// Agent model failures.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A path is not a canonical agent path.
    #[error("invalid agent path: {0}")]
    Path(&'static str),
    /// A record violates its bounds or the step protocol.
    #[error("invalid agent record: {0}")]
    Record(&'static str),
    /// The retained checkpoint belongs to another artifact codec.
    #[error("unsupported agent checkpoint format; start a fresh session")]
    IncompatibleCheckpointCodec,
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
    /// Preserve undecided host work across the guest boundary.
    pub(crate) fn into_guest(self) -> acyclic_actors::guest::GuestError {
        if matches!(self, Self::IncompatibleCheckpointCodec) {
            acyclic_actors::guest::GuestError::IncompatibleCodec
        } else {
            crate::Error::guest(self)
        }
    }
    /// Whether the failed work may yet succeed unchanged: a commit whose
    /// outcome is unknown, a store that left its outcome undecided, or a handler
    /// retaining Pending work, however wrapped. Only these retry; every other
    /// driver failure
    /// settles the agent as failed. A definitive failure inside an opened turn
    /// (executor, model or tool) is no driver failure: it settles only its
    /// turn (see [`TurnOutcome::Failed`]).
    #[must_use]
    pub fn uncertain(&self) -> bool {
        match self {
            Self::Unknown(_) => true,
            Self::Stream(error) => error.uncertain(),
            Self::Turn(error) => error.uncertain(),
            Self::Actor(error) => match &**error {
                acyclic_actors::local::Error::Unknown { .. } => true,
                acyclic_actors::local::Error::Stream(error) => error.uncertain(),
                acyclic_actors::local::Error::Handler(
                    acyclic_actors::guest::GuestError::Pending(_),
                ) => true,
                _ => false,
            },
            Self::Path(_)
            | Self::Record(_)
            | Self::IncompatibleCheckpointCodec
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
            Self::IncompatibleCheckpointCodec => "unsupported_checkpoint_codec",
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

impl From<crate::Error> for Error {
    fn from(error: crate::Error) -> Self {
        Self::Turn(Box::new(error))
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
        assert!(
            Error::Actor(Box::new(acyclic_actors::local::Error::Handler(
                acyclic_actors::guest::GuestError::Pending("undecided".into())
            )))
            .uncertain()
        );
        assert!(
            !Error::Actor(Box::new(acyclic_actors::local::Error::Handler(
                acyclic_actors::guest::GuestError::Handler("definitive".into())
            )))
            .uncertain()
        );
        assert!(matches!(
            Error::IncompatibleCheckpointCodec.into_guest(),
            acyclic_actors::guest::GuestError::IncompatibleCodec
        ));
        // A harness failure is classified by its wire class, whichever
        // variant carries it: a storage failure's outcome is undecided.
        assert!(Error::Turn(Box::new(crate::Error::Storage("unavailable".into()))).uncertain());
        assert!(!Error::Turn(Box::new(crate::Error::Invalid("definitive".into()))).uncertain());
    }
}
