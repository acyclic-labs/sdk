//! A turn's input: what it runs, the inbox sequence that carried it, and
//! which input a waiting turn takes ahead of the window.

use super::{
    AgentOutcome, AgentPath, ChildDone, ContextAction, InboxRecord, IntentId, Phase, Sender,
};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};

/// Input queued for the turn, in inbox order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnInput {
    /// A primitive event that explicitly requests a model call.
    Notice {
        /// Original producer event identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Complete event staged at the original immutable version.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        notice: Box<FileRef>,
    },
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
        /// Optional identity of the message being answered.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "wasm", tsify(type = "string", optional))]
        re: Option<IntentId>,
        /// Body.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        body: Box<FileRef>,
        /// Pinned files the body mentions (see [`super::Message::attachments`]).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef", optional))]
        attachments: Option<Box<FileRef>>,
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
    /// A context request; its entry is a checkpoint.
    Context {
        /// Requester.
        from: Sender,
        /// Request identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// The selection it makes.
        action: ContextAction,
    },
}

impl TurnInput {
    /// The selection a checkpoint entry's input makes; `None` for a turn.
    #[must_use]
    pub const fn context(&self) -> Option<&ContextAction> {
        match self {
            Self::Context { action, .. } => Some(action),
            _ => None,
        }
    }
}

/// One input held for the turn, at the inbox sequence of the record that
/// carried it (a brief: its `ForkAdopted`). The turn it starts settles one
/// conversation entry at the same sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Queued {
    /// Inbox sequence of the carrying record, as `Delivered::Committed` returned it.
    pub inbox: u64,
    /// This record may join the running turn at a model boundary. Explicit
    /// follow-ups and controls keep their own turn even when a later reply steers.
    pub steering: bool,
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

/// Refill the original bounded window without reading past its committed cut.
pub(super) async fn refill<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    state: &mut super::AgentState,
    end: u64,
) -> Result<(), super::Error> {
    if let Some((from, limit)) = state.overflowed(end) {
        let records = read_back(provider, &state.path, from, limit, end).await?;
        state.refill(records, end);
    }
    Ok(())
}

pub(super) async fn read_back<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    from: u64,
    limit: u32,
    end: u64,
) -> Result<Vec<(u64, InboxRecord)>, super::Error> {
    use super::AgentRecord as _;
    use futures::TryStreamExt as _;
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: agent.inbox()?,
            from,
            limit,
            at_least: Default::default(),
        })
        .await
        .map_err(super::Error::Stream)?;
    let mut read = Vec::new();
    while let Some(record) = records.try_next().await.map_err(super::Error::Stream)? {
        if record.sequence >= end {
            break;
        }
        read.push((record.sequence, InboxRecord::decode(&record.value)?));
    }
    Ok(read)
}

/// The turn input an inbox record at `sequence` carries.
pub(crate) fn input(sequence: u64, record: InboxRecord) -> Option<Queued> {
    let steering = matches!(
        &record,
        InboxRecord::Message(_) | InboxRecord::Notice { .. }
    );
    let input = match record {
        InboxRecord::ForkAdopted(adopted) => adopted.brief.map(|brief| TurnInput::Brief {
            brief: Box::new(brief),
        }),
        InboxRecord::Notice {
            id,
            notice: super::NoticePayload::Waking(notice),
        } => Some(TurnInput::Notice { id, notice }),
        InboxRecord::Notice {
            notice: super::NoticePayload::Quiet(_),
            ..
        } => None,
        InboxRecord::Message(message) | InboxRecord::FollowUp(message) => {
            Some(TurnInput::Message {
                from: message.from,
                id: message.id,
                body: message.body,
                re: message.re,
                attachments: message.attachments,
            })
        }
        // An archived child settles an archive its parent (or an ancestor's
        // cascade) asked for; it reports nothing the parent did not already
        // decide, so it starts no turn. A `wait` observes it on the child's status.
        InboxRecord::ChildDone(ChildDone {
            outcome: AgentOutcome::Archived,
            ..
        }) => None,
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
        InboxRecord::Context(request) => Some(TurnInput::Context {
            from: request.by,
            id: request.id,
            action: request.action,
        }),
        // Awaiting is control between a parent's turn and its child; neither
        // record is turn input (see [`super::machine`]).
        InboxRecord::WorkspaceChanged(_)
        | InboxRecord::Awaited
        | InboxRecord::ChildSettled(_)
        | InboxRecord::Cancel(_)
        | InboxRecord::Archive(_)
        | InboxRecord::Spawn(_) => None,
    };
    input.map(|input| Queued {
        inbox: sequence,
        steering,
        input,
    })
}

/// Whether the turn waiting in `phase` takes `input` ahead of the window: the
/// response to its approval request, or an event hook from the host it waits on.
pub(super) fn awaits(phase: &Phase, input: &TurnInput) -> bool {
    match (phase, input) {
        (Phase::WaitingApproval { id, .. }, TurnInput::Approval { id: response }) => id == response,
        (Phase::WaitingHost { host }, TurnInput::Hook { host: from, .. }) => host == from,
        _ => false,
    }
}
