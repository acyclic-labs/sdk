//! A turn's input: what it runs, the inbox sequence that carried it, and
//! which input a waiting turn takes ahead of the window.

use super::{AgentPath, ChildDone, InboxRecord, IntentId, Phase, Sender};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};

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

/// The turn input an inbox record at `sequence` carries.
pub(crate) fn input(sequence: u64, record: InboxRecord) -> Option<Queued> {
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
        InboxRecord::ForkAdopted(_)
        | InboxRecord::Cancel(_)
        | InboxRecord::Archive(_)
        | InboxRecord::Spawn(_) => None,
    };
    input.map(|input| Queued {
        inbox: sequence,
        input,
    })
}

/// Whether the turn waiting in `phase` takes `input` ahead of the window: the
/// response to its approval request, or an event hook from the host it waits on.
pub(super) fn awaits(phase: &Phase, input: &TurnInput) -> bool {
    match (phase, input) {
        (Phase::WaitingApproval { id }, TurnInput::Approval { id: response }) => id == response,
        (Phase::WaitingHost { host }, TurnInput::Hook { host: from, .. }) => host == from,
        _ => false,
    }
}
