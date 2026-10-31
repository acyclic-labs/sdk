//! Effects of one turn's agent tools: durable before the step that lowers them.
//!
//! A tool call records its effect once, keyed by the call, and the turn returns
//! the call's result to the model. A crash before the step commit re-runs the
//! turn, whose journal replays the call without executing it again, so the
//! effect must already be durable: the driver reads every recorded effect of
//! the turn when it settles and lowers them into the step's one commit. Nothing
//! reaches another agent before that commit.

use super::{AgentPath, AgentState, InboxRecord, IntentId, Message, Out, Sender};
use crate::{OperationId, conversation::FileRef};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One recorded effect of an agent tool call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnEffect {
    /// Spawn the child named by `intent`, adopted with `brief`.
    Spawn {
        /// Spawning intent; names the child.
        intent: IntentId,
        /// Task brief, staged on the spawning agent's private volume.
        brief: Box<FileRef>,
        /// Exact role/model/policy selection, committed with the child.
        profile: super::AgentProfilePin,
    },
    /// Send a message to the parent or an open child.
    Send {
        /// Recipient.
        to: AgentPath,
        /// Message identity.
        id: IntentId,
        /// Body, staged on the sender's private volume.
        body: Box<FileRef>,
    },
    /// Archive an open child.
    Archive {
        /// Target.
        target: AgentPath,
        /// Archive intent.
        id: IntentId,
    },
}

impl TurnEffect {
    /// The child a spawn effect of `parent` names.
    pub fn child(parent: &AgentPath, intent: &IntentId) -> Result<AgentPath, super::Error> {
        parent
            .child_for_intent(intent.as_str(), &mut None)
            .map_err(|_| super::Error::Path("agent nesting exceeds MAX_DEPTH"))
    }
}

/// The effects recorded by one turn's agent tools.
pub trait TurnEffects: acyclic_stream::ProviderPlatform {
    /// Original-work projection of an exact call lookup.
    fn recorded_call_with_io<'a>(
        &'a self,
        call: &'a str,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> crate::Result<crate::conversation::BoundedContentFuture<'a, Option<TurnEffect>>> {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work agent effect lookup is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || self.recorded_call(call))
    }
    /// Original-work projection of the same create-once effect publication.
    fn record_with_io<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> crate::Result<crate::conversation::BoundedContentFuture<'a, TurnEffect>> {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work agent effect publication is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || self.record(call, effect))
    }
    /// Original-work projection of this turn's recorded effects.
    fn recorded_with_io<'a>(
        &'a self,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> crate::Result<crate::conversation::BoundedContentFuture<'a, Vec<TurnEffect>>> {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work agent effects are unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || self.recorded())
    }
    /// The original effect of this exact call, without scanning the turn.
    fn recorded_call<'a>(
        &'a self,
        call: &'a str,
    ) -> BoxProviderFuture<'a, crate::Result<Option<TurnEffect>>>;
    /// Records `effect` as call `call`'s effect exactly once and returns the
    /// recorded effect: a repeated call returns the first, unchanged.
    fn record<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
    ) -> BoxProviderFuture<'a, crate::Result<TurnEffect>>;

    /// Every effect the turn recorded since its last commit, ordered by call key.
    fn recorded(&self) -> BoxProviderFuture<'_, crate::Result<Vec<TurnEffect>>>;

    /// The same pending-effect observation under the original work and cancellation.
    fn holds_with_io<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> crate::Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work pending agent effects are unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || self.holds(operation))
    }

    /// Whether a call other than `operation` recorded an effect that has not
    /// committed yet. Such effects commit before `operation` dispatches, so
    /// the call reaches every child, message and archive they make.
    fn holds(&self, operation: OperationId) -> BoxProviderFuture<'_, crate::Result<bool>>;
}

/// Path-safe key of the agent tool call `operation` at model step `step`:
/// effects of later steps order after earlier ones.
#[must_use]
pub fn call_key(step: u32, operation: OperationId) -> String {
    format!("{step:010}-{}", operation_key(operation))
}

/// Whether `call` is the key [`call_key`] gives `operation`.
#[must_use]
pub fn is_call_of(call: &str, operation: OperationId) -> bool {
    let Some((_, key)) = call.rsplit_once('-') else {
        return false;
    };
    if key.len() != 32 {
        return false;
    }
    let mut encoded = [0; 32];
    for (index, byte) in operation.into_bytes().iter().enumerate() {
        encoded[2 * index] = b"0123456789abcdef"[(byte >> 4) as usize];
        encoded[2 * index + 1] = b"0123456789abcdef"[(byte & 15) as usize];
    }
    key.as_bytes() == encoded
}

/// Lower-case hex of `operation`, as agent tool keys and intents name it.
#[must_use]
pub fn operation_key(operation: OperationId) -> String {
    operation
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The step effects of `recorded`, in order. A child spawned twice in one turn
/// is spawned once.
#[must_use]
pub fn outs(agent: &AgentState, recorded: Vec<TurnEffect>) -> Vec<Out> {
    let mut spawned = BTreeSet::new();
    recorded
        .into_iter()
        .filter_map(|effect| match effect {
            TurnEffect::Spawn {
                intent,
                brief,
                profile,
            } => spawned.insert(intent.clone()).then(|| Out::Spawn {
                intent,
                brief: Some(*brief),
                profile: Some(profile),
                volumes: Vec::new(),
            }),
            TurnEffect::Send { to, id, body } => Some(Out::Send {
                to,
                record: InboxRecord::Message(Message {
                    from: Sender::Agent {
                        path: agent.path.clone(),
                    },
                    id,
                    body,
                    attachments: None,
                }),
            }),
            TurnEffect::Archive { target, id } => Some(Out::Archive { target, id }),
        })
        .collect()
}
