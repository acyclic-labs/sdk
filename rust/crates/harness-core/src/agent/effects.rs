//! Effects of one turn's agent tools: durable before the step that lowers them.
//!
//! A tool call records its effect once, keyed by the call, and the turn returns
//! the call's result to the model. A crash before the step commit re-runs the
//! turn, whose journal replays the call without executing it again, so the
//! effect must already be durable: the driver reads every recorded effect of
//! the turn when it settles and lowers them into the step's one commit. Nothing
//! reaches another agent before that commit.

use super::{AgentId, AgentPath, AgentState, InboxRecord, IntentId, Message, Out, Sender};
use crate::conversation::FileRef;
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One recorded effect of an agent tool call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnEffect {
    /// Spawn the child named by `intent`, created holding `brief`.
    Spawn {
        /// Spawning intent; names the child.
        intent: IntentId,
        /// Task brief.
        brief: String,
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
        parent.child(&AgentId::derive(&parent.id(), intent.as_str()))
    }
}

/// The effects recorded by one turn's agent tools.
pub trait TurnEffects: acyclic_stream::ProviderPlatform {
    /// Records `effect` as call `call`'s effect exactly once and returns the
    /// recorded effect: a repeated call returns the first, unchanged.
    fn record<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
    ) -> BoxProviderFuture<'a, crate::Result<TurnEffect>>;

    /// Every effect the turn recorded, ordered by call key.
    fn recorded(&self) -> BoxProviderFuture<'_, crate::Result<Vec<TurnEffect>>>;
}

/// The step effects of `recorded`, in order. A child spawned twice in one turn
/// is spawned once.
#[must_use]
pub fn outs(agent: &AgentState, recorded: Vec<TurnEffect>) -> Vec<Out> {
    let mut spawned = BTreeSet::new();
    recorded
        .into_iter()
        .filter_map(|effect| match effect {
            TurnEffect::Spawn { intent, brief } => {
                spawned.insert(intent.clone()).then(|| Out::Spawn {
                    intent,
                    brief: Some(brief.into_bytes()),
                    volumes: Vec::new(),
                })
            }
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
