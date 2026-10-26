//! Shared host provisioning for in-process and public-client scenarios.
//!
//! An endpoint fixture can install [`Driver`], create [`adopted`], and send a
//! message with the same intent as [`request`]. Both routes exercise the same
//! turn and produce the same conversation bytes. Protocol fixtures must capture
//! their actual host binding, invocation and reply; these helpers invent none.

use crate::Result;
use acyclic_harness_core::agent::*;

/// Initial root used by creation, exact replay, spawn and restart scenarios.
pub fn adopted() -> ForkAdopted {
    ForkAdopted {
        agent: AgentId::from_bytes([1; 16]),
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }
}

/// Caller identity used by direct host controls.
pub fn sender() -> Sender {
    Sender::User {
        principal: "conformance".into(),
    }
}

/// File-free input for a host-side scenario. A public client can instead submit
/// a real message with this same intent; [`Driver`] applies the same oracle.
pub fn request(id: &str) -> Result<InboxRecord> {
    Ok(InboxRecord::ApprovalRequested(ApprovalRequested {
        child: AgentPath::root(&AgentId::from_bytes([1; 16]))
            .child(&AgentId::from_bytes([2; 16]))?,
        id: IntentId::try_from(id)?,
    }))
}

/// Deterministic turn shared by host and endpoint fixtures. `once` publishes
/// exactly its intent bytes; `spawn` also forks `child` at the saved cut;
/// `respawn` requests that same child with a conflicting prompt. A host fixture
/// can use delegated requests and a public client can use ordinary messages.
pub struct Driver;

impl TurnDriver for Driver {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let id = match input {
                Some(TurnInput::ApprovalRequested { id, .. } | TurnInput::Message { id, .. }) => id,
                _ => return Err(Error::Record("unexpected conformance input")),
            };
            let mut outs = vec![Out::Conversation(id.as_str().as_bytes().to_vec())];
            if matches!(id.as_str(), "spawn" | "respawn") {
                outs.push(Out::Spawn {
                    intent: IntentId::try_from("child")?,
                    brief: (id.as_str() == "respawn").then(|| b"different prompt".to_vec()),
                    volumes: Vec::new(),
                });
            }
            Ok(TurnStep {
                checkpoint: None,
                outs,
                wait: Wait::Input,
            })
        })
    }
}
