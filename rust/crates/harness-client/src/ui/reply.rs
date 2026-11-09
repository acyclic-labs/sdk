use super::{Error, Result, UiPath, blocks::InputBlock};
use crate::{AgentCommand, AgentPath, IntentId, ModelContent, PendingApproval};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Version-one ordinary prompt body. The inbox remains the decisions log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyPayload {
    /// Path relative to this agent's `ui/` root.
    pub ui: String,
    /// Opaque generation from the exact displayed file cut.
    pub generation: String,
    /// Block id to chosen values; unknown kinds use the same free-form values.
    pub answers: BTreeMap<String, Vec<String>>,
    /// Free-form input is always available.
    pub text: String,
}
/// A response collected from one displayed document.
pub struct Reply {
    /// Exact document, captured before user interaction.
    pub path: UiPath,
    /// Exact displayed generation; never replaced with a later head.
    pub generation: String,
    /// Answers to the displayed blocks.
    pub answers: BTreeMap<String, Vec<String>>,
    /// Optional free-form response.
    pub text: String,
}
impl Reply {
    /// Exactly one inbox prompt, independent of answer count or block kind.
    /// Approval blocks must use `approval_reply` with their trusted binding.
    pub fn command(
        self,
        agent: AgentPath,
        id: IntentId,
        blocks: &[InputBlock],
    ) -> Result<AgentCommand> {
        let mut ids = BTreeSet::new();
        for block in blocks {
            if let Some(id) = block.attributes.get("id") {
                if !ids.insert(id) {
                    return Err(Error::Invalid("duplicate input id"));
                }
            }
        }
        if self.answers.keys().any(|id| !ids.contains(id)) {
            return Err(Error::Invalid("answer does not name a displayed input"));
        }
        if blocks.iter().any(|b| b.attributes.contains_key("approval")) {
            return Err(Error::Invalid(
                "approval requires the pending action binding",
            ));
        }
        let payload = ReplyPayload {
            ui: self.path.0,
            generation: self.generation,
            answers: self.answers,
            text: self.text,
        };
        Ok(AgentCommand::Prompt {
            agent,
            id,
            body: ModelContent::Text(serde_json::to_string(&payload)?),
        })
    }
}

/// A projected approval can authorize only its original pending action. The
/// binding comes from the trusted status cut, never from markdown attributes.
pub fn approval_reply(
    agent: AgentPath,
    pending: &PendingApproval,
    block: &InputBlock,
    approved: bool,
    reason: Option<String>,
) -> Result<AgentCommand> {
    if block.attributes.get("approval") != Some(&pending.id.to_string()) {
        return Err(Error::Invalid(
            "approval block differs from displayed action",
        ));
    }
    Ok(AgentCommand::Decide {
        agent,
        id: pending.id.clone(),
        binding: pending.binding.clone(),
        approved,
        reason,
    })
}
