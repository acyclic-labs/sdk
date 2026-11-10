//! The headless wire boundary, before any conversation action is selected.
use acyclic_harness::{agent::IntentId, interaction::ApprovalBinding};

/// Each input names one action. Approval retries retain the original shown
/// binding instead of discovering a potentially different pending action.
#[derive(serde::Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Request {
    /// Submit a task to the selected conversation.
    Prompt {
        /// Task text.
        prompt: String,
        /// Optional stable identity for this input.
        id: Option<String>,
    },
    /// Resolve the original approval shown to the caller.
    Approval {
        /// Identity of the pending action.
        approve: IntentId,
        /// Original approval observation binding.
        binding: ApprovalBinding,
        /// Whether the caller permits the action.
        approved: bool,
    },
    /// Cancel the selected conversation operation.
    Cancel {
        /// Cancellation request flag.
        cancel: bool,
    },
}

/// Decode one complete JSONL request before selecting a conversation action.
pub fn parse(line: &str) -> serde_json::Result<Request> {
    serde_json::from_str(line)
}
