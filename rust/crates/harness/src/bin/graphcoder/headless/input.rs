//! The headless wire boundary, before any conversation action is selected.
use acyclic_harness::{agent::IntentId, interaction::ApprovalBinding};

/// Each input names one action. Approval retries retain the original shown
/// binding instead of discovering a potentially different pending action.
#[derive(serde::Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub(super) enum Request {
    Prompt {
        prompt: String,
        id: Option<String>,
    },
    Approval {
        approve: IntentId,
        binding: ApprovalBinding,
        approved: bool,
    },
    Cancel {
        cancel: bool,
    },
}

pub(super) fn parse(line: &str) -> serde_json::Result<Request> {
    serde_json::from_str(line)
}
