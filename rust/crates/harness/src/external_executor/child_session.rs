use super::{ExternalHost, request::valid_text};
use serde::{Deserialize, Serialize};

/// Native identity declared before preparing and admitting a child executor.
/// A child label is independent of this identity: separate host processes have
/// their own session, while host-managed agents can share a parent session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildSession {
    /// Native executor implementation used by the child.
    pub host: ExternalHost,
    /// The child's actual native session identity.
    pub session_id: String,
    /// Agent identity within that native session, if supplied by the host.
    pub caller: Option<String>,
}

impl ChildSession {
    pub(super) fn valid(&self) -> bool {
        valid_text(&self.session_id) && self.caller.as_deref().is_none_or(valid_text)
    }
}
