//! Disposable UI files over one immutable stream/volume cut. No tasks, IO or
//! retained per-agent state. Authored files come from `CommandFileRoot::Ui`.
mod attention;
pub mod blocks;
pub use attention::{attention, attention_wants};
mod project;
mod reply;
use crate::{AgentPath, AgentRecord, Reader, reader::Want};
use acyclic_harness_core::command_request::CommandFileRoot;
pub use project::{Affected, AgentRow, Cut, Node, Page, ReaderCut, View};
pub use reply::{Reply, ReplyPayload, approval_reply};

/// Projection or untrusted document failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An untrusted document exceeded a protective bound.
    #[error("UI document exceeds protective size bound")]
    Capacity,
    /// Invalid path, block, or reply.
    #[error("invalid UI input: {0}")]
    Invalid(&'static str),
    /// Required records or file bytes are outside the supplied cut.
    #[error("UI data not resident; satisfy the view's Wants and file reads")]
    Pending,
    /// Ordinary JSON encoding/decoding failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Agent stream path error.
    #[error(transparent)]
    Agent(#[from] acyclic_harness_core::agent::Error),
}
/// UI operation result.
pub type Result<T> = std::result::Result<T, Error>;

/// Ordered, case-insensitive subsequence matching over an authored title.
/// Callers page their existing query; matching never reads or scans a tree.
#[must_use]
pub fn title_matches(title: &str, query: &str) -> bool {
    let mut title = title.chars().flat_map(char::to_lowercase);
    query
        .chars()
        .flat_map(char::to_lowercase)
        .all(|needle| title.any(|c| c == needle))
}

/// Canonical relative path beneath the bound agent's private `ui/` root.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct UiPath(String);
impl UiPath {
    /// Accept root (`""`) or slash-separated names; refuse escape and aliases.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        if path.len() > blocks::MAX_HEADER_BYTES
            || path.contains(['\\', ':'])
            || path.chars().any(char::is_control)
            || (!path.is_empty() && path.split('/').any(|p| matches!(p, "" | "." | "..")))
        {
            return Err(Error::Invalid("path must be relative to ui/"));
        }
        Ok(Self(path))
    }
    /// Root used for ordinary authorized file queries.
    pub const ROOT: CommandFileRoot = CommandFileRoot::Ui;
    /// Path relative to that root.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// A real file wins, with default expansion only when explicitly embedded.
    pub fn resolve(
        &self,
        authored: Option<&str>,
        default: impl FnMut() -> Result<String>,
    ) -> Result<String> {
        match authored {
            Some(text) => blocks::embed(text, default),
            None => {
                let mut default = default;
                default()
            }
        }
    }
}

#[cfg(test)]
mod tests;

/// The selected agent's own adoption, using the inbox's primitive fork origin.
/// Absence stays unknown until the existing reader satisfies role_want.
pub fn agent_role(reader: &Reader, agent: &AgentPath) -> Option<String> {
    let stream = agent.inbox().ok()?;
    let cache = reader.cache();
    if !cache.origin_known(&stream) {
        return None;
    }
    let from = cache
        .fork(&stream)
        .map_or(0, |origin| origin.inherited.forked_at);
    let record =
        acyclic_harness_core::agent::InboxRecord::decode(cache.record(&stream, from)?).ok()?;
    let acyclic_harness_core::agent::InboxRecord::ForkAdopted(adopted) = record else {
        return None;
    };
    if adopted.agent != agent.id() {
        return None;
    }
    adopted.profile.as_ref()?.role().map(str::to_owned)
}

/// Bounded immutable role demand over the same reader and visible-agent pages.
pub fn role_want(reader: &Reader, agent: &AgentPath) -> Result<Want> {
    let stream = agent.inbox()?;
    let cache = reader.cache();
    if !cache.origin_known(&stream) {
        return Ok(Want::Last { stream, count: 1 });
    }
    let from = cache
        .fork(&stream)
        .map_or(0, |origin| origin.inherited.forked_at);
    Ok(Want::Range {
        stream,
        from,
        to: from
            .checked_add(1)
            .ok_or(Error::Invalid("adoption cursor"))?,
    })
}

/// Input stays with the closest known orchestrator while secondary roles are
/// inspected. An unknown ancestor keeps the root as the conservative target.
pub fn input_target(reader: &Reader, inspected: &AgentPath) -> AgentPath {
    let mut current = inspected.clone();
    loop {
        if agent_role(reader, &current).as_deref() == Some("orchestrator") {
            return current;
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => return current,
        }
    }
}
