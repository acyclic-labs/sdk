//! Disposable UI files over one immutable stream/volume cut. No tasks, IO or
//! retained per-agent state. Authored files come from `CommandFileRoot::Ui`.
mod attention;
pub mod blocks;
pub use attention::{attention, attention_wants};
mod project;
mod reply;
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
