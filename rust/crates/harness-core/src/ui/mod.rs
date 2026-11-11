//! Pure UI files over pinned stream and volume data. Shared by cached clients and filesystem sources.
pub mod blocks;
#[cfg(feature = "filesystem")]
mod filesystem;
mod project;
use crate::command_request::CommandFileRoot;
#[cfg(feature = "filesystem")]
pub use filesystem::{FilesystemCut, FilesystemView};
pub use project::{Affected, Children, Cut, Node, NodeRef, Page, View};

/// Projection or untrusted document failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The caller's output writer refused a projection.
    #[error(transparent)]
    Output(#[from] std::io::Error),
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
    Agent(#[from] crate::agent::Error),
}
/// UI operation result.
pub type Result<T> = std::result::Result<T, Error>;

/// Canonical relative path beneath the bound agent's private `ui/` root.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct UiPath(String);
impl AsRef<str> for UiPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl From<UiPath> for String {
    fn from(path: UiPath) -> Self {
        path.0
    }
}
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
