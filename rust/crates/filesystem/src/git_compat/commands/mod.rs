use super::*;

/// Reset behavior supported by the compatibility layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GitResetMode {
    /// Move compatibility HEAD without changing the workspace.
    Soft,
    /// Same as soft because there is no separate index.
    Mixed,
    /// Move HEAD and restore the workspace generation.
    Hard,
}

/// Whether a lazy Git working tree is known to differ from compatibility HEAD.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GitDirtyState {
    /// Every represented path is known equal.
    Clean,
    /// At least one represented path is known different.
    Dirty,
    /// Unresolved source state prevents an exact answer without a scan.
    Unknown,
}

/// Git-shaped status without an index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitStatus {
    /// Current compatibility branch.
    pub branch: String,
    /// Explicit compatibility HEAD.
    pub head: Option<GitCommitId>,
    /// Live workspace tree.
    pub workspace: GitTreeRef,
    /// Whether HEAD and the workspace differ.
    pub dirty: GitDirtyState,
    /// Always true: the compatibility layer stages all eligible changes.
    pub all_changes_staged: bool,
}

mod command;
pub use command::*;
mod action;
pub use action::*;
mod result;
pub use result::*;
mod output;
pub use output::*;
mod executor;
pub use executor::*;
