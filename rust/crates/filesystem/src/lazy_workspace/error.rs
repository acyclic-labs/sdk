use super::*;

/// Fail-closed lazy workspace errors.
#[derive(Debug, Error)]
pub enum LazyWorkspaceError {
    /// Authenticated workspace operation failed.
    #[error("workspace operation failed: {0}")]
    Workspace(String),
    /// Demand provider operation failed.
    #[error("source demand failed: {0}")]
    Demand(#[from] DemandError),
    /// Companion state failed.
    #[error("lazy state failed: {0}")]
    Store(String),
    /// Durable state changed too frequently to complete a bounded retry.
    #[error("lazy state changed concurrently")]
    Concurrent,
    /// Provider identity or invalidation epoch no longer matches the binding.
    #[error("lazy source binding is stale")]
    StaleSource,
    /// Requested path is absent after applying authored tombstones.
    #[error("path is absent")]
    NotFound,
    /// Requested subtree root is not a directory.
    #[error("path is not a directory")]
    NotDirectory,
    /// Requested operation requires a regular file.
    #[error("path is not a regular file")]
    NotRegularFile,
    /// Requested read exceeds the explicit bound.
    #[error("file exceeds the requested read bound")]
    TooLarge,
    /// A directory page must request at least one entry.
    #[error("directory page bound must be positive")]
    InvalidPageBound,
    /// Directory view changed after the supplied cursor was issued.
    #[error("lazy directory cursor is stale")]
    StaleCursor,
    /// The path no longer names the object the caller resolved.
    #[error("lazy filesystem object identity is stale")]
    StaleIdentity,
    /// The source fact cannot be represented by this workspace profile.
    #[error("source node cannot be represented by the authored workspace")]
    UnsupportedNode,
    /// Removing the last promoted binding would resurrect stale source bytes
    /// through an unresolved hard-link alias.
    #[error("source hard-link aliases must be promoted before removing the final authored binding")]
    UnresolvedHardLinks,
    /// Exact traversal was cancelled before every path was captured.
    #[error("lazy exact scan was cancelled")]
    Cancelled,
    /// Exact traversal exceeded an explicit work budget.
    #[error("lazy exact scan exceeded its work budget: {0}")]
    Work(String),
}
