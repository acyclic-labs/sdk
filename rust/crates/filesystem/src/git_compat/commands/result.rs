use super::*;

/// Typed result returned by a compatibility filesystem executor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[allow(
    clippy::large_enum_variant,
    reason = "keeps the public result shape direct; this is not a hot path"
)]
pub enum GitFilesystemResult {
    /// An eligible compatibility snapshot was captured.
    Captured {
        /// Filtered compatibility tree.
        tree: GitTreeRef,
        /// Paths represented by the captured compatibility history.
        tracked_paths: BTreeSet<String>,
        /// Authenticated origin when `tree` belongs to a filtered SDK fork.
        proof: Option<GitCaptureProof>,
    },
    /// A dedicated compatibility branch workspace was created.
    Forked {
        /// Stable SDK workspace identity.
        workspace_id: WorkspaceId,
    },
    /// A mutating action completed, optionally producing a new generation.
    Applied {
        /// Resulting tree when the action changes workspace state.
        tree: Option<GitTreeRef>,
        /// Exact resulting tracked paths for generated compatibility commits.
        tracked_paths: Option<BTreeSet<String>>,
    },
    /// Stable executor-defined inspection data.
    Data {
        /// Result category such as `diff`, `grep`, or `archive`.
        kind: String,
        /// Versioned machine-readable result body.
        value: serde_json::Value,
    },
}

impl GitFilesystemResult {
    pub(crate) fn resulting_tree(&self) -> Option<GitTreeRef> {
        match self {
            Self::Captured { tree, .. } => Some(*tree),
            Self::Applied { tree, .. } => *tree,
            Self::Forked { .. } | Self::Data { .. } => None,
        }
    }
}
