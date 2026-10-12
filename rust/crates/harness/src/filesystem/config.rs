use super::*;

/// Resolved mutable workspace head and immutable generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceObservation {
    /// Provider-owned mutable workspace identity.
    pub workspace: WorkspaceRef,
    /// Exact immutable head observed by this operation.
    pub generation: GenerationRef,
}

/// One code-defined member of an atomic workspace mutation batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceMutation {
    /// Creates every missing directory on an absolute path.
    CreateDirectory {
        /// Canonical absolute path.
        path: String,
    },
    /// Creates or replaces one complete file.
    PutFile {
        /// Canonical absolute path.
        path: String,
        /// Complete file content.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Removes one file or empty directory.
    Remove {
        /// Canonical absolute path.
        path: String,
    },
}

/// Reserved host-owned file families.
#[derive(Clone, Copy)]
pub(crate) enum InternalContentClass {
    Interaction,
    Workflow,
    Execution,
    ModelPrefix,
    Conversation,
}

impl InternalContentClass {
    pub(in crate::filesystem) fn prefix(self) -> &'static str {
        match self {
            Self::Interaction => ".system/interactions/",
            Self::Workflow => ".system/workflows/",
            Self::Execution => ".system/execution/",
            Self::ModelPrefix => ".system/inherited-conversation/model-prefix/",
            Self::Conversation => "turns/.canonical/",
        }
    }
}
