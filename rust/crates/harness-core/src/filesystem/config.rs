use super::*;

/// Authenticated private-volume binding and limits for a filesystem journal.
#[derive(Clone, Copy)]
pub struct FilesystemJournalBinding<'a> {
    /// Journal's private content volume.
    pub volume: &'a VolumeRef,
    /// Verifier for the original journal owner.
    pub verifier: &'a AuthorityVerifier,
    /// Original content authority.
    pub scope: &'a Scope,
    /// Original preparation accounting, when present.
    pub io: Option<&'a PreparationIo>,
}

/// Resolved mutable workspace head and immutable generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceObservation {
    /// Provider-owned mutable workspace identity.
    pub workspace: WorkspaceRef,
    /// Exact immutable head observed by this operation.
    pub generation: GenerationRef,
}

/// One code-defined member of an atomic workspace mutation batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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

/// Original namespace identity shared by a settled entry and its live output.
/// The directory name conveys no authority to read the source.
pub fn tool_result_directory(turn: crate::OperationId) -> String {
    format!("{}{turn}", InternalContentClass::ToolResult.prefix())
}

/// Publication families within the host-owned journal.
#[derive(Clone, Copy)]
pub(crate) enum InternalContentClass {
    Interaction,
    Execution,
    /// The original validated tool result, readable through tool-output observation.
    ToolResult,
    /// Files and streams published by the tool-output writer.
    ToolOutput,
    /// Settled turn outputs, inherited by an agent's forks.
    Conversation,
}

impl InternalContentClass {
    pub(in crate::filesystem) fn require_path(
        class: Option<Self>,
        path: &str,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if class.is_none_or(|class| class.contains_path(path)) {
            Ok(())
        } else {
            Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content path is outside its authorized storage class",
            ))
        }
    }

    pub(in crate::filesystem) fn contains_path(self, path: &str) -> bool {
        path.starts_with(self.prefix())
    }

    pub(in crate::filesystem) fn prefix(self) -> &'static str {
        match self {
            Self::Interaction => ".system/interactions/",
            Self::Execution => ".system/execution/",
            Self::ToolResult => "tools/.results/",
            Self::ToolOutput => "tools/",
            Self::Conversation => ".system/inherited-conversation/",
        }
    }
}
