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
    /// Maximum journal payload size.
    pub maximum_bytes: u64,
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
    pub(in crate::filesystem) fn require_path(
        class: Option<Self>,
        path: &str,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let permitted = match class {
            Some(class) => path.starts_with(class.prefix()),
            None => {
                !crate::conversation::is_internal_path(path)
                    && path != "turns/.canonical"
                    && !path.starts_with("turns/.canonical/")
            }
        };
        if permitted {
            Ok(())
        } else {
            Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                if class.is_some() {
                    "content path is outside its authorized storage class"
                } else {
                    "internal storage paths are reserved"
                },
            ))
        }
    }

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
