//! Checkpoint descriptors are a paged projection of the conversation stream.
use super::WorkspaceEntry;
use serde::{Deserialize, Serialize};

/// Which part of a saved turn becomes current. External effects and child
/// agents are never undone by a project generation restoration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum RestoreMode {
    /// Restore project files; keep the current model context.
    Files,
    /// Select the saved conversation cut; keep current project files.
    Context,
    /// Restore files and select the saved conversation cut atomically.
    Both,
}

impl RestoreMode {
    /// Whether project heads change.
    pub const fn files(self) -> bool {
        matches!(self, Self::Files | Self::Both)
    }
    /// Whether later model context changes.
    pub const fn context(self) -> bool {
        matches!(self, Self::Context | Self::Both)
    }
}

/// A selectable conversation prefix and its indexed context checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySelection {
    /// Exclusive inherited conversation prefix.
    pub conversation: u64,
    /// Latest context checkpoint before that prefix.
    pub checkpoint: Option<u64>,
}

/// Direct model-context cut. Stream forks share its immutable prefix, so
/// repeated history selection never creates a chain for the harness to walk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextCut {
    /// Canonical stream containing the selected conversation entries.
    pub path: acyclic_stream::StreamPath,
    /// Exclusive selected prefix.
    pub tail: u64,
    /// Latest summary in this context stream, if any.
    pub checkpoint: Option<u64>,
}

/// A project binding and its exact retained generation at a conversation cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryVolume {
    /// Original binding; a history fork retains its original generation.
    pub binding: WorkspaceEntry,
    /// Existing immutable generation, never a directory backup.
    pub generation: acyclic_fs::GenerationId,
}

/// A captured project head with its own retained volume generation. Retention
/// remains readable when the original agent or workspace is closed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedVolume {
    /// Original project binding.
    pub binding: WorkspaceEntry,
    /// Original project's head at this turn.
    pub generation: acyclic_fs::GenerationId,
    /// Structurally shared retention workspace, never a materialized directory.
    pub retained_workspace: String,
    /// Exact generation owned by the retention workspace.
    pub retained_generation: acyclic_fs::GenerationId,
}

/// Bounded history-picker projection. Saved roots stay at the original cut;
/// selecting this turn does not send those roots through the client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySummary {
    /// Exclusive settled conversation cut.
    pub conversation: u64,
    /// Exclusive selected model-context cut.
    pub context_tail: u64,
    /// Number of saved project bindings.
    pub volume_count: u64,
}

fn count_volumes<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Count;
    impl<'de> serde::de::Visitor<'de> for Count {
        type Value = u64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("saved project bindings")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut entries: A) -> Result<u64, A::Error> {
            let mut count = 0_u64;
            while entries.next_element::<serde::de::IgnoredAny>()?.is_some() {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| serde::de::Error::custom("project binding count overflow"))?;
            }
            Ok(count)
        }
    }
    deserializer.deserialize_seq(Count)
}

/// Version-three checkpoint, committed with its conversation entry and every
/// retention fork. Its sequence in `AgentPath::history` is the exclusive cut
/// in the conversation; zero is adoption. Readers use stream reads and follows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCheckpoint {
    /// Whether a child was still open when this boundary committed.
    pub live_children: bool,
    /// Descriptor format; readers refuse versions other than 3.
    pub version: u32,
    /// Exclusive conversation cut; zero is the original adoption boundary.
    pub conversation: u64,
    /// Latest context checkpoint at that cut, if any.
    pub checkpoint: Option<u64>,
    /// Direct selected model-context prefix, independent of chronological history.
    pub context: ContextCut,
    /// Project generations. Private journal state and session-shared volumes
    /// remain live; restoring them would erase provenance or other agents' work.
    pub volumes: Vec<SavedVolume>,
}

impl HistoryCheckpoint {
    /// Project an authenticated descriptor without allocating or copying saved
    /// workspace identities, roots or paths. Work is bounded by this record.
    pub fn summary(bytes: &[u8]) -> Result<HistorySummary, super::Error> {
        #[derive(Deserialize)]
        struct Context {
            tail: u64,
        }
        #[derive(Deserialize)]
        struct Summary {
            version: u32,
            conversation: u64,
            context: Context,
            #[serde(deserialize_with = "count_volumes")]
            volumes: u64,
        }
        let value: Summary = serde_json::from_slice(bytes)
            .map_err(|_| super::Error::Record("history checkpoint is malformed"))?;
        if value.version != 3 || value.context.tail > value.conversation {
            return Err(super::Error::Record(
                "history checkpoint version or cut is invalid",
            ));
        }
        Ok(HistorySummary {
            conversation: value.conversation,
            context_tail: value.context.tail,
            volume_count: value.volumes,
        })
    }

    /// Decode an authenticated descriptor before using its immutable roots.
    pub fn decode(bytes: &[u8]) -> Result<Self, super::Error> {
        let checkpoint: Self = serde_json::from_slice(bytes)
            .map_err(|_| super::Error::Record("history checkpoint is malformed"))?;
        checkpoint.check()?;
        Ok(checkpoint)
    }

    /// Decode an original history descriptor while retaining its allocation credit.
    pub fn decode_accounted(
        bytes: &[u8],
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<
        acyclic_fs::kernel::Retained<Self>,
        acyclic_fs::kernel::MetadataDecodeError<super::Error>,
    > {
        use acyclic_fs::kernel::MetadataDecodeError;
        let checkpoint = accounting
            .decode_json::<Self>(bytes)
            .map_err(|error| match error {
                MetadataDecodeError::Work(error) => MetadataDecodeError::Work(error),
                MetadataDecodeError::Codec(_) => MetadataDecodeError::Codec(super::Error::Record(
                    "history checkpoint is malformed",
                )),
            })?;
        checkpoint.check().map_err(MetadataDecodeError::Codec)?;
        Ok(checkpoint)
    }

    fn check(&self) -> Result<(), super::Error> {
        let checkpoint = self;
        if checkpoint.version != 3 {
            return Err(super::Error::Record(
                "history checkpoint version or cut is invalid",
            ));
        }
        super::records::check_volumes(checkpoint.volumes.iter().map(|volume| &volume.binding))?;
        if checkpoint.volumes.iter().any(|volume| {
            volume
                .binding
                .as_volume()
                .is_none_or(|source| source.class() != crate::conversation::VolumeClass::Project)
        }) {
            return Err(super::Error::Record("saved project bindings are invalid"));
        }
        if checkpoint
            .checkpoint
            .is_some_and(|at| at >= checkpoint.conversation)
        {
            return Err(super::Error::Record(
                "history context checkpoint exceeds its cut",
            ));
        }
        if checkpoint.context.tail > checkpoint.conversation
            || checkpoint
                .context
                .checkpoint
                .is_some_and(|at| at >= checkpoint.context.tail)
        {
            return Err(super::Error::Record("context checkpoint exceeds its cut"));
        }
        Ok(())
    }
}
