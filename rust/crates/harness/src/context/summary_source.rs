//! Immutable inputs and selections for conversation summary checkpoints.
use super::*;
use crate::{
    OperationId, TaskId,
    task_volume::{Cursor, FileKey},
};

/// The exact conversation boundary covered by one retained summary input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryBoundary {
    /// Task owning the covered turns.
    pub task: TaskId,
    /// Exclusive accepted cut for the task turns query.
    pub through: Cursor,
    /// Global task-volume position observed at source capture.
    pub captured_at: u64,
    /// Selected predecessor checkpoint, if any.
    pub previous: Option<FileKey>,
    /// The source already covers the terminal task outcome.
    pub outcome_included: bool,
    /// Capture follows an explicitly fixed rewind boundary.
    pub frozen: bool,
    /// Exact admitted context manifest.
    pub manifest: FileRef,
    /// Model selected when the source was published.
    pub model: String,
}

/// One pinned source file; execution never reserializes or republishes it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummarySource {
    /// Active summary plus subsequent turns.
    pub context: Context,
    /// History and configuration binding.
    pub boundary: SummaryBoundary,
}

/// Immutable checkpoint contents. Large source and summary bytes remain in their files.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryCheckpoint {
    /// Durable summary execution.
    pub operation: OperationId,
    /// History and configuration binding.
    pub boundary: SummaryBoundary,
    /// Exact source publication, also used by provenance.
    pub source: FileRef,
    /// Exact collected summary output.
    pub summary: FileRef,
}

/// A file selecting a checkpoint. A fixed cursor denotes a rewind; absence follows new turns.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummarySelection {
    /// Requested parent, present only in a protected continuation selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<TaskId>,
    /// Immutable protected checkpoint file.
    pub checkpoint: FileKey,
    /// Cut before selection publication; only a later source capture can advance it.
    pub selected_at: u64,
    /// Fixed rewind cut, or live subsequent turns.
    pub through: Option<Cursor>,
}

// Capture starts at the selected summary, never at the discarded conversation.
// The false variant is exercised by sequence properties as a deletion control.
pub(crate) fn retained_source<const KEEP: bool>(
    prior: Option<&str>,
    input: Option<&ModelContent>,
    limits: Limits,
) -> Result<String> {
    let mut text = String::new();
    let mut append = |value: &str| -> Result<()> {
        if text.len().saturating_add(value.len()).saturating_add(1) as u64 > limits.file_bytes {
            return Err(crate::Error::Invalid(
                "compaction source exceeds file bound".into(),
            ));
        }
        text.push_str(value);
        text.push('\n');
        Ok(())
    };
    if KEEP {
        if let Some(prior) = prior {
            append(prior)?;
        }
    }
    if let Some(input) = input {
        append(
            &serde_json::to_string(input)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
        )?;
    }
    Ok(text)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn checkpoint(through: Cursor) -> SummaryCheckpoint {
        let file = FileRef::new(
            crate::conversation::VolumeRef::new(
                crate::resources::ProviderRef::new(
                    "test".into(),
                    "filesystem".into(),
                    "1".into(),
                    &mut None,
                )
                .unwrap(),
                "private".into(),
                crate::conversation::VolumeClass::AgentPrivate,
                crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
                &mut None,
            )
            .unwrap(),
            "inputs/source.json".into(),
            "1".into(),
            crate::conversation::FileDescriptor::from_bytes(b"{}", "application/json", &mut None)
                .unwrap(),
            "source.json".into(),
            &mut None,
        )
        .unwrap();
        SummaryCheckpoint {
            operation: OperationId::from_bytes([1; 16]),
            boundary: SummaryBoundary {
                task: TaskId::from_bytes([1; 16]),
                captured_at: through.position,
                through,
                previous: None,
                outcome_included: false,
                frozen: false,
                manifest: file.clone(),
                model: "test".into(),
            },
            source: file.clone(),
            summary: file,
        }
    }

    #[test]
    fn checked_in_summary_seeds_parse() {
        serde_json::from_slice::<SummarySource>(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz-context/corpus/context_manifest/summary-source.json"
        )))
        .unwrap();
        serde_json::from_slice::<SummaryCheckpoint>(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz-context/corpus/context_manifest/summary-checkpoint.json"
        )))
        .unwrap();
        serde_json::from_slice::<SummarySelection>(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fuzz-context/corpus/context_manifest/summary-selection.json"
        )))
        .unwrap();
    }

    #[cfg(not(acyclic_harness_loom))]
    proptest::proptest! {
        #[test]
        fn checkpoint_and_selection_round_trip(
            position in 1u64..u64::MAX, fence in proptest::num::u64::ANY,
            text in ".{0,64}", frozen in proptest::bool::ANY,
        ) {
            let cursor = Cursor {volume:[1;32],epoch:uuid::Uuid::nil(),schema:crate::task_volume::SCHEMA,
                query:[2;32],position,accepted:position,durable:position,commit:[3;32]};
            let mut cp = checkpoint(cursor.clone());
            cp.boundary.captured_at = fence;
            cp.boundary.frozen = frozen;
            cp.boundary.model = text.clone();
            let key = FileKey::parse(".tasks/task/context/compaction-test/manifest.json").unwrap();
            cp.boundary.previous = Some(key.clone());
            let selected = SummarySelection {parent:frozen.then_some(TaskId::from_bytes([2;16])),checkpoint:key,selected_at:fence,through:frozen.then_some(cursor)};
            let mut context = Context::default();
            context.messages.push(crate::model::ModelMessage {role:crate::model::ModelRole::User,
                content:crate::model::ModelContent::Text(text)});
            let source = SummarySource {context,boundary:cp.boundary.clone()};
            macro_rules! round_trip { ($value:expr,$ty:ty) => {{
                let bytes = serde_json::to_vec(&$value).unwrap();
                proptest::prop_assert_eq!(serde_json::from_slice::<$ty>(&bytes).unwrap(),$value);
            }}; }
            round_trip!(source,SummarySource);
            round_trip!(cp,SummaryCheckpoint);
            round_trip!(selected,SummarySelection);
        }
    }
}
