//! Forced continuation metadata rides with the ordinary agent adoption.
use super::{AgentPath, HistoryVolume};
use serde::{Deserialize, Serialize};

/// One intervention, appended after the inherited prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContinuationOverride {
    /// An instruction before the next freely sampled assistant action.
    Instruction {
        /// Instruction delivered after the inherited conversation.
        text: String,
    },
    /// Replace the next assistant action; registered calls use normal execution.
    Assistant {
        /// Assistant text preceding the calls.
        text: String,
        /// Calls dispatched by the ordinary registered tool executor.
        calls: Vec<ForcedCall>,
    },
}

/// A registered tool call, without fabricated provider continuation data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForcedCall {
    /// Registered tool name.
    pub name: String,
    /// Arguments in that tool's model-visible schema.
    pub arguments: serde_json::Value,
}

/// Version-one input to the existing historical fork mechanism.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationRequest {
    /// Persisted input format; currently one.
    pub version: u32,
    /// Optional intervention before the first freely sampled action.
    pub override_item: Option<ContinuationOverride>,
}

/// Provenance retained in the new run's first own stream event. Children are
/// excluded by the ordinary agent fork; each continuation creates fresh ones.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    /// Persisted provenance format; currently one.
    pub version: u32,
    /// Canonical agent path of the source run.
    pub parent_run_id: AgentPath,
    /// Zero-based conversation stream event, inclusive.
    pub stream_offset: u64,
    /// Retained workspace generation for each binding at the cut.
    pub workspace_generations: Vec<HistoryVolume>,
    /// Immutable version-one request file; no override bytes enter checkpoints.
    pub override_item: Option<crate::conversation::FileRef>,
    /// Whether the decider had any unfinished child at the cut.
    pub live_children_at_cut: bool,
    /// Sources of nondeterminism that the fork cannot restore exactly.
    pub non_replay_manifest: Vec<String>,
}

impl Continuation {
    /// Provenance of the coupled cut selected by an ordinary history fork.
    pub fn at(
        source: &AgentPath,
        offset: u64,
        saved: &super::HistoryCheckpoint,
        override_item: Option<crate::conversation::FileRef>,
    ) -> Self {
        Self {
            version: 1,
            parent_run_id: source.clone(),
            stream_offset: offset,
            workspace_generations: saved
                .volumes
                .iter()
                .map(|volume| HistoryVolume {
                    binding: volume.binding.clone(),
                    generation: volume.generation,
                })
                .collect(),
            override_item,
            live_children_at_cut: saved.live_children,
            non_replay_manifest: [
                "wall_clocks",
                "web_network",
                "external_machine_state",
                "model_sampling",
            ]
            .map(str::to_owned)
            .to_vec(),
        }
    }
}

impl ContinuationOverride {
    pub(crate) fn events(&self) -> Option<Vec<crate::model::ModelEvent>> {
        let Self::Assistant { text, calls } = self else {
            return None;
        };
        use crate::model::{ModelEvent, ToolCall};
        let mut events = vec![ModelEvent::Content {
            delta: text.clone(),
        }];
        events.push(ModelEvent::TextPart {
            text: text.clone(),
            native: None,
        });
        events.extend(calls.iter().enumerate().map(|(index, call)| {
            ModelEvent::ToolCall(ToolCall::new(
                format!("forced-{index}"),
                call.name.clone(),
                call.arguments.clone(),
            ))
        }));
        events.push(ModelEvent::Completed {
            metadata: serde_json::json!({"forced_continuation": true}),
        });
        Some(events)
    }
}
