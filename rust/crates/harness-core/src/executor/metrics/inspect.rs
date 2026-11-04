//! One admitted model call per Inspect JSON shard. No upload or execution work.
//!
//! Source requests must come from `ExecutionPayloadField::AdmittedRequest`, not
//! the segment manifest in a later step's stored `Request`. Inspect messages are
//! a display projection; original structured content remains in message metadata.
//! Docent can consume the same file through its Inspect JSON loader.

use super::super::{
    ExecutionEvent, ExecutionJournal, ExecutionRecord, ModelPurpose, ModelUsageRecord, OperationId,
    model_usage::ModelUsageAdmission,
};
use crate::{
    Error, Result,
    model::{
        ModelContent, ModelContentPart, ModelMessage, ModelRequest, ModelRole, ToolResultContent,
    },
};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use std::io::Write;

/// Original admission identity. Scope is independent of any root agent.
#[derive(Serialize)]
pub struct Source<'a> {
    /// Original scope, supplied by the authorized source reader.
    pub scope: &'a str,
    /// Original agent whose journal contains this admission.
    pub agent_path: &'a str,
    /// Original execution operation, before summary-attempt derivation.
    pub operation: OperationId,
    /// Original stream identity; never an export filename or current alias.
    pub source_stream: &'a str,
}

/// A verified request admission, retaining just this call's request graph.
pub struct Call<'a> {
    source: Source<'a>,
    sequence: u64,
    step: u32,
    purpose: ModelPurpose,
    request_digest: [u8; 32],
    replay_key: [u8; 32],
    request: ModelRequest,
}

/// The original committed terminal observation of a verified admission.
/// Response content still comes from the original canonical settlement: a
/// terminal model event contains metadata, not the complete assistant message.
pub struct Settlement<'a, 'source> {
    call: &'a Call<'source>,
    sequence: u64,
    metadata: serde_json::Value,
    usage: Option<ModelUsageRecord>,
}

impl<'a> Call<'a> {
    /// Checks exact reconstructed request bytes against their original admission
    /// and obtains the shared B2 content key. No history or provider is read.
    pub fn new(source: Source<'a>, admission: &ExecutionRecord, request: &[u8]) -> Result<Self> {
        let ExecutionEvent::ModelStarted {
            step,
            purpose,
            request_digest,
            ..
        } = &admission.event
        else {
            return Err(invalid("source is not a model admission"));
        };
        if admission.operation_id != source.operation
            || admission.sequence == 0
            || blake3::hash(request).as_bytes() != request_digest
        {
            return Err(invalid("request differs from its original admission"));
        }
        let replay_key = crate::model::replay_key(request).map_err(invalid)?;
        let request = crate::contract::json_from_slice(request).map_err(invalid)?;
        Ok(Self {
            source,
            sequence: admission.sequence,
            step: *step,
            purpose: *purpose,
            request_digest: *request_digest,
            replay_key,
            request,
        })
    }

    /// Writes one complete Inspect log, version 2, with one call sample.
    /// `response` is the original settled assistant content, not streamed deltas.
    /// `created` is the caller's ISO-8601 export creation time; execution time is
    /// left unknown. Usage, if present, must be the original committed record.
    ///
    /// Serialization borrows the request and response. It retains no file index,
    /// joins, run history or growing collection. JSON tool-result display needs
    /// one temporary encoded string per result. The writer is not flushed or
    /// committed; on any error the caller must discard the unpublished shard.
    pub fn write(
        &self,
        writer: impl Write,
        created: &str,
        response: &ModelContent,
        usage: Option<&ModelUsageRecord>,
    ) -> Result<()> {
        self.validate_usage(usage)?;
        self.write_log(writer, created, response, usage, None)
    }

    /// Selects exactly one original terminal record. This reads only its model
    /// event and optional usage artifact, never prior events or a current tail.
    /// Foreign operations/steps and nonterminal observations refuse. Missing
    /// usage stays unknown. Each load finishes through the journal's original
    /// result or typed refusal; no readiness loop or producer is added.
    pub async fn settlement<'call>(
        &'call self,
        journal: &dyn ExecutionJournal,
        terminal: &ExecutionRecord,
    ) -> Result<Settlement<'call, 'a>> {
        if terminal.sequence <= self.sequence
            || !super::super::model_observation::matches_turn(
                &terminal.event,
                self.step,
                self.purpose,
            )
        {
            return Err(invalid("terminal differs from its original admission"));
        }
        let (metadata, usage) = super::super::model_usage::project_completion(
            journal,
            self.source.operation,
            self.source.agent_path,
            terminal,
        )
        .await?;
        self.validate_usage(usage.as_ref())?;
        Ok(Settlement {
            call: self,
            sequence: terminal.sequence,
            metadata,
            usage,
        })
    }

    fn validate_usage(&self, usage: Option<&ModelUsageRecord>) -> Result<()> {
        let Some(usage) = usage else {
            return Ok(());
        };
        let admitted = ModelUsageAdmission {
            dispatch: crate::model::ModelDispatch {
                operation_id: self
                    .purpose
                    .attempt_operation(self.source.operation, None)?,
                step: self.step,
                request_digest: self.request_digest,
            },
            purpose: self.purpose,
            provider: &self.request.model.provider,
            model: &self.request.model.name,
            agent_path: Some(self.source.agent_path),
            sampling: Some(self.request.sampling),
            max_output_tokens: self.request.max_output_tokens,
        };
        if usage.version != ModelUsageRecord::VERSION
            || !usage.completed
            || usage.admission() != admitted
        {
            return Err(invalid("usage differs from its original admission"));
        }
        Ok(())
    }

    fn write_log(
        &self,
        writer: impl Write,
        created: &str,
        response: &ModelContent,
        usage: Option<&ModelUsageRecord>,
        settlement: Option<(u64, &serde_json::Value)>,
    ) -> Result<()> {
        #[derive(Serialize)]
        struct Metadata<'a, 'b> {
            version: u32,
            source: &'a Source<'b>,
            sequence: u64,
            step: u32,
            purpose: ModelPurpose,
            request_digest: &'a str,
            replay_key: &'a str,
            requested_model: &'a crate::model::Model,
            sampling: crate::model::ModelSampling,
            max_output_tokens: Option<u32>,
            usage: Option<&'a ModelUsageRecord>,
            terminal_sequence: Option<u64>,
            terminal_metadata: Option<&'a serde_json::Value>,
        }
        #[derive(Serialize)]
        struct Eval<'a> {
            created: &'a str,
            task: &'static str,
            model: &'a str,
            dataset: Empty,
            config: Empty,
        }
        #[derive(Serialize)]
        struct Stats {
            started_at: &'static str,
            completed_at: &'static str,
        }
        #[derive(Serialize)]
        struct Sample<'a, 'b> {
            id: &'a str,
            epoch: u32,
            input: [(); 0],
            target: [(); 0],
            messages: Messages<'a>,
            metadata: Metadata<'a, 'b>,
        }
        #[derive(Serialize)]
        struct Log<'a, 'b> {
            version: u32,
            status: &'static str,
            eval: Eval<'a>,
            plan: Empty,
            stats: Stats,
            samples: [Sample<'a, 'b>; 1],
        }
        let key = blake3::Hash::from(self.replay_key).to_hex();
        let digest = blake3::Hash::from(self.request_digest).to_hex();
        let log = Log {
            version: 2,
            status: "success",
            eval: Eval {
                created,
                task: "acyclic-model-call",
                model: &self.request.model.name,
                dataset: Empty {},
                config: Empty {},
            },
            plan: Empty {},
            stats: Stats {
                started_at: "",
                completed_at: "",
            },
            samples: [Sample {
                id: key.as_str(),
                epoch: 1,
                // A model request does not establish an evaluation dataset's
                // input. Its actual context belongs in messages, exactly once.
                input: [],
                target: [],
                messages: Messages {
                    input: &self.request.messages,
                    response,
                },
                metadata: Metadata {
                    version: 1,
                    source: &self.source,
                    sequence: self.sequence,
                    step: self.step,
                    purpose: self.purpose,
                    request_digest: digest.as_str(),
                    replay_key: key.as_str(),
                    requested_model: &self.request.model,
                    sampling: self.request.sampling,
                    max_output_tokens: self.request.max_output_tokens,
                    usage,
                    terminal_sequence: settlement.map(|(sequence, _)| sequence),
                    terminal_metadata: settlement.map(|(_, metadata)| metadata),
                },
            }],
        };
        serde_json::to_writer(writer, &log).map_err(|error| {
            if error.is_io() {
                Error::Storage(format!(
                    "Inspect output failed: {error}; recovery: discard the unpublished shard and export the same original call to a writable destination"
                ))
            } else {
                invalid(error)
            }
        })
    }
}

impl Settlement<'_, '_> {
    /// Writes the shared Inspect projection with the original terminal sequence,
    /// raw provider metadata and validated optional usage. The canonical
    /// assistant content must be supplied by its original settlement owner;
    /// this metadata-only terminal never certifies arbitrary response content.
    pub fn write(&self, writer: impl Write, created: &str, response: &ModelContent) -> Result<()> {
        self.call.write_log(
            writer,
            created,
            response,
            self.usage.as_ref(),
            Some((self.sequence, &self.metadata)),
        )
    }
}

#[derive(Serialize)]
struct Empty {}

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(format!(
        "Inspect projection failed: {error}; recovery: discard the unpublished shard and export the same original admission with its reconstructed request and committed result"
    ))
}

struct Messages<'a> {
    input: &'a [ModelMessage],
    response: &'a ModelContent,
}
impl Serialize for Messages<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let mut seq = serializer.serialize_seq(None)?;
        for (index, message) in self.input.iter().enumerate() {
            if message.role == ModelRole::Tool {
                if message.content.parts().is_empty() {
                    return Err(S::Error::custom(
                        "tool message has no original paired result",
                    ));
                }
                for (part_index, part) in message.content.parts().iter().enumerate() {
                    seq.serialize_element(&Message {
                        role: message.role,
                        content: &message.content,
                        result: Some((index, part_index, part)),
                    })?;
                }
            } else {
                seq.serialize_element(&Message {
                    role: message.role,
                    content: &message.content,
                    result: None,
                })?;
            }
        }
        seq.serialize_element(&Message {
            role: ModelRole::Assistant,
            content: self.response,
            result: None,
        })?;
        seq.end()
    }
}

struct Message<'a> {
    role: ModelRole,
    content: &'a ModelContent,
    result: Option<(usize, usize, &'a ModelContentPart)>,
}
impl Serialize for Message<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("role", &self.role)?;
        if self.role == ModelRole::Tool {
            let Some((
                _,
                _,
                ModelContentPart::ToolResult {
                    call_id,
                    name,
                    content,
                },
            )) = self.result
            else {
                return Err(S::Error::custom(
                    "tool message requires an original paired result",
                ));
            };
            map.serialize_entry("tool_call_id", call_id)?;
            map.serialize_entry("function", name)?;
            match content {
                ToolResultContent::Json { value } => map.serialize_entry(
                    "content",
                    &serde_json::to_string(value).map_err(S::Error::custom)?,
                )?,
                ToolResultContent::Error { message } => {
                    #[derive(Serialize)]
                    struct Failure<'a> {
                        r#type: &'static str,
                        message: &'a str,
                    }
                    map.serialize_entry("content", message)?;
                    map.serialize_entry(
                        "error",
                        &Failure {
                            r#type: "unknown",
                            message,
                        },
                    )?;
                }
                ToolResultContent::Parts { parts } => {
                    struct Text<'a>(&'a [crate::model::ModelDataPart]);
                    impl Serialize for Text<'_> {
                        fn serialize<S: Serializer>(
                            &self,
                            serializer: S,
                        ) -> std::result::Result<S::Ok, S::Error> {
                            let mut seq = serializer.serialize_seq(None)?;
                            for part in self.0 {
                                if let crate::model::ModelDataPart::Text { text } = part {
                                    seq.serialize_element(&DisplayPart {
                                        r#type: "text",
                                        text: Some(text),
                                        reasoning: None,
                                    })?;
                                }
                            }
                            seq.end()
                        }
                    }
                    map.serialize_entry("content", &Text(parts))?;
                }
            }
        } else {
            map.serialize_entry("content", &DisplayContent(self.content))?;
            if self.role == ModelRole::Assistant {
                map.serialize_entry("tool_calls", &Calls(self.content.parts()))?;
            }
        }
        // No invented display text or media fetch for files/notices/configuration,
        // opaque reasoning, native continuity or structured tool projections.
        if !self.content.parts().is_empty() {
            #[derive(Serialize)]
            struct Metadata<'a> {
                #[serde(skip_serializing_if = "Option::is_none")]
                acyclic_content: Option<&'a ModelContent>,
                #[serde(skip_serializing_if = "Option::is_none")]
                original_message_index: Option<usize>,
                #[serde(skip_serializing_if = "Option::is_none")]
                original_part_index: Option<usize>,
            }
            map.serialize_entry(
                "metadata",
                &Metadata {
                    // Retain the complete original group once. Every result
                    // points back to its message and position without copying
                    // or serializing the group once per member.
                    acyclic_content: self
                        .result
                        .filter(|(_, part, _)| *part != 0)
                        .is_none()
                        .then_some(self.content),
                    original_message_index: self.result.map(|(message, _, _)| message),
                    original_part_index: self.result.map(|(_, part, _)| part),
                },
            )?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct DisplayPart<'a> {
    r#type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<&'a str>,
}
struct DisplayContent<'a>(&'a ModelContent);
impl Serialize for DisplayContent<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if let ModelContent::Text(text) = self.0 {
            return serializer.serialize_str(text);
        }
        let mut seq = serializer.serialize_seq(None)?;
        for part in self.0.parts() {
            match part {
                ModelContentPart::Text { text, .. } => seq.serialize_element(&DisplayPart {
                    r#type: "text",
                    text: Some(text),
                    reasoning: None,
                })?,
                ModelContentPart::Reasoning { block } => {
                    if let Some(text) = block.text.as_deref() {
                        seq.serialize_element(&DisplayPart {
                            r#type: "reasoning",
                            text: None,
                            reasoning: Some(text),
                        })?;
                    }
                }
                _ => {}
            }
        }
        seq.end()
    }
}
struct Calls<'a>(&'a [ModelContentPart]);
impl Serialize for Calls<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for part in self.0 {
            if let ModelContentPart::ToolCall(call) = part {
                #[derive(Serialize)]
                struct Call<'a> {
                    id: &'a str,
                    function: &'a str,
                    arguments: Arguments<'a>,
                    r#type: &'static str,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    parse_error: Option<&'static str>,
                }
                seq.serialize_element(&Call {
                    id: &call.call_id,
                    function: &call.name,
                    arguments: Arguments(&call.arguments),
                    r#type: "function",
                    parse_error: (!call.arguments.is_object()).then_some(
                        "Original arguments are not an object; retained in message metadata",
                    ),
                })?;
            }
        }
        seq.end()
    }
}

/// Inspect requires an object even for a refused call. Its parse error marks
/// this empty display; the unchanged original arguments remain in metadata.
struct Arguments<'a>(&'a serde_json::Value);
impl Serialize for Arguments<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if self.0.is_object() {
            self.0.serialize(serializer)
        } else {
            Empty {}.serialize(serializer)
        }
    }
}

#[cfg(test)]
mod tests;
