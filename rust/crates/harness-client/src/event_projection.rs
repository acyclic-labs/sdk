//! Lossless JSONL projection of the existing multiplexed stream deliveries.
use crate::AgentDelivery;
use acyclic_harness_core::{agent::AgentPath, executor::ExecutionRecord};
use acyclic_harness_core::{conversation::FileRef, executor::ExecutionPayloadField};
use acyclic_stream::wire;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An exclusive provider cursor. Opaque bytes are returned unchanged on resume.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCursor {
    /// Next record to deliver.
    pub next: u64,
    /// Base64 of the provider-owned cursor bytes.
    pub opaque: String,
}

impl EventCursor {
    /// Decode without interpreting the provider's opaque continuation.
    pub fn wire(&self) -> Result<wire::SessionCursor, base64::DecodeError> {
        Ok(wire::SessionCursor {
            next: self.next,
            opaque: STANDARD.decode(&self.opaque)?.into(),
        })
    }
}

/// Version-one event envelope. The original protobuf frame preserves every
/// record header and filesystem change without a competing transcript store.
/// Per-resource ordering is authoritative; no global cross-stream order is implied.
#[derive(Debug, Serialize)]
pub struct StreamEvent<'a> {
    /// Envelope schema version, always 1.
    pub version: u32,
    /// Stable caller-owned operation, also used for idempotent input delivery.
    pub operation: &'a str,
    /// Exact followed resource, part of the record's deduplication identity.
    pub resource: &'a str,
    /// Record sequence, when this delivery carries an append.
    pub sequence: Option<u64>,
    /// Delivery kind: record, opened, watermark, moved or closed.
    pub kind: &'static str,
    /// Exclusive cursor after this delivery, when supplied by the provider.
    pub after: Option<EventCursor>,
    /// Decoded application JSON, when the application record is JSON.
    pub value: Option<serde_json::Value>,
    /// Base64 of the exact canonical SessionFrame protobuf. Binary changes,
    /// immutable output references, keys, commit IDs and unknown fields in
    /// application payloads remain available without lossy text conversion.
    pub frame: String,
}

/// A constant-size continuation across an agent's original input journals.
/// Completed journals retire into `input`; no per-turn cursor map accumulates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPosition {
    /// Original inbox sequence, excluding inherited inputs at a fork.
    pub input: u64,
    /// Exclusive journal sequence within that input.
    pub after: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSelection {
    agent: AgentPath,
    position: ExecutionPosition,
}

/// One canonical execution observation. Deduplicate by its original operation
/// and sequence. Private payloads remain immutable, authorized file references.
#[derive(Serialize)]
pub struct ExecutionStreamEvent<'a> {
    /// Envelope schema, always 1.
    pub version: u32,
    /// The caller's stable exec identity, distinct from the original turn ID.
    pub operation: &'a str,
    /// Always `execution`.
    pub kind: &'static str,
    /// Exact selected agent.
    pub agent: &'a AgentPath,
    /// Original inbox input owning this observation.
    pub input: u64,
    /// Canonical observation, including original turn identity and sequence.
    pub record: &'a ExecutionRecord,
    /// Last completely flushed observation. Referenced payloads hold this at
    /// the previous observation until their final chunk has also flushed.
    pub after: ExecutionPosition,
}

/// Exact bytes of an original private payload, in bounded base64 chunks.
/// Deduplicate by agent, original turn, sequence, field and byte offset.
/// The source observation is acknowledged only after all its chunks flush;
/// a crash may repeat that one observation and its already-flushed chunks.
#[derive(Serialize)]
pub struct ExecutionPayloadEvent<'a> {
    /// Envelope schema, always 1.
    pub version: u32,
    /// Stable caller-owned exec identity.
    pub operation: &'a str,
    /// Always `execution_payload`.
    pub kind: &'static str,
    /// Original owning agent.
    pub agent: &'a AgentPath,
    /// Original inbox input.
    pub input: u64,
    /// Original turn.
    pub turn: acyclic_harness_core::OperationId,
    /// One-based original observation sequence.
    pub sequence: u64,
    /// Selected original payload field.
    pub field: ExecutionPayloadField,
    /// Original immutable identity, including complete length and digest.
    pub file: &'a FileRef,
    /// Byte offset within the original immutable file.
    pub offset: u64,
    /// Standard padded base64 bytes of this range.
    pub data: String,
    /// Last complete observation; the final chunk alone may retire this one.
    pub after: ExecutionPosition,
}

/// Bounded bytes of the immutable request selected by an original status.
/// Deduplicate by resource, status sequence and offset. Only the final chunk
/// acknowledges the status; interruption replays its header and flushed chunks.
#[derive(Serialize)]
pub struct ApprovalPayloadEvent<'a> {
    /// Envelope schema, always 1.
    pub version: u32,
    /// Stable caller-owned exec identity.
    pub operation: &'a str,
    /// Always `approval_payload`.
    pub kind: &'static str,
    /// Original status stream resource.
    pub resource: &'a str,
    /// Original zero-based status sequence naming the request.
    pub sequence: u64,
    /// Complete immutable request length.
    pub length: u64,
    /// Byte offset within that request.
    pub offset: u64,
    /// Standard padded base64 bytes of this range.
    pub data: String,
    /// Last completely flushed status, advancing only on the final chunk.
    pub after: EventCursor,
}

fn encode_payload(bytes: &[u8], offset: u64, length: u64) -> Result<String, &'static str> {
    if offset > length || bytes.len() as u64 != (length - offset).min(crate::MAX_READ_BYTES) {
        return Err("payload chunk differs from its original bounded range");
    }
    Ok(STANDARD.encode(bytes))
}

/// Versioned resume state for the bounded set of selected sources.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventProjection {
    version: u32,
    operation: String,
    resources: BTreeMap<String, EventCursor>,
    execution: Option<ExecutionSelection>,
}

impl EventProjection {
    /// A new operation starts at each source's beginning.
    #[must_use]
    pub fn new(operation: String) -> Self {
        Self {
            version: 1,
            operation,
            resources: BTreeMap::new(),
            execution: None,
        }
    }

    /// Reject another format or operation before any command can be sent.
    pub fn validate(&self, operation: &str) -> Result<(), &'static str> {
        if self.version != 1 || self.operation != operation {
            return Err("event cursor version or operation differs");
        }
        for cursor in self.resources.values() {
            cursor.wire().map_err(|_| "invalid event cursor")?;
        }
        Ok(())
    }

    /// A cursor cannot be applied to a different selected agent or resource set.
    pub fn validate_sources(&self, sources: &[String]) -> Result<(), &'static str> {
        if self
            .resources
            .keys()
            .any(|source| !sources.contains(source))
        {
            return Err("event cursor belongs to a different source selection");
        }
        Ok(())
    }

    /// The original source-specific exclusive continuation.
    #[must_use]
    pub fn cursor(&self, resource: &str) -> EventCursor {
        self.resources.get(resource).cloned().unwrap_or_default()
    }

    /// Retain only the selected agent's current input and journal continuation.
    /// Selection is checked before any original journal query is issued.
    pub fn execution_position(&self, agent: &AgentPath) -> Result<ExecutionPosition, &'static str> {
        match &self.execution {
            Some(selected) if &selected.agent != agent => {
                Err("event cursor belongs to another agent")
            }
            Some(selected) => Ok(selected.position),
            None => Ok(ExecutionPosition::default()),
        }
    }

    /// Borrow the original observation without cloning its payload or identity.
    pub fn execution_event<'a>(
        &'a self,
        agent: &'a AgentPath,
        input: u64,
        record: &'a ExecutionRecord,
        after: ExecutionPosition,
    ) -> ExecutionStreamEvent<'a> {
        ExecutionStreamEvent {
            version: 1,
            operation: &self.operation,
            kind: "execution",
            agent,
            input,
            record,
            after,
        }
    }

    /// Frame an authorized chunk without cloning its observation or file.
    /// Callers retain the previous observation cursor until every field flushes.
    pub fn execution_payload_event<'a>(
        &'a self,
        agent: &'a AgentPath,
        input: u64,
        record: &'a ExecutionRecord,
        field: ExecutionPayloadField,
        offset: u64,
        bytes: &[u8],
        after: ExecutionPosition,
    ) -> Result<ExecutionPayloadEvent<'a>, &'static str> {
        let file = field
            .select(&record.event)
            .ok_or("execution payload field is absent")?;
        let data = encode_payload(bytes, offset, file.descriptor().byte_length())?;
        Ok(ExecutionPayloadEvent {
            version: 1,
            operation: &self.operation,
            kind: "execution_payload",
            agent,
            input,
            turn: record.operation_id,
            sequence: record.sequence,
            field,
            file,
            offset,
            data,
            after,
        })
    }

    /// Project the original request's authorized range without advancing state.
    pub fn approval_payload_event<'a>(
        &'a self,
        delivery: &'a AgentDelivery,
        offset: u64,
        bytes: &[u8],
        length: u64,
    ) -> Result<ApprovalPayloadEvent<'a>, &'static str> {
        let Some(wire::session_frame::Body::Data(data)) = &delivery.frame.body else {
            return Err("approval payload requires an original status record");
        };
        let Some(wire::session_data::Item::Record(record)) = &data.item else {
            return Err("approval payload requires an ALL status record");
        };
        let cursor = data.after.as_ref().ok_or("status cursor is absent")?;
        let encoded = encode_payload(bytes, offset, length)?;
        let after = if offset + bytes.len() as u64 == length {
            EventCursor {
                next: cursor.next,
                opaque: STANDARD.encode(&cursor.opaque),
            }
        } else {
            self.cursor(&delivery.resource)
        };
        Ok(ApprovalPayloadEvent {
            version: 1,
            operation: &self.operation,
            kind: "approval_payload",
            resource: &delivery.resource,
            sequence: record.sequence,
            length,
            offset,
            data: encoded,
            after,
        })
    }

    /// Acknowledge only after every observation before this position was
    /// flushed. Advancing an input retires its entire completed journal cursor.
    pub fn accepted_execution(
        &mut self,
        agent: &AgentPath,
        position: ExecutionPosition,
    ) -> Result<bool, &'static str> {
        let previous = self.execution_position(agent)?;
        if position.input < previous.input
            || (position.input == previous.input && position.after < previous.after)
        {
            return Err("execution cursor moved backwards");
        }
        if let Some(selected) = &mut self.execution {
            if selected.position == position {
                return Ok(false);
            }
            selected.position = position;
        } else {
            self.execution = Some(ExecutionSelection {
                agent: agent.clone(),
                position,
            });
        }
        Ok(true)
    }

    /// Project one delivered frame; no state changes until the writer accepts it.
    pub fn event<'a>(&'a self, delivery: &'a AgentDelivery) -> StreamEvent<'a> {
        use wire::session_frame::Body;
        let (kind, cursor, sequence, bytes) = match &delivery.frame.body {
            Some(Body::Data(data)) => match &data.item {
                Some(wire::session_data::Item::Record(record)) => (
                    "record",
                    data.after.as_ref(),
                    Some(record.sequence),
                    Some(&record.value),
                ),
                Some(wire::session_data::Item::LatestValue(value)) => {
                    ("latest", data.after.as_ref(), None, Some(&value.value))
                }
                _ => ("data", data.after.as_ref(), None, None),
            },
            Some(Body::Opened(opened)) => ("opened", opened.resume.as_ref(), None, None),
            Some(Body::Watermark(mark)) => ("watermark", mark.after.as_ref(), None, None),
            Some(Body::Moved(moved)) => ("moved", moved.resume.as_ref(), None, None),
            Some(Body::Closed(_)) => ("closed", None, None, None),
            _ => ("control", None, None, None),
        };
        StreamEvent {
            version: 1,
            operation: &self.operation,
            resource: &delivery.resource,
            sequence,
            kind,
            after: cursor.map(|cursor| EventCursor {
                next: cursor.next,
                opaque: STANDARD.encode(&cursor.opaque),
            }),
            value: bytes.and_then(|bytes| serde_json::from_slice(bytes).ok()),
            frame: STANDARD.encode(delivery.frame.encode_to_vec()),
        }
    }

    /// Save only after the complete JSONL line has been flushed. A crash between
    /// output and saving can replay that line; consumers deduplicate by source
    /// and record sequence. Saving never advances past an unwritten line.
    pub fn accepted(&mut self, resource: String, cursor: Option<EventCursor>) -> bool {
        let Some(cursor) = cursor else {
            return false;
        };
        if self.resources.get(&resource) == Some(&cursor) {
            return false;
        }
        self.resources.insert(resource, cursor);
        true
    }
}

/// A finite cut of a newly opened set of follows. The registration watermark
/// on each source fixes its target; later appends never extend the drain. Keep
/// consuming and flushing ordinary deliveries until this cut is reached.
#[derive(Debug)]
pub struct EventDrain {
    sources: BTreeMap<String, (Option<u64>, u64)>,
}

impl EventDrain {
    /// Create with each follow's actual starting cursor, immediately before
    /// opening. Opening observations never advance an unread resume cursor.
    pub fn new(sources: impl IntoIterator<Item = (String, u64)>) -> Self {
        Self {
            sources: sources
                .into_iter()
                .map(|(source, from)| (source, (None, from)))
                .collect(),
        }
    }

    /// Account for a flushed delivery. An absent stream contributes an empty
    /// cut; other terminal failures are still returned by the normal decoder.
    pub fn accepted(&mut self, delivery: &AgentDelivery) {
        use wire::session_frame::Body;
        let Some((target, next)) = self.sources.get_mut(&delivery.resource) else {
            return;
        };
        let (accepted, after) = match &delivery.frame.body {
            Some(Body::Opened(opened)) => (None, opened.resume.as_ref()),
            Some(Body::Data(data)) => (None, data.after.as_ref()),
            Some(Body::Watermark(mark)) => (mark.opening_tail, mark.after.as_ref()),
            Some(Body::Closed(closed)) if closed.code == "not_found" => (Some(0), None),
            _ => (None, None),
        };
        if target.is_none() {
            *target = accepted;
        }
        if let Some(after) = after {
            *next = after.next;
        }
    }

    /// Every selected source has delivered through its fixed opening cut.
    pub fn complete(&self) -> bool {
        self.sources
            .values()
            .all(|(target, next)| target.is_some_and(|target| *next >= target))
    }
}
