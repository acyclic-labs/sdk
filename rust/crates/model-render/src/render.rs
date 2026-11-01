use crate::selection::{Selection, select};
use crate::{Family, ItemKind, Provenance, RENDERER_VERSION, WireItem};
use serde_json::{Map, Value, value::RawValue};
use std::{
    fmt,
    io::{self, Write},
};

/// Refusal from admission, projection, or the caller's output sink.
#[derive(Debug)]
pub enum RenderError {
    /// The immutable item violates its durable contract.
    InvalidItem(&'static str),
    /// The selected message has no admitted visible projection for the target family.
    MissingProjection(Family),
    /// Model JSON encoding failed.
    Json(serde_json::Error),
    /// The caller's writer ended or refused output; rendering never retries it.
    Write(io::Error),
}
impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidItem(reason) => write!(f, "model item admission: {reason}"),
            Self::MissingProjection(family) => {
                write!(f, "model rendering: missing {family:?} item projection")
            }
            Self::Json(error) => write!(f, "model JSON encoding: {error}"),
            Self::Write(error) => write!(f, "model request writer: {error}"),
        }
    }
}
impl std::error::Error for RenderError {}

/// Frozen epoch header. Encoding is paid once, independently of item count.
#[derive(Clone, Debug)]
pub struct Header {
    target: Provenance,
    epoch: [u8; 32],
    prefix: Vec<u8>,
    summary_permitted: bool,
}
impl Header {
    /// Pins the renderer version, epoch, target protocol and fixed root fields.
    ///
    /// # Errors
    /// Refuses another renderer version, a second conversation input field, or
    /// per-step tool controls that could conflict with the request purpose.
    pub fn new(
        target: Provenance,
        epoch: [u8; 32],
        version: u32,
        fields: &Map<String, Value>,
    ) -> Result<Self, RenderError> {
        if target.provider.trim().is_empty()
            || target.endpoint.trim().is_empty()
            || target.model.trim().is_empty()
        {
            return Err(RenderError::InvalidItem("missing target provider identity"));
        }
        let family = target.family;
        if version != RENDERER_VERSION {
            return Err(RenderError::InvalidItem("unsupported renderer version"));
        }
        if ["messages", "input", "contents"]
            .iter()
            .any(|key| fields.contains_key(*key))
        {
            return Err(RenderError::InvalidItem(
                "header cannot contain conversation items",
            ));
        }
        if ["tool_choice", "toolConfig"]
            .iter()
            .any(|key| fields.contains_key(*key))
        {
            return Err(RenderError::InvalidItem(
                "per-step tool controls belong outside the frozen header",
            ));
        }
        // GenerateContent's NONE controls function declarations only. It
        // cannot establish that a built-in server tool will not execute.
        let summary_permitted = family != Family::Gemini
            || fields.get("tools").is_none_or(|tools| {
                tools.as_array().is_some_and(|tools| {
                    tools.iter().all(|tool| {
                        tool.as_object().is_some_and(|tool| {
                            tool.len() == 1
                                && tool
                                    .get("functionDeclarations")
                                    .is_some_and(Value::is_array)
                        })
                    })
                })
            });
        let mut prefix = serde_json::to_vec(fields).map_err(RenderError::Json)?;
        prefix.pop(); // The encoded object always ends with its closing brace.
        if !fields.is_empty() {
            prefix.push(b',');
        }
        serde_json::to_writer(&mut prefix, family.input()).map_err(RenderError::Json)?;
        prefix.extend_from_slice(b":[");
        Ok(Self {
            target,
            epoch,
            prefix,
            summary_permitted,
        })
    }
    /// Epoch identity; a changed header requires a changed epoch in the owner.
    pub fn epoch(&self) -> &[u8; 32] {
        &self.epoch
    }
    /// Stable prefix before the conversation array, with no per-request state.
    pub fn bytes(&self) -> &[u8] {
        &self.prefix
    }
}

/// Request closers. Moving per-request state belongs here, never in an item.
#[derive(Clone, Copy, Debug, Default)]
pub struct Tail;
impl Tail {
    /// Bytes outside the cacheable conversation prefix.
    pub const fn bytes(self) -> &'static [u8] {
        b"]}"
    }
}

/// Pure per-item projection. Reasoning requires the original provider, endpoint
/// and known credential ownership scope. It survives a model change unchanged;
/// foreign or unowned reasoning is dropped, never displayed as text. A guarded
/// message instead selects its admitted visible projection, including when the
/// protocol is unchanged. Its message grouping remains one segment.
///
/// # Errors
/// A foreign message without its write-time capability projection refuses.
pub fn seg<'a>(
    target: &Provenance,
    item: &'a WireItem,
) -> Result<Option<&'a RawValue>, RenderError> {
    let origin = item.provenance();
    let same_family = origin.family == target.family;
    let same_owner = item.kind() == ItemKind::Message
        || (same_family
            && origin.provider == target.provider
            && origin.endpoint == target.endpoint
            && origin.credential_scope.is_some()
            && origin.credential_scope == target.credential_scope);
    match select(item.kind(), same_family, same_owner) {
        Selection::Native => Ok(Some(item.wire())),
        Selection::Drop => Ok(None),
        Selection::Projection => item
            .projection(target.family)
            .map(Some)
            .ok_or(RenderError::MissingProjection(target.family)),
    }
}

/// Writes exactly the caller-selected epoch items. There is no history window,
/// look-behind, reordering, request-sized allocation or hidden input read.
/// Each emitted item costs one separator and one borrowed-byte write.
///
/// # Errors
/// Returns the first projection or writer refusal; it never waits or retries.
pub fn render<'a>(
    header: &Header,
    items: impl IntoIterator<Item = &'a WireItem>,
    tail: Tail,
    writer: &mut impl Write,
) -> Result<(), RenderError> {
    render_request(header, items, tail, None, writer)
}

/// Appends one transient summary instruction to the selected epoch range and
/// disables tool invocation for that request. The header and prior item bytes
/// are identical to ordinary rendering; provider cache hits are a separate
/// guarantee (Anthropic tool-choice changes invalidate message caching).
///
/// # Errors
/// Refuses Gemini built-in tools before writing any request bytes: its function
/// calling control cannot disable those tools. Other failures are the same as
/// [`render`]. A nonempty Gemini range must end with an admitted model turn;
/// otherwise rendering refuses after the unchanged prefix, before appending an
/// instruction. No summary instruction is persisted as conversation history.
pub fn render_summary<'a>(
    header: &Header,
    items: impl IntoIterator<Item = &'a WireItem>,
    instruction: &str,
    writer: &mut impl Write,
) -> Result<(), RenderError> {
    if instruction.is_empty() {
        return Err(RenderError::InvalidItem("missing summary instruction"));
    }
    if !header.summary_permitted {
        return Err(RenderError::InvalidItem(
            "Gemini built-in tools cannot be disabled for summary generation",
        ));
    }
    render_request(header, items, Tail, Some(instruction), writer)
}

fn render_request<'a>(
    header: &Header,
    items: impl IntoIterator<Item = &'a WireItem>,
    tail: Tail,
    summary: Option<&str>,
    writer: &mut impl Write,
) -> Result<(), RenderError> {
    let mut writer = crate::sink::Sink(writer);
    writer
        .write_all(header.bytes())
        .map_err(RenderError::Write)?;
    let mut separator = b"".as_slice();
    let mut last_gemini_model = None;
    for item in items {
        if let Some(raw) = seg(&header.target, item)? {
            last_gemini_model = Some(item.gemini_model_turn(raw));
            writer.write_all(separator).map_err(RenderError::Write)?;
            writer
                .write_all(raw.get().as_bytes())
                .map_err(RenderError::Write)?;
            separator = b",";
        }
    }
    if let Some(instruction) = summary {
        if header.target.family == Family::Gemini
            && last_gemini_model.is_some_and(|role| role != Some(true))
        {
            return Err(RenderError::InvalidItem(
                "Gemini summary range must end at a model turn",
            ));
        }
        writer.write_all(separator).map_err(RenderError::Write)?;
        // Serialize borrowed leaves directly into the original sink. There
        // is no Value tree, second message encoder or request-sized buffer.
        match header.target.family {
            Family::Responses | Family::Chat | Family::Anthropic => {
                #[derive(serde::Serialize)]
                struct Message<'a> {
                    role: &'static str,
                    content: &'a str,
                }
                serde_json::to_writer(
                    &mut writer,
                    &Message {
                        role: "user",
                        content: instruction,
                    },
                )
            }
            Family::Gemini => {
                #[derive(serde::Serialize)]
                struct Part<'a> {
                    text: &'a str,
                }
                #[derive(serde::Serialize)]
                struct Content<'a> {
                    role: &'static str,
                    parts: [Part<'a>; 1],
                }
                serde_json::to_writer(
                    &mut writer,
                    &Content {
                        role: "user",
                        parts: [Part { text: instruction }],
                    },
                )
            }
        }
        .map_err(|error| match error.io_error_kind() {
            Some(kind) => RenderError::Write(io::Error::new(kind, error)),
            None => RenderError::Json(error),
        })?;
        let controls = match header.target.family {
            Family::Responses | Family::Chat => b"],\"tool_choice\":\"none\"}".as_slice(),
            Family::Anthropic => b"],\"tool_choice\":{\"type\":\"none\"}}",
            Family::Gemini => b"],\"toolConfig\":{\"functionCallingConfig\":{\"mode\":\"NONE\"}}}",
        };
        writer.write_all(controls).map_err(RenderError::Write)
    } else {
        writer.write_all(tail.bytes()).map_err(RenderError::Write)
    }
}
