use crate::selection::{Selection, select};
use crate::{Family, Fields, ItemKind, Provenance, RENDERER_VERSION, WireItem};
use serde_json::{Value, value::RawValue};
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
    prefix: Vec<u8>,
    summary_permitted: bool,
    summary_artifact: Option<usize>,
}
impl Header {
    /// Pins the renderer version, target protocol and the header slots of
    /// `fields`, in their fixed schema order, then the encoded tools.
    /// `summary_artifact` pins one index in the epoch's frozen summary set;
    /// `None` selects portable text and is the alloy default. Changing that
    /// choice requires another epoch, just like changing the root fields.
    /// Call controls are never part of the header: [`Tail`] encodes them.
    ///
    /// # Errors
    /// Refuses another renderer version or an invalid target.
    pub fn new(
        target: Provenance,
        version: u32,
        fields: &Fields,
        summary_artifact: Option<usize>,
    ) -> Result<Self, RenderError> {
        target.validate()?;
        if version != RENDERER_VERSION {
            return Err(RenderError::InvalidItem("unsupported renderer version"));
        }
        let mut prefix = vec![b'{'];
        for (name, value) in fields.header() {
            entry(&mut prefix, name, value)?;
            prefix.push(b',');
        }
        if let Some(tools) = fields.tools() {
            entry(&mut prefix, "tools", tools)?;
            prefix.push(b',');
        }
        serde_json::to_writer(&mut prefix, target.family.input()).map_err(RenderError::Json)?;
        prefix.extend_from_slice(b":[");
        Ok(Self {
            summary_permitted: target.family != Family::Gemini || !fields.server_tools(),
            target,
            prefix,
            summary_artifact,
        })
    }
    /// Stable prefix before the conversation array, with no per-request state.
    pub fn bytes(&self) -> &[u8] {
        &self.prefix
    }

    /// Immutable producing scope used by this epoch's item and summary selection.
    pub const fn target(&self) -> &Provenance {
        &self.target
    }

    /// The optional summary artifact pinned with this provider's epoch header.
    pub const fn summary_artifact(&self) -> Option<usize> {
        self.summary_artifact
    }
}

fn entry(
    out: &mut Vec<u8>,
    name: &str,
    value: &(impl serde::Serialize + ?Sized),
) -> Result<(), RenderError> {
    serde_json::to_writer(&mut *out, name).map_err(RenderError::Json)?;
    out.push(b':');
    serde_json::to_writer(out, value).map_err(RenderError::Json)
}

/// Request closer: the conversation array's end, then the per-call controls.
/// Moving per-request state belongs here, never in the header or an item.
/// Ordinary and summary calls share this one encoder.
#[derive(Clone, Debug)]
pub struct Tail {
    bytes: Vec<u8>,
    summary: bool,
}
impl Tail {
    /// The ordinary trailer for `fields`' call controls.
    ///
    /// # Errors
    /// Model JSON encoding failed.
    pub fn new(fields: &Fields) -> Result<Self, RenderError> {
        Self::encode(fields.call_controls(), None)
    }

    /// The same trailer with tool invocation disabled in the target family's
    /// own control. Suppression replaces any ordinary tool choice.
    ///
    /// # Errors
    /// Model JSON encoding failed.
    pub fn summary(fields: &Fields, family: Family) -> Result<Self, RenderError> {
        let none = match family {
            Family::Responses | Family::Chat => Value::from("none"),
            Family::Anthropic => serde_json::json!({"type":"none"}),
            Family::Gemini => serde_json::json!({"functionCallingConfig":{"mode":"NONE"}}),
        };
        let name = if family == Family::Gemini {
            "toolConfig"
        } else {
            "tool_choice"
        };
        let controls = fields
            .call_controls()
            .filter(|(field, _)| !matches!(*field, "tool_choice" | "toolConfig"));
        Self::encode(controls, Some((name, &none)))
    }

    fn encode<'a>(
        controls: impl Iterator<Item = (&'static str, &'a Value)>,
        suppression: Option<(&'static str, &'a Value)>,
    ) -> Result<Self, RenderError> {
        let mut bytes = vec![b']'];
        for (name, value) in controls.chain(suppression) {
            bytes.push(b',');
            entry(&mut bytes, name, value)?;
        }
        bytes.push(b'}');
        Ok(Self {
            bytes,
            summary: suppression.is_some(),
        })
    }

    /// Bytes outside the cacheable conversation prefix.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// One conversation array element. The renderer owns separators, so an
/// element writes only its own JSON, or nothing when the target drops it.
pub trait Segment {
    /// Writes this element for `target`. For a written Gemini element,
    /// returns whether it is a model turn when that is known.
    ///
    /// # Errors
    /// A projection refusal or the first writer refusal; never retried.
    fn write(
        &self,
        target: &Provenance,
        writer: &mut impl Write,
    ) -> Result<Option<bool>, RenderError>;
}

impl<T: Segment + ?Sized> Segment for &T {
    fn write(
        &self,
        target: &Provenance,
        writer: &mut impl Write,
    ) -> Result<Option<bool>, RenderError> {
        (**self).write(target, writer)
    }
}

impl Segment for WireItem {
    fn write(
        &self,
        target: &Provenance,
        writer: &mut impl Write,
    ) -> Result<Option<bool>, RenderError> {
        let Some(raw) = seg(target, self)? else {
            return Ok(None);
        };
        writer
            .write_all(raw.get().as_bytes())
            .map_err(RenderError::Write)?;
        Ok(self.gemini_model_turn(raw))
    }
}

/// Maps a serializer failure, keeping the caller's writer error typed.
pub fn json_error(error: serde_json::Error) -> RenderError {
    match error.io_error_kind() {
        Some(kind) => RenderError::Write(io::Error::new(kind, error)),
        None => RenderError::Json(error),
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
    let same_owner = !matches!(item.kind(), ItemKind::Reasoning | ItemKind::GuardedMessage)
        || origin.same_owner(target);
    match select(item.kind(), same_family, same_owner) {
        Selection::Native => Ok(Some(item.wire())),
        Selection::Drop => Ok(None),
        Selection::Projection => item
            .projection(target.family)
            .map(Some)
            .ok_or(RenderError::MissingProjection(target.family)),
    }
}

/// Selects one frozen summary artifact with the same ownership rule as a
/// guarded message. The portable representation is required and never dropped.
/// The caller supplies at most one artifact from the epoch's immutable set;
/// absence means portable-only, including the default for an alloy.
///
/// Selection borrows the original reference: no file read, payload copy,
/// allocation, artifact search or wait occurs here. File authority and body
/// verification remain the shared file reader's responsibility.
pub fn summary_artifact<'a, T: ?Sized>(
    target: &Provenance,
    portable: &'a T,
    native: Option<(&Provenance, &'a T)>,
) -> &'a T {
    let Some((producer, artifact)) = native else {
        return portable;
    };
    crate::selection::guarded_artifact(
        artifact,
        portable,
        producer.family == target.family,
        producer.same_owner(target),
    )
}

/// Writes exactly the caller-selected epoch items. There is no history window,
/// look-behind, reordering, request-sized allocation or hidden input read.
/// Each emitted item costs one separator and its own write.
///
/// # Errors
/// Returns the first projection or writer refusal; it never waits or retries.
pub fn render<S: Segment>(
    header: &Header,
    items: impl IntoIterator<Item = S>,
    tail: &Tail,
    writer: &mut impl Write,
) -> Result<(), RenderError> {
    if tail.summary {
        return Err(RenderError::InvalidItem(
            "a summary trailer requires a summary instruction",
        ));
    }
    render_request(header, items, tail, None, writer)
}

/// Appends one transient summary instruction to the selected epoch range and
/// disables tool invocation through the summary [`Tail`]. The header and prior
/// item bytes are identical to ordinary rendering; provider cache hits are a
/// separate guarantee (Anthropic tool-choice changes invalidate message caching).
///
/// # Errors
/// Refuses Gemini built-in tools before writing any request bytes: its function
/// calling control cannot disable those tools. Other failures are the same as
/// [`render`]. A nonempty Gemini range must end with an admitted model turn;
/// otherwise rendering refuses after the unchanged prefix, before appending an
/// instruction. No summary instruction is persisted as conversation history.
pub fn render_summary<S: Segment>(
    header: &Header,
    items: impl IntoIterator<Item = S>,
    instruction: &str,
    tail: &Tail,
    writer: &mut impl Write,
) -> Result<(), RenderError> {
    if instruction.is_empty() {
        return Err(RenderError::InvalidItem("missing summary instruction"));
    }
    if !tail.summary {
        return Err(RenderError::InvalidItem(
            "summary generation requires the summary trailer",
        ));
    }
    if !header.summary_permitted {
        return Err(RenderError::InvalidItem(
            "Gemini built-in tools cannot be disabled for summary generation",
        ));
    }
    render_request(header, items, tail, Some(instruction), writer)
}

/// Writes the renderer's separator lazily, just before an element's first
/// byte, so a dropped element leaves no trace.
struct Element<'a, W> {
    writer: &'a mut W,
    separator: &'static [u8],
    wrote: bool,
}

impl<W: Write> Write for Element<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if !self.wrote {
            self.writer.write_all(self.separator)?;
            self.wrote = true;
        }
        self.writer.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn render_request<S: Segment>(
    header: &Header,
    items: impl IntoIterator<Item = S>,
    tail: &Tail,
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
        let mut element = Element {
            writer: &mut writer,
            separator,
            wrote: false,
        };
        let turn = item.write(&header.target, &mut crate::sink::Sink(&mut element))?;
        if element.wrote {
            last_gemini_model = Some(turn);
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
        .map_err(json_error)?;
    }
    writer.write_all(tail.bytes()).map_err(RenderError::Write)
}
