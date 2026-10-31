use crate::{Family, ItemKind, RENDERER_VERSION, WireItem};
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
    /// A foreign message has no admitted projection for the target family.
    MissingProjection(Family),
    /// Header JSON encoding failed.
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
            Self::Json(error) => write!(f, "model header encoding: {error}"),
            Self::Write(error) => write!(f, "model request writer: {error}"),
        }
    }
}
impl std::error::Error for RenderError {}

/// Frozen epoch header. Encoding is paid once, independently of item count.
#[derive(Clone, Debug)]
pub struct Header {
    family: Family,
    provider: String,
    epoch: [u8; 32],
    prefix: Vec<u8>,
}
impl Header {
    /// Pins the renderer version, epoch, target protocol and fixed root fields.
    ///
    /// # Errors
    /// Refuses another renderer version or a second conversation input field.
    pub fn new(
        family: Family,
        provider: String,
        epoch: [u8; 32],
        version: u32,
        fields: &Map<String, Value>,
    ) -> Result<Self, RenderError> {
        if provider.trim().is_empty() {
            return Err(RenderError::InvalidItem("missing target provider identity"));
        }
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
        let mut prefix = serde_json::to_vec(fields).map_err(RenderError::Json)?;
        prefix.pop(); // The encoded object always ends with its closing brace.
        if !fields.is_empty() {
            prefix.push(b',');
        }
        serde_json::to_writer(&mut prefix, family.input()).map_err(RenderError::Json)?;
        prefix.extend_from_slice(b":[");
        Ok(Self {
            family,
            provider,
            epoch,
            prefix,
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

/// Pure per-item projection. Same-provider reasoning is replayed unchanged even
/// when the model changes; foreign reasoning is dropped, never displayed as text.
///
/// # Errors
/// A foreign message without its write-time capability projection refuses.
pub fn seg<'a>(
    target: Family,
    provider: &str,
    item: &'a WireItem,
) -> Result<Option<&'a RawValue>, RenderError> {
    if item.kind() == ItemKind::Reasoning && item.provenance().provider != provider {
        return Ok(None);
    }
    if item.provenance().family == target {
        return Ok(Some(item.wire()));
    }
    if item.kind() == ItemKind::Reasoning {
        return Ok(None);
    }
    item.projection(target)
        .map(Some)
        .ok_or(RenderError::MissingProjection(target))
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
    writer
        .write_all(header.bytes())
        .map_err(RenderError::Write)?;
    let mut separator = b"".as_slice();
    for item in items {
        if let Some(raw) = seg(header.family, &header.provider, item)? {
            writer.write_all(separator).map_err(RenderError::Write)?;
            writer
                .write_all(raw.get().as_bytes())
                .map_err(RenderError::Write)?;
            separator = b",";
        }
    }
    writer.write_all(tail.bytes()).map_err(RenderError::Write)
}
