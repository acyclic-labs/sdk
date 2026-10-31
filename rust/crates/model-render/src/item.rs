use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// A wire protocol family, independent of model or registry aliases.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Family {
    /// Anthropic Messages.
    Anthropic,
    /// `OpenAI` Responses.
    Responses,
    /// OpenAI-compatible Chat Completions.
    Chat,
    /// Gemini generateContent.
    Gemini,
}

impl Family {
    pub(crate) fn input(self) -> &'static str {
        match self {
            Self::Responses => "input",
            Self::Gemini => "contents",
            Self::Anthropic | Self::Chat => "messages",
        }
    }
}

/// Nonsecret origin of an immutable wire item. Never include credentials here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    /// Producing protocol family.
    pub family: Family,
    /// Stable nonsecret service identity, independent of its wire protocol.
    pub provider: String,
    /// Producing model, retained even when another model replays the item.
    pub model: String,
    /// Nonsecret endpoint identity, supplied by the admitted transport.
    pub endpoint: String,
}

/// Semantic class fixed by the ingress parser, never inferred during rendering.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    /// User or assistant message, call, result, or context update.
    Message,
    /// Native reasoning, signature, encrypted or redacted continuation.
    Reasoning,
}

/// Versioned durable IR item. Raw JSON remains raw through storage and replay.
///
/// A foreign message projection is fixed at admission, not recomputed from its
/// neighbours. There are at most four entries: lookup work cannot grow with the
/// conversation, agent count or number of models in an alloy.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct WireItem(Record);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    provenance: Provenance,
    kind: ItemKind,
    wire: Box<RawValue>,
    projections: BTreeMap<Family, Box<RawValue>>,
}

impl WireItem {
    /// Ingress boundary: validate without normalizing and retain exact bytes.
    ///
    /// `projections` are native message shapes chosen once by the capability
    /// binder for other families. Reasoning may never have foreign projections.
    ///
    /// # Errors
    /// Refuses malformed JSON, missing provenance, or a rewritten native or
    /// reasoning projection. No partial item is returned.
    pub fn capture(
        provenance: Provenance,
        kind: ItemKind,
        wire: Box<RawValue>,
        projections: BTreeMap<Family, Box<RawValue>>,
    ) -> Result<Self, super::RenderError> {
        Self::admit(Record {
            version: 1,
            provenance,
            kind,
            wire,
            projections,
        })
    }

    fn admit(record: Record) -> Result<Self, super::RenderError> {
        if record.version != 1 {
            return Err(super::RenderError::InvalidItem("unsupported item version"));
        }
        if record.provenance.provider.trim().is_empty()
            || record.provenance.model.trim().is_empty()
            || record.provenance.endpoint.trim().is_empty()
        {
            return Err(super::RenderError::InvalidItem("missing wire provenance"));
        }
        if record.projections.contains_key(&record.provenance.family)
            || (record.kind == ItemKind::Reasoning && !record.projections.is_empty())
        {
            return Err(super::RenderError::InvalidItem(
                "native continuity cannot be replaced",
            ));
        }
        if !super::strict::object(record.wire.get())
            || record
                .projections
                .values()
                .any(|raw| !super::strict::object(raw.get()))
        {
            return Err(super::RenderError::InvalidItem(
                "wire item must be a strict JSON object",
            ));
        }
        Ok(Self(record))
    }

    /// Original nonsecret wire origin.
    pub fn provenance(&self) -> &Provenance {
        &self.0.provenance
    }
    /// Immutable semantic class.
    pub fn kind(&self) -> ItemKind {
        self.0.kind
    }
    /// Exact admitted producing-family bytes, including whitespace and escapes.
    pub fn wire(&self) -> &RawValue {
        &self.0.wire
    }

    pub(crate) fn projection(&self, family: Family) -> Option<&RawValue> {
        self.0.projections.get(&family).map(AsRef::as_ref)
    }
}

impl<'de> Deserialize<'de> for WireItem {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::admit(Record::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
