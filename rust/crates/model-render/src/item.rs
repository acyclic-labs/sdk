use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// A wire protocol family, independent of model or registry aliases.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
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

/// Versioned durable IR item. Storage encodes each raw JSON body as an exact
/// UTF-8 string; rendering emits that body's original JSON bytes verbatim.
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
    #[serde(with = "stored_wire")]
    wire: Box<RawValue>,
    #[serde(with = "stored_projections")]
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

    /// Borrow the fixed set of admitted foreign projections without decoding.
    /// There are at most three entries, independent of conversation length.
    pub fn projections(&self) -> impl ExactSizeIterator<Item = (Family, &RawValue)> {
        self.0
            .projections
            .iter()
            .map(|(family, wire)| (*family, wire.as_ref()))
    }

    pub(crate) fn projection(&self, family: Family) -> Option<&RawValue> {
        self.0.projections.get(&family).map(AsRef::as_ref)
    }
}

// Storage contains UTF-8 bytes, not an embedded JSON tree. In particular,
// serde's private RawValue marker remains an ordinary byte sequence here.
// Rendering still borrows RawValue and emits its original JSON verbatim.
mod stored_wire {
    use super::*;
    use serde::Serializer;

    pub(super) fn serialize<S: Serializer>(
        wire: &RawValue,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(wire.get())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Box<RawValue>, D::Error> {
        let text = String::deserialize(deserializer)?;
        let length = text.len();
        let wire = RawValue::from_string(text).map_err(serde::de::Error::custom)?;
        if wire.get().len() != length {
            return Err(serde::de::Error::custom(
                "stored wire bytes cannot be trimmed",
            ));
        }
        Ok(wire)
    }
}

mod stored_projections {
    use super::*;
    use serde::{Serializer, de::MapAccess, ser::SerializeMap};

    struct Borrowed<'a>(&'a RawValue);
    impl Serialize for Borrowed<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            stored_wire::serialize(self.0, serializer)
        }
    }
    struct Owned(Box<RawValue>);
    impl<'de> Deserialize<'de> for Owned {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            stored_wire::deserialize(deserializer).map(Self)
        }
    }

    pub(super) fn serialize<S: Serializer>(
        projections: &BTreeMap<Family, Box<RawValue>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(projections.len()))?;
        for (family, wire) in projections {
            map.serialize_entry(family, &Borrowed(wire))?;
        }
        map.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Family, Box<RawValue>>, D::Error> {
        struct Projections;
        impl<'de> serde::de::Visitor<'de> for Projections {
            type Value = BTreeMap<Family, Box<RawValue>>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("immutable wire projections by protocol family")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut projections = BTreeMap::new();
                while let Some((family, Owned(wire))) = map.next_entry::<Family, Owned>()? {
                    if projections.insert(family, wire).is_some() {
                        return Err(serde::de::Error::custom("duplicate wire projection"));
                    }
                }
                Ok(projections)
            }
        }
        deserializer.deserialize_map(Projections)
    }
}

impl<'de> Deserialize<'de> for WireItem {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::admit(Record::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
