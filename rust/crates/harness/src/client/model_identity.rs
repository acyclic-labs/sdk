//! Strict streaming validation retains only the three model identity strings.
use crate::model::Model;
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use std::fmt;

pub(super) fn read(bytes: &[u8]) -> Option<Model> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let model = decoder.deserialize_map(Request).ok()?;
    decoder.end().ok()?;
    Some(model)
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    Model,
    Provider,
    Name,
    Revision,
    #[serde(other)]
    Other,
}

struct Request;
impl<'de> Visitor<'de> for Request {
    type Value = Model;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a model request")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Model, A::Error> {
        let mut model = None;
        while let Some(field) = map.next_key::<Field>()? {
            if matches!(field, Field::Model) {
                if model.is_some() {
                    return Err(de::Error::duplicate_field("model"));
                }
                model = Some(map.next_value_seed(Identity)?);
            } else {
                map.next_value::<StrictIgnored>()?;
            }
        }
        model.ok_or_else(|| de::Error::missing_field("model"))
    }
}

struct Identity;
impl<'de> DeserializeSeed<'de> for Identity {
    type Value = Model;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Model, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Identity {
    type Value = Model;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a model identity")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Model, A::Error> {
        let (mut provider, mut name, mut revision) = (None, None, None);
        while let Some(field) = map.next_key::<Field>()? {
            let (slot, key) = match field {
                Field::Provider => (&mut provider, "provider"),
                Field::Name => (&mut name, "name"),
                Field::Revision => (&mut revision, "revision"),
                _ => {
                    map.next_value::<StrictIgnored>()?;
                    continue;
                }
            };
            if slot.is_some() {
                return Err(de::Error::duplicate_field(key));
            }
            *slot = Some(map.next_value::<String>()?);
        }
        Model::new(
            provider.ok_or_else(|| de::Error::missing_field("provider"))?,
            name.ok_or_else(|| de::Error::missing_field("name"))?,
            revision.ok_or_else(|| de::Error::missing_field("revision"))?,
            serde_json::Value::Null,
        )
        .map_err(de::Error::custom)
    }
}

// IgnoredAny skips string validation in serde_json. deserialize_any instead
// visits every string (including map keys), using only the decoder's bounded
// scratch buffer and recursion limit, never a Value tree for the prompt/options.
struct StrictIgnored;
impl<'de> Deserialize<'de> for StrictIgnored {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        decoder.deserialize_any(Self)
    }
}
impl<'de> Visitor<'de> for StrictIgnored {
    type Value = Self;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self, A::Error> {
        while seq.next_element::<Self>()?.is_some() {}
        Ok(self)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self, A::Error> {
        while map.next_entry::<Self, Self>()?.is_some() {}
        Ok(self)
    }
}
