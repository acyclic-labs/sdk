//! Preserve the original typed decoder while requiring its object wire shape.
use serde::de::{DeserializeSeed, MapAccess, Visitor, value::MapAccessDeserializer};
use std::fmt;

/// Struct derives also accept positional arrays. This seed forwards only a map
/// to the same derive, borrowing its original keys and values without buffering.
pub(crate) struct JsonObject<S>(pub S);

/// Field adapter over the same seed, with the field's original value layout.
pub(crate) fn deserialize_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    JsonObject(std::marker::PhantomData::<T>).deserialize(deserializer)
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for JsonObject<S> {
    type Value = S::Value;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de, S: DeserializeSeed<'de>> Visitor<'de> for JsonObject<S> {
    type Value = S::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M: MapAccess<'de>>(self, map: M) -> std::result::Result<Self::Value, M::Error> {
        self.0.deserialize(MapAccessDeserializer::new(map))
    }
}
