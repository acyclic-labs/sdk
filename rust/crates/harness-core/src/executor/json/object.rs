//! Preserve the original typed decoder while requiring its object wire shape.
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor, value::MapAccessDeserializer};
use std::fmt;

/// Struct derives also accept positional arrays. This seed forwards only a map
/// to the same derive, borrowing its original keys and values without buffering.
pub(crate) struct JsonObject<S>(pub S);

/// One complete object, with no intermediate JSON tree or trailing input.
pub(crate) fn decode_object<'de, T: serde::Deserialize<'de>>(
    bytes: &'de [u8],
) -> serde_json::Result<T> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = deserialize_object(&mut decoder)?;
    decoder.end()?;
    Ok(value)
}

/// Field adapter over the same seed, with the field's original value layout.
pub(crate) fn deserialize_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    JsonObject(std::marker::PhantomData::<T>).deserialize(deserializer)
}

/// A sequence of typed objects, decoded directly into its final vector.
pub(crate) fn deserialize_objects<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct Objects<T>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>> Visitor<'de> for Objects<T> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array of JSON objects")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
            let mut values = Vec::new();
            while let Some(value) =
                sequence.next_element_seed(JsonObject(std::marker::PhantomData::<T>))?
            {
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Objects(std::marker::PhantomData))
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
