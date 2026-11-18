//! Exact decimal strings for 64-bit positions on JSON client boundaries.
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use std::collections::BTreeMap;

/// Serialize a position without narrowing it to a JavaScript number.
pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

/// Admit canonical unsigned decimal strings only.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let text = String::deserialize(deserializer)?;
    if text.is_empty()
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(D::Error::custom("expected canonical decimal uint64 string"));
    }
    text.parse().map_err(D::Error::custom)
}

#[derive(Serialize, Deserialize)]
struct Decimal(#[serde(with = "self")] u64);

/// Preserve the same exact wire representation for maps of stream positions.
pub mod map {
    use super::*;
    use serde::ser::SerializeMap as _;

    /// Encode map values with the shared decimal position representation.
    pub fn serialize<K: Serialize, S: Serializer>(
        values: &BTreeMap<K, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(values.len()))?;
        for (key, value) in values {
            map.serialize_entry(key, &Decimal(*value))?;
        }
        map.end()
    }

    /// Decode canonical decimal positions without a floating-point intermediary.
    pub fn deserialize<'de, K: Deserialize<'de> + Ord, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<K, u64>, D::Error> {
        BTreeMap::<K, Decimal>::deserialize(deserializer).map(|values| {
            values
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect()
        })
    }
}
