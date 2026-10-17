use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor, value::SeqAccessDeserializer},
};

/// Exact workspace/request bytes; strings keep ordinary cases short.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum FileBytes {
    Text(String),
    Binary(Vec<u8>),
}

impl<'de> Deserialize<'de> for FileBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;
        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = FileBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("text or an array of bytes")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FileBytes::Text(value.into()))
            }

            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(FileBytes::Text(value))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, values: A) -> Result<Self::Value, A::Error> {
                Vec::deserialize(SeqAccessDeserializer::new(values)).map(FileBytes::Binary)
            }
        }
        deserializer.deserialize_any(BytesVisitor)
    }
}

impl FileBytes {
    pub(super) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Text(text) => text.as_bytes(),
            Self::Binary(bytes) => bytes,
        }
    }
}
