//! Native JavaScript projection of the same original status as the WASM adapter.
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use serde::{Serialize, Serializer};

/// A member's original status. Buffers transfer to JavaScript without another
/// copy; metadata retains duplicate keys and raw HTTP header bytes.
#[napi(object)]
#[derive(Serialize)]
pub struct NativeRefusal {
    /// Original numeric gRPC status.
    pub status: i32,
    /// Original status message.
    pub message: String,
    /// Original detail bytes, including malformed or unknown payloads.
    #[serde(serialize_with = "serialize_bytes")]
    pub details: Buffer,
    /// Product metadata, including binary values' original HTTP encoding.
    #[serde(serialize_with = "serialize_metadata")]
    pub metadata: Vec<(String, Buffer)>,
}

impl NativeRefusal {
    /// Copies each retained byte view once into the native JavaScript owner.
    /// No payload is decoded and no attribution is inferred.
    #[must_use]
    pub fn from_parts(parts: &super::Parts) -> Self {
        Self {
            status: parts.code,
            message: parts.message.clone(),
            details: parts.details.to_vec().into(),
            metadata: parts
                .metadata
                .iter()
                .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec().into()))
                .collect(),
        }
    }
}

fn serialize_bytes<S: Serializer>(bytes: &Buffer, serializer: S) -> Result<S::Ok, S::Error> {
    bytes.as_ref().serialize(serializer)
}

fn serialize_metadata<S: Serializer>(
    metadata: &[(String, Buffer)],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(metadata.iter().map(|(name, value)| (name, value.as_ref())))
}
