//! Descriptor-authoritative Protobuf JSON mapping shared by native and WASM clients.
use crate::error::{invalid, unavailable};
use crate::{Error, FILE_DESCRIPTOR_SET, wire};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

fn pool() -> Result<&'static DescriptorPool, Error> {
    static POOL: std::sync::OnceLock<Option<DescriptorPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| DescriptorPool::decode(FILE_DESCRIPTOR_SET).ok())
        .as_ref()
        .ok_or_else(unavailable)
}
/// Encodes generated binary wire bytes through the canonical descriptor JSON mapping.
pub fn encode_binary(name: &str, bytes: &[u8], maximum: Option<usize>) -> Result<Vec<u8>, Error> {
    if maximum.is_some_and(|maximum| bytes.len() > maximum) {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    acyclic_protobuf_json::encode(descriptor(name)?, bytes).map_err(|_| invalid())
}
/// Decodes bounded descriptor JSON to generated binary wire bytes.
pub fn decode_binary(name: &str, bytes: &[u8], maximum: Option<usize>) -> Result<Vec<u8>, Error> {
    Ok(decode_json(name, bytes, maximum)?.encode_to_vec())
}
fn decode_json(name: &str, bytes: &[u8], maximum: Option<usize>) -> Result<DynamicMessage, Error> {
    if maximum.is_some_and(|maximum| bytes.len() > maximum) {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    acyclic_protobuf_json::decode_admitted(descriptor(name)?, bytes, |_, _| invalid())
}
/// Encodes a canonical message using its generated descriptor, without parallel JSON shapes.
pub fn encode(name: &str, value: &impl Message) -> Result<Vec<u8>, Error> {
    encode_binary(name, &value.encode_to_vec(), None)
}
fn descriptor(name: &str) -> Result<MessageDescriptor, Error> {
    pool()?
        .get_message_by_name(&format!("acyclic.objects.v1.{name}"))
        .ok_or_else(invalid)
}
/// Decodes one JSON message under an optional byte policy.
/// Unknown fields, trailing JSON, and invalid wire values fail.
pub fn decode<T: Message + Default>(
    name: &str,
    bytes: &[u8],
    maximum: Option<usize>,
) -> Result<T, Error> {
    decode_json(name, bytes, maximum)?
        .transcode_to()
        .map_err(|_| invalid())
}
