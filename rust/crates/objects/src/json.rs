//! Descriptor-authoritative Protobuf JSON mapping shared by native and WASM clients.
use crate::{Error, FILE_DESCRIPTOR_SET, wire};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

fn pool() -> Result<&'static DescriptorPool, Error> {
    static POOL: std::sync::OnceLock<Result<DescriptorPool, Error>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).map_err(|_| wire::ErrorCode::Unavailable.into())
    })
    .as_ref()
    .map_err(Clone::clone)
}
/// Encodes generated binary wire bytes through the canonical descriptor JSON mapping.
pub fn encode_binary(name: &str, bytes: &[u8], maximum: Option<usize>) -> Result<Vec<u8>, Error> {
    if maximum.is_some_and(|maximum| bytes.len() > maximum) {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    acyclic_protobuf_json::encode(descriptor(name)?, bytes)
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
/// Decodes bounded descriptor JSON to generated binary wire bytes.
pub fn decode_binary(name: &str, bytes: &[u8], maximum: Option<usize>) -> Result<Vec<u8>, Error> {
    Ok(decode_json(name, bytes, maximum)?.encode_to_vec())
}
fn decode_json(name: &str, bytes: &[u8], maximum: Option<usize>) -> Result<DynamicMessage, Error> {
    if maximum.is_some_and(|maximum| bytes.len() > maximum) {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    acyclic_protobuf_json::decode_admitted(descriptor(name)?, bytes, |_, _| {
        Error::from(wire::ErrorCode::InvalidArgument)
    })
}
/// Encodes a canonical message using its generated descriptor, without parallel JSON shapes.
pub fn encode(name: &str, value: &impl Message) -> Result<Vec<u8>, Error> {
    acyclic_protobuf_json::encode(descriptor(name)?, &value.encode_to_vec())
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
fn descriptor(name: &str) -> Result<MessageDescriptor, Error> {
    pool()?
        .get_message_by_name(&format!("acyclic.objects.v1.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))
}
/// Decodes one JSON message under an optional byte policy.
/// Unknown fields, trailing JSON, and invalid wire values fail.
pub fn decode<T: Message + Default>(
    name: &str,
    bytes: &[u8],
    maximum: impl Into<Option<usize>>,
) -> Result<T, Error> {
    decode_json(name, bytes, maximum.into())?
        .transcode_to()
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
