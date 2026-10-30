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
    .map_err(|error| *error)
}
/// Encodes generated binary wire bytes through the canonical descriptor JSON mapping.
pub fn encode_binary(name: &str, bytes: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    if bytes.len() > maximum {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    acyclic_protobuf_json::encode(descriptor(name)?, bytes)
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
/// Decodes bounded descriptor JSON to generated binary wire bytes.
pub fn decode_binary(name: &str, bytes: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    Ok(decode_json(name, bytes, maximum)?.encode_to_vec())
}
fn decode_json(name: &str, bytes: &[u8], maximum: usize) -> Result<DynamicMessage, Error> {
    if bytes.len() > maximum {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let descriptor = descriptor(name)?;
    let mut json = serde_json::Deserializer::from_slice(bytes);
    let message = DynamicMessage::deserialize(descriptor, &mut json)
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    json.end()
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    Ok(message)
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
/// Decodes one bounded JSON message. Unknown fields, trailing JSON, and invalid wire values fail.
pub fn decode<T: Message + Default>(name: &str, bytes: &[u8], maximum: usize) -> Result<T, Error> {
    decode_json(name, bytes, maximum)?
        .transcode_to()
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uint64_and_bytes_follow_descriptor_json_mapping_exactly() -> Result<(), Error> {
        let value = wire::PutBlobRequest {
            digest: Some(wire::Digest {
                blake3: vec![7; 32],
            }),
            body: vec![0, 255].into(),
            references: Vec::new(),
        };
        let encoded = encode("PutBlobRequest", &value)?;
        let json: serde_json::Value = serde_json::from_slice(&encoded)
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
        assert_eq!(
            json.get("body").and_then(serde_json::Value::as_str),
            Some("AP8=")
        );
        assert_eq!(
            decode::<wire::PutBlobRequest>("PutBlobRequest", &encoded, 1024)?,
            value
        );
        let usage = wire::UsageResponse {
            blobs: 1,
            unique_bytes: 9_007_199_254_740_993,
        };
        let encoded = encode("UsageResponse", &usage)?;
        let json: serde_json::Value = serde_json::from_slice(&encoded)
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
        assert_eq!(
            json.get("uniqueBytes").and_then(serde_json::Value::as_str),
            Some("9007199254740993")
        );
        Ok(())
    }
    #[test]
    fn malformed_unknown_trailing_and_oversized_json_are_rejected() {
        for bytes in [
            br#"{"root":"AQ==","unknown":true}"#.as_slice(),
            br#"{"root":"AQ=="} {}"#,
            br#"{"root":12}"#,
        ] {
            assert!(decode::<wire::ReleaseRequest>("ReleaseRequest", bytes, 1024).is_err());
        }
        assert_eq!(
            decode::<wire::UsageRequest>("UsageRequest", b"{}", 1)
                .err()
                .map(|error| error.code),
            Some(wire::ErrorCode::QuotaExceeded)
        );
    }
}
