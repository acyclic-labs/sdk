//! Descriptor-authoritative Protobuf JSON mapping shared by native and WASM clients.
use super::{Error, FILE_DESCRIPTOR_SET, wire};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
use std::sync::Arc;

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
    let message = decode_input(name, bytes)?;
    serde_json::to_vec(&message).map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
/// Decodes bounded descriptor JSON to generated binary wire bytes.
pub fn decode_binary(name: &str, bytes: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    Ok(decode_json(name, bytes, maximum)?.encode_to_vec())
}
fn decode_json(name: &str, bytes: &[u8], maximum: usize) -> Result<DynamicMessage, Error> {
    if bytes.len() > maximum {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let descriptor = pool()?
        .get_message_by_name(&format!("acyclic.objects.v1.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    let mut json = serde_json::Deserializer::from_slice(bytes);
    let message = DynamicMessage::deserialize(descriptor, &mut json)
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    json.end()
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    Ok(message)
}
/// Encodes a canonical message using its generated descriptor, without parallel JSON shapes.
pub fn encode(name: &str, value: &impl Message) -> Result<Vec<u8>, Error> {
    let message = decode_input(name, &value.encode_to_vec())?;
    serde_json::to_vec(&message).map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
fn decode_input(name: &str, bytes: &[u8]) -> Result<DynamicMessage, Error> {
    let descriptor = pool()?
        .get_message_by_name(&format!("acyclic.objects.v1.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    DynamicMessage::decode(descriptor, bytes)
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))
}
/// Decodes one bounded JSON message. Unknown fields, trailing JSON, and invalid wire values fail.
pub fn decode<T: Message + Default>(name: &str, bytes: &[u8], maximum: usize) -> Result<T, Error> {
    decode_json(name, bytes, maximum)?
        .transcode_to()
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
}

#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub(crate) fn encode_native(
    name: &str,
    query: &impl Message,
    controls: &Arc<super::NativeTransportErrors>,
) -> Result<(Vec<u8>, acyclic_native_runtime::ReadAllocation), Error> {
    let maximum = query
        .encoded_len()
        .checked_mul(12)
        .and_then(|value| value.checked_add(256))
        .ok_or_else(super::response::invalid)?;
    let mut allocation = acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, maximum)
        .map_err(|error| controls.fail(error))?;
    controls
        .meter
        .admit_codec(maximum as u64, 0, 0)
        .map_err(|error| controls.fail(error))?;
    let binary = controls.binary(query)?;
    let message = decode_input(name, &binary.0)?;
    drop(binary);
    let bytes =
        serde_json::to_vec(&message).map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    drop(message);
    if let Err(error) = allocation.reconcile_capacity(bytes.capacity()) {
        return Err(
            controls.fail(super::NativeObjectsError::from(error).with_wire(bytes, allocation))
        );
    }
    if let Err(error) = controls.meter.record_codec(bytes.len() as u64, 0, 0) {
        return Err(
            controls.fail(super::NativeObjectsError::from(error).with_wire(bytes, allocation))
        );
    }
    Ok((bytes, allocation))
}

#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub(crate) fn decode_get_native(
    bytes: &[u8],
    maximum: usize,
    controls: &Arc<super::NativeTransportErrors>,
    mut allocation: acyclic_native_runtime::ReadAllocation,
) -> Result<
    (
        wire::GetObjectResponse,
        acyclic_native_runtime::ReadAllocation,
    ),
    Error,
> {
    let frame: wire::GetObjectResponse = decode("GetObjectResponse", bytes, maximum)?;
    let capacity = match &frame.frame {
        Some(wire::get_object_response::Frame::Header(header)) => {
            super::response::header_capacity(header)
        }
        Some(wire::get_object_response::Frame::Body(body)) => body.data.capacity(),
        Some(wire::get_object_response::Frame::Error(error)) => error.request_id.capacity(),
        None => 0,
    };
    if let Err(error) = allocation.reconcile_capacity(capacity) {
        let cause = super::NativeObjectsError::from(error);
        let cause = match frame.frame {
            Some(wire::get_object_response::Frame::Header(header)) => {
                cause.with_header(header, allocation)
            }
            Some(wire::get_object_response::Frame::Body(body)) => {
                cause.with_wire(body.data, allocation)
            }
            Some(wire::get_object_response::Frame::Error(detail)) => {
                cause.with_wire(detail.request_id.into_bytes(), allocation)
            }
            None => cause,
        };
        return Err(controls.fail(cause));
    }
    Ok((frame, allocation))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uint64_and_bytes_follow_descriptor_json_mapping_exactly() -> Result<(), Error> {
        let part = wire::UploadedPart {
            part_number: 7,
            etag: "opaque".into(),
            size: 9_007_199_254_740_993,
        };
        let encoded = encode("UploadedPart", &part)?;
        let json: serde_json::Value = serde_json::from_slice(&encoded)
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
        assert_eq!(
            json.get("size").and_then(serde_json::Value::as_str),
            Some("9007199254740993")
        );
        assert_eq!(
            decode::<wire::UploadedPart>("UploadedPart", &encoded, 1024)?,
            part
        );
        let frame = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Body(vec![0, 255])),
        };
        let encoded = encode("PutObjectRequest", &frame)?;
        let json: serde_json::Value = serde_json::from_slice(&encoded)
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
        assert_eq!(
            json.get("body").and_then(serde_json::Value::as_str),
            Some("AP8=")
        );
        assert_eq!(
            decode::<wire::PutObjectRequest>("PutObjectRequest", &encoded, 1024)?,
            frame
        );
        Ok(())
    }
    #[test]
    fn malformed_unknown_trailing_and_oversized_json_are_rejected() {
        for bytes in [
            br#"{"name":"customer.inputs","unknown":true}"#.as_slice(),
            br#"{"name":"customer.inputs"} {}"#,
            br#"{"name":12}"#,
        ] {
            assert!(
                decode::<wire::CreateBucketRequest>("CreateBucketRequest", bytes, 1024).is_err()
            );
        }
        assert_eq!(
            decode::<wire::Bucket>("Bucket", b"{}", 1)
                .err()
                .map(|error| error.code),
            Some(wire::ErrorCode::QuotaExceeded)
        );
    }
}
