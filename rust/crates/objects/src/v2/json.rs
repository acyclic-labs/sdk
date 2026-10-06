//! Descriptor-authoritative Protobuf JSON mapping shared by native and WASM clients.
use super::{Error, FILE_DESCRIPTOR_SET, wire};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};

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
    let descriptor = pool()?
        .get_message_by_name(&format!("acyclic.objects.v2.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    let message = DynamicMessage::decode(descriptor, bytes)
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    serde_json::to_vec(&message).map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
/// Decodes bounded descriptor JSON to generated binary wire bytes.
pub fn decode_binary(name: &str, bytes: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    if bytes.len() > maximum {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let descriptor = pool()?
        .get_message_by_name(&format!("acyclic.objects.v2.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    let mut json = serde_json::Deserializer::from_slice(bytes);
    let message = DynamicMessage::deserialize(descriptor, &mut json)
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    json.end()
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    Ok(message.encode_to_vec())
}
/// Encodes a canonical message using its generated descriptor, without parallel JSON shapes.
/// Message names are unqualified Objects v2 type names from the canonical route inventory.
pub fn encode(name: &str, value: &impl Message) -> Result<Vec<u8>, Error> {
    let descriptor = pool()?
        .get_message_by_name(&format!("acyclic.objects.v2.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    let message = DynamicMessage::decode(descriptor, value.encode_to_vec().as_slice())
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    serde_json::to_vec(&message).map_err(|_| wire::ErrorCode::InvalidArgument.into())
}
/// Decodes one bounded JSON message. Unknown fields, trailing JSON, and invalid wire values fail.
pub fn decode<T: Message + Default>(name: &str, bytes: &[u8], maximum: usize) -> Result<T, Error> {
    if bytes.len() > maximum {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let descriptor = pool()?
        .get_message_by_name(&format!("acyclic.objects.v2.{name}"))
        .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    let mut json = serde_json::Deserializer::from_slice(bytes);
    let message = DynamicMessage::deserialize(descriptor, &mut json)
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    json.end()
        .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    message
        .transcode_to()
        .map_err(|_| wire::ErrorCode::InvalidArgument.into())
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
    fn streaming_record_vectors_match_the_objects_http_envelope() -> Result<(), Error> {
        let body = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Body(vec![0, 255])),
        };
        let body_bytes = encode("PutObjectRequest", &body)?;
        assert_eq!(body_bytes, br#"{"body":"AP8="}"#);
        assert!(body_bytes.len() < super::super::HTTP_JSON_FRAME_BYTES);

        let complete = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Complete(true)),
        };
        assert_eq!(
            encode("PutObjectRequest", &complete)?,
            br#"{"complete":true}"#
        );

        let error = wire::GetObjectResponse {
            frame: Some(wire::get_object_response::Frame::Error(wire::ErrorDetail {
                code: wire::ErrorCode::NotFound as i32,
                request_id: "fixture-request".into(),
            })),
        };
        let error_bytes = encode("GetObjectResponse", &error)?;
        let error_json: serde_json::Value = serde_json::from_slice(&error_bytes)
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
        assert_eq!(error_json["error"]["requestId"], "fixture-request");
        assert!(error_json["error"]["code"].is_string());
        assert!(error_bytes.len() < super::super::HTTP_JSON_FRAME_BYTES);
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
