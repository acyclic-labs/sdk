//! Core response and persisted-metadata validation, independent of transport features.
use super::{Error, request, wire};

/// Validates a generated response against its original request, shared with WASM clients.
#[allow(clippy::too_many_lines)]
pub fn validate_binary(
    route: &str,
    query: &[u8],
    bytes: &[u8],
    body_length: u64,
) -> Result<(), Error> {
    use prost::Message;
    macro_rules! decode {
        ($name:ident, $bytes:expr) => {
            wire::$name::decode($bytes).map_err(|_| invalid())?
        };
    }
    match route {
        "buckets/create" => {
            let query = decode!(CreateBucketRequest, query);
            bucket(
                &decode!(Bucket, bytes),
                &wire::BucketRef { name: query.name },
            )?;
        }
        "buckets/head" => {
            let query = decode!(HeadBucketRequest, query);
            bucket(
                &decode!(Bucket, bytes),
                query.bucket.as_ref().ok_or_else(invalid)?,
            )?;
        }
        "objects/put" => {
            let info = decode!(ObjectInfo, bytes);
            object_info(&info)?;
            if info.size != body_length {
                return Err(invalid());
            }
        }
        "objects/get" => {
            let query = decode!(GetObjectRequest, query);
            let frame = decode!(GetObjectResponse, bytes);
            match frame.frame.ok_or_else(invalid)? {
                wire::get_object_response::Frame::Header(header) => {
                    get_header(&header, &query.range, body_length)?;
                }
                wire::get_object_response::Frame::Body(body) => {
                    if body.len() > super::HTTP_BODY_FRAME_BYTES {
                        return Err(invalid());
                    }
                }
                wire::get_object_response::Frame::Error(detail) => {
                    let code = wire::ErrorCode::try_from(detail.code).map_err(|_| invalid())?;
                    if code == wire::ErrorCode::Unspecified {
                        return Err(invalid());
                    }
                }
            }
        }
        "objects/head" => {
            let info = decode!(HeadObjectResponse, bytes);
            object_info(info.object.as_ref().ok_or_else(invalid)?)?;
        }
        "objects/list" => listing(
            &decode!(ListObjectsRequest, query),
            &decode!(ListObjectsResponse, bytes),
        )?,
        "multipart/create" => {
            let upload = decode!(MultipartUpload, bytes);
            if request::upload_id(&upload.upload_id).is_err() {
                return Err(invalid());
            }
        }
        "multipart/upload-part" => {
            let query = decode!(UploadPartHeader, query);
            let part = decode!(UploadedPart, bytes);
            if part.part_number != query.part_number
                || part.size != body_length
                || part.etag.is_empty()
                || part.etag.len() > 8192
                || part.etag.contains(['\r', '\n', '\0'])
            {
                return Err(invalid());
            }
        }
        "multipart/list-parts" => parts(
            &decode!(ListPartsRequest, query),
            &decode!(ListPartsResponse, bytes),
        )?,
        "multipart/complete" => object_info(&decode!(ObjectInfo, bytes))?,
        "buckets/delete" => {
            decode!(DeleteBucketResponse, bytes);
        }
        "objects/delete" => {
            decode!(DeleteObjectResponse, bytes);
        }
        "multipart/abort" => {
            decode!(AbortMultipartResponse, bytes);
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

/// Validates the first download frame and returns its exact selected decoded size.
pub fn validate_get_header(query: &[u8], bytes: &[u8], maximum: u64) -> Result<u64, Error> {
    use prost::Message;
    let query = wire::GetObjectRequest::decode(query).map_err(|_| invalid())?;
    let header = wire::GetObjectHeader::decode(bytes).map_err(|_| invalid())?;
    get_header(&header, &query.range, maximum)
}

/// Validates one bounded download body frame and returns the remaining bytes.
/// Native and WASM transports use this same state transition so frame-size and
/// selected-range accounting cannot drift between adapters.
pub fn validate_get_body(body_length: u64, remaining: u64) -> Result<u64, Error> {
    if body_length > super::HTTP_BODY_FRAME_BYTES as u64 || body_length > remaining {
        return Err(invalid());
    }
    Ok(remaining - body_length)
}

/// Maps one HTTP status and optional wire error detail to the canonical
/// Objects error vocabulary used by native and browser transports.
pub fn http_error_code(status: u16, detail: Option<i32>) -> wire::ErrorCode {
    if let Some(detail) = detail
        && let Ok(code) = wire::ErrorCode::try_from(detail)
        && code != wire::ErrorCode::Unspecified
    {
        return code;
    }
    match status {
        304 => wire::ErrorCode::NotModified,
        400 => wire::ErrorCode::InvalidArgument,
        401 | 403 => wire::ErrorCode::AccessDenied,
        404 => wire::ErrorCode::NotFound,
        409 => wire::ErrorCode::AlreadyExists,
        412 => wire::ErrorCode::PreconditionFailed,
        413 | 429 => wire::ErrorCode::QuotaExceeded,
        416 => wire::ErrorCode::RangeNotSatisfiable,
        501 => wire::ErrorCode::Unsupported,
        _ => wire::ErrorCode::Unavailable,
    }
}

/// Maps a Connect/tonic status and optional wire detail to the canonical
/// Objects error vocabulary before it crosses a native or WASM boundary.
pub fn grpc_error_code(status: u32, detail: Option<i32>) -> wire::ErrorCode {
    if let Some(detail) = detail
        && let Ok(code) = wire::ErrorCode::try_from(detail)
        && code != wire::ErrorCode::Unspecified
    {
        return code;
    }
    match status {
        3 => wire::ErrorCode::InvalidArgument,
        5 => wire::ErrorCode::NotFound,
        6 => wire::ErrorCode::AlreadyExists,
        7 | 16 => wire::ErrorCode::AccessDenied,
        8 => wire::ErrorCode::QuotaExceeded,
        9 => wire::ErrorCode::PreconditionFailed,
        12 => wire::ErrorCode::Unsupported,
        _ => wire::ErrorCode::Unavailable,
    }
}

/// Validates the endpoint and transport policy shared by native and WASM HTTP clients.
pub fn validate_http_endpoint(endpoint: &str) -> Result<(), Error> {
    let endpoint = url::Url::parse(endpoint).map_err(|_| wire::ErrorCode::InvalidArgument)?;
    let loopback = matches!(
        endpoint.host_str(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    );
    if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(wire::ErrorCode::InvalidArgument.into());
    }
    Ok(())
}

pub(crate) fn invalid() -> Error {
    wire::ErrorCode::Unavailable.into()
}
/// Validates the canonical protobuf timestamp range and nanosecond normalization.
pub fn timestamp(value: &prost_types::Timestamp) -> Result<(), Error> {
    if !(-62_135_596_800..=253_402_300_799).contains(&value.seconds)
        || !(0..1_000_000_000).contains(&value.nanos)
    {
        return Err(invalid());
    }
    Ok(())
}
/// Validates current-object metadata, opaque `ETag` and timestamp.
pub fn object_info(value: &wire::ObjectInfo) -> Result<(), Error> {
    let timestamp = value.last_modified.as_ref().ok_or_else(invalid)?;
    self::timestamp(timestamp)?;
    if value.etag.is_empty() || value.etag.len() > 8192 || value.etag.contains(['\r', '\n', '\0']) {
        return Err(invalid());
    }
    request::metadata(&value.metadata).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use super::{grpc_error_code, http_error_code, validate_get_body, validate_http_endpoint};
    use crate::v2::wire;

    #[test]
    fn http_status_projection_prefers_wire_detail_and_covers_object_statuses() {
        assert_eq!(
            http_error_code(500, Some(wire::ErrorCode::NotFound as i32)),
            wire::ErrorCode::NotFound
        );
        assert_eq!(http_error_code(304, None), wire::ErrorCode::NotModified);
        assert_eq!(http_error_code(401, None), wire::ErrorCode::AccessDenied);
        assert_eq!(
            http_error_code(412, None),
            wire::ErrorCode::PreconditionFailed
        );
        assert_eq!(
            http_error_code(416, None),
            wire::ErrorCode::RangeNotSatisfiable
        );
        assert_eq!(http_error_code(503, None), wire::ErrorCode::Unavailable);
    }

    #[test]
    fn http_endpoint_policy_allows_https_and_loopback_http_only() {
        for endpoint in [
            "https://objects.example",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
        ] {
            assert!(validate_http_endpoint(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "http://objects.example",
            "https://user@objects.example",
            "https://objects.example/?query=1",
            "https://objects.example/#fragment",
        ] {
            assert!(validate_http_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn download_body_accounting_is_bounded_and_request_relative() {
        assert_eq!(
            validate_get_body(super::super::HTTP_BODY_FRAME_BYTES as u64, 100_000).unwrap(),
            34_464
        );
        assert!(validate_get_body(65_537, 100_000).is_err());
        assert!(validate_get_body(2, 1).is_err());
    }

    #[test]
    fn grpc_status_projection_prefers_wire_detail_and_covers_connect_codes() {
        assert_eq!(
            grpc_error_code(14, Some(wire::ErrorCode::NotFound as i32)),
            wire::ErrorCode::NotFound
        );
        assert_eq!(grpc_error_code(3, None), wire::ErrorCode::InvalidArgument);
        assert_eq!(grpc_error_code(16, None), wire::ErrorCode::AccessDenied);
        assert_eq!(grpc_error_code(8, None), wire::ErrorCode::QuotaExceeded);
        assert_eq!(grpc_error_code(12, None), wire::ErrorCode::Unsupported);
        assert_eq!(grpc_error_code(14, None), wire::ErrorCode::Unavailable);
    }
}
/// Validates a complete or ranged download selection within its decoded allocation bound.
pub fn get_header(
    header: &wire::GetObjectHeader,
    selected: &Option<wire::ByteRange>,
    maximum: u64,
) -> Result<u64, Error> {
    let object = header.object.as_ref().ok_or_else(invalid)?;
    object_info(object)?;
    let expected = request::range(selected, object.size).map_err(|_| invalid())?;
    if expected != header.content_range {
        return Err(invalid());
    }
    let size = expected.map_or(object.size, |range| range.end - range.start + 1);
    if size > maximum || usize::try_from(size).is_err() {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(size)
}
/// Validates persisted bucket metadata against its canonical logical identity.
pub fn bucket(value: &wire::Bucket, expected: &wire::BucketRef) -> Result<(), Error> {
    request::bucket_name(&expected.name).map_err(|_| invalid())?;
    if value.bucket.as_ref() != Some(expected) || value.created_at.is_none() {
        return Err(invalid());
    }
    timestamp(value.created_at.as_ref().ok_or_else(invalid)?)?;
    Ok(())
}
/// Validates ordered bounded multipart results against the original page request.
pub fn parts(
    query: &wire::ListPartsRequest,
    response: &wire::ListPartsResponse,
) -> Result<(), Error> {
    let limit = request::page_size(query.page_size)?;
    let mut previous = query.after_part_number;
    for part in &response.parts {
        if part.part_number <= previous
            || part.part_number > 10_000
            || part.etag.is_empty()
            || part.etag.len() > 8192
            || part.etag.contains(['\r', '\n', '\0'])
            || part.size > 5 * 1024 * 1024 * 1024
        {
            return Err(invalid());
        }
        previous = part.part_number;
    }
    if response.parts.len() > limit
        || response.is_truncated
            && (response.parts.is_empty() || response.next_part_number != previous)
        || !response.is_truncated && response.next_part_number != 0
    {
        return Err(invalid());
    }
    Ok(())
}
/// Validates an eventual listing's keys, prefixes, page bounds and continuation shape.
pub fn listing(
    query: &wire::ListObjectsRequest,
    response: &wire::ListObjectsResponse,
) -> Result<(), Error> {
    let limit = request::page_size(query.page_size)?;
    if response.entries.len() + response.common_prefixes.len() > limit
        || response.is_truncated == response.continuation_token.is_empty()
        || response.continuation_token.len() > 8192
    {
        return Err(invalid());
    }
    let mut previous: Option<&str> = None;
    for entry in &response.entries {
        request::key(&entry.object_key).map_err(|_| invalid())?;
        let suffix = entry
            .object_key
            .strip_prefix(&query.prefix)
            .ok_or_else(invalid)?;
        if previous.is_some_and(|previous| previous >= entry.object_key.as_str())
            || !query.delimiter.is_empty() && suffix.contains(&query.delimiter)
        {
            return Err(invalid());
        }
        previous = Some(&entry.object_key);
        object_info(entry.object.as_ref().ok_or_else(invalid)?)?;
    }
    previous = None;
    for prefix in &response.common_prefixes {
        let suffix = prefix.strip_prefix(&query.prefix).ok_or_else(invalid)?;
        if query.delimiter.is_empty()
            || !suffix.ends_with(&query.delimiter)
            || suffix
                .strip_suffix(&query.delimiter)
                .is_none_or(|value| value.contains(&query.delimiter))
            || previous.is_some_and(|previous| previous >= prefix.as_str())
        {
            return Err(invalid());
        }
        previous = Some(prefix);
    }
    Ok(())
}
