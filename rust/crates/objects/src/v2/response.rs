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
