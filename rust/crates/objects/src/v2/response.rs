//! Core response and persisted-metadata validation, independent of transport features.
use super::{Error, request, wire};

/// Validates a generated response against its original request, shared with WASM clients.
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

/// Decodes one download body frame of at most `remaining` selected bytes.
///
/// The declared length is checked against the per-frame and remaining bounds
/// before any allocation or decompression, and the decoded length must match it.
pub fn body(frame: wire::Body, remaining: u64) -> Result<Vec<u8>, Error> {
    let maximum = wire::ObjectsLimit::MaxBodyFrameBytes as u64;
    if frame.decoded_length > maximum.min(remaining) {
        return Err(invalid());
    }
    let length = usize::try_from(frame.decoded_length).map_err(|_| invalid())?;
    match wire::Codec::try_from(frame.codec) {
        Ok(wire::Codec::None) if frame.data.len() == length => Ok(frame.data),
        Ok(wire::Codec::Zstd) => zstd(&frame.data, length).ok_or_else(invalid),
        _ => Err(invalid()),
    }
}
/// Decompresses concatenated Zstandard frames to exactly `length` bytes.
fn zstd(mut input: &[u8], length: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut output = Vec::with_capacity(length);
    while !input.is_empty() {
        let allowed = (length - output.len()) as u64 + 1;
        let decoder = ruzstd::decoding::StreamingDecoder::new(&mut input).ok()?;
        decoder.take(allowed).read_to_end(&mut output).ok()?;
        if output.len() > length {
            return None;
        }
    }
    (output.len() == length).then_some(output)
}
/// Builds an uncompressed body frame for a provider that does not compress.
#[must_use]
pub fn plain_body(data: Vec<u8>) -> wire::Body {
    wire::Body {
        codec: wire::Codec::None as i32,
        decoded_length: data.len() as u64,
        data,
    }
}

#[cfg(test)]
pub(crate) fn zstd_body(data: &[u8]) -> wire::Body {
    wire::Body {
        codec: wire::Codec::Zstd as i32,
        decoded_length: data.len() as u64,
        data: ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest),
    }
}

#[cfg(test)]
mod body_tests {
    use super::{body, plain_body, wire, zstd_body};

    const MAXIMUM: u64 = wire::ObjectsLimit::MaxBodyFrameBytes as u64;

    fn pattern(length: usize) -> Vec<u8> {
        (0..=250u8).cycle().take(length).collect()
    }

    #[test]
    fn compressed_frame_decodes_to_the_same_bytes() {
        for length in [0, 1, 4096, 65_536] {
            let data = pattern(length);
            let frame = zstd_body(&data);
            assert!(length < 4096 || frame.data.len() < length);
            assert_eq!(body(frame, MAXIMUM).ok(), Some(data.clone()));
            assert_eq!(body(plain_body(data.clone()), MAXIMUM).ok(), Some(data));
        }
    }

    #[test]
    fn concatenated_zstd_frames_decode_in_order() {
        let (first, second) = (pattern(3000), vec![7; 5000]);
        let mut frame = zstd_body(&first);
        frame.data.extend(zstd_body(&second).data);
        frame.decoded_length = 8000;
        assert_eq!(body(frame, MAXIMUM).ok(), Some([first, second].concat()));
    }

    #[test]
    fn mixed_frames_reassemble_in_order() {
        let data = pattern(150_000);
        let frames = data
            .chunks(65_536)
            .enumerate()
            .map(|(index, chunk)| {
                if index % 2 == 0 {
                    zstd_body(chunk)
                } else {
                    plain_body(chunk.to_vec())
                }
            })
            .collect::<Vec<_>>();
        let mut remaining = data.len() as u64;
        let mut result = Vec::new();
        for frame in frames {
            let bytes = body(frame, remaining).expect("valid frame");
            remaining -= bytes.len() as u64;
            result.extend(bytes);
        }
        assert_eq!(remaining, 0);
        assert_eq!(result, data);
    }

    #[test]
    fn oversized_declared_length_is_refused_before_decompressing() {
        // Garbage data proves the length bound fails before any decompression.
        for (length, remaining) in [(MAXIMUM + 1, u64::MAX), (u64::MAX, u64::MAX), (10, 9)] {
            for codec in [wire::Codec::None, wire::Codec::Zstd] {
                let frame = wire::Body {
                    codec: codec as i32,
                    data: vec![0xff; 16],
                    decoded_length: length,
                };
                assert!(body(frame, remaining).is_err());
            }
        }
        // A frame that would expand beyond its declaration is cut off and refused.
        let mut frame = zstd_body(&vec![0; 65_536]);
        frame.decoded_length = 1024;
        assert!(body(frame, MAXIMUM).is_err());
    }

    #[test]
    fn mismatched_length_unknown_codec_and_corrupt_data_are_refused() {
        let mut short = zstd_body(&pattern(1000));
        short.decoded_length = 1001;
        assert!(body(short, MAXIMUM).is_err());
        let mut plain = plain_body(pattern(10));
        plain.decoded_length = 11;
        assert!(body(plain, MAXIMUM).is_err());
        let mut unknown = zstd_body(&pattern(10));
        unknown.codec = 2;
        assert!(body(unknown, MAXIMUM).is_err());
        let mut corrupt = zstd_body(&pattern(1000));
        corrupt.data.truncate(corrupt.data.len() / 2);
        assert!(body(corrupt, MAXIMUM).is_err());
    }
}
