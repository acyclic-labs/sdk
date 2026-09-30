//! Shared validation for every logical Objects provider and transport.
use super::{Error, wire};
use wire::ErrorCode::InvalidArgument;

/// Canonical body/completion discipline after a validated upload header.
#[derive(Default)]
pub struct UploadFraming {
    size: u64,
    complete: bool,
}
impl UploadFraming {
    /// Accepts a bounded decoded body frame before completion.
    pub fn body(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.complete || bytes.len() > 65_536 {
            return Err(InvalidArgument.into());
        }
        self.size = self
            .size
            .checked_add(bytes.len() as u64)
            .ok_or(Error::from(wire::ErrorCode::QuotaExceeded))?;
        upload_length(self.size)
    }
    /// Accepts exactly one explicit true completion marker.
    pub fn complete(&mut self, complete: bool) -> Result<(), Error> {
        if self.complete || !complete {
            return Err(InvalidArgument.into());
        }
        self.complete = true;
        Ok(())
    }
    /// Requires valid EOF after completion and returns the exact decoded size.
    pub fn finish(self) -> Result<u64, Error> {
        if !self.complete {
            return Err(InvalidArgument.into());
        }
        Ok(self.size)
    }
}

/// Canonical incremental identity for a streamed publication. Frame boundaries are excluded.
pub struct BodyDigest {
    header: Vec<u8>,
    operation: &'static str,
    body: blake3::Hasher,
    bytes: u64,
}
impl BodyDigest {
    /// Adds decoded body bytes, enforcing the single-put/part size ceiling.
    pub fn update(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let size = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(Error::from(wire::ErrorCode::QuotaExceeded))?;
        upload_length(size)?;
        self.body.update(bytes);
        self.bytes = size;
        Ok(())
    }
    /// Returns the versioned retry digest after the complete body has arrived.
    #[must_use]
    pub fn finish(self) -> [u8; 32] {
        finish_digest(
            self.operation,
            &self.header,
            self.bytes,
            *self.body.finalize().as_bytes(),
        )
    }
}
/// Checks the maximum decoded size of a single PUT or multipart part.
pub fn upload_length(size: u64) -> Result<(), Error> {
    if size > 5 * 1024 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(())
}

/// Validates a canonical binary request before any transport or provider side effect.
/// Upload routes accept their generated header message, with the decoded body length separate.
#[allow(clippy::too_many_lines)]
pub fn validate_binary(route: &str, bytes: &[u8], body_length: u64) -> Result<(), Error> {
    use prost::Message;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    if body_length != 0 && !matches!(route, "objects/put" | "multipart/upload-part") {
        return Err(InvalidArgument.into());
    }
    macro_rules! decode {
        ($name:ident) => {
            wire::$name::decode(bytes).map_err(|_| Error::from(InvalidArgument))?
        };
    }
    match route {
        "buckets/create" => {
            create_bucket_digest(&decode!(CreateBucketRequest))?;
        }
        "buckets/head" => {
            bucket(&decode!(HeadBucketRequest).bucket)?;
        }
        "buckets/delete" => {
            delete_bucket_digest(&decode!(DeleteBucketRequest))?;
        }
        "objects/put" => {
            put_stream_digest(&decode!(PutObjectHeader))?;
            upload_length(body_length)?;
        }
        "objects/get" => {
            let value = decode!(GetObjectRequest);
            bucket(&value.bucket)?;
            key(&value.object_key)?;
            read_filters(&value.if_match, &value.if_none_match)?;
            if let Some(range) = value.range {
                match range.selection {
                    Some(wire::byte_range::Selection::Bytes(range))
                        if range.end.is_none_or(|end| end >= range.start) => {}
                    Some(wire::byte_range::Selection::SuffixLength(length)) if length > 0 => (),
                    _ => return Err(wire::ErrorCode::RangeNotSatisfiable.into()),
                }
            }
        }
        "objects/head" => {
            let value = decode!(HeadObjectRequest);
            bucket(&value.bucket)?;
            key(&value.object_key)?;
            read_filters(&value.if_match, &value.if_none_match)?;
        }
        "objects/delete" => {
            delete_digest(&decode!(DeleteObjectRequest))?;
        }
        "objects/list" => {
            let value = decode!(ListObjectsRequest);
            bucket(&value.bucket)?;
            page_size(value.page_size)?;
            if value.prefix.len() > 1024
                || value.delimiter.len() > 1024
                || value.prefix.contains('\0')
                || value.delimiter.contains('\0')
                || value.continuation_token.len() > 8192
                || value.continuation_token.contains('\0')
            {
                return Err(InvalidArgument.into());
            }
        }
        "multipart/create" => {
            create_multipart_digest(&decode!(CreateMultipartRequest))?;
        }
        "multipart/upload-part" => {
            upload_part_stream_digest(&decode!(UploadPartHeader))?;
            upload_length(body_length)?;
        }
        "multipart/list-parts" => {
            let value = decode!(ListPartsRequest);
            bucket(&value.bucket)?;
            key(&value.object_key)?;
            upload_id(&value.upload_id)?;
            page_size(value.page_size)?;
            if value.after_part_number > 10_000 {
                return Err(InvalidArgument.into());
            }
        }
        "multipart/complete" => {
            complete_multipart_digest(&decode!(CompleteMultipartRequest))?;
        }
        "multipart/abort" => {
            abort_multipart_digest(&decode!(AbortMultipartRequest))?;
        }
        _ => return Err(InvalidArgument.into()),
    }
    Ok(())
}
fn finish_digest(operation: &str, header: &[u8], length: u64, body: [u8; 32]) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"acyclic.objects.v2.mutation\0");
    digest.update(&(operation.len() as u64).to_le_bytes());
    digest.update(operation.as_bytes());
    digest.update(&(header.len() as u64).to_le_bytes());
    digest.update(header);
    digest.update(&length.to_le_bytes());
    digest.update(&body);
    *digest.finalize().as_bytes()
}
fn message_digest(operation: &str, message: &impl prost::Message) -> [u8; 32] {
    finish_digest(
        operation,
        &message.encode_to_vec(),
        0,
        *blake3::hash(&[]).as_bytes(),
    )
}
/// Starts canonical header/body hashing for a streaming PUT. Retry identity is excluded.
pub fn put_stream_digest(header: &wire::PutObjectHeader) -> Result<BodyDigest, Error> {
    bucket(&header.bucket)?;
    key(&header.object_key)?;
    metadata(&header.metadata)?;
    preconditions(&header.preconditions)?;
    identity(&header.mutation)?;
    let mut normalized = header.clone();
    normalized.mutation = None;
    Ok(BodyDigest {
        header: prost::Message::encode_to_vec(&normalized),
        operation: "put",
        body: blake3::Hasher::new(),
        bytes: 0,
    })
}
/// Validates a PUT and hashes exactly the complete decoded bytes.
pub fn put_digest(header: &wire::PutObjectHeader, body: &[u8]) -> Result<[u8; 32], Error> {
    let mut digest = put_stream_digest(header)?;
    digest.update(body)?;
    Ok(digest.finish())
}
/// Starts canonical header/body hashing for one staged multipart part.
pub fn upload_part_stream_digest(header: &wire::UploadPartHeader) -> Result<BodyDigest, Error> {
    bucket(&header.bucket)?;
    key(&header.object_key)?;
    upload_id(&header.upload_id)?;
    identity(&header.mutation)?;
    if !(1..=10_000).contains(&header.part_number) {
        return Err(InvalidArgument.into());
    }
    let mut normalized = header.clone();
    normalized.mutation = None;
    Ok(BodyDigest {
        header: prost::Message::encode_to_vec(&normalized),
        operation: "upload_part",
        body: blake3::Hasher::new(),
        bytes: 0,
    })
}
/// Validates a part publication and hashes its complete decoded bytes.
pub fn upload_part_digest(header: &wire::UploadPartHeader, body: &[u8]) -> Result<[u8; 32], Error> {
    let mut digest = upload_part_stream_digest(header)?;
    digest.update(body)?;
    Ok(digest.finish())
}
/// Validates and hashes logical bucket creation, excluding the caller retry key.
pub fn create_bucket_digest(value: &wire::CreateBucketRequest) -> Result<[u8; 32], Error> {
    bucket_name(&value.name)?;
    identity(&value.mutation)?;
    let mut value = value.clone();
    value.mutation = None;
    Ok(message_digest("create_bucket", &value))
}
/// Validates and hashes logical bucket deletion, excluding the caller retry key.
pub fn delete_bucket_digest(value: &wire::DeleteBucketRequest) -> Result<[u8; 32], Error> {
    bucket(&value.bucket)?;
    identity(&value.mutation)?;
    let mut value = value.clone();
    value.mutation = None;
    Ok(message_digest("delete_bucket", &value))
}
/// Validates and hashes current-value deletion, excluding the caller retry key.
pub fn delete_digest(value: &wire::DeleteObjectRequest) -> Result<[u8; 32], Error> {
    bucket(&value.bucket)?;
    key(&value.object_key)?;
    preconditions(&value.preconditions)?;
    identity(&value.mutation)?;
    let mut value = value.clone();
    value.mutation = None;
    Ok(message_digest("delete", &value))
}
/// Validates and hashes staged multipart creation, excluding the caller retry key.
pub fn create_multipart_digest(value: &wire::CreateMultipartRequest) -> Result<[u8; 32], Error> {
    bucket(&value.bucket)?;
    key(&value.object_key)?;
    metadata(&value.metadata)?;
    identity(&value.mutation)?;
    let mut value = value.clone();
    value.mutation = None;
    Ok(message_digest("create_multipart", &value))
}
/// Validates ordered part selection and hashes completion. State/receipt checks remain atomic at the provider.
pub fn complete_multipart_digest(
    value: &wire::CompleteMultipartRequest,
) -> Result<[u8; 32], Error> {
    bucket(&value.bucket)?;
    key(&value.object_key)?;
    upload_id(&value.upload_id)?;
    preconditions(&value.preconditions)?;
    identity(&value.mutation)?;
    if value.parts.is_empty() || value.parts.len() > 10_000 {
        return Err(InvalidArgument.into());
    }
    let mut previous = 0;
    for part in &value.parts {
        if part.part_number <= previous
            || part.part_number > 10_000
            || part.etag.is_empty()
            || part.etag.len() > 8192
            || part.etag.contains(['\r', '\n', '\0'])
            || part.size > 5 * 1024 * 1024 * 1024
        {
            return Err(InvalidArgument.into());
        }
        previous = part.part_number;
    }
    let mut value = value.clone();
    value.mutation = None;
    Ok(message_digest("complete_multipart", &value))
}
/// Validates and hashes multipart cancellation, excluding the caller retry key.
pub fn abort_multipart_digest(value: &wire::AbortMultipartRequest) -> Result<[u8; 32], Error> {
    bucket(&value.bucket)?;
    key(&value.object_key)?;
    upload_id(&value.upload_id)?;
    identity(&value.mutation)?;
    let mut value = value.clone();
    value.mutation = None;
    Ok(message_digest("abort_multipart", &value))
}

/// Validates an S3 general-purpose logical bucket name, including dotted names.
pub fn bucket_name(name: &str) -> Result<(), Error> {
    let edge = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !(3..=63).contains(&name.len())
        || !name.as_bytes().first().is_some_and(|byte| edge(*byte))
        || !name.as_bytes().last().is_some_and(|byte| edge(*byte))
        || !name
            .bytes()
            .all(|byte| edge(byte) || matches!(byte, b'-' | b'.'))
        || name.contains("..")
        || name.parse::<std::net::Ipv4Addr>().is_ok()
        || ["xn--", "sthree-", "amzn-s3-demo-"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
        || ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|suffix| name.ends_with(suffix))
    {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Requires a logical bucket reference and validates its name.
pub fn bucket(value: &Option<wire::BucketRef>) -> Result<&str, Error> {
    let name = &value.as_ref().ok_or(Error::from(InvalidArgument))?.name;
    bucket_name(name)?;
    Ok(name)
}
/// Checks a nonempty UTF-8 object key without normalizing slashes or dot segments.
pub fn key(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > wire::ObjectsLimit::MaxKeyBytes as usize
        || value.contains('\0')
    {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Validates an optional caller retry identity; no expiry is inferred by this validator.
pub fn identity(value: &Option<wire::MutationIdentity>) -> Result<(), Error> {
    if value.as_ref().is_some_and(|value| {
        value.idempotency_key.is_empty()
            || value.idempotency_key.len() > wire::ObjectsLimit::MaxIdempotencyKeyBytes as usize
            || value.idempotency_key.contains('\0')
    }) {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Rejects absent/false condition variants and empty `ETags` before mutation.
pub fn preconditions(value: &Option<wire::Preconditions>) -> Result<(), Error> {
    match value {
        None => Ok(()),
        Some(value) => match &value.condition {
            Some(wire::preconditions::Condition::IfAbsent(true)) => Ok(()),
            Some(wire::preconditions::Condition::IfMatch(etag))
                if !etag.is_empty() && etag.len() <= 8192 && !etag.contains(['\r', '\n', '\0']) =>
            {
                Ok(())
            }
            _ => Err(InvalidArgument.into()),
        },
    }
}
/// Checks metadata widths and rejects invalid header values before any mutation.
pub fn metadata(value: &Option<wire::ObjectMetadata>) -> Result<(), Error> {
    let Some(value) = value else {
        return Ok(());
    };
    let fixed = [
        &value.content_type,
        &value.content_encoding,
        &value.cache_control,
        &value.content_disposition,
        &value.content_language,
    ];
    let header_value = |value: &str| {
        value
            .bytes()
            .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte))
    };
    if fixed.iter().map(|value| value.len()).sum::<usize>() > 8192
        || fixed.iter().any(|value| !header_value(value))
        || value
            .expires_unix_seconds
            .is_some_and(|seconds| !(-62_167_219_200..=253_402_300_799).contains(&seconds))
    {
        return Err(InvalidArgument.into());
    }
    let mut bytes = 0_usize;
    let mut names = std::collections::BTreeSet::new();
    for (key, value) in &value.user {
        if key.is_empty()
            || !key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    )
            })
            || !header_value(value)
            || !names.insert(key.to_ascii_lowercase())
        {
            return Err(InvalidArgument.into());
        }
        bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
    }
    if bytes > wire::ObjectsLimit::MaxUserMetadataBytes as usize {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Validates opaque retry/read handles before provider lookup or transport encoding.
pub fn upload_id(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 8192 || value.contains(['\r', '\n', '\0']) {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Checks bounded conditional read headers. Empty strings mean no condition.
pub fn read_filters(if_match: &str, if_none_match: &str) -> Result<(), Error> {
    if [if_match, if_none_match]
        .iter()
        .any(|value| value.len() > 8192 || value.contains(['\r', '\n', '\0']))
    {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Returns the default S3 page size or validates a caller-supplied bound.
pub fn page_size(value: u32) -> Result<usize, Error> {
    if value > wire::ObjectsLimit::MaxPageEntries as u32 {
        return Err(InvalidArgument.into());
    }
    Ok(if value == 0 {
        wire::ObjectsLimit::MaxPageEntries as usize
    } else {
        value as usize
    })
}
/// Resolves one range against a complete representation without integer overflow.
pub fn range(
    value: &Option<wire::ByteRange>,
    size: u64,
) -> Result<Option<wire::ContentRange>, Error> {
    let Some(value) = value else {
        return Ok(None);
    };
    let invalid = || Error::from(wire::ErrorCode::RangeNotSatisfiable);
    if size == 0 {
        return Err(invalid());
    }
    let (start, end) = match value.selection {
        Some(wire::byte_range::Selection::Bytes(bytes)) => {
            if bytes.start >= size || bytes.end.is_some_and(|end| end < bytes.start) {
                return Err(invalid());
            }
            (bytes.start, bytes.end.unwrap_or(size - 1).min(size - 1))
        }
        Some(wire::byte_range::Selection::SuffixLength(length)) if length > 0 => {
            (size.saturating_sub(length), size - 1)
        }
        _ => return Err(invalid()),
    };
    Ok(Some(wire::ContentRange {
        start,
        end,
        total: size,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn streamed_digest_excludes_frame_boundaries_and_retry_identity() -> Result<(), Error> {
        let header = wire::PutObjectHeader {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "value".into(),
            mutation: Some(wire::MutationIdentity {
                idempotency_key: "first".into(),
            }),
            ..Default::default()
        };
        let expected = put_digest(&header, b"whole-body")?;
        let mut changed_identity = header.clone();
        changed_identity.mutation = Some(wire::MutationIdentity {
            idempotency_key: "second".into(),
        });
        let mut streamed = put_stream_digest(&changed_identity)?;
        streamed.update(b"whole-")?;
        streamed.update(b"body")?;
        assert_eq!(streamed.finish(), expected);
        assert_ne!(put_digest(&header, b"different")?, expected);
        changed_identity.object_key = "different".into();
        assert_ne!(put_digest(&changed_identity, b"whole-body")?, expected);
        let mut full = put_stream_digest(&header)?;
        full.bytes = 5 * 1024 * 1024 * 1024;
        assert_eq!(
            full.update(b"x").err().map(|error| error.code),
            Some(wire::ErrorCode::QuotaExceeded)
        );
        Ok(())
    }
    #[test]
    fn upload_completion_is_required_once_and_ends_the_body() -> Result<(), Error> {
        assert!(UploadFraming::default().finish().is_err());
        assert!(UploadFraming::default().complete(false).is_err());
        let mut empty = UploadFraming::default();
        empty.complete(true)?;
        assert_eq!(empty.finish()?, 0);
        let mut upload = UploadFraming::default();
        upload.body(b"whole-body")?;
        upload.complete(true)?;
        assert!(upload.complete(true).is_err());
        assert!(upload.body(b"").is_err());
        assert!(upload.body(b"later").is_err());
        assert_eq!(upload.finish()?, 10);
        assert!(UploadFraming::default().body(&vec![0; 65_537]).is_err());
        Ok(())
    }
    #[test]
    fn multipart_digest_rejects_ambiguous_part_selection_and_bad_metadata() {
        let mut complete = wire::CompleteMultipartRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            object_key: "value".into(),
            upload_id: "upload".into(),
            parts: vec![wire::UploadedPart {
                part_number: 1,
                etag: "receipt".into(),
                size: 1,
            }],
            ..Default::default()
        };
        assert!(complete_multipart_digest(&complete).is_ok());
        complete
            .parts
            .push(complete.parts.first().cloned().unwrap_or_default());
        assert!(complete_multipart_digest(&complete).is_err());
        let invalid = Some(wire::ObjectMetadata {
            user: [
                ("X-Key".into(), "one".into()),
                ("x-key".into(), "two".into()),
            ]
            .into(),
            ..Default::default()
        });
        assert!(metadata(&invalid).is_err());
    }
    #[test]
    fn logical_bucket_names_support_s3_dots_without_exposing_placement() {
        assert!(bucket_name("customer.inputs").is_ok());
        for name in ["127.0.0.1", "bad..name", "ab", "UPPER", "name--x-s3"] {
            assert!(bucket_name(name).is_err());
        }
        assert!(key("a/../b//c").is_ok());
    }
    #[test]
    fn ranges_clip_suffixes_and_ends_without_wrapping() -> Result<(), Error> {
        let suffix = Some(wire::ByteRange {
            selection: Some(wire::byte_range::Selection::SuffixLength(u64::MAX)),
        });
        assert_eq!(
            range(&suffix, 4)?,
            Some(wire::ContentRange {
                start: 0,
                end: 3,
                total: 4
            })
        );
        assert!(range(&suffix, 0).is_err());
        let open = Some(wire::ByteRange {
            selection: Some(wire::byte_range::Selection::Bytes(wire::InclusiveRange {
                start: 2,
                end: Some(u64::MAX),
            })),
        });
        assert_eq!(
            range(&open, 4)?,
            Some(wire::ContentRange {
                start: 2,
                end: 3,
                total: 4
            })
        );
        Ok(())
    }
}
