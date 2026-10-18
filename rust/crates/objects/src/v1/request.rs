//! Shared validation for every logical Objects provider and transport.
use super::{Error, wire};
use prost::Message;
use wire::ErrorCode::InvalidArgument;

/// Validates every publication before admitting a batch.
pub fn put_batch(header: &wire::PutObjectsHeader) -> Result<(), Error> {
    batch_count(
        header.objects.len(),
        prost::encoding::message::encoded_len(1, header),
    )?;
    for object in &header.objects {
        put(object)?;
    }
    Ok(())
}

/// Validates every bounded selection before admitting a batch.
pub fn get_batch(query: &wire::GetObjectsRequest) -> Result<(), Error> {
    batch_count(query.objects.len(), query.encoded_len())?;
    for object in &query.objects {
        get(object
            .request
            .as_ref()
            .ok_or(Error::from(InvalidArgument))?)?;
    }
    Ok(())
}

fn batch_count(count: usize, bytes: usize) -> Result<(), Error> {
    if count == 0 {
        return Err(InvalidArgument.into());
    }
    if count > 1000 || bytes > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(())
}

/// The single-object route and canonical request bytes each batch item authorizes as.
pub type BatchRequests = (&'static str, Vec<Vec<u8>>);

/// Projects validated batch selections to their canonical per-object permissions.
/// This performs no admission or work: callers verify one credential, check every
/// selection, then admit the entire batch before executing any item.
pub fn batch_requests(route: &str, bytes: &[u8]) -> Result<Option<BatchRequests>, Error> {
    let invalid = || Error::from(InvalidArgument);
    match route {
        "objects/put-batch" => {
            let header = wire::PutObjectsHeader::decode(bytes).map_err(|_| invalid())?;
            put_batch(&header)?;
            Ok(Some((
                "objects/put",
                header.objects.iter().map(Message::encode_to_vec).collect(),
            )))
        }
        "objects/get-batch" => {
            let query = wire::GetObjectsRequest::decode(bytes).map_err(|_| invalid())?;
            get_batch(&query)?;
            Ok(Some((
                "objects/get",
                query
                    .objects
                    .into_iter()
                    .map(|selection| {
                        selection
                            .request
                            .ok_or_else(invalid)
                            .map(|query| query.encode_to_vec())
                    })
                    .collect::<Result<_, _>>()?,
            )))
        }
        _ => Ok(None),
    }
}

/// Enforces ordered item completion followed by explicit batch completion and EOF.
pub struct BatchUploadFraming {
    count: usize,
    index: usize,
    item: UploadFraming,
    complete: bool,
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "Tests index fixtures whose shape they construct."
)]
mod batch_tests {
    use super::*;

    fn header() -> wire::PutObjectsHeader {
        wire::PutObjectsHeader {
            objects: (0..2)
                .map(|index| wire::PutObjectHeader {
                    bucket: Some(wire::BucketRef {
                        name: "batch".into(),
                    }),
                    object_key: format!("key-{index}"),
                    ..Default::default()
                })
                .collect(),
        }
    }

    fn chunk(index: u32, frame: wire::put_objects_chunk::Frame) -> wire::PutObjectsRequest {
        wire::PutObjectsRequest {
            frame: Some(wire::put_objects_request::Frame::Chunk(
                wire::PutObjectsChunk {
                    index,
                    frame: Some(frame),
                },
            )),
        }
    }

    #[test]
    fn batch_framing_requires_every_item_in_order_and_final_completion() {
        use wire::put_objects_chunk::Frame as C;
        let header = header();
        let mut framing = BatchUploadFraming::new(&header).unwrap();
        assert!(framing.accept(&chunk(1, C::Body(vec![1]))).is_err());
        assert!(framing.accept(&chunk(0, C::Body(vec![0; 65_537]))).is_err());
        assert!(framing.accept(&chunk(0, C::Complete(false))).is_err());
        framing.accept(&chunk(0, C::Complete(true))).unwrap();
        assert!(framing.accept(&chunk(0, C::Complete(true))).is_err());
        let end = wire::PutObjectsRequest {
            frame: Some(wire::put_objects_request::Frame::Complete(true)),
        };
        assert!(framing.accept(&end).is_err());
        framing.accept(&chunk(1, C::Complete(true))).unwrap();
        assert!(framing.finish().is_err());
        let mut framing = BatchUploadFraming::new(&header).unwrap();
        for index in 0..2 {
            framing.accept(&chunk(index, C::Complete(true))).unwrap();
        }
        framing.accept(&end).unwrap();
        assert!(framing.accept(&end).is_err());
        framing.finish().unwrap();
    }

    #[test]
    fn batch_header_limit_includes_its_request_envelope() {
        let mut object = header().objects.into_iter().next().unwrap();
        object.metadata = Some(wire::ObjectMetadata {
            content_type: "x".repeat(8192),
            ..Default::default()
        });
        object.preconditions = Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfMatch("x".repeat(8192))),
        });
        let mut header = wire::PutObjectsHeader {
            objects: vec![object; 1000],
        };
        let limit = 16 * 1024 * 1024;
        let mut last = 0;
        for index in 0..header.objects.len() {
            let remaining = limit - header.encoded_len();
            header
                .objects
                .get_mut(index)
                .unwrap()
                .metadata
                .as_mut()
                .unwrap()
                .user
                .insert("a".into(), "x".repeat(remaining.min(2047)));
            let size = header.encoded_len();
            if size >= limit {
                let value = header
                    .objects
                    .get_mut(index)
                    .unwrap()
                    .metadata
                    .as_mut()
                    .unwrap()
                    .user
                    .get_mut("a")
                    .unwrap();
                value.truncate(value.len() - (size - limit));
                last = index;
                break;
            }
        }
        for object in &header.objects {
            put(object).unwrap();
        }
        assert!(header.encoded_len() <= limit);
        assert!(prost::encoding::message::encoded_len(1, &header) > limit);
        assert_eq!(
            put_batch(&header).unwrap_err().code,
            wire::ErrorCode::QuotaExceeded
        );
        let value = header
            .objects
            .get_mut(last)
            .unwrap()
            .metadata
            .as_mut()
            .unwrap()
            .user
            .get_mut("a")
            .unwrap();
        value.truncate(value.len() - 8);
        assert!(prost::encoding::message::encoded_len(1, &header) <= limit);
        put_batch(&header).unwrap();
    }

    #[test]
    fn batch_validation_and_projection_cover_every_selection() {
        let mut header = header();
        let (route, requests) = batch_requests("objects/put-batch", &header.encode_to_vec())
            .unwrap()
            .unwrap();
        assert_eq!(route, "objects/put");
        assert_eq!(requests.len(), 2);
        for (bytes, expected) in requests.iter().zip(&header.objects) {
            assert_eq!(
                wire::PutObjectHeader::decode(bytes.as_slice()).unwrap(),
                *expected
            );
        }
        header.objects.get_mut(1).unwrap().object_key.clear();
        assert!(put_batch(&header).is_err());
        assert!(put_batch(&wire::PutObjectsHeader::default()).is_err());
        header.objects = vec![header.objects.first().unwrap().clone(); 1001];
        assert!(put_batch(&header).is_err());
        assert!(
            get_batch(&wire::GetObjectsRequest {
                objects: vec![wire::GetObjectSelection::default()]
            })
            .is_err()
        );
    }

    proptest::proptest! {
        #[test]
        fn batch_parser_matches_an_independent_order_and_completion_model(
            count in 1usize..5,
            actions in proptest::collection::vec((0u8..4, 0u32..6, proptest::prelude::any::<bool>()), 0..24),
        ) {
            use wire::{put_objects_chunk::Frame as C, put_objects_request::Frame as F};
            let mut header = header();
            header.objects.resize(count, header.objects[0].clone());
            let mut parser = BatchUploadFraming::new(&header).unwrap();
            let (mut index, mut complete) = (0usize, false);
            for (tag, selected, flag) in actions {
                let frame = match tag {
                    0 => chunk(selected, C::Body(vec![1; if flag { 65_537 } else { 3 }])),
                    1 => chunk(selected, C::Complete(flag)),
                    2 => wire::PutObjectsRequest { frame: Some(F::Complete(flag)) },
                    _ => wire::PutObjectsRequest::default(),
                };
                let expected = !complete && match tag {
                    0 => (selected as usize) == index && index < count && !flag,
                    1 => (selected as usize) == index && index < count && flag,
                    2 => index == count && flag,
                    _ => false,
                };
                proptest::prop_assert_eq!(parser.accept(&frame).is_ok(), expected);
                if !expected { return Ok(()); }
                if tag == 1 { index += 1; }
                if tag == 2 { complete = true; }
            }
            proptest::prop_assert_eq!(parser.finish().is_ok(), complete);
        }
    }
}

impl BatchUploadFraming {
    /// Starts after the batch header has been validated.
    pub fn new(header: &wire::PutObjectsHeader) -> Result<Self, Error> {
        put_batch(header)?;
        Ok(Self {
            count: header.objects.len(),
            index: 0,
            item: UploadFraming::default(),
            complete: false,
        })
    }

    /// Accepts the next bounded item frame or final batch completion marker.
    pub fn accept(&mut self, frame: &wire::PutObjectsRequest) -> Result<(), Error> {
        use wire::{put_objects_chunk::Frame as C, put_objects_request::Frame as F};
        if self.complete {
            return Err(InvalidArgument.into());
        }
        match &frame.frame {
            Some(F::Chunk(chunk))
                if self.index < self.count
                    && usize::try_from(chunk.index).ok() == Some(self.index) =>
            {
                match &chunk.frame {
                    Some(C::Body(body)) => self.item.body(body)?,
                    Some(C::Complete(complete)) => {
                        self.item.complete(*complete)?;
                        std::mem::take(&mut self.item).finish()?;
                        self.index += 1;
                    }
                    _ => return Err(InvalidArgument.into()),
                }
            }
            Some(F::Complete(true)) if self.index == self.count => self.complete = true,
            _ => return Err(InvalidArgument.into()),
        }
        Ok(())
    }

    /// Requires EOF after all declared items and the batch completion marker.
    pub fn finish(self) -> Result<(), Error> {
        if !self.complete {
            return Err(InvalidArgument.into());
        }
        Ok(())
    }
}

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

/// Checks the maximum decoded size of a single PUT or multipart part.
pub fn upload_length(size: u64) -> Result<(), Error> {
    if size > 5 * 1024 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(())
}

/// Validates a canonical binary request before any transport or provider side effect.
pub fn validate_binary(route: &str, bytes: &[u8], body_length: u64) -> Result<(), Error> {
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
        "objects/put-batch" => put_batch(&decode!(PutObjectsHeader))?,
        "objects/get-batch" => get_batch(&decode!(GetObjectsRequest))?,
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
            put(&decode!(PutObjectHeader))?;
            upload_length(body_length)?;
        }
        "objects/get" => {
            get(&decode!(GetObjectRequest))?;
        }
        "objects/head" => {
            head(&decode!(HeadObjectRequest))?;
        }
        "objects/delete" => {
            delete_digest(&decode!(DeleteObjectRequest))?;
        }
        "objects/restore" => {
            restore(&decode!(RestoreObjectsRequest))?;
        }
        "objects/list" => {
            list(&decode!(ListObjectsRequest))?;
        }
        "multipart/create" => {
            create_multipart_digest(&decode!(CreateMultipartRequest))?;
        }
        "multipart/upload-part" => {
            upload_part(&decode!(UploadPartHeader))?;
            upload_length(body_length)?;
        }
        "multipart/list-parts" => {
            list_parts(&decode!(ListPartsRequest))?;
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
pub(crate) fn head(query: &wire::HeadObjectRequest) -> Result<&str, Error> {
    if query.encoded_len() > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let name = bucket(&query.bucket)?;
    key(&query.object_key)?;
    read_filters(&query.if_match, &query.if_none_match)?;
    Ok(name)
}

/// Validates a prefix restore without consulting the serving retention window.
pub fn restore(query: &wire::RestoreObjectsRequest) -> Result<&str, Error> {
    if query.encoded_len() > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let name = bucket(&query.bucket)?;
    if query.prefix.len() > 1024 || query.prefix.contains('\0') {
        return Err(InvalidArgument.into());
    }
    let at = query.at.as_ref().ok_or(Error::from(InvalidArgument))?;
    super::response::timestamp(at).map_err(|_| Error::from(InvalidArgument))?;
    if at.seconds < 0 || (at.seconds == 0 && at.nanos < 1000) || at.nanos % 1000 != 0 {
        return Err(InvalidArgument.into());
    }
    let mutation = query
        .mutation
        .as_ref()
        .ok_or(Error::from(InvalidArgument))?;
    if mutation.idempotency_key.is_empty() {
        return Err(InvalidArgument.into());
    }
    identity(&query.mutation)?;
    Ok(name)
}

/// Retry identity excludes the retry key, never the restore time or prefix.
pub fn restore_digest(query: &wire::RestoreObjectsRequest) -> Result<[u8; 32], Error> {
    restore(query)?;
    let mut normalized = query.clone();
    normalized.mutation = None;
    Ok(message_digest("restore", &normalized))
}

pub(crate) fn list(query: &wire::ListObjectsRequest) -> Result<(&str, usize), Error> {
    if query.encoded_len() > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let name = bucket(&query.bucket)?;
    let limit = page_size(query.page_size)?;
    if query.prefix.len() > 1024
        || query.delimiter.len() > 1024
        || query.prefix.contains('\0')
        || query.delimiter.contains('\0')
        || query.continuation_token.len() > 8192
        || query.continuation_token.contains('\0')
    {
        return Err(InvalidArgument.into());
    }
    Ok((name, limit))
}

pub(crate) fn list_parts(query: &wire::ListPartsRequest) -> Result<(&str, usize), Error> {
    if query.encoded_len() > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let name = bucket(&query.bucket)?;
    key(&query.object_key)?;
    upload_id(&query.upload_id)?;
    let limit = page_size(query.page_size)?;
    if query.after_part_number > 10_000 {
        return Err(InvalidArgument.into());
    }
    Ok((name, limit))
}

pub(crate) fn get(query: &wire::GetObjectRequest) -> Result<&str, Error> {
    if prost::Message::encoded_len(query) > 16 * 1024 * 1024 {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    let name = bucket(&query.bucket)?;
    key(&query.object_key)?;
    read_filters(&query.if_match, &query.if_none_match)?;
    if let Some(range) = query.range {
        match range.selection {
            Some(wire::byte_range::Selection::Bytes(range))
                if range.end.is_none_or(|end| end >= range.start) => {}
            Some(wire::byte_range::Selection::SuffixLength(length)) if length > 0 => {}
            _ => return Err(wire::ErrorCode::RangeNotSatisfiable.into()),
        }
    }
    Ok(name)
}

pub(crate) fn finish_digest(
    operation: &str,
    header: &[u8],
    length: u64,
    body: [u8; 32],
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"acyclic.objects.v1.mutation\0");
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
pub(crate) fn put(header: &wire::PutObjectHeader) -> Result<&str, Error> {
    let name = bucket(&header.bucket)?;
    key(&header.object_key)?;
    metadata(&header.metadata)?;
    preconditions(&header.preconditions)?;
    identity(&header.mutation)?;
    Ok(name)
}
/// Validates a PUT and hashes its complete decoded body without the retry key.
pub fn put_digest(header: &wire::PutObjectHeader, body: &[u8]) -> Result<[u8; 32], Error> {
    put(header)?;
    upload_length(body.len() as u64)?;
    let mut normalized = header.clone();
    normalized.mutation = None;
    Ok(finish_digest(
        "put",
        &prost::Message::encode_to_vec(&normalized),
        body.len() as u64,
        *blake3::hash(body).as_bytes(),
    ))
}

pub(crate) fn upload_part(header: &wire::UploadPartHeader) -> Result<(), Error> {
    bucket(&header.bucket)?;
    key(&header.object_key)?;
    upload_id(&header.upload_id)?;
    identity(&header.mutation)?;
    if !(1..=10_000).contains(&header.part_number) {
        return Err(InvalidArgument.into());
    }
    Ok(())
}
/// Validates a multipart publication and hashes its decoded body without the retry key.
pub fn upload_part_digest(header: &wire::UploadPartHeader, body: &[u8]) -> Result<[u8; 32], Error> {
    upload_part(header)?;
    upload_length(body.len() as u64)?;
    let mut normalized = header.clone();
    normalized.mutation = None;
    Ok(finish_digest(
        "upload_part",
        &prost::Message::encode_to_vec(&normalized),
        body.len() as u64,
        *blake3::hash(body).as_bytes(),
    ))
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
    const MAX: usize = wire::ObjectsLimit::MaxUserMetadataBytes as usize;
    let mut names = [""; MAX];
    if value.user.len() > MAX {
        return Err(InvalidArgument.into());
    }
    for ((key, value), name) in value.user.iter().zip(&mut names) {
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
        {
            return Err(InvalidArgument.into());
        }
        bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
        *name = key.as_str();
    }
    if bytes > wire::ObjectsLimit::MaxUserMetadataBytes as usize {
        return Err(InvalidArgument.into());
    }
    let names = names.get_mut(..value.user.len()).ok_or(InvalidArgument)?;
    names.sort_unstable_by(|left, right| {
        left.bytes()
            .map(|byte| byte.to_ascii_lowercase())
            .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase()))
    });
    if names
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left.eq_ignore_ascii_case(right)))
    {
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
    fn mutation_digest_excludes_retry_identity() -> Result<(), Error> {
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
        assert_eq!(put_digest(&changed_identity, b"whole-body")?, expected);
        assert_ne!(put_digest(&header, b"different")?, expected);
        changed_identity.object_key = "different".into();
        assert_ne!(put_digest(&changed_identity, b"whole-body")?, expected);
        assert_eq!(
            upload_length(5 * 1024 * 1024 * 1024 + 1)
                .err()
                .map(|error| error.code),
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
    fn restores_validate_their_target_and_bind_retry_identity_to_prefix_and_time() {
        let query = wire::RestoreObjectsRequest {
            bucket: Some(wire::BucketRef {
                name: "customer.inputs".into(),
            }),
            prefix: "orders/".into(),
            at: Some(prost_types::Timestamp {
                seconds: 42,
                nanos: 1000,
            }),
            mutation: Some(wire::MutationIdentity {
                idempotency_key: "restore-1".into(),
            }),
        };
        assert!(validate_binary("objects/restore", &query.encode_to_vec(), 0).is_ok());
        let digest = restore_digest(&query).unwrap();
        let mut changed = query.clone();
        changed.mutation.as_mut().unwrap().idempotency_key = "restore-2".into();
        assert_eq!(restore_digest(&changed).unwrap(), digest);
        changed.prefix.clear();
        assert_ne!(restore_digest(&changed).unwrap(), digest);
        changed = query.clone();
        changed.at.as_mut().unwrap().seconds += 1;
        assert_ne!(restore_digest(&changed).unwrap(), digest);
        for invalid in [
            wire::RestoreObjectsRequest {
                bucket: None,
                ..query.clone()
            },
            wire::RestoreObjectsRequest {
                at: None,
                ..query.clone()
            },
            wire::RestoreObjectsRequest {
                mutation: None,
                ..query.clone()
            },
            wire::RestoreObjectsRequest {
                prefix: "bad\0prefix".into(),
                ..query.clone()
            },
            wire::RestoreObjectsRequest {
                prefix: "x".repeat(1025),
                ..query.clone()
            },
            wire::RestoreObjectsRequest {
                at: Some(prost_types::Timestamp {
                    seconds: 42,
                    nanos: 1,
                }),
                ..query
            },
        ] {
            assert_eq!(restore_digest(&invalid).unwrap_err().code, InvalidArgument);
        }
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
