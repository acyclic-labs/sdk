struct SelectedRange {
    offset: u64,
    length: u64,
    content_range: Option<String>,
}

fn select_range(length: u64, requested: Option<&Range>) -> S3Result<SelectedRange> {
    let Some(requested) = requested else {
        return Ok(SelectedRange {
            offset: 0,
            length,
            content_range: None,
        });
    };
    if length == 0 {
        return Err(s3s::s3_error!(InvalidRange));
    }
    let range = requested.check(length)?;
    Ok(SelectedRange {
        offset: range.start,
        length: range.end - range.start,
        content_range: Some(format!(
            "bytes {}-{}/{}",
            range.start,
            range.end - 1,
            length
        )),
    })
}

fn parse_crc32(value: Option<&str>) -> S3Result<Option<u32>> {
    value
        .map(|value| {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|_| s3s::s3_error!(InvalidArgument))?;
            let bytes: [u8; 4] = bytes
                .try_into()
                .map_err(|_| s3s::s3_error!(InvalidArgument))?;
            Ok(u32::from_be_bytes(bytes))
        })
        .transpose()
}

fn verify_crc32(source: &StreamingBodySource, expected: Option<u32>) -> S3Result {
    if let Some(expected) = expected
        && source
            .crc32
            .as_ref()
            .is_none_or(|digest| u32::try_from(digest.finalize()).ok() != Some(expected))
    {
        return Err(s3s::s3_error!(BadDigest));
    }
    Ok(())
}

struct StreamingBodySource {
    body: Option<StreamingBlob>,
    pending: Bytes,
    crc32: Option<crc_fast::Digest>,
}

impl StreamingBodySource {
    fn new(body: Option<StreamingBlob>, checksum_crc32: bool) -> Self {
        Self {
            body,
            pending: Bytes::new(),
            crc32: checksum_crc32
                .then(|| crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc)),
        }
    }
}

impl AsyncBlobSource for StreamingBodySource {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "S3 request body cancelled",
            ));
        }
        while self.pending.is_empty() {
            let Some(body) = self.body.as_mut() else {
                return Ok(0);
            };
            match body.next().await {
                Some(Ok(chunk)) if !chunk.is_empty() => self.pending = chunk,
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(std::io::Error::other(error)),
                None => {
                    self.body = None;
                    return Ok(0);
                }
            }
        }
        let count = destination.len().min(self.pending.len());
        if let (Some(destination_slice), Some(pending_slice)) =
            (destination.get_mut(..count), self.pending.get(..count))
        {
            destination_slice.copy_from_slice(pending_slice);
            if let Some(digest) = self.crc32.as_mut() {
                digest.update(pending_slice);
            }
        }
        self.pending.advance(count);
        Ok(count)
    }
}

fn validate_http_key(key: &str) -> S3Result {
    if key.is_empty() {
        return Err(s3s::s3_error!(InvalidArgument));
    }
    Ok(())
}

fn encode_multipart_created(
    key: &str,
    maximum_parts: Option<u32>,
    maximum_part_bytes: Option<u64>,
    minimum_part_bytes: u64,
) -> S3Result<Bytes> {
    let key_length = u32::try_from(key.len()).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let mut output = Vec::with_capacity(2 + 4 + key.len() + 4 + 8 + 8);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 1]);
    output.extend_from_slice(&key_length.to_le_bytes());
    output.extend_from_slice(key.as_bytes());
    output.extend_from_slice(&maximum_parts.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&maximum_part_bytes.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&minimum_part_bytes.to_le_bytes());
    Ok(Bytes::from(output))
}

fn encode_multipart_part(
    part_number: u32,
    staged: StagedContent,
    operation: IdempotencyKey,
    etag_value: &str,
) -> Bytes {
    let mut output = Vec::with_capacity(2 + 4 + staged.root().encoded_len() + 8 + 16 + 32);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 2]);
    output.extend_from_slice(&part_number.to_le_bytes());
    output.extend_from_slice(&staged.root().to_bytes());
    output.extend_from_slice(&staged.logical_bytes().to_le_bytes());
    output.extend_from_slice(&operation.into_bytes());
    let mut etag_digest = [0_u8; 32];
    let unquoted = etag_value.trim_matches('"');
    if hex::decode_to_slice(unquoted, &mut etag_digest).is_err() {
        etag_digest = *blake3::hash(etag_value.as_bytes()).as_bytes();
    }
    output.extend_from_slice(&etag_digest);
    Bytes::from(output)
}

fn encode_multipart_completing(parts: &[(u32, String)]) -> S3Result<Bytes> {
    let count = u32::try_from(parts.len()).map_err(|_| s3s::s3_error!(InvalidRequest))?;
    let mut output = Vec::with_capacity(6 + parts.len().saturating_mul(36));
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 3]);
    output.extend_from_slice(&count.to_le_bytes());
    for (number, etag_value) in parts {
        output.extend_from_slice(&number.to_le_bytes());
        let mut digest = [0_u8; 32];
        hex::decode_to_slice(etag_value.trim_matches('"'), &mut digest)
            .map_err(|_| s3s::s3_error!(InvalidPart))?;
        output.extend_from_slice(&digest);
    }
    Ok(Bytes::from(output))
}

fn encode_multipart_completed(generation: crate::GenerationId) -> Bytes {
    let mut output = Vec::with_capacity(2 + generation.object_id().encoded_len());
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 4]);
    output.extend_from_slice(&generation.to_bytes());
    Bytes::from(output)
}

fn encode_multipart_aborted() -> Bytes {
    Bytes::from_static(&[MULTIPART_RECORD_VERSION, 5])
}

async fn read_multipart_snapshot<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    limits: FilesystemS3Limits,
) -> S3Result<MultipartSnapshot> {
    let mut work = crate::WorkCounters {
        backend_read_operations: 1,
        ..crate::WorkCounters::default()
    };
    let tail = provider
        .tail(path.clone())
        .await
        .map_err(|error| stream_read_error(&error))?;
    if tail == 0 {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    let mut replay: Option<MultipartReplay> = None;
    let mut from = 0_u64;
    while from < tail {
        let remaining = tail - from;
        let limit = u32::try_from(remaining.min(acyclic_stream::MAX_ITEMS as u64))
            .map_err(|_| s3s::s3_error!(InternalError))?;
        let mut page = provider
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: path.clone(),
                from,
                limit,
                at_least: Default::default(),
            })
            .await
            .map_err(|error| stream_read_error(&error))?;
        work.backend_read_operations = work
            .backend_read_operations
            .checked_add(1)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
        let mut received = 0_u64;
        while let Some(record) = page.next().await {
            let record = record.map_err(|error| stream_read_error(&error))?;
            if received >= u64::from(limit) || record.sequence != from + received {
                return Err(s3s::s3_error!(InternalError));
            }
            work.authority_records_read = work
                .authority_records_read
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            work.authority_bytes_read = work
                .authority_bytes_read
                .checked_add(record.value.len() as u64)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            work.items_examined = work
                .items_examined
                .checked_add(1)
                .ok_or_else(|| s3s::s3_error!(InternalError))?;
            if let Some(replay) = &mut replay {
                replay.push(&record.value)?;
            } else {
                replay = Some(MultipartReplay::new(&record.value, tail, limits)?);
            }
            received += 1;
        }
        if received == 0 {
            return Err(s3s::s3_error!(InternalError));
        }
        from = from
            .checked_add(received)
            .ok_or_else(|| s3s::s3_error!(InternalError))?;
    }
    let mut snapshot = replay.ok_or_else(|| s3s::s3_error!(NoSuchUpload))?.snapshot;
    snapshot.work = work;
    Ok(snapshot)
}

/// Persisted multipart refusal, projected only at the S3 service boundary.
#[derive(Debug, thiserror::Error)]
enum MultipartDecodeError {
    #[error("multipart upload does not exist")]
    NoSuchUpload,
    #[error("multipart record is malformed")]
    Malformed,
    #[error("multipart record version {0} is unsupported")]
    UnsupportedVersion(u8),
}

type MultipartDecodeResult<T = ()> = Result<T, MultipartDecodeError>;

impl From<MultipartDecodeError> for s3s::S3Error {
    fn from(error: MultipartDecodeError) -> Self {
        match error {
            MultipartDecodeError::NoSuchUpload => s3s::s3_error!(NoSuchUpload),
            MultipartDecodeError::Malformed => s3s::s3_error!(InvalidRequest),
            MultipartDecodeError::UnsupportedVersion(_) => s3s::s3_error!(NotImplemented),
        }
    }
}

/// Decodes and validates the leading `created` record of a multipart record stream, returning the upload's key and its negotiated part bounds.
fn decode_multipart_header(
    created: &acyclic_stream::OwnedBytes,
    limits: FilesystemS3Limits,
) -> MultipartDecodeResult<(String, Option<u32>, Option<u64>, u64)> {
    let mut cursor = RecordCursor::new(created);
    cursor.header(1)?;
    let key_length = cursor.u32()?;
    if key_length == 0 {
        return Err(MultipartDecodeError::Malformed);
    }
    let key = std::str::from_utf8(cursor.take(key_length as usize)?)
        .map_err(|_| MultipartDecodeError::Malformed)?
        .to_owned();
    let maximum_parts = std::num::NonZeroU32::new(cursor.u32()?).map(std::num::NonZeroU32::get);
    let maximum_part_bytes =
        std::num::NonZeroU64::new(cursor.u64()?).map(std::num::NonZeroU64::get);
    let minimum_part_bytes = cursor.u64()?;
    cursor.finish()?;
    if limits
        .maximum_multipart_parts
        .is_some_and(|maximum| maximum_parts.is_none_or(|saved| saved > maximum))
        || limits
            .maximum_multipart_part_bytes
            .is_some_and(|maximum| maximum_part_bytes.is_none_or(|saved| saved > maximum))
        || maximum_part_bytes.is_some_and(|maximum| minimum_part_bytes > maximum)
    {
        return Err(MultipartDecodeError::Malformed);
    }
    Ok((key, maximum_parts, maximum_part_bytes, minimum_part_bytes))
}

/// Replays one canonical record at a time; only current parts and exact retry
/// identities survive a page. Historical payloads retire before the next page.
struct MultipartReplay {
    snapshot: MultipartSnapshot,
    maximum_parts: Option<u32>,
    maximum_part_bytes: Option<u64>,
}

impl MultipartReplay {
    fn new(
        created: &acyclic_stream::OwnedBytes,
        tail: u64,
        limits: FilesystemS3Limits,
    ) -> MultipartDecodeResult<Self> {
        let (key, maximum_parts, maximum_part_bytes, minimum_part_bytes) =
            decode_multipart_header(created, limits)?;
        Ok(Self {
            snapshot: MultipartSnapshot {
                key,
                tail,
                minimum_part_bytes,
                maximum_parts,
                maximum_part_bytes,
                parts: BTreeMap::new(),
                operations: BTreeMap::new(),
                terminal: None,
                work: crate::WorkCounters::default(),
            },
            maximum_parts,
            maximum_part_bytes,
        })
    }

    fn push(&mut self, record: &acyclic_stream::OwnedBytes) -> MultipartDecodeResult<()> {
        let MultipartSnapshot {
            parts,
            operations,
            terminal,
            ..
        } = &mut self.snapshot;
        let mut cursor = RecordCursor::new(record);
        let tag = cursor.any_header()?;
        if terminal.is_some() && tag != 4 {
            return Err(MultipartDecodeError::Malformed);
        }
        match tag {
            2 => {
                let part_number = cursor.u32()?;
                let root = cursor.object()?;
                let logical_bytes = cursor.u64()?;
                let operation = cursor.array16()?;
                let etag_digest = cursor.array32()?;
                cursor.finish()?;
                if part_number == 0
                    || self
                        .maximum_parts
                        .is_some_and(|maximum| part_number > maximum)
                    || self
                        .maximum_part_bytes
                        .is_some_and(|maximum| logical_bytes > maximum)
                {
                    return Err(MultipartDecodeError::Malformed);
                }
                let etag = format!("\"{}\"", hex::encode(etag_digest));
                if let Some((prior_number, prior_etag)) = operations.get(&operation)
                    && (*prior_number != part_number || prior_etag != &etag)
                {
                    return Err(MultipartDecodeError::Malformed);
                }
                operations.insert(operation, (part_number, etag.clone()));
                parts.insert(
                    part_number,
                    MultipartPart {
                        staged: StagedContent::from_canonical_parts(root, logical_bytes),
                        etag,
                    },
                );
            }
            3 => {
                let count = cursor.u32()?;
                if count == 0 || self.maximum_parts.is_some_and(|maximum| count > maximum) {
                    return Err(MultipartDecodeError::Malformed);
                }
                if usize::try_from(count)
                    .ok()
                    .and_then(|count| count.checked_mul(36))
                    != Some(cursor.bytes.len() - cursor.offset)
                {
                    return Err(MultipartDecodeError::Malformed);
                }
                let mut selected = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    selected.push((
                        cursor.u32()?,
                        format!("\"{}\"", hex::encode(cursor.array32()?)),
                    ));
                }
                cursor.finish()?;
                *terminal = Some(MultipartTerminal::Completing(selected));
            }
            4 => {
                let generation =
                    crate::GenerationId::from_bytes(cursor.take(record.len().saturating_sub(2))?)
                        .ok_or(MultipartDecodeError::Malformed)?;
                cursor.finish()?;
                if !matches!(terminal, Some(MultipartTerminal::Completing(_))) {
                    return Err(MultipartDecodeError::Malformed);
                }
                *terminal = Some(MultipartTerminal::Completed(generation));
            }
            5 => {
                cursor.finish()?;
                *terminal = Some(MultipartTerminal::Aborted);
            }
            _ => return Err(MultipartDecodeError::Malformed),
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
fn decode_multipart(
    records: Vec<acyclic_stream::OwnedBytes>,
    tail: u64,
    limits: FilesystemS3Limits,
) -> MultipartDecodeResult<MultipartSnapshot> {
    let mut records = records.into_iter();
    let created = records.next().ok_or(MultipartDecodeError::NoSuchUpload)?;
    let mut replay = MultipartReplay::new(&created, tail, limits)?;
    for record in records {
        replay.push(&record)?;
    }
    Ok(replay.snapshot)
}

struct RecordCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> RecordCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> MultipartDecodeResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(MultipartDecodeError::Malformed)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(MultipartDecodeError::Malformed)?;
        self.offset = end;
        Ok(value)
    }

    fn object(&mut self) -> MultipartDecodeResult<ObjectId> {
        let (object, remaining) = ObjectId::from_prefix(
            self.bytes
                .get(self.offset..)
                .ok_or(MultipartDecodeError::Malformed)?,
        )
        .map_err(|_| MultipartDecodeError::Malformed)?;
        self.offset = self.bytes.len() - remaining.len();
        Ok(object)
    }

    fn u8(&mut self) -> MultipartDecodeResult<u8> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(MultipartDecodeError::Malformed)
    }

    fn u32(&mut self) -> MultipartDecodeResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| MultipartDecodeError::Malformed)?,
        ))
    }

    fn u64(&mut self) -> MultipartDecodeResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| MultipartDecodeError::Malformed)?,
        ))
    }

    fn array32(&mut self) -> MultipartDecodeResult<[u8; 32]> {
        self.take(32)?
            .try_into()
            .map_err(|_| MultipartDecodeError::Malformed)
    }

    fn array16(&mut self) -> MultipartDecodeResult<[u8; 16]> {
        self.take(16)?
            .try_into()
            .map_err(|_| MultipartDecodeError::Malformed)
    }

    fn any_header(&mut self) -> MultipartDecodeResult<u8> {
        let version = self
            .bytes
            .get(self.offset)
            .copied()
            .ok_or(MultipartDecodeError::UnsupportedVersion(0))?;
        if version != MULTIPART_RECORD_VERSION {
            return Err(MultipartDecodeError::UnsupportedVersion(version));
        }
        self.u8()?;
        self.u8()
    }

    fn header(&mut self, tag: u8) -> MultipartDecodeResult {
        if self.any_header()? != tag {
            return Err(MultipartDecodeError::Malformed);
        }
        Ok(())
    }

    fn finish(&self) -> MultipartDecodeResult {
        if self.offset != self.bytes.len() {
            return Err(MultipartDecodeError::Malformed);
        }
        Ok(())
    }
}

#[cfg(kani)]
mod multipart_proofs {
    use super::*;

    #[kani::proof]
    #[kani::unwind(3)]
    fn multipart_header_refuses_versions_before_record_fields() {
        let input: [u8; 2] = kani::any();
        let length: usize = kani::any();
        kani::assume(length <= input.len());
        let mut cursor = RecordCursor::new(&input[..length]);
        let result = cursor.any_header();
        if length == 0 || input[0] != MULTIPART_RECORD_VERSION {
            assert!(matches!(
                result,
                Err(MultipartDecodeError::UnsupportedVersion(found))
                    if found == if length == 0 { 0 } else { input[0] }
            ));
            assert_eq!(cursor.offset, 0);
        } else if length == 1 {
            assert!(matches!(result, Err(MultipartDecodeError::Malformed)));
            assert_eq!(cursor.offset, 1);
        } else {
            assert_eq!(result.unwrap(), input[1]);
            assert_eq!(cursor.offset, 2);
        }
    }
}

fn completed_parts(
    upload: Option<&s3s::dto::CompletedMultipartUpload>,
) -> S3Result<Vec<(u32, String)>> {
    let parts = upload
        .and_then(|upload| upload.parts.as_ref())
        .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
    if parts.is_empty() {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    parts
        .iter()
        .map(|part: &CompletedPart| {
            let number = part
                .part_number
                .ok_or_else(|| s3s::s3_error!(InvalidPart))?;
            let number = u32::try_from(number).map_err(|_| s3s::s3_error!(InvalidPart))?;
            let etag = part
                .e_tag
                .as_ref()
                .ok_or_else(|| s3s::s3_error!(InvalidPart))?;
            Ok((number, format!("\"{}\"", etag.value())))
        })
        .collect()
}

fn validate_completed_parts(
    available: &BTreeMap<u32, MultipartPart>,
    requested: &[(u32, String)],
    minimum_part_bytes: u64,
) -> S3Result {
    let mut previous = 0;
    for (index, (number, requested_etag)) in requested.iter().enumerate() {
        if *number <= previous {
            return Err(s3s::s3_error!(InvalidPartOrder));
        }
        let part = available
            .get(number)
            .ok_or_else(|| s3s::s3_error!(InvalidPart))?;
        let parsed = etag(requested_etag)?;
        let actual = etag(&part.etag)?;
        if !parsed.strong_cmp(&actual) {
            return Err(s3s::s3_error!(InvalidPart));
        }
        if index + 1 < requested.len() && part.staged.logical_bytes() < minimum_part_bytes {
            return Err(s3s::s3_error!(EntityTooSmall));
        }
        previous = *number;
    }
    Ok(())
}

fn stream_read_error(error: &acyclic_stream::StreamError) -> s3s::S3Error {
    match error {
        acyclic_stream::StreamError::NotFound => s3s::s3_error!(NoSuchUpload),
        acyclic_stream::StreamError::AccessDenied => s3s::s3_error!(AccessDenied),
        acyclic_stream::StreamError::Unavailable => s3s::s3_error!(ServiceUnavailable),
        _ => s3s::s3_error!(InternalError),
    }
}

fn stream_write_error(error: &acyclic_stream::StreamError) -> s3s::S3Error {
    match error {
        acyclic_stream::StreamError::IdempotencyMismatch => s3s::s3_error!(InvalidRequest),
        acyclic_stream::StreamError::Capacity | acyclic_stream::StreamError::LimitExceeded => {
            s3s::s3_error!(SlowDown)
        }
        other => stream_read_error(other),
    }
}
