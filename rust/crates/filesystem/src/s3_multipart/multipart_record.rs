/// Encodes the key and part bounds of a new upload.
pub fn encode_multipart_created(
    key: &str,
    maximum_parts: Option<u32>,
    maximum_part_bytes: Option<u64>,
    minimum_part_bytes: u64,
) -> MultipartStorageResult<Bytes> {
    let key_length = u32::try_from(key.len()).map_err(|_| MultipartStorageError::KeyTooLong)?;
    let mut output = Vec::with_capacity(2 + 4 + key.len() + 4 + 8 + 8);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 1]);
    output.extend_from_slice(&key_length.to_le_bytes());
    output.extend_from_slice(key.as_bytes());
    output.extend_from_slice(&maximum_parts.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&maximum_part_bytes.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(&minimum_part_bytes.to_le_bytes());
    Ok(Bytes::from(output))
}

/// Encodes a part reference and the operation that uploaded it.
pub fn encode_multipart_part(
    part_number: u32,
    root: ObjectId,
    logical_bytes: u64,
    operation: IdempotencyKey,
    etag_value: &str,
) -> Bytes {
    let mut output = Vec::with_capacity(2 + 4 + root.encoded_len() + 8 + 16 + 32);
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 2]);
    output.extend_from_slice(&part_number.to_le_bytes());
    output.extend_from_slice(&root.to_bytes());
    output.extend_from_slice(&logical_bytes.to_le_bytes());
    output.extend_from_slice(&operation.into_bytes());
    let mut etag_digest = [0_u8; 32];
    let unquoted = etag_value.trim_matches('"');
    if hex::decode_to_slice(unquoted, &mut etag_digest).is_err() {
        etag_digest = *blake3::hash(etag_value.as_bytes()).as_bytes();
    }
    output.extend_from_slice(&etag_digest);
    Bytes::from(output)
}

/// Encodes the ordered set of parts selected for completion.
pub fn encode_multipart_completing(parts: &[(u32, String)]) -> MultipartStorageResult<Bytes> {
    let count = u32::try_from(parts.len()).map_err(|_| MultipartStorageError::TooManyParts)?;
    let mut output = Vec::with_capacity(6 + parts.len().saturating_mul(36));
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 3]);
    output.extend_from_slice(&count.to_le_bytes());
    for (number, etag_value) in parts {
        output.extend_from_slice(&number.to_le_bytes());
        let mut digest = [0_u8; 32];
        hex::decode_to_slice(etag_value.trim_matches('"'), &mut digest)
            .map_err(|_| MultipartStorageError::InvalidPartDigest)?;
        output.extend_from_slice(&digest);
    }
    Ok(Bytes::from(output))
}

/// Encodes the generation published by completion.
pub fn encode_multipart_completed(generation: crate::GenerationId) -> Bytes {
    let mut output = Vec::with_capacity(2 + generation.object_id().encoded_len());
    output.extend_from_slice(&[MULTIPART_RECORD_VERSION, 4]);
    output.extend_from_slice(&generation.to_bytes());
    Bytes::from(output)
}

/// Encodes an upload abort.
pub fn encode_multipart_aborted() -> Bytes {
    Bytes::from_static(&[MULTIPART_RECORD_VERSION, 5])
}

/// Replays every record up to the observed upload stream tail.
pub async fn read_multipart_snapshot<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    limits: FilesystemS3Limits,
) -> MultipartStorageResult<MultipartSnapshot> {
    let mut work = crate::WorkCounters {
        backend_read_operations: 1,
        ..crate::WorkCounters::default()
    };
    let tail = provider
        .tail(path.clone())
        .await
        .map_err(MultipartStorageError::Stream)?;
    if tail == 0 {
        return Err(MultipartStorageError::Malformed);
    }
    let mut replay: Option<MultipartReplay> = None;
    let mut from = 0_u64;
    while from < tail {
        let remaining = tail - from;
        let limit = u32::try_from(remaining.min(acyclic_stream::MAX_ITEMS as u64))
            .map_err(|_| MultipartStorageError::InconsistentStream)?;
        let mut page = provider
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: path.clone(),
                from,
                limit,
                at_least: Default::default(),
            })
            .await
            .map_err(MultipartStorageError::Stream)?;
        work.backend_read_operations = work
            .backend_read_operations
            .checked_add(1)
            .ok_or(MultipartStorageError::InconsistentStream)?;
        let mut received = 0_u64;
        while let Some(record) = page.next().await {
            let record = record.map_err(MultipartStorageError::Stream)?;
            if received >= u64::from(limit) || record.sequence != from + received {
                return Err(MultipartStorageError::InconsistentStream);
            }
            work.authority_records_read = work
                .authority_records_read
                .checked_add(1)
                .ok_or(MultipartStorageError::InconsistentStream)?;
            work.authority_bytes_read = work
                .authority_bytes_read
                .checked_add(record.value.len() as u64)
                .ok_or(MultipartStorageError::InconsistentStream)?;
            work.items_examined = work
                .items_examined
                .checked_add(1)
                .ok_or(MultipartStorageError::InconsistentStream)?;
            if let Some(replay) = &mut replay {
                replay.push(&record.value)?;
            } else {
                replay = Some(MultipartReplay::new(&record.value, tail, limits)?);
            }
            received += 1;
        }
        if received == 0 {
            return Err(MultipartStorageError::InconsistentStream);
        }
        from = from
            .checked_add(received)
            .ok_or(MultipartStorageError::InconsistentStream)?;
    }
    let mut snapshot = replay.ok_or(MultipartStorageError::NoSuchUpload)?.snapshot;
    snapshot.work = work;
    Ok(snapshot)
}

/// Persisted multipart refusal, projected only at the S3 service boundary.
#[derive(Debug, thiserror::Error)]
pub enum MultipartStorageError {
    /// Multipart record key is too long.
    #[error("multipart record key is too long")]
    KeyTooLong,
    /// Multipart record has too many parts.
    #[error("multipart record has too many parts")]
    TooManyParts,
    /// Multipart part digest is invalid.
    #[error("multipart part digest is invalid")]
    InvalidPartDigest,
    /// Multipart limits are invalid.
    #[error("multipart limits are invalid")]
    InvalidLimits,
    /// Multipart retention was cancelled.
    #[error("multipart retention was cancelled")]
    Cancelled,
    /// Multipart retention exceeded its budget.
    #[error("multipart retention exceeded its budget")]
    BudgetExceeded,
    /// Multipart upload does not exist.
    #[error("multipart upload does not exist")]
    NoSuchUpload,
    /// Multipart record is malformed.
    #[error("multipart record is malformed")]
    Malformed,
    /// Multipart record version number is unsupported.
    #[error("multipart record version {0} is unsupported")]
    UnsupportedVersion(u8),
    /// Multipart stream read failed.
    #[error("multipart stream read failed")]
    Stream(#[source] acyclic_stream::StreamError),
    /// Multipart stream append or commit failed.
    #[error("multipart stream write failed")]
    StreamWrite(#[source] acyclic_stream::StreamError),
    /// Multipart stream returned inconsistent records or accounting.
    #[error("multipart stream returned inconsistent records or accounting")]
    InconsistentStream,
}

/// Result of a multipart record or storage operation.
pub type MultipartStorageResult<T = ()> = Result<T, MultipartStorageError>;

/// Decodes and validates the leading `created` record of a multipart record stream, returning the upload's key and its negotiated part bounds.
pub fn decode_multipart_header(
    created: &acyclic_stream::OwnedBytes,
    limits: FilesystemS3Limits,
) -> MultipartStorageResult<(String, Option<u32>, Option<u64>, u64)> {
    let mut cursor = RecordCursor::new(created);
    cursor.header(1)?;
    let key_length = cursor.u32()?;
    if key_length == 0 {
        return Err(MultipartStorageError::Malformed);
    }
    let key = std::str::from_utf8(cursor.take(key_length as usize)?)
        .map_err(|_| MultipartStorageError::Malformed)?
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
        return Err(MultipartStorageError::Malformed);
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
    ) -> MultipartStorageResult<Self> {
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

    fn push(&mut self, record: &acyclic_stream::OwnedBytes) -> MultipartStorageResult<()> {
        let MultipartSnapshot {
            parts,
            operations,
            terminal,
            ..
        } = &mut self.snapshot;
        let mut cursor = RecordCursor::new(record);
        let tag = cursor.any_header()?;
        if terminal.is_some() && tag != 4 {
            return Err(MultipartStorageError::Malformed);
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
                    return Err(MultipartStorageError::Malformed);
                }
                let etag = format!("\"{}\"", hex::encode(etag_digest));
                if let Some((prior_number, prior_etag)) = operations.get(&operation)
                    && (*prior_number != part_number || prior_etag != &etag)
                {
                    return Err(MultipartStorageError::Malformed);
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
                    return Err(MultipartStorageError::Malformed);
                }
                if usize::try_from(count)
                    .ok()
                    .and_then(|count| count.checked_mul(36))
                    != Some(cursor.remaining_len())
                {
                    return Err(MultipartStorageError::Malformed);
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
                        .ok_or(MultipartStorageError::Malformed)?;
                cursor.finish()?;
                if !matches!(terminal, Some(MultipartTerminal::Completing(_))) {
                    return Err(MultipartStorageError::Malformed);
                }
                *terminal = Some(MultipartTerminal::Completed(generation));
            }
            5 => {
                cursor.finish()?;
                *terminal = Some(MultipartTerminal::Aborted);
            }
            _ => return Err(MultipartStorageError::Malformed),
        }
        Ok(())
    }
}

/// Replays supplied record bytes for codec checks.
#[cfg(any(test, feature = "test-support"))]
pub fn decode_multipart(
    records: Vec<acyclic_stream::OwnedBytes>,
    tail: u64,
    limits: FilesystemS3Limits,
) -> MultipartStorageResult<MultipartSnapshot> {
    let mut records = records.into_iter();
    let created = records.next().ok_or(MultipartStorageError::NoSuchUpload)?;
    let mut replay = MultipartReplay::new(&created, tail, limits)?;
    for record in records {
        replay.push(&record)?;
    }
    Ok(replay.snapshot)
}

/// Returns the decoded record tag and the byte count consumed by header validation.
#[cfg(feature = "test-support")]
fn inspect_multipart_header(bytes: &[u8]) -> (MultipartStorageResult<u8>, usize) {
    let mut cursor = RecordCursor::new(bytes);
    let tag = cursor.any_header();
    (tag, cursor.offset)
}

/// Cursor for the versioned multipart record format.
struct RecordCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> RecordCursor<'a> {
    /// Starts reading a record at its version byte.
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> MultipartStorageResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(MultipartStorageError::Malformed)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(MultipartStorageError::Malformed)?;
        self.offset = end;
        Ok(value)
    }

    fn object(&mut self) -> MultipartStorageResult<ObjectId> {
        let (object, remaining) = ObjectId::from_prefix(
            self.bytes
                .get(self.offset..)
                .ok_or(MultipartStorageError::Malformed)?,
        )
        .map_err(|_| MultipartStorageError::Malformed)?;
        self.offset = self.bytes.len() - remaining.len();
        Ok(object)
    }

    fn u8(&mut self) -> MultipartStorageResult<u8> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(MultipartStorageError::Malformed)
    }

    fn u32(&mut self) -> MultipartStorageResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| MultipartStorageError::Malformed)?,
        ))
    }

    fn u64(&mut self) -> MultipartStorageResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| MultipartStorageError::Malformed)?,
        ))
    }

    fn array32(&mut self) -> MultipartStorageResult<[u8; 32]> {
        self.take(32)?
            .try_into()
            .map_err(|_| MultipartStorageError::Malformed)
    }

    fn array16(&mut self) -> MultipartStorageResult<[u8; 16]> {
        self.take(16)?
            .try_into()
            .map_err(|_| MultipartStorageError::Malformed)
    }

    /// Validates the version and returns the record tag.
    fn any_header(&mut self) -> MultipartStorageResult<u8> {
        let version = self
            .bytes
            .get(self.offset)
            .copied()
            .ok_or(MultipartStorageError::UnsupportedVersion(0))?;
        if version != MULTIPART_RECORD_VERSION {
            return Err(MultipartStorageError::UnsupportedVersion(version));
        }
        self.u8()?;
        self.u8()
    }

    fn header(&mut self, tag: u8) -> MultipartStorageResult {
        if self.any_header()? != tag {
            return Err(MultipartStorageError::Malformed);
        }
        Ok(())
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn finish(&self) -> MultipartStorageResult {
        if self.remaining_len() != 0 {
            return Err(MultipartStorageError::Malformed);
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
                Err(MultipartStorageError::UnsupportedVersion(found))
                    if found == if length == 0 { 0 } else { input[0] }
            ));
            assert_eq!(cursor.offset, 0);
        } else if length == 1 {
            assert!(matches!(result, Err(MultipartStorageError::Malformed)));
            assert_eq!(cursor.offset, 1);
        } else {
            assert_eq!(result.unwrap(), input[1]);
            assert_eq!(cursor.offset, 2);
        }
    }
}
