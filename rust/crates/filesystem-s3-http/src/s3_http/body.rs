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

fn multipart_storage_error(error: MultipartStorageError) -> s3s::S3Error {
    match error {
        MultipartStorageError::KeyTooLong => s3s::s3_error!(InvalidArgument),
        MultipartStorageError::TooManyParts => s3s::s3_error!(InvalidRequest),
        MultipartStorageError::InvalidPartDigest => s3s::s3_error!(InvalidPart),
        MultipartStorageError::InvalidLimits => s3s::s3_error!(InvalidArgument),
        MultipartStorageError::Cancelled => s3s::s3_error!(RequestTimeout),
        MultipartStorageError::BudgetExceeded => s3s::s3_error!(SlowDown),
        MultipartStorageError::NoSuchUpload => s3s::s3_error!(NoSuchUpload),
        MultipartStorageError::Malformed => s3s::s3_error!(InvalidRequest),
        MultipartStorageError::UnsupportedVersion(_) => s3s::s3_error!(NotImplemented),
        MultipartStorageError::Stream(error) => stream_read_error(&error),
        MultipartStorageError::InconsistentStream => s3s::s3_error!(InternalError),
    }
}

fn stream_read_error(error: &acyclic_stream::StreamError) -> s3s::S3Error {
    match error.category() {
        acyclic_stream::StreamCategory::NotFound => s3s::s3_error!(NoSuchUpload),
        acyclic_stream::StreamCategory::AccessDenied => s3s::s3_error!(AccessDenied),
        acyclic_stream::StreamCategory::Unavailable => {
            s3s::s3_error!(ServiceUnavailable)
        }
        _ => s3s::s3_error!(InternalError),
    }
}

fn stream_write_error(error: &acyclic_stream::StreamError) -> s3s::S3Error {
    match error.category() {
        acyclic_stream::StreamCategory::IdempotencyMismatch => s3s::s3_error!(InvalidRequest),
        acyclic_stream::StreamCategory::Capacity
        | acyclic_stream::StreamCategory::LimitExceeded => {
            s3s::s3_error!(SlowDown)
        }
        _ => stream_read_error(error),
    }
}
