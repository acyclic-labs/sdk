/// Validates and converts a canonical wire record without copying its payload.
pub fn record(value: wire::Record) -> Result<Record, StreamError> {
    let body = append_record(wire::AppendRecord {
        value: value.value,
        keys: value.keys,
        attributes: value.attributes,
    })?;
    Ok(Record {
        sequence: value.sequence,
        value: body.value,
        commit_id: commit_id(&value.commit_id)?,
        committed_at_micros: value.committed_at_micros,
        headers: body.headers,
    })
}

/// Converts a domain record into its canonical wire representation.
pub fn record_wire(value: Record) -> wire::Record {
    let (keys, attributes) = headers_wire(&value.headers);
    wire::Record {
        sequence: value.sequence,
        value: bytes::Bytes::from_owner(value.value),
        commit_id: bytes::Bytes::copy_from_slice(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros,
        keys,
        attributes,
    }
}

fn headers(
    keys: Vec<Bytes>,
    attributes: Vec<wire::Attribute>,
) -> Result<crate::RecordHeaders, StreamError> {
    if keys.len() > crate::MAX_RECORD_KEYS || attributes.len() > crate::MAX_RECORD_ATTRIBUTES {
        return Err(StreamError::LimitExceeded);
    }
    crate::RecordHeaders::new(
        keys.into_iter().map(Into::into).collect(),
        attributes
            .into_iter()
            .map(|attribute| crate::Attribute {
                name: attribute.name,
                value: attribute.value.into(),
            })
            .collect(),
    )
}

fn headers_wire(headers: &crate::RecordHeaders) -> (Vec<Bytes>, Vec<wire::Attribute>) {
    (
        headers
            .keys()
            .iter()
            .cloned()
            .map(Bytes::from_owner)
            .collect(),
        headers
            .attributes()
            .iter()
            .map(|attribute| wire::Attribute {
                name: attribute.name.clone(),
                value: Bytes::from_owner(attribute.value.clone()),
            })
            .collect(),
    )
}

/// Validates the value and indexed headers from every append or transaction path.
pub fn append_record(value: wire::AppendRecord) -> Result<crate::AppendRecord, StreamError> {
    let record = crate::AppendRecord {
        value: value.value.into(),
        headers: headers(value.keys, value.attributes)?,
    };
    if record.payload_bytes() > crate::MAX_RECORD_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(record)
}

/// Preserves indexed headers in every write transport and persisted envelope.
pub fn append_record_wire(value: crate::AppendRecord) -> wire::AppendRecord {
    let (keys, attributes) = headers_wire(&value.headers);
    wire::AppendRecord {
        value: Bytes::from_owner(value.value),
        keys,
        attributes,
    }
}

/// Builds an uncompressed Read or Follow frame carrying consecutive records.
#[must_use]
pub fn read_response_wire(records: Vec<wire::Record>) -> wire::ReadResponse {
    use prost::Message;
    let data = wire::RecordBatch { records }.encode_to_vec();
    wire::ReadResponse {
        delivery_token: Bytes::new(),
        codec: wire::Codec::None as i32,
        decoded_length: data.len() as u64,
        data: data.into(),
    }
}

/// Decodes one Read or Follow frame to its encoded `RecordBatch`.
pub fn read_response_batch(value: wire::ReadResponse) -> Result<Bytes, StreamError> {
    let length = usize::try_from(value.decoded_length)
        .ok()
        .filter(|length| *length <= crate::MAX_COMMAND_BYTES)
        .ok_or(StreamError::Unavailable)?;
    match wire::Codec::try_from(value.codec) {
        Ok(wire::Codec::None) if value.data.len() == length => Ok(value.data),
        Ok(wire::Codec::Zstd) => zstd(&value.data, length)
            .map(Bytes::from)
            .ok_or(StreamError::Unavailable),
        _ => Err(StreamError::Unavailable),
    }
}

/// Decodes one Read or Follow frame to its records, in frame order.
pub fn read_response_records(value: wire::ReadResponse) -> Result<Vec<wire::Record>, StreamError> {
    use prost::Message;
    wire::RecordBatch::decode(read_response_batch(value)?)
        .map(|batch| batch.records)
        .map_err(|_| StreamError::Unavailable)
}

/// Decompresses one or more complete Zstandard frames to exactly `length` bytes.
fn zstd(mut input: &[u8], length: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    if input.is_empty() {
        return None;
    }
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

#[cfg(test)]
pub(crate) fn zstd_read_response(records: Vec<wire::Record>) -> wire::ReadResponse {
    let plain = read_response_wire(records);
    wire::ReadResponse {
        delivery_token: Bytes::new(),
        codec: wire::Codec::Zstd as i32,
        decoded_length: plain.decoded_length,
        data: ruzstd::encoding::compress_to_vec(
            &plain.data[..],
            ruzstd::encoding::CompressionLevel::Fastest,
        )
        .into(),
    }
}
