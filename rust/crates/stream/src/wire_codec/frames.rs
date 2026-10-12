use super::*;

/// Builds an uncompressed Read or Follow frame carrying consecutive records.
#[must_use]
pub fn read_response_wire(records: Vec<wire::Record>) -> wire::ReadResponse {
    use prost::Message;
    let data = wire::RecordBatch { records }.encode_to_vec();
    wire::ReadResponse {
        codec: wire::Codec::None as i32,
        decoded_length: data.len() as u64,
        data: data.into(),
        delivery_token: None,
    }
}

/// Decodes one Read or Follow frame to its encoded `RecordBatch`.
pub fn read_response_batch(value: wire::ReadResponse) -> Result<Bytes, StreamError> {
    if value
        .delivery_token
        .as_ref()
        .is_some_and(|token| token.len() != 32)
    {
        return Err(StreamError::Unavailable);
    }
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
        codec: wire::Codec::Zstd as i32,
        decoded_length: plain.decoded_length,
        data: ruzstd::encoding::compress_to_vec(
            &plain.data[..],
            ruzstd::encoding::CompressionLevel::Fastest,
        )
        .into(),
        delivery_token: None,
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod read_response_tests;
